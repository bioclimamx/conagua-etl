package validate

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
)

// diurnalRangeThreshold flags day-rows where tmax - tmin exceeds it as
// physically implausible. 25 °C catches a known Mérida case
// (tmin 2.0 °C with surrounding days at 22-24 °C — an implied 32+ °C
// diurnal swing) without false-positiving on legitimately continental
// high-altitude stations like Toluca.
const diurnalRangeThreshold = 25.0

// zSigma is the threshold for flagging a daily reading as an outlier
// against its (station, calendar month) distribution. 3σ keeps the
// false-positive rate around 0.3% for normally-distributed data, which
// over 71M daily rows still flags ~200k days — but aggregated per
// (station, month) the finding count is in the low thousands.
const zSigma = 3.0

// topPerGroup is how many extreme samples a summarized finding inlines
// in its issue text. Three is enough to spot the pattern without
// bloating the row.
const topPerGroup = 3

// ruleDailySanity flags suspicious daily observations. Two sub-checks
// over daily_observations rows in period:
//
//   - Diurnal range: tmax - tmin > diurnalRangeThreshold. Catches
//     missing-digit transcription errors (the Mérida 2 °C case above)
//     and sensor glitches where one of tmax/tmin slipped a decade.
//
//   - σ-z-score: per (station, calendar month), rows where tmax or tmin
//     sits beyond zSigma standard deviations from the monthly mean.
//
// Both emit aggregate warn findings — one per station for the diurnal
// check, one per (station, calendar month, variable) for the z-score
// check — each summarizing the count plus inline samples of the worst
// topPerGroup days, so parsing_warnings stays at summary granularity
// (millions of per-row warnings would bury the signal). Scanned counts
// the offending rows across both checks.
//
// The per-bucket findings are emitted by ranging over Go maps, so their
// order within the rule — and with it the row order under
// 'validate:daily-sanity' — is nondeterministic from run to run. The
// set is deterministic, so any comparison of two runs' output must be a
// multiset.
func ruleDailySanity(period string) func(ctx context.Context, db *sql.DB) (RuleResult, error) {
	return func(ctx context.Context, db *sql.DB) (RuleResult, error) {
		start, end, err := periodBounds(period)
		if err != nil {
			return RuleResult{}, err
		}

		var out RuleResult

		diurnal, scanned, err := scanDiurnal(ctx, db, start, end)
		if err != nil {
			return RuleResult{}, fmt.Errorf("diurnal scan: %w", err)
		}
		out.Scanned += scanned
		out.Findings = append(out.Findings, diurnal...)

		zscore, scanned, err := scanZScore(ctx, db, start, end)
		if err != nil {
			return RuleResult{}, fmt.Errorf("z-score scan: %w", err)
		}
		out.Scanned += scanned
		out.Findings = append(out.Findings, zscore...)

		return out, nil
	}
}

// periodBounds turns "1991-2020" into ("1991-01-01", "2020-12-31").
// Any other shape is an error so a typo does not silently widen the
// scope.
func periodBounds(period string) (start, end string, err error) {
	parts := strings.Split(period, "-")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("malformed period %q", period)
	}
	return parts[0] + "-01-01", parts[1] + "-12-31", nil
}

// scanDiurnal aggregates rows where tmax - tmin > threshold per
// station, surfacing one summarized finding per affected station with
// the count and the topPerGroup most extreme days inline.
func scanDiurnal(ctx context.Context, db *sql.DB, start, end string) ([]Finding, int, error) {
	rows, err := db.QueryContext(ctx, `
SELECT station_id, date, tmax, tmin
  FROM daily_observations
 WHERE date BETWEEN ? AND ?
   AND tmax IS NOT NULL AND tmin IS NOT NULL
   AND (tmax - tmin) > ?
 ORDER BY station_id, (tmax - tmin) DESC, date`,
		start, end, diurnalRangeThreshold)
	if err != nil {
		return nil, 0, fmt.Errorf("scan daily_observations: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	type sample struct {
		date string
		span float64
	}
	bucket := map[int64]*struct {
		count   int
		samples []sample
	}{}
	scanned := 0
	for rows.Next() {
		var sid int64
		var date string
		var tmax, tmin float64
		if err := rows.Scan(&sid, &date, &tmax, &tmin); err != nil {
			return nil, 0, fmt.Errorf("scan daily_observations: %w", err)
		}
		scanned++
		b, ok := bucket[sid]
		if !ok {
			b = &struct {
				count   int
				samples []sample
			}{}
			bucket[sid] = b
		}
		b.count++
		if len(b.samples) < topPerGroup {
			b.samples = append(b.samples, sample{date: date, span: tmax - tmin})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("scan daily_observations: %w", err)
	}

	var fs []Finding
	for sid, b := range bucket {
		stid := sid
		var topParts []string
		for _, s := range b.samples {
			topParts = append(topParts, fmt.Sprintf("%s (%.1f°C)", s.date, s.span))
		}
		fs = append(fs, Finding{
			StationID: &stid,
			RuleID:    "daily-sanity",
			Severity:  SeverityWarn,
			Issue: fmt.Sprintf(
				"diurnal range > %.0f°C on %d day(s); top: %s",
				diurnalRangeThreshold, b.count, strings.Join(topParts, "; ")),
		})
	}
	return fs, scanned, nil
}

// scanZScore is a two-step pass per variable: compute the per (station,
// calendar month) mean and variance, then emit the rows whose distance
// from that mean exceeds zSigma standard deviations. tmax and tmin are
// separate passes so the finding text can say which variable tripped.
func scanZScore(ctx context.Context, db *sql.DB, start, end string) ([]Finding, int, error) {
	var (
		findings []Finding
		scanned  int
	)

	for _, v := range []string{"tmax", "tmin"} {
		fs, n, err := scanZScoreVar(ctx, db, start, end, v)
		if err != nil {
			return nil, 0, fmt.Errorf("z-score %s: %w", v, err)
		}
		findings = append(findings, fs...)
		scanned += n
	}
	return findings, scanned, nil
}

// scanZScoreVar runs the z-score pass for one of tmax / tmin. SQLite has
// no native stddev; the variance is computed as E[x²] − E[x]² inside the
// CTE, over months with at least 30 readings and a positive variance.
func scanZScoreVar(ctx context.Context, db *sql.DB, start, end, variable string) ([]Finding, int, error) {
	// variable is one of the two column literals above, never input.
	q := fmt.Sprintf(`
WITH stats AS (
  SELECT station_id,
         CAST(strftime('%%m', date) AS INTEGER) AS m,
         AVG(%[1]s) AS mean,
         AVG(%[1]s * %[1]s) - AVG(%[1]s) * AVG(%[1]s) AS var
    FROM daily_observations
   WHERE date BETWEEN ? AND ?
     AND %[1]s IS NOT NULL
   GROUP BY station_id, m
   HAVING COUNT(*) >= 30 AND var > 0
)
SELECT d.station_id, d.date, d.%[1]s, s.mean, s.var
  FROM daily_observations d
  JOIN stats s
    ON s.station_id = d.station_id
   AND s.m = CAST(strftime('%%m', d.date) AS INTEGER)
 WHERE d.date BETWEEN ? AND ?
   AND d.%[1]s IS NOT NULL
   AND ABS(d.%[1]s - s.mean) > ? * SQRT(s.var)
 ORDER BY d.station_id, ABS(d.%[1]s - s.mean) / SQRT(s.var) DESC, d.date`, variable)

	rows, err := db.QueryContext(ctx, q, start, end, start, end, zSigma)
	if err != nil {
		return nil, 0, fmt.Errorf("scan daily_observations: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	type sample struct {
		date  string
		val   float64
		sigma float64
	}
	type bucketKey struct {
		sid int64
		m   int
	}
	bucket := map[bucketKey]*struct {
		mean    float64
		stddev  float64
		count   int
		samples []sample
	}{}
	scanned := 0

	for rows.Next() {
		var sid int64
		var date string
		var val, mean, varv float64
		if err := rows.Scan(&sid, &date, &val, &mean, &varv); err != nil {
			return nil, 0, fmt.Errorf("scan daily_observations: %w", err)
		}
		scanned++
		mInt := monthFromDate(date)
		k := bucketKey{sid: sid, m: mInt}
		b, ok := bucket[k]
		if !ok {
			b = &struct {
				mean    float64
				stddev  float64
				count   int
				samples []sample
			}{mean: mean, stddev: math.Sqrt(varv)}
			bucket[k] = b
		}
		b.count++
		if len(b.samples) < topPerGroup {
			sigma := (val - mean) / b.stddev
			b.samples = append(b.samples, sample{date: date, val: val, sigma: sigma})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("scan daily_observations: %w", err)
	}

	var fs []Finding
	for k, b := range bucket {
		sid := k.sid
		var topParts []string
		for _, s := range b.samples {
			topParts = append(topParts, fmt.Sprintf("%s %s=%.1f (%+.1fσ)", s.date, variable, s.val, s.sigma))
		}
		fs = append(fs, Finding{
			StationID: &sid,
			RuleID:    "daily-sanity",
			Severity:  SeverityWarn,
			Issue: fmt.Sprintf(
				"%s outliers > %.1fσ in calendar month=%02d: %d day(s) (mean=%.1f σ=%.1f); top: %s",
				variable, zSigma, k.m, b.count, b.mean, b.stddev, strings.Join(topParts, "; ")),
		})
	}
	return fs, scanned, nil
}

// monthFromDate parses the month component of an ISO date string
// without allocating a time.Time; 0 for a string too short to hold one.
// daily_observations.date is 'YYYY-MM-DD' by construction.
func monthFromDate(iso string) int {
	if len(iso) < 7 {
		return 0
	}
	return int(iso[5]-'0')*10 + int(iso[6]-'0')
}
