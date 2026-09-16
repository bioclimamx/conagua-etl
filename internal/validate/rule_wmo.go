package validate

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

// WMO §4.4.1 within-month invalidation rule:
//   - month is invalid if 11 or more day-readings are missing, OR
//   - 5 or more consecutive day-readings are missing.
//
// Checked against daily_observations rows where tmax (the principal
// variable of the WMO 80% rule ingest already scores per period) is
// non-null. tmin would produce essentially the same signal — daily
// files publish the temperature pair together — so the rule reports on
// tmax only and avoids duplicate warnings for the same missing-day
// pattern.
const (
	wmoMissingThreshold     = 11
	wmoConsecutiveThreshold = 5
)

// ruleWMOMonthCompleteness flags per (station, calendar month) when the
// within-month rule fails for at least one year of period. Ingest
// records the period-level WMO scores (stations.wmo_completeness_*);
// this rule is the finer-grain check — it tells which calendar months
// of which station are the pinch points so a downstream UI can mark
// them.
//
// Output granularity: one warn finding per (station, calendar month) in
// the period, ordered by (station, month), summarizing how many years
// failed and giving the first topPerGroup failing years. That is at
// most 12 findings per station — tens of thousands of rows nationally.
// Scanned counts the (station, year, month) groups examined.
func ruleWMOMonthCompleteness(period string) func(ctx context.Context, db *sql.DB) (RuleResult, error) {
	return func(ctx context.Context, db *sql.DB) (RuleResult, error) {
		startDate, endDate, err := periodBounds(period)
		if err != nil {
			return RuleResult{}, err
		}
		startYear, endYear, err := periodYearBounds(period)
		if err != nil {
			return RuleResult{}, err
		}

		// One row per (station, year, month) with the days-of-data count
		// and a concatenation of which day-of-month each observation
		// covers. The day list lets the gap be reconstructed without a
		// second pass.
		rows, err := db.QueryContext(ctx, `
SELECT station_id,
       CAST(strftime('%Y', date) AS INTEGER) AS y,
       CAST(strftime('%m', date) AS INTEGER) AS m,
       COUNT(*) AS days,
       GROUP_CONCAT(strftime('%d', date), ',') AS day_list
  FROM daily_observations
 WHERE date BETWEEN ? AND ?
   AND tmax IS NOT NULL
 GROUP BY station_id, y, m`, startDate, endDate)
		if err != nil {
			return RuleResult{}, fmt.Errorf("scan daily_observations: %w", err)
		}
		defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

		type yearKey struct {
			sid int64
			m   int
		}
		type yearFinding struct {
			year         int
			missingDays  int
			maxConsec    int
			daysExpected int
		}
		bucket := map[yearKey][]yearFinding{}

		scanned := 0
		for rows.Next() {
			var sid int64
			var y, m, days int
			var dayList string
			if err := rows.Scan(&sid, &y, &m, &days, &dayList); err != nil {
				return RuleResult{}, fmt.Errorf("scan daily_observations: %w", err)
			}
			scanned++

			if y < startYear || y > endYear {
				continue
			}
			expected := daysInMonth(y, m)
			if expected == 0 {
				continue
			}
			missing := expected - days
			gap := longestGap(dayList, expected)

			if missing >= wmoMissingThreshold || gap >= wmoConsecutiveThreshold {
				k := yearKey{sid: sid, m: m}
				bucket[k] = append(bucket[k], yearFinding{
					year:         y,
					missingDays:  missing,
					maxConsec:    gap,
					daysExpected: expected,
				})
			}
		}
		if err := rows.Err(); err != nil {
			return RuleResult{}, fmt.Errorf("scan daily_observations: %w", err)
		}

		// One finding per (station, calendar month) — the failing years
		// collected and summarized.
		var fs []Finding
		keys := make([]yearKey, 0, len(bucket))
		for k := range bucket {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].sid != keys[j].sid {
				return keys[i].sid < keys[j].sid
			}
			return keys[i].m < keys[j].m
		})

		for _, k := range keys {
			findings := bucket[k]
			sort.Slice(findings, func(i, j int) bool { return findings[i].year < findings[j].year })

			var topParts []string
			for _, f := range findings {
				if len(topParts) >= topPerGroup {
					break
				}
				topParts = append(topParts,
					fmt.Sprintf("%d (%d/%d missing, gap=%d)", f.year, f.missingDays, f.daysExpected, f.maxConsec))
			}
			sid := k.sid
			fs = append(fs, Finding{
				StationID: &sid,
				RuleID:    "wmo-month-completeness",
				Severity:  SeverityWarn,
				Issue: fmt.Sprintf(
					"WMO §4.4.1 fail in period %s, calendar month=%02d: %d year(s) violate (≥%d missing or ≥%d consecutive); top: %s",
					period, k.m, len(findings),
					wmoMissingThreshold, wmoConsecutiveThreshold,
					strings.Join(topParts, "; ")),
			})
		}
		return RuleResult{Findings: fs, Scanned: scanned}, nil
	}
}

// periodYearBounds turns "1991-2020" into (1991, 2020) integers.
// Distinct from periodBounds (which returns ISO date strings) so the
// rule can compare integer years from strftime.
func periodYearBounds(period string) (start, end int, err error) {
	parts := strings.Split(period, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("malformed period %q", period)
	}
	if _, err := fmt.Sscanf(parts[0]+"-"+parts[1], "%d-%d", &start, &end); err != nil {
		return 0, 0, fmt.Errorf("parse period years %q: %w", period, err)
	}
	return start, end, nil
}

// daysInMonth returns the number of calendar days in (year, month), 0
// for a month outside 1..12. Uses time.Date with day 0 of (month+1) —
// the canonical Go idiom for "last day of (year, month)".
func daysInMonth(year, month int) int {
	if month < 1 || month > 12 {
		return 0
	}
	return time.Date(year, time.Month(month+1), 0, 0, 0, 0, 0, time.UTC).Day()
}

// longestGap returns the length of the longest run of consecutive
// missing day-of-month values, given a comma-separated list of observed
// days (1..expected). The list is sorted before the scan because
// GROUP_CONCAT's ordering is not guaranteed.
func longestGap(dayList string, expected int) int {
	if dayList == "" || expected == 0 {
		return expected
	}
	parts := strings.Split(dayList, ",")
	days := make([]int, 0, len(parts))
	for _, p := range parts {
		var d int
		if _, err := fmt.Sscanf(p, "%d", &d); err != nil {
			continue
		}
		if d >= 1 && d <= expected {
			days = append(days, d)
		}
	}
	if len(days) == 0 {
		return expected
	}
	sort.Ints(days)

	maxGap := days[0] - 1 // gap before the first observed day
	for i := 1; i < len(days); i++ {
		if g := days[i] - days[i-1] - 1; g > maxGap {
			maxGap = g
		}
	}
	if g := expected - days[len(days)-1]; g > maxGap {
		maxGap = g
	}
	return maxGap
}
