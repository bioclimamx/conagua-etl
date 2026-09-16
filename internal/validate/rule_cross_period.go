package validate

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
)

// crossPeriodMaxDeltaC is the max plausible difference between
// same-month normals across two reference periods. Climate change moves
// monthly means by tenths-to-low-units of °C across 30-year windows;
// jumps over this threshold are almost always a station identity or
// units issue — a station conflated with a neighbor, or one period's
// parser landing °F as °C.
const crossPeriodMaxDeltaC = 3.0 // °C, applies to tmax / tmin / tmean

// ruleCrossPeriod compares same-(station, month) normals across the
// four normals periods (1961-1990, 1971-2000, 1981-2010, 1991-2020).
// For every pair of periods where both have a value, it flags
// differences exceeding crossPeriodMaxDeltaC.
//
// Output granularity: one warn finding per (station, variable), ordered
// by (station, variable), listing the first topPerGroup offending months
// with the period pair and the delta. Scanned counts the (station,
// month, period-pair) rows of the self-join.
func ruleCrossPeriod(ctx context.Context, db *sql.DB) (RuleResult, error) {
	rows, err := db.QueryContext(ctx, `
SELECT a.station_id, a.month, a.period AS period_a, b.period AS period_b,
       a.tmax, b.tmax, a.tmin, b.tmin, a.tmean, b.tmean
  FROM monthly_normals a
  JOIN monthly_normals b
    ON a.station_id = b.station_id
   AND a.month = b.month
   AND a.period < b.period
 ORDER BY a.station_id, a.month, a.period, b.period`)
	if err != nil {
		return RuleResult{}, fmt.Errorf("scan monthly_normals: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	type sample struct {
		month   int
		periodA string
		periodB string
		va      float64
		vb      float64
		delta   float64
	}
	type bucketKey struct {
		sid int64
		v   string // "tmax" | "tmin" | "tmean"
	}
	bucket := map[bucketKey]*struct {
		count   int
		samples []sample
	}{}
	scanned := 0

	for rows.Next() {
		var sid int64
		var m int
		var periodA, periodB string
		var aTmax, bTmax, aTmin, bTmin, aTmean, bTmean sql.NullFloat64
		if err := rows.Scan(&sid, &m, &periodA, &periodB,
			&aTmax, &bTmax, &aTmin, &bTmin, &aTmean, &bTmean); err != nil {
			return RuleResult{}, fmt.Errorf("scan monthly_normals: %w", err)
		}
		scanned++

		check := func(v string, a, b sql.NullFloat64) {
			if !a.Valid || !b.Valid {
				return
			}
			delta := a.Float64 - b.Float64
			if math.Abs(delta) <= crossPeriodMaxDeltaC {
				return
			}
			k := bucketKey{sid: sid, v: v}
			bk, ok := bucket[k]
			if !ok {
				bk = &struct {
					count   int
					samples []sample
				}{}
				bucket[k] = bk
			}
			bk.count++
			if len(bk.samples) < topPerGroup {
				bk.samples = append(bk.samples, sample{
					month: m, periodA: periodA, periodB: periodB,
					va: a.Float64, vb: b.Float64, delta: delta,
				})
			}
		}
		check("tmax", aTmax, bTmax)
		check("tmin", aTmin, bTmin)
		check("tmean", aTmean, bTmean)
	}
	if err := rows.Err(); err != nil {
		return RuleResult{}, fmt.Errorf("scan monthly_normals: %w", err)
	}

	// Stable iteration order so the finding rows are reproducible across
	// runs.
	keys := make([]bucketKey, 0, len(bucket))
	for k := range bucket {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].sid != keys[j].sid {
			return keys[i].sid < keys[j].sid
		}
		return keys[i].v < keys[j].v
	})

	var fs []Finding
	for _, k := range keys {
		b := bucket[k]
		var topParts []string
		for _, s := range b.samples {
			topParts = append(topParts,
				fmt.Sprintf("m=%02d %s vs %s: %.1f vs %.1f (Δ%+.1f°C)",
					s.month, s.periodA, s.periodB, s.va, s.vb, s.delta))
		}
		sid := k.sid
		fs = append(fs, Finding{
			StationID: &sid,
			RuleID:    "cross-period",
			Severity:  SeverityWarn,
			Issue: fmt.Sprintf(
				"%s cross-period delta exceeds ±%.1f°C in %d (month, period-pair)(s); top: %s",
				k.v, crossPeriodMaxDeltaC, b.count, strings.Join(topParts, "; ")),
		})
	}
	return RuleResult{Findings: fs, Scanned: scanned}, nil
}
