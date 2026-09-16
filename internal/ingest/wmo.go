package ingest

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

// WMO data-completeness scoring (WMO-No. 1203 §4.4.2) uses the
// principal-parameter set from §4.2 Table 1. For CONAGUA's normals
// files the three variables we score are tmax, tmin and precip:
//
//   - tmean is derived from tmax/tmin and would double-count
//   - sunshine and pressure aren't carried in CONAGUA's normals
//   - relative humidity is not in the WMO principal set
//
// The 2017 edition of WMO-No. 1203 explicitly retired the earlier
// "no 3 consecutive missing years" criterion (§4.4.2), so we score
// purely on the 80%-of-years threshold.
const (
	wmoCoreVarsPerMonth = 3
	wmoMonthsPerPeriod  = 12
	wmoCellsPerPeriod   = wmoCoreVarsPerMonth * wmoMonthsPerPeriod // 36
	wmoThresholdYears   = 24                                       // 80% of 30
	wmoDenomYears       = 30
)

// CellCounts carries the per-month years_with_data counts for the three
// WMO principal variables. Nil pointers represent NULL in the source
// row (or an absent variable section in the CONAGUA file). Month is
// 1-indexed for readability but not used by ScoreWMO itself — it's
// kept so callers can debug which row carried which count.
type CellCounts struct {
	Month               int
	TmaxYearsWithData   *int
	TminYearsWithData   *int
	PrecipYearsWithData *int
}

// WMOScore carries both scoring systems that schema.sql wmo_completeness_*
// stores side by side:
//
//   - Binary: share of 36 core-variable-months with years_with_data ≥ 24
//     (the WMO 80% threshold). A coarse "is this normal trustworthy?"
//     indicator.
//   - Continuous: mean data density across the same 36 cells, with each
//     count clamped to 30. Smoother gradient for threshold-free use.
//
// Both are in [0, 1]. Present here as value types; the orchestrator's
// StoreWMOCompleteness writes NULL for periods absent from its map.
type WMOScore struct {
	Binary     float64
	Continuous float64
}

// ScoreWMO computes both scoring systems for one (station, period)
// given the per-month core-variable counts. The denominator is always
// wmoCellsPerPeriod (36): months or variables missing from cells
// contribute 0 to both systems — a missing data cell is, by
// definition, not complete.
//
// The second return value is true iff at least one cell had a
// non-NULL count. Callers should interpret hadAny=false as "period
// has no scorable data" and store NULL in the DB columns rather than
// a misleading 0.
func ScoreWMO(cells []CellCounts) (WMOScore, bool) {
	binPassing := 0
	contSum := 0.0
	hadAny := false

	score := func(p *int) {
		if p == nil {
			return
		}
		hadAny = true
		n := *p
		if n >= wmoThresholdYears {
			binPassing++
		}
		if n > wmoDenomYears {
			n = wmoDenomYears
		}
		contSum += float64(n) / float64(wmoDenomYears)
	}

	for _, c := range cells {
		score(c.TmaxYearsWithData)
		score(c.TminYearsWithData)
		score(c.PrecipYearsWithData)
	}

	if !hadAny {
		return WMOScore{}, false
	}
	return WMOScore{
		Binary:     float64(binPassing) / float64(wmoCellsPerPeriod),
		Continuous: contSum / float64(wmoCellsPerPeriod),
	}, true
}

// LoadExtrasForScoring fetches monthly_normals_extras years_with_data
// columns for one station and groups them by period. Only the three
// core variables are selected; ScoreWMO doesn't use the others.
//
// Periods with no extras rows at all are simply absent from the
// returned map, so the orchestrator can distinguish "period not
// present" from "period present, every cell NULL" by checking
// ScoreWMO's hadAny return.
func LoadExtrasForScoring(ctx context.Context, tx *sql.Tx, stationID int64) (map[string][]CellCounts, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT period, month,
       tmax_years_with_data, tmin_years_with_data, precip_years_with_data
  FROM monthly_normals_extras
 WHERE station_id = ?
 ORDER BY period, month;`, stationID)
	if err != nil {
		return nil, fmt.Errorf("load extras for scoring (station %d): %w", stationID, err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; no recovery possible

	out := map[string][]CellCounts{}
	for rows.Next() {
		var period string
		var cell CellCounts
		var tmax, tmin, precip sql.NullInt64
		if err := rows.Scan(&period, &cell.Month, &tmax, &tmin, &precip); err != nil {
			return nil, fmt.Errorf("scan extras row: %w", err)
		}
		cell.TmaxYearsWithData = nullInt64ToIntPtr(tmax)
		cell.TminYearsWithData = nullInt64ToIntPtr(tmin)
		cell.PrecipYearsWithData = nullInt64ToIntPtr(precip)
		out[period] = append(out[period], cell)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate extras: %w", err)
	}
	return out, nil
}

// StoreWMOCompleteness updates the eight wmo_completeness_* columns on
// stations for stationID in a single statement. Scores is keyed by
// period string ("1961-1990", "1971-2000", "1981-2010", "1991-2020");
// a period absent from the map writes NULL to both of its columns.
//
// This "absent → NULL" contract means a re-ingest whose snapshot lost
// a period's normals file correctly resets the column from a stale
// prior score back to NULL. Callers should only add entries where
// ScoreWMO returned hadAny=true.
//
// The bind arguments derive from conagua.NormalsKinds (oldest period
// first), whose order matches the fixed column list in the SQL text —
// the one place the period vocabulary is still spelled in SQL, covered
// by the schema/vocabulary lockstep specs.
func StoreWMOCompleteness(ctx context.Context, tx *sql.Tx, stationID int64, scores map[string]WMOScore) error {
	binArg := func(period string) any {
		if s, ok := scores[period]; ok {
			return s.Binary
		}
		return nil
	}
	contArg := func(period string) any {
		if s, ok := scores[period]; ok {
			return s.Continuous
		}
		return nil
	}

	args := make([]any, 0, 2*len(conagua.NormalsKinds)+1)
	for _, k := range conagua.NormalsKinds {
		period, _ := conagua.PeriodForKind(k)
		args = append(args, binArg(period))
	}
	for _, k := range conagua.NormalsKinds {
		period, _ := conagua.PeriodForKind(k)
		args = append(args, contArg(period))
	}
	args = append(args, stationID)

	_, err := tx.ExecContext(ctx, `
UPDATE stations SET
    wmo_completeness_bin_1961_1990  = ?,
    wmo_completeness_bin_1971_2000  = ?,
    wmo_completeness_bin_1981_2010  = ?,
    wmo_completeness_bin_1991_2020  = ?,
    wmo_completeness_cont_1961_1990 = ?,
    wmo_completeness_cont_1971_2000 = ?,
    wmo_completeness_cont_1981_2010 = ?,
    wmo_completeness_cont_1991_2020 = ?
 WHERE id = ?;`, args...)
	if err != nil {
		return fmt.Errorf("store wmo completeness (station %d): %w", stationID, err)
	}
	return nil
}

func nullInt64ToIntPtr(n sql.NullInt64) *int {
	if !n.Valid {
		return nil
	}
	v := int(n.Int64)
	return &v
}
