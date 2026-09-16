package parity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// CompareDBs compares two ingest databases on the (source, external_id)
// natural key and returns the full per-table result. Surrogate ids are
// used only to address each side's own child tables and are never
// compared — two ingests of the same snapshot assign them
// independently.
//
// Memory stays bounded: stations, normals, extras, and warnings are
// small; daily_observations (~71M rows nationally) is walked one
// station at a time, each side's per-station slice merged in date
// order. Both handles should come from schema.OpenReadOnly — the
// comparator itself never writes, and the read-only opener makes that
// a hard guarantee for the base DB.
func CompareDBs(ctx context.Context, baseDB, newDB *sql.DB) (*DBComparison, error) {
	cmp := &DBComparison{
		Stations: TableComparison{Table: "stations"},
		Normals:  TableComparison{Table: "monthly_normals"},
		Extras:   TableComparison{Table: "monthly_normals_extras"},
		Daily:    TableComparison{Table: "daily_observations"},
		Warnings: TableComparison{Table: "parsing_warnings"},
	}

	baseStations, err := loadStationRows(ctx, baseDB)
	if err != nil {
		return nil, fmt.Errorf("base stations: %w", err)
	}
	newStations, err := loadStationRows(ctx, newDB)
	if err != nil {
		return nil, fmt.Errorf("new stations: %w", err)
	}
	common := compareStationUniverse(cmp, baseStations, newStations)

	for _, p := range common {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := compareStationChildren(ctx, cmp, baseDB, newDB, p); err != nil {
			return nil, fmt.Errorf("station %s/%s: %w", p.key.source, p.key.externalID, err)
		}
	}

	if err := compareWarnings(ctx, cmp, baseDB, newDB); err != nil {
		return nil, fmt.Errorf("parsing_warnings: %w", err)
	}

	if cmp.BaseRun, err = latestRun(ctx, baseDB); err != nil {
		return nil, fmt.Errorf("base ingest_runs: %w", err)
	}
	if cmp.NewRun, err = latestRun(ctx, newDB); err != nil {
		return nil, fmt.Errorf("new ingest_runs: %w", err)
	}
	return cmp, nil
}

// stationKey is the stations natural key.
type stationKey struct {
	source     string
	externalID string
}

// stationPair aligns one common station across the two databases: the
// shared natural key plus each side's own surrogate id.
type stationPair struct {
	key    stationKey
	baseID int64
	newID  int64
}

// stationRow carries one stations row: the side-local surrogate id
// plus every compared column.
type stationRow struct {
	id           int64
	name         sql.NullString
	state        sql.NullString
	municipality sql.NullString
	lat          sql.NullFloat64
	lon          sql.NullFloat64
	altitudeM    sql.NullFloat64
	status       sql.NullString
	firstYear    sql.NullInt64
	lastYear     sql.NullInt64
	wmo          [8]sql.NullFloat64
}

// wmoColumns names stationRow.wmo's slots, in the schema's column order.
var wmoColumns = [8]string{
	"wmo_completeness_bin_1961_1990",
	"wmo_completeness_bin_1971_2000",
	"wmo_completeness_bin_1981_2010",
	"wmo_completeness_bin_1991_2020",
	"wmo_completeness_cont_1961_1990",
	"wmo_completeness_cont_1971_2000",
	"wmo_completeness_cont_1981_2010",
	"wmo_completeness_cont_1991_2020",
}

func loadStationRows(ctx context.Context, db *sql.DB) (map[stationKey]stationRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, source, external_id, name, state, municipality,
		       lat, lon, altitude_m, status, first_year, last_year,
		       wmo_completeness_bin_1961_1990, wmo_completeness_bin_1971_2000,
		       wmo_completeness_bin_1981_2010, wmo_completeness_bin_1991_2020,
		       wmo_completeness_cont_1961_1990, wmo_completeness_cont_1971_2000,
		       wmo_completeness_cont_1981_2010, wmo_completeness_cont_1991_2020
		FROM stations`)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	stations := make(map[stationKey]stationRow)
	for rows.Next() {
		var k stationKey
		var r stationRow
		if err := rows.Scan(&r.id, &k.source, &k.externalID,
			&r.name, &r.state, &r.municipality,
			&r.lat, &r.lon, &r.altitudeM, &r.status, &r.firstYear, &r.lastYear,
			&r.wmo[0], &r.wmo[1], &r.wmo[2], &r.wmo[3],
			&r.wmo[4], &r.wmo[5], &r.wmo[6], &r.wmo[7]); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		stations[k] = r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return stations, nil
}

// compareStationUniverse set-compares the two station universes and
// column-diffs every common station. It returns the common stations in
// (source, external_id) order — the deterministic iteration order for
// the per-station child comparisons.
func compareStationUniverse(cmp *DBComparison, base, updated map[stationKey]stationRow) []stationPair {
	keys := slices.SortedFunc(maps.Keys(mergedKeySet(base, updated)), compareStationKeys)

	common := make([]stationPair, 0, min(len(base), len(updated)))
	for _, k := range keys {
		b, inBase := base[k]
		n, inNew := updated[k]
		switch {
		case !inNew:
			cmp.Stations.BaseOnly.Add(DBKey{Source: k.source, ExternalID: k.externalID})
		case !inBase:
			cmp.Stations.NewOnly.Add(DBKey{Source: k.source, ExternalID: k.externalID})
		default:
			cmp.Stations.RowsCompared++
			c := dbDiffCollector{key: DBKey{Source: k.source, ExternalID: k.externalID}}
			diffStationRow(&c, b, n)
			if len(c.diffs) == 0 {
				cmp.Stations.RowsIdentical++
			}
			for _, d := range c.diffs {
				cmp.Stations.ValueDiffs.Add(d)
			}
			common = append(common, stationPair{key: k, baseID: b.id, newID: n.id})
		}
	}
	return common
}

func mergedKeySet(a, b map[stationKey]stationRow) map[stationKey]struct{} {
	keys := make(map[stationKey]struct{}, len(a)+len(b))
	for k := range a {
		keys[k] = struct{}{}
	}
	for k := range b {
		keys[k] = struct{}{}
	}
	return keys
}

func compareStationKeys(a, b stationKey) int {
	if c := strings.Compare(a.source, b.source); c != 0 {
		return c
	}
	return strings.Compare(a.externalID, b.externalID)
}

// diffStationRow itemizes every compared stations column by its DB
// column name.
func diffStationRow(c *dbDiffCollector, base, updated stationRow) {
	c.nullStr("name", base.name, updated.name)
	c.nullStr("state", base.state, updated.state)
	c.nullStr("municipality", base.municipality, updated.municipality)
	c.nullFloat("lat", base.lat, updated.lat)
	c.nullFloat("lon", base.lon, updated.lon)
	c.nullFloat("altitude_m", base.altitudeM, updated.altitudeM)
	c.nullStr("status", base.status, updated.status)
	c.nullInt("first_year", base.firstYear, updated.firstYear)
	c.nullInt("last_year", base.lastYear, updated.lastYear)
	for i, col := range wmoColumns {
		c.nullFloat(col, base.wmo[i], updated.wmo[i])
	}
}

// compareStationChildren compares one common station's normals, extras,
// and daily rows, each side addressed through its own surrogate id.
func compareStationChildren(ctx context.Context, cmp *DBComparison, baseDB, newDB *sql.DB, p stationPair) error {
	baseNormals, err := loadNormalsRows(ctx, baseDB, p.baseID)
	if err != nil {
		return fmt.Errorf("base monthly_normals: %w", err)
	}
	newNormals, err := loadNormalsRows(ctx, newDB, p.newID)
	if err != nil {
		return fmt.Errorf("new monthly_normals: %w", err)
	}
	mergeRows(&cmp.Normals, p.key, baseNormals, newNormals, normalsRow.rowKey, diffNormalsDBRow)

	baseExtras, err := loadExtrasRows(ctx, baseDB, p.baseID)
	if err != nil {
		return fmt.Errorf("base monthly_normals_extras: %w", err)
	}
	newExtras, err := loadExtrasRows(ctx, newDB, p.newID)
	if err != nil {
		return fmt.Errorf("new monthly_normals_extras: %w", err)
	}
	mergeRows(&cmp.Extras, p.key, baseExtras, newExtras, extrasRow.rowKey, diffExtrasDBRow)

	baseDaily, err := loadDailyRows(ctx, baseDB, p.baseID)
	if err != nil {
		return fmt.Errorf("base daily_observations: %w", err)
	}
	newDaily, err := loadDailyRows(ctx, newDB, p.newID)
	if err != nil {
		return fmt.Errorf("new daily_observations: %w", err)
	}
	mergeRows(&cmp.Daily, p.key, baseDaily, newDaily, dailyDBRow.rowKey, diffDailyDBRow)
	return nil
}

// mergeRows walks two per-station row slices — each ordered ascending
// by rowKey, as their SELECTs guarantee — and classifies every row as
// base-only, new-only, or aligned (then column-diffed). Memory stays
// bounded at one station's rows per side.
func mergeRows[R any](tc *TableComparison, station stationKey, base, updated []R,
	rowKey func(R) string, diffRow func(c *dbDiffCollector, base, updated R)) {
	key := func(row string) DBKey {
		return DBKey{Source: station.source, ExternalID: station.externalID, Row: row}
	}
	i, j := 0, 0
	for i < len(base) || j < len(updated) {
		switch {
		case j == len(updated) || (i < len(base) && rowKey(base[i]) < rowKey(updated[j])):
			tc.BaseOnly.Add(key(rowKey(base[i])))
			i++
		case i == len(base) || rowKey(updated[j]) < rowKey(base[i]):
			tc.NewOnly.Add(key(rowKey(updated[j])))
			j++
		default:
			tc.RowsCompared++
			c := dbDiffCollector{key: key(rowKey(base[i]))}
			diffRow(&c, base[i], updated[j])
			if len(c.diffs) == 0 {
				tc.RowsIdentical++
			}
			for _, d := range c.diffs {
				tc.ValueDiffs.Add(d)
			}
			i++
			j++
		}
	}
}

// normalsRow carries one monthly_normals row's key and value columns.
type normalsRow struct {
	period string
	month  int

	tmax   sql.NullFloat64
	tmin   sql.NullFloat64
	tmean  sql.NullFloat64
	precip sql.NullFloat64
	evap   sql.NullFloat64
}

// rowKey renders "<period>/mMM"; the zero-padded month keeps string
// order equal to the SELECT's ORDER BY period, month.
func (r normalsRow) rowKey() string { return fmt.Sprintf("%s/m%02d", r.period, r.month) }

func loadNormalsRows(ctx context.Context, db *sql.DB, stationID int64) ([]normalsRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT period, month, tmax, tmin, tmean, precip, evap
		FROM monthly_normals WHERE station_id = ? ORDER BY period, month`, stationID)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []normalsRow
	for rows.Next() {
		var r normalsRow
		if err := rows.Scan(&r.period, &r.month, &r.tmax, &r.tmin, &r.tmean, &r.precip, &r.evap); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return out, nil
}

func diffNormalsDBRow(c *dbDiffCollector, base, updated normalsRow) {
	c.nullFloat("tmax", base.tmax, updated.tmax)
	c.nullFloat("tmin", base.tmin, updated.tmin)
	c.nullFloat("tmean", base.tmean, updated.tmean)
	c.nullFloat("precip", base.precip, updated.precip)
	c.nullFloat("evap", base.evap, updated.evap)
}

// extrasRow carries one monthly_normals_extras row's key and all 19
// value columns.
type extrasRow struct {
	period string
	month  int

	tmaxMonthlyExtreme       sql.NullFloat64
	tmaxMonthlyExtremeYear   sql.NullInt64
	tmaxDailyExtreme         sql.NullFloat64
	tmaxDailyExtremeDate     sql.NullString
	tminMonthlyExtreme       sql.NullFloat64
	tminMonthlyExtremeYear   sql.NullInt64
	tminDailyExtreme         sql.NullFloat64
	tminDailyExtremeDate     sql.NullString
	precipMonthlyExtreme     sql.NullFloat64
	precipMonthlyExtremeYear sql.NullInt64
	precipDailyExtreme       sql.NullFloat64
	precipDailyExtremeDate   sql.NullString
	tmaxYearsWithData        sql.NullInt64
	tminYearsWithData        sql.NullInt64
	tmeanYearsWithData       sql.NullInt64
	precipYearsWithData      sql.NullInt64
	evapYearsWithData        sql.NullInt64
	rainDays                 sql.NullFloat64
	rainDaysYearsWithData    sql.NullInt64
}

func (r extrasRow) rowKey() string { return fmt.Sprintf("%s/m%02d", r.period, r.month) }

func loadExtrasRows(ctx context.Context, db *sql.DB, stationID int64) ([]extrasRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT period, month,
		       tmax_monthly_extreme, tmax_monthly_extreme_year,
		       tmax_daily_extreme, tmax_daily_extreme_date,
		       tmin_monthly_extreme, tmin_monthly_extreme_year,
		       tmin_daily_extreme, tmin_daily_extreme_date,
		       precip_monthly_extreme, precip_monthly_extreme_year,
		       precip_daily_extreme, precip_daily_extreme_date,
		       tmax_years_with_data, tmin_years_with_data, tmean_years_with_data,
		       precip_years_with_data, evap_years_with_data,
		       rain_days, rain_days_years_with_data
		FROM monthly_normals_extras WHERE station_id = ? ORDER BY period, month`, stationID)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []extrasRow
	for rows.Next() {
		var r extrasRow
		if err := rows.Scan(&r.period, &r.month,
			&r.tmaxMonthlyExtreme, &r.tmaxMonthlyExtremeYear,
			&r.tmaxDailyExtreme, &r.tmaxDailyExtremeDate,
			&r.tminMonthlyExtreme, &r.tminMonthlyExtremeYear,
			&r.tminDailyExtreme, &r.tminDailyExtremeDate,
			&r.precipMonthlyExtreme, &r.precipMonthlyExtremeYear,
			&r.precipDailyExtreme, &r.precipDailyExtremeDate,
			&r.tmaxYearsWithData, &r.tminYearsWithData, &r.tmeanYearsWithData,
			&r.precipYearsWithData, &r.evapYearsWithData,
			&r.rainDays, &r.rainDaysYearsWithData); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return out, nil
}

func diffExtrasDBRow(c *dbDiffCollector, base, updated extrasRow) {
	c.nullFloat("tmax_monthly_extreme", base.tmaxMonthlyExtreme, updated.tmaxMonthlyExtreme)
	c.nullInt("tmax_monthly_extreme_year", base.tmaxMonthlyExtremeYear, updated.tmaxMonthlyExtremeYear)
	c.nullFloat("tmax_daily_extreme", base.tmaxDailyExtreme, updated.tmaxDailyExtreme)
	c.nullStr("tmax_daily_extreme_date", base.tmaxDailyExtremeDate, updated.tmaxDailyExtremeDate)
	c.nullFloat("tmin_monthly_extreme", base.tminMonthlyExtreme, updated.tminMonthlyExtreme)
	c.nullInt("tmin_monthly_extreme_year", base.tminMonthlyExtremeYear, updated.tminMonthlyExtremeYear)
	c.nullFloat("tmin_daily_extreme", base.tminDailyExtreme, updated.tminDailyExtreme)
	c.nullStr("tmin_daily_extreme_date", base.tminDailyExtremeDate, updated.tminDailyExtremeDate)
	c.nullFloat("precip_monthly_extreme", base.precipMonthlyExtreme, updated.precipMonthlyExtreme)
	c.nullInt("precip_monthly_extreme_year", base.precipMonthlyExtremeYear, updated.precipMonthlyExtremeYear)
	c.nullFloat("precip_daily_extreme", base.precipDailyExtreme, updated.precipDailyExtreme)
	c.nullStr("precip_daily_extreme_date", base.precipDailyExtremeDate, updated.precipDailyExtremeDate)
	c.nullInt("tmax_years_with_data", base.tmaxYearsWithData, updated.tmaxYearsWithData)
	c.nullInt("tmin_years_with_data", base.tminYearsWithData, updated.tminYearsWithData)
	c.nullInt("tmean_years_with_data", base.tmeanYearsWithData, updated.tmeanYearsWithData)
	c.nullInt("precip_years_with_data", base.precipYearsWithData, updated.precipYearsWithData)
	c.nullInt("evap_years_with_data", base.evapYearsWithData, updated.evapYearsWithData)
	c.nullFloat("rain_days", base.rainDays, updated.rainDays)
	c.nullInt("rain_days_years_with_data", base.rainDaysYearsWithData, updated.rainDaysYearsWithData)
}

// dailyDBRow carries one daily_observations row's key and value columns.
type dailyDBRow struct {
	date string

	tmax   sql.NullFloat64
	tmin   sql.NullFloat64
	precip sql.NullFloat64
	evap   sql.NullFloat64
}

// rowKey is the ISO date, whose string order equals date order.
func (r dailyDBRow) rowKey() string { return r.date }

func loadDailyRows(ctx context.Context, db *sql.DB, stationID int64) ([]dailyDBRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT date, tmax, tmin, precip, evap
		FROM daily_observations WHERE station_id = ? ORDER BY date`, stationID)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []dailyDBRow
	for rows.Next() {
		var r dailyDBRow
		if err := rows.Scan(&r.date, &r.tmax, &r.tmin, &r.precip, &r.evap); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return out, nil
}

func diffDailyDBRow(c *dbDiffCollector, base, updated dailyDBRow) {
	c.nullFloat("tmax", base.tmax, updated.tmax)
	c.nullFloat("tmin", base.tmin, updated.tmin)
	c.nullFloat("precip", base.precip, updated.precip)
	c.nullFloat("evap", base.evap, updated.evap)
}

// warningTuple is the parsing_warnings multiset key: the station
// resolved to its natural key — the surrogate station_id is never
// compared — plus the tuple columns. sql.Null* fields keep NULL-ness
// faithful inside the key itself; the fields are comparable, so the
// tuple works directly as a map key.
type warningTuple struct {
	source     string
	externalID string
	sourceFile sql.NullString
	line       sql.NullInt64
	severity   string
	issue      string
}

// render flattens the non-station tuple columns into the DBKey row
// discriminator.
func (t warningTuple) render() string {
	return fmt.Sprintf("%s:%s %s %q",
		renderNullStr(t.sourceFile), renderNullInt(t.line), t.severity, t.issue)
}

func (t warningTuple) key() DBKey {
	return DBKey{Source: t.source, ExternalID: t.externalID, Row: t.render()}
}

// compareWarnings compares the two parsing_warnings multisets: a tuple
// present on both sides with equal multiplicity is identical, unequal
// multiplicity is a "count" value diff, and a tuple on one side only is
// base-only / new-only.
func compareWarnings(ctx context.Context, cmp *DBComparison, baseDB, newDB *sql.DB) error {
	base, err := loadWarningCounts(ctx, baseDB)
	if err != nil {
		return fmt.Errorf("base: %w", err)
	}
	updated, err := loadWarningCounts(ctx, newDB)
	if err != nil {
		return fmt.Errorf("new: %w", err)
	}

	tuples := make(map[warningTuple]struct{}, len(base)+len(updated))
	for t := range base {
		tuples[t] = struct{}{}
	}
	for t := range updated {
		tuples[t] = struct{}{}
	}
	sorted := slices.SortedFunc(maps.Keys(tuples), func(a, b warningTuple) int {
		return strings.Compare(a.key().String(), b.key().String())
	})

	for _, t := range sorted {
		b, n := base[t], updated[t]
		switch {
		case n == 0:
			cmp.Warnings.BaseOnly.Add(t.key())
		case b == 0:
			cmp.Warnings.NewOnly.Add(t.key())
		default:
			cmp.Warnings.RowsCompared++
			if b == n {
				cmp.Warnings.RowsIdentical++
			} else {
				cmp.Warnings.ValueDiffs.Add(DBDiff{
					Key: t.key(), Column: "count",
					Base: strconv.Itoa(b), New: strconv.Itoa(n),
				})
			}
		}
	}
	return nil
}

func loadWarningCounts(ctx context.Context, db *sql.DB) (map[warningTuple]int, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT w.station_id, s.source, s.external_id,
		       w.source_file, w.line, w.severity, w.issue
		FROM parsing_warnings w LEFT JOIN stations s ON s.id = w.station_id`)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	counts := make(map[warningTuple]int)
	for rows.Next() {
		var stationID sql.NullInt64
		var source, externalID sql.NullString
		var t warningTuple
		if err := rows.Scan(&stationID, &source, &externalID,
			&t.sourceFile, &t.line, &t.severity, &t.issue); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		t.source = source.String
		t.externalID = externalID.String
		if stationID.Valid && !externalID.Valid {
			// A station_id with no stations row cannot resolve to a
			// natural key; surface the dangling surrogate rather than
			// conflating it with a NULL station.
			t.externalID = "unresolved-station-id:" + strconv.FormatInt(stationID.Int64, 10)
		}
		counts[t]++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return counts, nil
}

// latestRun fetches the highest-id ingest_runs row, or nil when the
// table is empty.
func latestRun(ctx context.Context, db *sql.DB) (*RunCounters, error) {
	var r RunCounters
	err := db.QueryRowContext(ctx, `
		SELECT id, started_at, finished_at, snapshot_date, sink_kind, etl_git_sha,
		       status, stations_attempted, stations_succeeded, stations_failed,
		       daily_rows, normals_rows, extras_rows, warnings_total
		FROM ingest_runs ORDER BY id DESC LIMIT 1`).Scan(
		&r.ID, &r.StartedAt, &r.FinishedAt, &r.SnapshotDate, &r.SinkKind, &r.ETLGitSHA,
		&r.Status, &r.StationsAttempted, &r.StationsSucceeded, &r.StationsFailed,
		&r.DailyRows, &r.NormalsRows, &r.ExtrasRows, &r.WarningsTotal)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	return &r, nil
}

// dbDiffCollector accumulates column-level diffs for one natural-key
// row. Floats compare exactly, never approximately: both sides come
// from the same parsers and the same float64 math, so bit-identical
// values are the expectation and any difference is a finding.
type dbDiffCollector struct {
	key   DBKey
	diffs []DBDiff
}

func (c *dbDiffCollector) record(column, baseVal, newVal string) {
	c.diffs = append(c.diffs, DBDiff{Key: c.key, Column: column, Base: baseVal, New: newVal})
}

func (c *dbDiffCollector) nullStr(column string, base, updated sql.NullString) {
	if base.Valid != updated.Valid || (base.Valid && base.String != updated.String) {
		c.record(column, renderNullStr(base), renderNullStr(updated))
	}
}

func (c *dbDiffCollector) nullFloat(column string, base, updated sql.NullFloat64) {
	if base.Valid != updated.Valid || (base.Valid && base.Float64 != updated.Float64) {
		c.record(column, renderNullFloat(base), renderNullFloat(updated))
	}
}

func (c *dbDiffCollector) nullInt(column string, base, updated sql.NullInt64) {
	if base.Valid != updated.Valid || (base.Valid && base.Int64 != updated.Int64) {
		c.record(column, renderNullInt(base), renderNullInt(updated))
	}
}

// renderedNull stands in for SQL NULL in rendered values, mirroring the
// snapshot comparator's convention.
const renderedNull = "NULL"

func renderNullStr(v sql.NullString) string {
	if !v.Valid {
		return renderedNull
	}
	return v.String
}

func renderNullFloat(v sql.NullFloat64) string {
	if !v.Valid {
		return renderedNull
	}
	return strconv.FormatFloat(v.Float64, 'g', -1, 64)
}

func renderNullInt(v sql.NullInt64) string {
	if !v.Valid {
		return renderedNull
	}
	return strconv.FormatInt(v.Int64, 10)
}
