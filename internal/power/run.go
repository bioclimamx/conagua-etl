package power

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
)

// Period is a normals window. Label formats it as the schema's
// monthly_normals.period and monthly_supplement.period CHECK accepts.
type Period struct{ StartYear, EndYear int }

// Label renders the window in the "1991-2020" form the schema's
// period CHECK constraints accept.
func (p Period) Label() string { return fmt.Sprintf("%d-%d", p.StartYear, p.EndYear) }

// TemporalMode picks the POWER endpoint and target table for a run.
// `monthly` (the default) hits the monthly endpoint and writes
// `monthly_supplement` keyed by (cell, period, month). `daily` hits
// the daily endpoint and writes `daily_supplement` keyed by
// (cell, date).
type TemporalMode string

// The two temporal modes, matching the power_runs.temporal_mode CHECK.
const (
	TemporalMonthly TemporalMode = "monthly"
	TemporalDaily   TemporalMode = "daily"
)

// Options configures one `power` invocation. Most fields apply to
// both temporal modes; the StartDate/EndDate pair is used only when
// Temporal == TemporalDaily, and Period only when
// Temporal == TemporalMonthly.
type Options struct {
	Client     *Client
	Endpoint   string
	Parameters []string // defaults to DefaultParameters() when nil/empty
	Community  string   // defaults to DefaultCommunity
	Temporal   TemporalMode

	// Monthly-mode span.
	Period Period

	// Daily-mode span. ISO "YYYY-MM-DD".
	StartDate string
	EndDate   string

	MaxCells  int // 0 = no cap; positive = process the first N cells (test/dev)
	ETLGitSHA string
}

// ProgressFunc is invoked, when non-nil, after each cell completes
// (success or failure). The CLI passes one to render append-only
// progress lines; tests pass nil.
type ProgressFunc func(ev ProgressEvent)

// ProgressEvent is one cell's outcome — enough for the CLI to
// render a line and update its EMA-style ETA.
type ProgressEvent struct {
	Index, Total int
	CellID       string
	StationCount int
	OK           bool
	Err          error
}

// RunManifest is the reproducibility-relevant subset of Options.
// Stored in power_runs and surfaced in any future published JSON.
//
// UnitConversions documents the per-variable unit mapping applied at
// the writer. It generalizes SolarConversion (kept as a back-compat
// column for reproducibility consumers) to every variable the run
// writes. Serialized to power_runs.unit_conversions as JSON.
//
// Temporal distinguishes monthly vs. daily runs. For monthly runs
// Period is the climatological window and StartDate/EndDate are
// empty. For daily runs StartDate/EndDate are the bounding ISO
// dates and Period.StartYear / .EndYear are populated with the
// year-component of those dates (so readers that only consult the
// year columns still see a sensible coarse span).
type RunManifest struct {
	Endpoint        string
	Parameters      []string
	Community       string
	Temporal        TemporalMode
	Period          Period
	StartDate       string
	EndDate         string
	GridResolution  string
	SolarConversion float64
	UnitConversions map[string]UnitConversion
}

// CellResult is one cell's outcome inside a Report.
type CellResult struct {
	Cell         Cell
	StationCount int
	OK           bool
	Err          string
	// RowsWritten is the count of supplement rows the cell produced.
	// Monthly cells write 12 rows on success (one per calendar month).
	// Daily cells write up to N rows where N is the number of days in
	// the requested window that POWER returned non-fill values for.
	RowsWritten int
}

// Report is the full record of a `power` invocation. It drives the
// CLI's stdout summary.
type Report struct {
	RunID           int64
	StartedAt       time.Time
	FinishedAt      time.Time
	Status          string // 'complete' | 'aborted'
	StationsCovered int
	UniqueCells     int
	CellsAttempted  int
	CellsSucceeded  int
	CellsFailed     int
	SupplementRows  int
	PerCell         []CellResult
	Manifest        RunManifest
}

// Run is the power orchestrator. Reads stations from the DB, snaps
// each to its POWER cell, fetches POWER time series per unique cell,
// writes the per-mode supplement table, and updates power_runs with
// the final counters.
//
// Two modes:
//
//   - TemporalMonthly (default): fetches monthly time series, rolls
//     up to climatological monthly means over [Period.StartYear,
//     Period.EndYear], and writes monthly_supplement keyed by
//     (cell, period, month).
//   - TemporalDaily: fetches daily time series over
//     [StartDate, EndDate], converts units, and writes
//     daily_supplement keyed by (cell, date) as-is (no rollup —
//     POWER's daily values are 24-h means; wind direction is the
//     daily vector mean upstream so no circular averaging here).
//
// Provenance is encoded by table: monthly_supplement and
// daily_supplement both hold POWER reanalysis keyed by cell.
// Stations resolve through station_power_cell at read time.
//
// Cell-level failures are recorded in the report but don't abort
// the run — POWER occasionally times out on individual points and
// we'd rather report partial success than lose hours of progress.
// Cancellation is a loop-level stop, checked between cells: the
// remaining cells are cancellation casualties, never failures, and
// the run row closes as 'aborted' with honest counters. Re-runs
// converge by UPSERT — resubmitting the same span is the whole
// resume story.
func Run(ctx context.Context, db *sql.DB, opts Options, progress ProgressFunc) (*Report, error) {
	opts = opts.withDefaults()

	report := &Report{
		StartedAt: time.Now().UTC(),
		Manifest: RunManifest{
			Endpoint:        opts.Endpoint,
			Parameters:      append([]string(nil), opts.Parameters...),
			Community:       opts.Community,
			Temporal:        opts.Temporal,
			Period:          opts.Period,
			StartDate:       opts.StartDate,
			EndDate:         opts.EndDate,
			GridResolution:  Resolution,
			SolarConversion: SolarMJpm2dToWm2,
			UnitConversions: conversionsFor(opts.Parameters),
		},
	}

	stations, err := loadStations(ctx, db)
	if err != nil {
		return report, fmt.Errorf("load stations: %w", err)
	}
	report.StationsCovered = len(stations)

	cellMap, perCell := buildCellMap(stations)
	report.UniqueCells = len(cellMap)

	runID, err := openRun(ctx, db, report.Manifest, opts.ETLGitSHA, report.StartedAt)
	if err != nil {
		return report, fmt.Errorf("open power_runs: %w", err)
	}
	report.RunID = runID

	if err := upsertCells(ctx, db, cellMap, stations); err != nil {
		report.FinishedAt = time.Now().UTC()
		report.Status = "aborted"
		_ = closeRun(db, runID, report.Status, report)
		return report, fmt.Errorf("upsert cells: %w", err)
	}

	cells := sortedCellIDs(cellMap)
	if opts.MaxCells > 0 && opts.MaxCells < len(cells) {
		cells = cells[:opts.MaxCells]
	}

	for i, cellID := range cells {
		// Cancellation stops the loop rather than failing every
		// remaining cell fast against a dead ctx — a cancellation
		// casualty is not a cell error. closeRun ignores the dead ctx,
		// so the run row records 'aborted' instead of stranding
		// 'running'.
		if ctx.Err() != nil {
			report.FinishedAt = time.Now().UTC()
			report.Status = "aborted"
			if cerr := closeRun(db, runID, report.Status, report); cerr != nil {
				return report, fmt.Errorf("close power_runs after cancellation: %w", cerr)
			}
			return report, fmt.Errorf("aborted after %d of %d cells: %w", report.CellsAttempted, len(cells), ctx.Err())
		}

		cell := cellMap[cellID]
		stationCount := len(perCell[cellID])
		report.CellsAttempted++

		var result CellResult
		switch opts.Temporal {
		case TemporalDaily:
			result = fetchAndWriteCellDaily(ctx, db, opts, cell, runID, stationCount)
		default:
			result = fetchAndWriteCell(ctx, db, opts, cell, runID, stationCount)
		}
		report.PerCell = append(report.PerCell, result)
		if result.OK {
			report.CellsSucceeded++
			report.SupplementRows += result.RowsWritten
		} else {
			report.CellsFailed++
		}
		if progress != nil {
			var pErr error
			if result.Err != "" {
				pErr = errors.New(result.Err)
			}
			progress(ProgressEvent{
				Index: i + 1, Total: len(cells),
				CellID:       cellID,
				StationCount: stationCount,
				OK:           result.OK, Err: pErr,
			})
		}
	}

	report.FinishedAt = time.Now().UTC()
	report.Status = "complete"
	if err := closeRun(db, runID, report.Status, report); err != nil {
		return report, fmt.Errorf("close power_runs: %w", err)
	}
	return report, nil
}

func (o Options) withDefaults() Options {
	if len(o.Parameters) == 0 {
		o.Parameters = DefaultParameters()
	}
	if o.Community == "" {
		o.Community = DefaultCommunity
	}
	if o.Temporal == "" {
		o.Temporal = TemporalMonthly
	}
	if o.Endpoint == "" {
		switch o.Temporal {
		case TemporalDaily:
			o.Endpoint = DefaultEndpointDaily
		default:
			o.Endpoint = DefaultEndpointMonthly
		}
	}
	return o
}

// stationRow is the minimal projection of stations needed to drive
// the cell pipeline. id is the surrogate; lat/lon are the CONAGUA
// coordinates we snap to a POWER centroid.
type stationRow struct {
	ID  int64
	Lat float64
	Lon float64
}

// loadStations selects the conventional-network stations with usable
// coordinates — the population the cell pipeline covers.
func loadStations(ctx context.Context, db *sql.DB) ([]stationRow, error) {
	rows, err := db.QueryContext(ctx, `
SELECT id, lat, lon
  FROM stations
 WHERE source = ?
   AND lat IS NOT NULL
   AND lon IS NOT NULL`, string(ingest.SourceConaguaConventional))
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck // read-side close; no recovery possible
	var out []stationRow
	for rows.Next() {
		var s stationRow
		if err := rows.Scan(&s.ID, &s.Lat, &s.Lon); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// buildCellMap snaps each station to a Cell and groups station IDs
// by their cell so the per-cell fetch knows how many stations a
// failed cell affects (surfaced in the run report).
func buildCellMap(stations []stationRow) (map[string]Cell, map[string][]int64) {
	cells := map[string]Cell{}
	per := map[string][]int64{}
	for _, s := range stations {
		c := CellFor(s.Lat, s.Lon)
		cells[c.ID] = c
		per[c.ID] = append(per[c.ID], s.ID)
	}
	return cells, per
}

func sortedCellIDs(m map[string]Cell) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// upsertCells writes nasa_power_grid_cells (one row per unique cell)
// and station_power_cell (one row per station, with distance to its
// centroid). Idempotent — re-running overwrites existing rows.
func upsertCells(ctx context.Context, db *sql.DB, cellMap map[string]Cell, stations []stationRow) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	cellStmt, err := tx.PrepareContext(ctx, `
INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution)
VALUES (?, ?, ?, ?)
ON CONFLICT(cell_id) DO UPDATE SET
    lat = excluded.lat,
    lon = excluded.lon,
    grid_resolution = excluded.grid_resolution`)
	if err != nil {
		return err
	}
	defer cellStmt.Close() //nolint:errcheck // teardown close; the tx outcome is already determined
	for _, c := range cellMap {
		if _, err := cellStmt.ExecContext(ctx, c.ID, c.Lat, c.Lon, c.Resolution); err != nil {
			return fmt.Errorf("upsert cell %s: %w", c.ID, err)
		}
	}

	spcStmt, err := tx.PrepareContext(ctx, `
INSERT INTO station_power_cell (station_id, cell_id, distance_km)
VALUES (?, ?, ?)
ON CONFLICT(station_id) DO UPDATE SET
    cell_id = excluded.cell_id,
    distance_km = excluded.distance_km`)
	if err != nil {
		return err
	}
	defer spcStmt.Close() //nolint:errcheck // teardown close; the tx outcome is already determined
	for _, s := range stations {
		c := CellFor(s.Lat, s.Lon)
		d := HaversineKm(s.Lat, s.Lon, c.Lat, c.Lon)
		if _, err := spcStmt.ExecContext(ctx, s.ID, c.ID, d); err != nil {
			return fmt.Errorf("upsert station_power_cell %d: %w", s.ID, err)
		}
	}
	return tx.Commit()
}

// fetchAndWriteCell runs the per-cell monthly pipeline: hit POWER,
// roll up, write monthly_supplement rows. Returns a CellResult
// describing the outcome; never returns an error so the caller's
// loop is simple.
func fetchAndWriteCell(ctx context.Context, db *sql.DB, opts Options, cell Cell, runID int64, stationCount int) CellResult {
	res := CellResult{Cell: cell, StationCount: stationCount}

	resp, err := opts.Client.FetchBatched(ctx, FetchRequest{
		Lat: cell.Lat, Lon: cell.Lon,
		Parameters: opts.Parameters,
		Community:  opts.Community,
		StartYear:  opts.Period.StartYear,
		EndYear:    opts.Period.EndYear,
	}, MaxParametersPerRequestMonthly)
	if err != nil {
		res.Err = err.Error()
		return res
	}
	rollup := RollUp(resp, opts.Period.StartYear, opts.Period.EndYear)
	written, err := writeSupplement(ctx, db, cell.ID, opts.Period.Label(), runID, rollup)
	if err != nil {
		res.Err = err.Error()
		return res
	}
	res.OK = true
	res.RowsWritten = written
	return res
}

// fetchAndWriteCellDaily runs the per-cell daily pipeline: hit POWER's
// daily endpoint, apply per-variable unit conversions, write
// daily_supplement rows. POWER's daily values are already 24-h means
// (including the vector mean for WD2M/WD10M), so no rollup math
// applies — values are stored as-returned modulo unit conversion.
// RowsWritten reflects the actual number of (cell, date) rows inserted.
func fetchAndWriteCellDaily(ctx context.Context, db *sql.DB, opts Options, cell Cell, runID int64, stationCount int) CellResult {
	res := CellResult{Cell: cell, StationCount: stationCount}

	resp, err := opts.Client.FetchBatched(ctx, FetchRequest{
		Lat: cell.Lat, Lon: cell.Lon,
		Parameters: opts.Parameters,
		Community:  opts.Community,
		StartDate:  opts.StartDate,
		EndDate:    opts.EndDate,
	}, MaxParametersPerRequestDaily)
	if err != nil {
		res.Err = err.Error()
		return res
	}
	series := ExtractDaily(resp, opts.StartDate, opts.EndDate)
	written, err := writeDailySupplement(ctx, db, cell.ID, runID, series)
	if err != nil {
		res.Err = err.Error()
		return res
	}
	res.OK = true
	res.RowsWritten = written
	return res
}

// supplementInsertSQL is built once at init from the Registry so the
// column list, placeholder count, and UPSERT clause stay in lockstep
// with the parameter set — adding a variable is one Registry row plus
// one DDL column. Column-order regressions in the prepared statement
// are caught by the round-trip tests.
var supplementInsertSQL = buildSupplementInsertSQL()

// dailySupplementInsertSQL mirrors supplementInsertSQL for the
// (cell_id, date) key of daily_supplement. Same column set as
// monthly_supplement (daily mirrors monthly column-for-column), only
// the key shape differs.
var dailySupplementInsertSQL = buildDailySupplementInsertSQL()

// registryInsertParts derives the value-column list, placeholder list,
// and UPSERT assignment list from the Registry — the shared building
// blocks of both supplement INSERT statements, so the two can never
// disagree on the column set or its order.
func registryInsertParts() (cols, placeholders, upserts []string) {
	cols = make([]string, 0, len(Registry))
	placeholders = make([]string, 0, len(Registry))
	upserts = make([]string, 0, len(Registry))
	for _, p := range Registry {
		cols = append(cols, p.Column)
		placeholders = append(placeholders, "?")
		upserts = append(upserts, fmt.Sprintf("%s = excluded.%s", p.Column, p.Column))
	}
	return cols, placeholders, upserts
}

func buildSupplementInsertSQL() string {
	cols, placeholders, upserts := registryInsertParts()
	return fmt.Sprintf(`
INSERT INTO monthly_supplement (
    cell_id, period, month,
    %s,
    power_run_id
) VALUES (?, ?, ?,
    %s,
    ?)
ON CONFLICT(cell_id, period, month) DO UPDATE SET
    %s,
    power_run_id = excluded.power_run_id`,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(upserts, ",\n    "))
}

func buildDailySupplementInsertSQL() string {
	cols, placeholders, upserts := registryInsertParts()
	return fmt.Sprintf(`
INSERT INTO daily_supplement (
    cell_id, date,
    %s,
    power_run_id
) VALUES (?, ?,
    %s,
    ?)
ON CONFLICT(cell_id, date) DO UPDATE SET
    %s,
    power_run_id = excluded.power_run_id`,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(upserts, ",\n    "))
}

// writeSupplement upserts the (cell, period, month) supplement rows.
// Per-variable unit conversions from the Registry are applied here so
// downstream consumers always see the stored unit. Rows where every
// value is NULL are skipped to keep the table sparse. Returns the
// number of rows the call upserted.
func writeSupplement(ctx context.Context, db *sql.DB, cellID, period string, runID int64, rollup map[string]map[int]MonthlyValue) (int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, supplementInsertSQL)
	if err != nil {
		return 0, err
	}
	defer stmt.Close() //nolint:errcheck // teardown close; the tx outcome is already determined

	values := make([]optFloat, len(Registry))
	args := make([]any, 0, 3+len(Registry)+1)
	rows := 0

	for m := 1; m <= 12; m++ {
		anyValid := false
		for i, p := range Registry {
			v := pick(rollup, p.Name, m)
			if v.Valid {
				if p.Factor != 1.0 {
					v.V = v.V * p.Factor
				}
				anyValid = true
			}
			values[i] = v
		}
		if !anyValid {
			continue
		}
		args = args[:0]
		args = append(args, cellID, period, m)
		for _, v := range values {
			args = append(args, v.OrNil())
		}
		args = append(args, runID)
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return rows, fmt.Errorf("insert supplement %s/%s/%d: %w", cellID, period, m, err)
		}
		rows++
	}
	if err := tx.Commit(); err != nil {
		return rows, err
	}
	return rows, nil
}

// writeDailySupplement upserts the (cell, date) supplement rows over
// the union of dates POWER returned for any parameter. Per-variable
// unit conversions from the Registry are applied here so downstream
// consumers always see the stored unit. Rows where every value is
// NULL are skipped to keep the table sparse — the per-cell
// transaction commits all rows in one round-trip.
func writeDailySupplement(ctx context.Context, db *sql.DB, cellID string, runID int64, series map[string]map[string]float64) (int, error) {
	// Collect the union of dates across all parameters and sort
	// ascending so inserts land in a predictable order.
	dateSet := map[string]struct{}{}
	for _, days := range series {
		for d := range days {
			dateSet[d] = struct{}{}
		}
	}
	dates := make([]string, 0, len(dateSet))
	for d := range dateSet {
		dates = append(dates, d)
	}
	sort.Strings(dates)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, dailySupplementInsertSQL)
	if err != nil {
		return 0, err
	}
	defer stmt.Close() //nolint:errcheck // teardown close; the tx outcome is already determined

	values := make([]optFloat, len(Registry))
	args := make([]any, 0, 2+len(Registry)+1)
	rows := 0

	for _, d := range dates {
		anyValid := false
		for i, p := range Registry {
			v := pickDaily(series, p.Name, d)
			if v.Valid {
				if p.Factor != 1.0 {
					v.V = v.V * p.Factor
				}
				anyValid = true
			}
			values[i] = v
		}
		if !anyValid {
			continue
		}
		args = args[:0]
		args = append(args, cellID, d)
		for _, v := range values {
			args = append(args, v.OrNil())
		}
		args = append(args, runID)
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return rows, fmt.Errorf("insert daily supplement %s/%s: %w", cellID, d, err)
		}
		rows++
	}
	if err := tx.Commit(); err != nil {
		return rows, err
	}
	return rows, nil
}

func pickDaily(series map[string]map[string]float64, param, date string) optFloat {
	if days, ok := series[param]; ok {
		if v, present := days[date]; present {
			return optFloat{V: v, Valid: true}
		}
	}
	return optFloat{}
}

// conversionsFor projects the global Conversions table down to the
// subset of parameters this run actually requested. Unknown parameters
// (e.g. an experimental variable a future caller passes via Options)
// get a placeholder entry tagged "unknown" so the manifest is still
// self-describing — readers spot the gap.
func conversionsFor(params []string) map[string]UnitConversion {
	out := make(map[string]UnitConversion, len(params))
	for _, p := range params {
		if c, ok := Conversions[p]; ok {
			out[p] = c
		} else {
			out[p] = UnitConversion{PowerUnit: "unknown", StoredUnit: "unknown", Factor: 1.0}
		}
	}
	return out
}

// optFloat is a (value, valid) pair the writer uses to keep the
// SQL INSERT call site readable. Equivalent to sql.NullFloat64 but
// produces a plain interface{} for the driver.
type optFloat struct {
	V     float64
	Valid bool
}

func pick(rollup map[string]map[int]MonthlyValue, param string, m int) optFloat {
	if monthMap, ok := rollup[param]; ok {
		if v, present := monthMap[m]; present {
			return optFloat{V: v.Mean, Valid: true}
		}
	}
	return optFloat{}
}

func (o optFloat) OrNil() any {
	if !o.Valid {
		return nil
	}
	return o.V
}

func openRun(ctx context.Context, db *sql.DB, m RunManifest, etlGitSHA string, startedAt time.Time) (int64, error) {
	conversionsJSON, err := json.Marshal(m.UnitConversions)
	if err != nil {
		return 0, fmt.Errorf("marshal unit_conversions: %w", err)
	}

	// Year columns are populated in both modes so a reader that only
	// consults them still sees a coarse span. In daily mode they're
	// the year component of the start/end dates; the authoritative
	// date span lives in period_*_date.
	startYear := m.Period.StartYear
	endYear := m.Period.EndYear
	var startDate, endDate any
	temporal := string(m.Temporal)
	if temporal == "" {
		temporal = string(TemporalMonthly)
	}
	if m.Temporal == TemporalDaily {
		sy, ey, err := yearsFromDates(m.StartDate, m.EndDate)
		if err != nil {
			return 0, fmt.Errorf("derive year columns from dates: %w", err)
		}
		startYear = sy
		endYear = ey
		startDate = m.StartDate
		endDate = m.EndDate
	}

	res, err := db.ExecContext(ctx, `
INSERT INTO power_runs (
    started_at, status,
    endpoint_url, parameters, community,
    period_start_year, period_end_year, grid_resolution, solar_conversion,
    unit_conversions, temporal_mode, period_start_date, period_end_date,
    etl_git_sha
) VALUES (?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		startedAt.UTC().Format(time.RFC3339),
		m.Endpoint,
		strings.Join(m.Parameters, ","),
		m.Community,
		startYear, endYear,
		m.GridResolution,
		m.SolarConversion,
		string(conversionsJSON),
		temporal,
		startDate,
		endDate,
		etlGitSHA)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// yearsFromDates extracts the year component from "YYYY-MM-DD" inputs
// so power_runs.period_start_year / period_end_year stay populated in
// daily mode. Validation is minimal — a length guard plus strconv.Atoi
// on the year prefix; a malformed date surfaces as an error rather
// than being repaired.
func yearsFromDates(start, end string) (int, int, error) {
	if len(start) < 4 || len(end) < 4 {
		return 0, 0, fmt.Errorf("date too short: %q %q", start, end)
	}
	sy, err := strconv.Atoi(start[:4])
	if err != nil {
		return 0, 0, fmt.Errorf("parse start year: %w", err)
	}
	ey, err := strconv.Atoi(end[:4])
	if err != nil {
		return 0, 0, fmt.Errorf("parse end year: %w", err)
	}
	return sy, ey, nil
}

// closeRun finalizes the power_runs row. It deliberately runs on
// context.Background(), never the run's ctx: the close must succeed
// even after cancellation, or an aborted run would strand a 'running'
// row forever.
func closeRun(db *sql.DB, runID int64, status string, r *Report) error {
	_, err := db.ExecContext(context.Background(), `
UPDATE power_runs SET
    finished_at = ?,
    status = ?,
    cells_attempted = ?, cells_succeeded = ?, cells_failed = ?,
    supplement_rows = ?
WHERE id = ?`,
		time.Now().UTC().Format(time.RFC3339),
		status,
		r.CellsAttempted, r.CellsSucceeded, r.CellsFailed,
		r.SupplementRows,
		runID)
	return err
}
