package publish

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// QAReport is QA-REPORT.md's data model: the lean report publish emits
// beside the archives, every number computed from the shipped DB by this
// package — never copied from a hand-written audit. The seven
// sections are typed fields so the same value renders as Markdown and
// marshals as JSON: nullable values are pointers (an explicit null,
// never a zero), lists are never nil. Nothing in it depends on the wall
// clock — the gate's timings are left behind at load (NewQAGate) — so
// two builds of the same DB produce the same report in either form.
type QAReport struct {
	SnapshotDate string      `json:"snapshot_date"`
	Runs         Runs        `json:"runs"`
	Counts       TableCounts `json:"counts"`
	Coverage     QACoverage  `json:"coverage"`
	// Nulls is the null / gap summary of the five value tables, in the
	// order qaNullTables lists them.
	Nulls []QANullTable `json:"nulls"`
	// Gate is the publish gate's result as the report carries it; nil when
	// the report was loaded without one.
	Gate     *QAGate    `json:"gate"`
	Counters QACounters `json:"counters"`
	Warnings QAWarnings `json:"parsing_warnings"`
}

// QACoverage is the coverage section: what the DB holds, by the axes a
// consumer filters on.
type QACoverage struct {
	StationsByStatus []QAStatusCount `json:"stations_by_status"`
	StationsByState  []QAStateCount  `json:"stations_by_state"`
	// Daily is the observed daily series, daily_observations, whole.
	Daily QASeriesExtent `json:"daily_observations"`
	// Normals is monthly_normals and monthly_normals_extras per
	// reference period, every period of the vocabulary listed (zeros for
	// one with no rows), oldest first.
	Normals []QAPeriodCoverage `json:"normals"`
	Power   QAPowerCoverage    `json:"power"`
}

// QAStatusCount is the number of stations rows with one status value;
// Status nil is the NULL status.
type QAStatusCount struct {
	Status   *string `json:"status"`
	Stations int64   `json:"stations"`
}

// QAStateCount is the number of stations rows with one state code, with
// the official state name (the code → name lookup). Code nil is the NULL state;
// Name nil is a code with no official name — neither reaches a publish
// run (LoadStates refuses both), and the report describes rather than
// refuses.
type QAStateCount struct {
	Code     *string `json:"code"`
	Name     *string `json:"name"`
	Stations int64   `json:"stations"`
}

// QASeriesExtent is the extent of a dated series: its row count, the
// number of distinct keys (stations, or cells) with at least one row,
// its first and last date — nil when the series is empty — and the
// first and last date of a row that carries at least one value.
//
// The two pairs differ because the source mirrors rows that carry no
// measurement at all: CONAGUA publishes a handful of all-NULL daily
// rows dated after the snapshot, so the row bounds reach into the
// future while no observation does. The value bounds are the dataset's
// advertised extent; the row bounds stay in the QA report, where the
// rows themselves are disclosed. Both value dates are nil when no row
// carries a value.
type QASeriesExtent struct {
	Rows           int64   `json:"rows"`
	Keys           int64   `json:"keys"`
	FirstDate      *string `json:"first_date"`
	LastDate       *string `json:"last_date"`
	ValueFirstDate *string `json:"value_first_date"`
	ValueLastDate  *string `json:"value_last_date"`
}

// QAPeriodCoverage is one reference period's normals footprint: the
// stations with at least one row, and the rows, in each of the two
// normals tables.
type QAPeriodCoverage struct {
	Period          string `json:"period"`
	NormalsStations int64  `json:"normals_stations"`
	NormalsRows     int64  `json:"normals_rows"`
	ExtrasStations  int64  `json:"extras_stations"`
	ExtrasRows      int64  `json:"extras_rows"`
}

// QAPowerCoverage is the POWER footprint: the registered cells, the
// stations linked to one, the daily series whole (keys are cells), the
// same series per value column, and the monthly climatology per
// reference period, every period listed.
type QAPowerCoverage struct {
	Cells          int64          `json:"cells"`
	LinkedStations int64          `json:"linked_stations"`
	Daily          QASeriesExtent `json:"daily_supplement"`
	// DailyColumnExtents is Daily broken out per value column, in the
	// POWER-31's registry order — every column listed, one with no rows
	// at all carrying nil dates. The whole-table extent is the rows',
	// not every column's: the POWER parameters have different upstream
	// start dates (the CERES-based streams begin years after the MERRA-2
	// ones), so a reader told only Daily.FirstDate would read a column's
	// honest absence as missing data. LoadQA always fills it; a
	// hand-built coverage may leave it nil.
	DailyColumnExtents []QAColumnExtent `json:"daily_supplement_columns"`
	Monthly            []QAPowerPeriod  `json:"monthly_supplement"`
}

// QAColumnExtent is one value column's own extent inside a dated
// series: the rows that carry a value for it, and the first and last
// date one of those rows falls on — both nil when the column holds no
// value anywhere, where the bounds are undefined rather than the
// table's.
type QAColumnExtent struct {
	Column    string  `json:"column"`
	NonNull   int64   `json:"non_null"`
	FirstDate *string `json:"first_date"`
	LastDate  *string `json:"last_date"`
}

// QAPowerPeriod is one reference period's monthly_supplement footprint:
// the cells with at least one row, and the rows.
type QAPowerPeriod struct {
	Period string `json:"period"`
	Cells  int64  `json:"cells"`
	Rows   int64  `json:"rows"`
}

// QANullTable is one value table's null / gap summary: its row count
// and, per value column in DDL order, the non-null count.
type QANullTable struct {
	Table   string         `json:"table"`
	Rows    int64          `json:"rows"`
	Columns []QANullColumn `json:"columns"`
}

// QANullColumn is one column's non-null count and its share of the
// table's rows — nil when the table is empty, where the share is
// undefined rather than zero.
type QANullColumn struct {
	Column  string   `json:"column"`
	NonNull int64    `json:"non_null"`
	Percent *float64 `json:"percent"`
}

// QACounters is the informational counters-vs-live section for the
// power runs — the ingest runs need no loading of their own: their
// stored counters are the Runs refs and their live counterparts are the
// table counts. Never a gate: a partial re-run
// legitimately breaks the equality.
type QACounters struct {
	// Power is one entry per provenance power run, in Runs.Power order.
	Power []QAPowerLive `json:"power"`
	// The supplement rows no run claims (power_run_id NULL) — a gate
	// error, listed here so the live totals reconcile.
	MonthlyRowsWithoutRun int64 `json:"monthly_supplement_rows_without_run"`
	DailyRowsWithoutRun   int64 `json:"daily_supplement_rows_without_run"`
}

// QAPowerLive is one power run's stored supplement_rows counter beside
// the live rows referencing the run in each supplement table.
type QAPowerLive struct {
	RunLabel       string `json:"run_label"`
	SupplementRows *int64 `json:"supplement_rows"`
	MonthlyRows    int64  `json:"monthly_supplement_rows"`
	DailyRows      int64  `json:"daily_supplement_rows"`
}

// QAWarnings is the existing parsing_warnings table by severity and by
// source, numbers only.
type QAWarnings struct {
	Total      int64             `json:"total"`
	BySeverity []QASeverityCount `json:"by_severity"`
	BySource   []QASourceCount   `json:"by_source"`
}

// QASeverityCount is the rows of one severity.
type QASeverityCount struct {
	Severity string `json:"severity"`
	Rows     int64  `json:"rows"`
}

// QASourceCount is the rows per severity of one warning source — the
// source_file's producer prefix (warningSource): "validate" for the
// validate verb's rows, the snapshot kind directory for ingest's. Source
// nil is a NULL source_file.
type QASourceCount struct {
	Source *string `json:"source"`
	Warn   int64   `json:"warn"`
	Error  int64   `json:"error"`
}

// qaWarnSampleCap bounds the warn-severity findings the gate section
// lists per rule; every error finding is listed.
const qaWarnSampleCap = 20

// toolCommand names the command a generated metadata artifact credits
// for its numbers: this repo's binary (cmd/conagua-etl). Stated once so
// the QA report and the data dictionary cannot come to credit two
// different tools — a deposit that contradicts itself about which
// program produced it is a provenance defect, not a typo.
const toolCommand = "conagua-etl publish"

// LoadQA loads the QA report over the DB: the coverage, null / gap,
// counters, and warnings sections from one aggregate pass per table,
// beside the inputs the run has already resolved — the snapshot, the
// provenance runs, the table counts, the gate result, and the states
// (the official names of the stations-by-state rows; LoadStates' full
// list, so the report describes the whole DB whatever --state
// selected). Read-only: every query is a SELECT.
func LoadQA(ctx context.Context, db *sql.DB, snapshotDate string, runs Runs, counts TableCounts,
	gate *validate.GateReport, states []State,
) (*QAReport, error) {
	q := &QAReport{SnapshotDate: snapshotDate, Runs: runs, Counts: counts, Gate: NewQAGate(gate)}
	var err error
	if q.Coverage.StationsByStatus, err = loadStationsByStatus(ctx, db); err != nil {
		return nil, err
	}
	if q.Coverage.StationsByState, err = loadStationsByState(ctx, db, states); err != nil {
		return nil, err
	}
	if q.Coverage.Normals, err = loadNormalsCoverage(ctx, db); err != nil {
		return nil, err
	}
	q.Coverage.Power.Cells = counts.NasaPowerGridCells
	q.Coverage.Power.LinkedStations = counts.StationPowerCell
	if q.Coverage.Power.Monthly, err = loadPowerMonthlyCoverage(ctx, db); err != nil {
		return nil, err
	}
	if err := loadNulls(ctx, db, q); err != nil {
		return nil, err
	}
	if err := loadPowerCounters(ctx, db, q); err != nil {
		return nil, err
	}
	if q.Warnings, err = loadWarnings(ctx, db); err != nil {
		return nil, err
	}
	return q, nil
}

func loadStationsByStatus(ctx context.Context, db *sql.DB) ([]QAStatusCount, error) {
	out := []QAStatusCount{}
	err := queryRows(ctx, db, "stations by status",
		`SELECT status, COUNT(*) FROM stations GROUP BY status ORDER BY status IS NULL, status`,
		func(rows *sql.Rows) error {
			var (
				status sql.NullString
				n      int64
			)
			if err := rows.Scan(&status, &n); err != nil {
				return err
			}
			out = append(out, QAStatusCount{Status: nullString(status), Stations: n})
			return nil
		})
	return out, err
}

// loadStationsByState counts stations per stored state code, naming
// each from states, or from the code table for a code the run did not
// resolve.
func loadStationsByState(ctx context.Context, db *sql.DB, states []State) ([]QAStateCount, error) {
	names := make(map[string]string, len(states))
	for _, st := range states {
		names[st.Code] = st.Name
	}
	out := []QAStateCount{}
	err := queryRows(ctx, db, "stations by state",
		`SELECT state, COUNT(*) FROM stations GROUP BY state ORDER BY state IS NULL, state`,
		func(rows *sql.Rows) error {
			var (
				code sql.NullString
				n    int64
			)
			if err := rows.Scan(&code, &n); err != nil {
				return err
			}
			c := QAStateCount{Code: nullString(code), Stations: n}
			if code.Valid {
				name, ok := names[code.String]
				if !ok {
					if st, err := stateFromCode(code.String); err == nil {
						name, ok = st.Name, true
					}
				}
				if ok {
					c.Name = &name
				}
			}
			out = append(out, c)
			return nil
		})
	return out, err
}

// periodFootprint is one period's (distinct keys, rows) pair from a
// GROUP BY period query.
type periodFootprint struct {
	keys, rows int64
}

// loadPeriodFootprints runs one GROUP BY period over table, counting
// the distinct key column and the rows.
func loadPeriodFootprints(ctx context.Context, db *sql.DB, table, key string) (map[string]periodFootprint, error) {
	out := map[string]periodFootprint{}
	// table and key are the package's own literals, never input.
	err := queryRows(ctx, db, table+" by period",
		"SELECT period, COUNT(DISTINCT "+key+"), COUNT(*) FROM "+table+" GROUP BY period",
		func(rows *sql.Rows) error {
			var (
				period string
				f      periodFootprint
			)
			if err := rows.Scan(&period, &f.keys, &f.rows); err != nil {
				return err
			}
			out[period] = f
			return nil
		})
	return out, err
}

func loadNormalsCoverage(ctx context.Context, db *sql.DB) ([]QAPeriodCoverage, error) {
	normals, err := loadPeriodFootprints(ctx, db, "monthly_normals", "station_id")
	if err != nil {
		return nil, err
	}
	extras, err := loadPeriodFootprints(ctx, db, "monthly_normals_extras", "station_id")
	if err != nil {
		return nil, err
	}
	out := make([]QAPeriodCoverage, 0, len(normalsPeriods))
	for _, p := range normalsPeriods {
		out = append(out, QAPeriodCoverage{
			Period:          p,
			NormalsStations: normals[p].keys, NormalsRows: normals[p].rows,
			ExtrasStations: extras[p].keys, ExtrasRows: extras[p].rows,
		})
	}
	return out, nil
}

func loadPowerMonthlyCoverage(ctx context.Context, db *sql.DB) ([]QAPowerPeriod, error) {
	monthly, err := loadPeriodFootprints(ctx, db, "monthly_supplement", "cell_id")
	if err != nil {
		return nil, err
	}
	out := make([]QAPowerPeriod, 0, len(normalsPeriods))
	for _, p := range normalsPeriods {
		out = append(out, QAPowerPeriod{Period: p, Cells: monthly[p].keys, Rows: monthly[p].rows})
	}
	return out, nil
}

// qaNullTables lists the value tables of the null / gap summary, in
// report order, each with its file spec — the spec's non-key columns
// are the table's value columns in DDL order, held to the embedded
// schema by the lockstep specs, so the summary covers every value
// column without a second transcription.
var qaNullTables = []FileSpec{DailyObservations, MonthlyNormals, MonthlyNormalsExtras, PowerMonthly, PowerDaily}

// valueDBColumns is the DDL names of a spec's value columns.
func valueDBColumns(spec FileSpec) []string {
	values := valueColumns(spec)
	cols := make([]string, len(values))
	for i, c := range values {
		cols[i] = c.DB
	}
	return cols
}

// loadNulls fills the null / gap summary and, from the same scans, the
// two daily extents and the supplement rows without a run: one
// aggregate pass per table computes COUNT(*), COUNT(col) for every value
// column, and the table's extra aggregates, so the largest tables are
// read exactly once here.
func loadNulls(ctx context.Context, db *sql.DB, q *QAReport) error {
	q.Nulls = make([]QANullTable, 0, len(qaNullTables))
	for _, spec := range qaNullTables {
		var (
			extras     []string
			dst        []any
			extent     *QASeriesExtent
			withoutRun *int64
			// columns is the per-column extent of a dated series, filled
			// by this same pass; nil for a table that reports none.
			columns []QAColumnExtent
		)
		values := valueDBColumns(spec)
		switch spec.Table {
		case "daily_observations":
			extent = &q.Coverage.Daily
			extras, dst = extentAggregates("station_id", values, extent)
		case "daily_supplement":
			extent = &q.Coverage.Power.Daily
			extras, dst = extentAggregates("cell_id", values, extent)
			withoutRun = &q.Counters.DailyRowsWithoutRun
			var (
				colExprs []string
				colDst   []any
			)
			columns, colExprs, colDst = columnExtentAggregates("date", values)
			extras, dst = append(extras, colExprs...), append(dst, colDst...)
		case "monthly_supplement":
			withoutRun = &q.Counters.MonthlyRowsWithoutRun
		}
		var withRun int64
		if withoutRun != nil {
			extras = append(extras, "COUNT(power_run_id)")
			dst = append(dst, &withRun)
		}
		t, err := scanNullTable(ctx, db, spec.Table, values, extras, dst)
		if err != nil {
			return err
		}
		if extent != nil {
			extent.Rows = t.Rows
		}
		if withoutRun != nil {
			*withoutRun = t.Rows - withRun
		}
		if columns != nil {
			// Both lists are values, in order, so a column's non-null count
			// is the null summary's COUNT(col) — read across rather than
			// aggregated a second time.
			for i := range columns {
				columns[i].NonNull = t.Columns[i].NonNull
			}
			q.Coverage.Power.DailyColumnExtents = columns
		}
		q.Nulls = append(q.Nulls, t)
	}
	return nil
}

// extentAggregates is the aggregate list of a dated series' extent —
// its distinct keys, its row date bounds, and the date bounds of the
// rows carrying a value in any of values — with the scan destinations
// that fill e, folded into the table's single null / gap pass. The date
// bounds scan through NULL-aware targets that e's pointers are set from
// after the scan, so an empty series leaves them nil.
func extentAggregates(key string, values []string, e *QASeriesExtent) (exprs []string, dst []any) {
	carried := make([]string, len(values))
	for i, v := range values {
		// values are the package's own column literals, never input.
		carried[i] = v + " IS NOT NULL"
	}
	when := "CASE WHEN " + strings.Join(carried, " OR ") + " THEN date END"
	exprs = []string{"COUNT(DISTINCT " + key + ")", "MIN(date)", "MAX(date)", "MIN(" + when + ")", "MAX(" + when + ")"}
	dst = []any{&e.Keys, &nullStringTarget{ptr: &e.FirstDate}, &nullStringTarget{ptr: &e.LastDate},
		&nullStringTarget{ptr: &e.ValueFirstDate}, &nullStringTarget{ptr: &e.ValueLastDate}}
	return exprs, dst
}

// columnExtentAggregates is the per-column extent of a dated series —
// for each of columns, the first and last dateCol a row carrying that
// column falls on — as two conditional aggregates per column, so the
// whole breakdown folds into the table's single null / gap pass rather
// than costing one scan per column (daily_supplement is the deposit's
// largest table by an order of magnitude). Returns the rows in columns'
// order with their dates left for the scan to fill through dst.
func columnExtentAggregates(dateCol string, columns []string) (out []QAColumnExtent, exprs []string, dst []any) {
	// Allocated once: dst holds pointers into out's backing array.
	out = make([]QAColumnExtent, len(columns))
	exprs = make([]string, 0, 2*len(columns))
	dst = make([]any, 0, 2*len(columns))
	for i, c := range columns {
		out[i].Column = c
		// columns and dateCol are the package's own literals, never input.
		when := "CASE WHEN " + c + " IS NOT NULL THEN " + dateCol + " END"
		exprs = append(exprs, "MIN("+when+")", "MAX("+when+")")
		dst = append(dst, &nullStringTarget{ptr: &out[i].FirstDate}, &nullStringTarget{ptr: &out[i].LastDate})
	}
	return out, exprs, dst
}

// nullStringTarget scans a nullable TEXT aggregate straight into a
// *string field: NULL leaves it nil.
type nullStringTarget struct {
	ptr **string
}

func (t *nullStringTarget) Scan(src any) error {
	var v sql.NullString
	if err := v.Scan(src); err != nil {
		return err
	}
	*t.ptr = nullString(v)
	return nil
}

// scanNullTable runs the one aggregate pass over table: COUNT(*), then
// the extra aggregates into dst, then COUNT(col) per value column.
func scanNullTable(ctx context.Context, db *sql.DB, table string, columns, extras []string, dst []any,
) (QANullTable, error) {
	t := QANullTable{Table: table, Columns: make([]QANullColumn, len(columns))}
	exprs := append([]string{"COUNT(*)"}, extras...)
	targets := append([]any{&t.Rows}, dst...)
	for i, c := range columns {
		exprs = append(exprs, "COUNT("+c+")")
		t.Columns[i].Column = c
		targets = append(targets, &t.Columns[i].NonNull)
	}
	// table and columns are the package's own literals, never input.
	if err := db.QueryRowContext(ctx, "SELECT "+strings.Join(exprs, ", ")+" FROM "+table).Scan(targets...); err != nil {
		return QANullTable{}, fmt.Errorf("scan %s nulls: %w", table, err)
	}
	if t.Rows > 0 {
		for i := range t.Columns {
			pct := float64(t.Columns[i].NonNull) * 100 / float64(t.Rows)
			t.Columns[i].Percent = &pct
		}
	}
	return t, nil
}

// loadPowerCounters fills the live rows per provenance power run: one
// GROUP BY power_run_id per supplement table (neither indexes the
// column, so each is one scan), joined to the runs by the natural run label
// through the power_runs id → label map. A label's live rows are the
// sum over the ids carrying it: LoadRuns has refused two referenced
// runs sharing a label, so at most one of them holds rows.
func loadPowerCounters(ctx context.Context, db *sql.DB, q *QAReport) error {
	labels := map[int64]string{}
	err := queryRows(ctx, db, "power run labels",
		`SELECT id, temporal_mode, period_start_year, period_end_year FROM power_runs`,
		func(rows *sql.Rows) error {
			var (
				id, startYear, endYear int64
				mode                   string
			)
			if err := rows.Scan(&id, &mode, &startYear, &endYear); err != nil {
				return err
			}
			labels[id] = RunLabel(mode, startYear, endYear)
			return nil
		})
	if err != nil {
		return err
	}
	monthly, err := loadRowsPerRun(ctx, db, "monthly_supplement", labels)
	if err != nil {
		return err
	}
	daily, err := loadRowsPerRun(ctx, db, "daily_supplement", labels)
	if err != nil {
		return err
	}
	q.Counters.Power = make([]QAPowerLive, 0, len(q.Runs.Power))
	for _, r := range q.Runs.Power {
		q.Counters.Power = append(q.Counters.Power, QAPowerLive{
			RunLabel: r.RunLabel, SupplementRows: r.SupplementRows,
			MonthlyRows: monthly[r.RunLabel], DailyRows: daily[r.RunLabel],
		})
	}
	return nil
}

// loadRowsPerRun counts table's rows per run label. A row whose
// power_run_id is absent from power_runs (dangling — a gate error) is
// counted under no label.
func loadRowsPerRun(ctx context.Context, db *sql.DB, table string, labels map[int64]string) (map[string]int64, error) {
	out := map[string]int64{}
	// table is the package's own literal, never input.
	err := queryRows(ctx, db, table+" rows per run",
		"SELECT power_run_id, COUNT(*) FROM "+table+" WHERE power_run_id IS NOT NULL GROUP BY power_run_id",
		func(rows *sql.Rows) error {
			var id, n int64
			if err := rows.Scan(&id, &n); err != nil {
				return err
			}
			if label, ok := labels[id]; ok {
				out[label] += n
			}
			return nil
		})
	return out, err
}

func loadWarnings(ctx context.Context, db *sql.DB) (QAWarnings, error) {
	w := QAWarnings{BySeverity: []QASeverityCount{}, BySource: []QASourceCount{}}
	err := queryRows(ctx, db, "warnings by severity",
		`SELECT severity, COUNT(*) FROM parsing_warnings GROUP BY severity ORDER BY severity`,
		func(rows *sql.Rows) error {
			var c QASeverityCount
			if err := rows.Scan(&c.Severity, &c.Rows); err != nil {
				return err
			}
			w.Total += c.Rows
			w.BySeverity = append(w.BySeverity, c)
			return nil
		})
	if err != nil {
		return QAWarnings{}, err
	}
	// Grouped per file in SQL, folded to the producer prefix here: the
	// fold needs the path split warningSource does, and the per-file set
	// is bounded by the files ingest read.
	bySource := map[string]*QASourceCount{}
	var nullSource *QASourceCount
	err = queryRows(ctx, db, "warnings by source",
		`SELECT source_file, severity, COUNT(*) FROM parsing_warnings GROUP BY source_file, severity`,
		func(rows *sql.Rows) error {
			var (
				file     sql.NullString
				severity string
				n        int64
			)
			if err := rows.Scan(&file, &severity, &n); err != nil {
				return err
			}
			var c *QASourceCount
			if !file.Valid {
				if nullSource == nil {
					nullSource = &QASourceCount{}
				}
				c = nullSource
			} else {
				source := warningSource(file.String)
				c = bySource[source]
				if c == nil {
					c = &QASourceCount{Source: &source}
					bySource[source] = c
				}
			}
			if severity == validate.SeverityError {
				c.Error += n
			} else {
				c.Warn += n
			}
			return nil
		})
	if err != nil {
		return QAWarnings{}, err
	}
	sources := make([]string, 0, len(bySource))
	for s := range bySource {
		sources = append(sources, s)
	}
	slices.Sort(sources)
	for _, s := range sources {
		w.BySource = append(w.BySource, *bySource[s])
	}
	if nullSource != nil {
		w.BySource = append(w.BySource, *nullSource)
	}
	return w, nil
}

// warningSource folds a parsing_warnings.source_file to its producer:
// the prefix before the first ':' for a namespaced value (the validate
// verb writes 'validate:<rule-id>'), else the directory of a path (an
// ingest row names the snapshot file it parsed,
// conagua-raw/<date>/<kind>/<station>.txt, so its source is the kind
// directory's path, conagua-raw/<date>/<kind> — a DB ingested from two
// snapshot dates reads as one row per date and kind), else the value
// itself.
func warningSource(sourceFile string) string {
	if i := strings.IndexByte(sourceFile, ':'); i >= 0 {
		return sourceFile[:i]
	}
	if i := strings.LastIndexByte(sourceFile, '/'); i > 0 {
		return sourceFile[:i]
	}
	return sourceFile
}

// queryRows runs query and hands every row to scan, wrapping any error
// with what.
func queryRows(ctx context.Context, db *sql.DB, what, query string, scan func(*sql.Rows) error) error {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("load %s: %w", what, err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported
	for rows.Next() {
		if err := scan(rows); err != nil {
			return fmt.Errorf("load %s: scan: %w", what, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("load %s: %w", what, err)
	}
	return nil
}

// QAGate is the gate section: the rules in execution order and the
// finding totals across them — the gate's result without its timings.
type QAGate struct {
	Rules    []QAGateRule `json:"rules"`
	Warnings int64        `json:"warnings"`
	Errors   int64        `json:"errors"`
}

// QAGateRule is one gate rule as the report lists it: the counts, every
// error finding's text, and the first qaWarnSampleCap warn findings.
type QAGateRule struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Scanned     int64    `json:"scanned"`
	Warnings    int64    `json:"warnings"`
	Errors      int64    `json:"errors"`
	ErrorIssues []string `json:"error_issues"`
	WarnSamples []string `json:"warn_samples"`
}

// NewQAGate reads a gate result into the report's gate section — the
// one place the report depends on the gate's shape. The gate's
// StartedAt, FinishedAt, and per-rule Elapsed are left behind, so the
// section is wall-clock free; nil in, nil out.
func NewQAGate(g *validate.GateReport) *QAGate {
	if g == nil {
		return nil
	}
	out := &QAGate{Rules: make([]QAGateRule, 0, len(g.Rules)), Warnings: int64(g.Warnings), Errors: int64(g.Errors)}
	for _, r := range g.Rules {
		row := QAGateRule{
			ID: r.ID, Name: r.Name, Scanned: int64(r.Scanned),
			Warnings: int64(r.Warnings), Errors: int64(r.Errors),
			ErrorIssues: []string{}, WarnSamples: []string{},
		}
		for _, f := range r.Findings {
			switch f.Severity {
			case validate.SeverityError:
				row.ErrorIssues = append(row.ErrorIssues, f.Issue)
			case validate.SeverityWarn:
				if len(row.WarnSamples) < qaWarnSampleCap {
					row.WarnSamples = append(row.WarnSamples, f.Issue)
				}
			}
		}
		out.Rules = append(out.Rules, row)
	}
	return out
}

// RenderQA writes q as QA-REPORT.md: deterministic Markdown — LF line
// ends, a trailing newline, fixed section order and headings, tables
// with pinned columns, integers as digits, percentages at one decimal
// through the fixed-decimal formatter, dates and labels verbatim, and no
// wall-clock anywhere. meta supplies the header's identity: the dataset,
// the schema version, the snapshot, and the build's git SHA.
func RenderQA(w io.Writer, q *QAReport, meta ProfileMeta) error {
	var b bytes.Buffer
	m := mdWriter{b: &b}
	m.header(meta)
	m.runsSection(q)
	m.countsSection(q)
	m.coverageSection(q)
	m.nullsSection(q)
	m.gateSection(q)
	m.countersSection(q)
	m.warningsSection(q)
	// Sections end in a blank line; the file ends in exactly one newline.
	out := append(bytes.TrimRight(b.Bytes(), "\n"), '\n')
	if _, err := w.Write(out); err != nil {
		return fmt.Errorf("write QA report: %w", err)
	}
	return nil
}

// mdWriter accumulates the report's Markdown.
type mdWriter struct {
	b *bytes.Buffer
}

func (m mdWriter) line(format string, args ...any) {
	fmt.Fprintf(m.b, format, args...)
	m.b.WriteByte('\n')
}

func (m mdWriter) blank() { m.b.WriteByte('\n') }

// table writes a Markdown table: the header, the separator, one row per
// entry. Cells are escaped so a '|' in a value cannot break the row.
func (m mdWriter) table(header []string, rows [][]string) {
	m.line("| %s |", strings.Join(escapeCells(header), " | "))
	m.line("|%s", strings.Repeat("---|", len(header)))
	for _, r := range rows {
		m.line("| %s |", strings.Join(escapeCells(r), " | "))
	}
	m.blank()
}

// bullet writes one list item, its text on a single line.
func (m mdWriter) bullet(text string) {
	m.line("- %s", strings.ReplaceAll(text, "\n", " "))
}

func escapeCells(cells []string) []string {
	out := make([]string, len(cells))
	for i, c := range cells {
		out[i] = strings.ReplaceAll(strings.ReplaceAll(c, "\n", " "), "|", `\|`)
	}
	return out
}

// The report's null renderings: a NULL value, and a share that is
// undefined over an empty table.
const (
	mdNull = "null"
	mdNA   = "n/a"
)

func mdString(s *string) string {
	if s == nil {
		return mdNull
	}
	return *s
}

func mdInt(n *int64) string {
	if n == nil {
		return mdNull
	}
	return formatInt(*n)
}

func mdPercent(p *float64) string {
	if p == nil {
		return mdNA
	}
	return formatReal(*p, 1)
}

func mdCode(s string) string {
	if s == "" {
		return "(none)"
	}
	return "`" + s + "`"
}

func (m mdWriter) header(meta ProfileMeta) {
	m.line("# QA report — %s", meta.Dataset.Title)
	m.blank()
	m.line("Version %s. Snapshot %s, schema version %d, ETL git SHA %s.",
		meta.Dataset.Version, mdCode(meta.SnapshotDate), meta.SchemaVersion, mdCode(meta.ETLGitSHA))
	m.blank()
	m.line("Every number in this report is computed by `%s` from the", toolCommand)
	m.line("shipped database at build time. The report carries no timestamp: two builds")
	m.line("of the same database and binary render the same text. Tables and columns are")
	m.line("named as in the database; the data dictionary maps them to the file names.")
	m.blank()
}

func (m mdWriter) runsSection(q *QAReport) {
	m.line("## 1. Snapshot and runs")
	m.blank()
	m.line("Snapshot date: %s.", mdCode(q.SnapshotDate))
	m.blank()
	m.line("### Ingest runs")
	m.blank()
	m.line("The latest complete ingest run per snapshot date. Run timestamps are")
	m.line("provenance, not QA, and ride in manifest.json and provenance/.")
	m.blank()
	rows := make([][]string, 0, len(q.Runs.Ingest))
	for _, r := range q.Runs.Ingest {
		rows = append(rows, []string{r.SnapshotDate, r.Status, r.SinkKind, mdString(r.ETLGitSHA)})
	}
	m.table([]string{"snapshot_date", "status", "sink_kind", "etl_git_sha"}, rows)
	m.line("### Power runs")
	m.blank()
	m.line("Every power run at least one supplement row references, by run label.")
	m.blank()
	rows = rows[:0]
	for _, r := range q.Runs.Power {
		rows = append(rows, []string{r.RunLabel, r.TemporalMode, powerSpan(r), r.Status, mdString(r.ETLGitSHA)})
	}
	m.table([]string{"run_label", "temporal_mode", "span", "status", "etl_git_sha"}, rows)
}

// powerSpan is a power run's requested span: the date bounds of a daily
// run when both are stored, else the year bounds.
func powerSpan(r PowerRunRef) string {
	if r.PeriodStartDate != nil && r.PeriodEndDate != nil {
		return *r.PeriodStartDate + " to " + *r.PeriodEndDate
	}
	return formatInt(r.PeriodStartYear) + " to " + formatInt(r.PeriodEndYear)
}

func (m mdWriter) countsSection(q *QAReport) {
	m.line("## 2. Per-table row counts")
	m.blank()
	c := q.Counts
	m.table([]string{"table", "rows"}, [][]string{
		{"stations", formatInt(c.Stations)},
		{"monthly_normals", formatInt(c.MonthlyNormals)},
		{"monthly_normals_extras", formatInt(c.MonthlyNormalsExtras)},
		{"daily_observations", formatInt(c.DailyObservations)},
		{"parsing_warnings", formatInt(c.ParsingWarnings)},
		{"ingest_runs", formatInt(c.IngestRuns)},
		{"power_runs", formatInt(c.PowerRuns)},
		{"nasa_power_grid_cells", formatInt(c.NasaPowerGridCells)},
		{"station_power_cell", formatInt(c.StationPowerCell)},
		{"monthly_supplement", formatInt(c.MonthlySupplement)},
		{"daily_supplement", formatInt(c.DailySupplement)},
	})
}

func (m mdWriter) coverageSection(q *QAReport) {
	c := q.Coverage
	m.line("## 3. Coverage")
	m.blank()
	m.line("### Stations by status")
	m.blank()
	rows := make([][]string, 0, len(c.StationsByStatus))
	for _, s := range c.StationsByStatus {
		rows = append(rows, []string{mdString(s.Status), formatInt(s.Stations)})
	}
	m.table([]string{"status", "stations"}, rows)
	m.line("### Stations by state")
	m.blank()
	rows = rows[:0]
	for _, s := range c.StationsByState {
		rows = append(rows, []string{mdString(s.Code), mdString(s.Name), formatInt(s.Stations)})
	}
	m.table([]string{"code", "state", "stations"}, rows)
	m.line("### Observed daily series (daily_observations)")
	m.blank()
	m.extent(c.Daily, "Stations with rows")
	m.line("### Normals by reference period")
	m.blank()
	rows = rows[:0]
	for _, p := range c.Normals {
		rows = append(rows, []string{p.Period, formatInt(p.NormalsStations), formatInt(p.NormalsRows),
			formatInt(p.ExtrasStations), formatInt(p.ExtrasRows)})
	}
	m.table([]string{"period", "monthly_normals stations", "monthly_normals rows",
		"monthly_normals_extras stations", "monthly_normals_extras rows"}, rows)
	m.line("### NASA POWER")
	m.blank()
	m.bullet("Grid cells registered: " + formatInt(c.Power.Cells))
	m.bullet("Stations linked to a cell: " + formatInt(c.Power.LinkedStations))
	m.blank()
	m.line("Daily reanalysis series (daily_supplement):")
	m.blank()
	m.extent(c.Power.Daily, "Cells with rows")
	m.line("Monthly climatology (monthly_supplement) by reference period:")
	m.blank()
	rows = rows[:0]
	for _, p := range c.Power.Monthly {
		rows = append(rows, []string{p.Period, formatInt(p.Cells), formatInt(p.Rows)})
	}
	m.table([]string{"period", "cells", "rows"}, rows)
}

// extent writes a dated series' extent as a bullet list; keysLabel
// names what its keys are.
func (m mdWriter) extent(e QASeriesExtent, keysLabel string) {
	m.bullet("Rows: " + formatInt(e.Rows))
	m.bullet(keysLabel + ": " + formatInt(e.Keys))
	m.bullet("First date: " + mdString(e.FirstDate))
	m.bullet("Last date: " + mdString(e.LastDate))
	m.bullet("First date with a value: " + mdString(e.ValueFirstDate))
	m.bullet("Last date with a value: " + mdString(e.ValueLastDate))
	m.blank()
}

func (m mdWriter) nullsSection(q *QAReport) {
	m.line("## 4. Null / gap summary")
	m.blank()
	m.line("Per value column: rows holding a value, and their share of the table's rows.")
	m.line("A NULL mirrors a gap in the source; nothing is imputed.")
	m.blank()
	for _, t := range q.Nulls {
		m.line("### %s (%s rows)", t.Table, formatInt(t.Rows))
		m.blank()
		rows := make([][]string, 0, len(t.Columns))
		for _, c := range t.Columns {
			rows = append(rows, []string{c.Column, formatInt(c.NonNull), mdPercent(c.Percent)})
		}
		m.table([]string{"column", "non-null rows", "%"}, rows)
	}
}

func (m mdWriter) gateSection(q *QAReport) {
	m.line("## 5. Gate results")
	m.blank()
	m.line("The publish gate, evaluated read-only before any archive is")
	m.line("written: an error-severity finding refuses the build, a warn-severity finding")
	m.line("is reported here and never blocks. Every error finding is listed; warn")
	m.line("findings are listed up to %d per rule.", qaWarnSampleCap)
	m.blank()
	if q.Gate == nil {
		m.line("The gate was not evaluated for this report.")
		m.blank()
		return
	}
	rules := q.Gate.Rules
	m.line("Rules: %s. Error findings: %s. Warn findings: %s.",
		formatInt(int64(len(rules))), formatInt(q.Gate.Errors), formatInt(q.Gate.Warnings))
	m.blank()
	// The gate's scope, stated where its totals are read: nine clean
	// structural rules are not a clean bill of climatological health,
	// and the reader is owed the name of what does assess that.
	m.line("These rules check referential and structural integrity and coordinate")
	m.line("plausibility; none of them assesses climatological plausibility. Within-month")
	m.line("completeness, daily-series sanity and cross-period consistency are the separate")
	m.line("`validate` verb's rules, evaluated there and not part of this gate.")
	m.blank()
	rows := make([][]string, 0, len(rules))
	for _, r := range rules {
		rows = append(rows, []string{r.ID, r.Name, formatInt(r.Scanned), formatInt(r.Warnings), formatInt(r.Errors)})
	}
	m.table([]string{"rule", "name", "scanned", "warn", "error"}, rows)
	for _, r := range rules {
		if r.Warnings == 0 && r.Errors == 0 {
			continue
		}
		m.line("### %s", r.ID)
		m.blank()
		if r.Errors > 0 {
			m.line("Errors (%s):", formatInt(r.Errors))
			m.blank()
			for _, issue := range r.ErrorIssues {
				m.bullet(issue)
			}
			m.blank()
		}
		if r.Warnings > 0 {
			m.line("Warnings (%s of %s):", formatInt(int64(len(r.WarnSamples))), formatInt(r.Warnings))
			m.blank()
			for _, issue := range r.WarnSamples {
				m.bullet(issue)
			}
			m.blank()
		}
	}
}

func (m mdWriter) countersSection(q *QAReport) {
	m.line("## 6. Run counters vs live counts")
	m.blank()
	m.line("Informational, never a gate: a run's counters record what that run did,")
	m.line("and a partial re-ingest or a re-run legitimately leaves them different from")
	m.line("the rows the tables hold now.")
	m.blank()
	m.line("### Ingest runs")
	m.blank()
	c := q.Counts
	for _, r := range q.Runs.Ingest {
		m.line("Run for snapshot %s:", mdCode(r.SnapshotDate))
		m.blank()
		m.table([]string{"counter", "stored", "live table", "live rows"}, [][]string{
			{"stations_attempted", mdInt(r.StationsAttempted), "stations", formatInt(c.Stations)},
			{"stations_succeeded", mdInt(r.StationsSucceeded), "stations", formatInt(c.Stations)},
			{"stations_failed", mdInt(r.StationsFailed), "", ""},
			{"daily_rows", mdInt(r.DailyRows), "daily_observations", formatInt(c.DailyObservations)},
			{"normals_rows", mdInt(r.NormalsRows), "monthly_normals", formatInt(c.MonthlyNormals)},
			{"extras_rows", mdInt(r.ExtrasRows), "monthly_normals_extras", formatInt(c.MonthlyNormalsExtras)},
			{"warnings_total", mdInt(r.WarningsTotal), "parsing_warnings", formatInt(c.ParsingWarnings)},
		})
	}
	m.line("### Power runs")
	m.blank()
	m.line("Stored supplement_rows beside the rows referencing the run in each table.")
	m.blank()
	rows := make([][]string, 0, len(q.Counters.Power))
	for _, p := range q.Counters.Power {
		rows = append(rows, []string{p.RunLabel, mdInt(p.SupplementRows), formatInt(p.MonthlyRows), formatInt(p.DailyRows)})
	}
	m.table([]string{"run_label", "supplement_rows (stored)", "monthly_supplement rows", "daily_supplement rows"}, rows)
	m.bullet("monthly_supplement rows with no run (power_run_id NULL): " + formatInt(q.Counters.MonthlyRowsWithoutRun))
	m.bullet("daily_supplement rows with no run (power_run_id NULL): " + formatInt(q.Counters.DailyRowsWithoutRun))
	m.blank()
}

func (m mdWriter) warningsSection(q *QAReport) {
	w := q.Warnings
	m.line("## 7. Parsing warnings")
	m.blank()
	// What the counts are of: rows earlier runs left in the table, not an
	// assessment made at build time — so an empty table is not a verdict.
	m.line("The parsing_warnings rows the database holds (%s): what ingest's parsers", formatInt(w.Total))
	m.line("wrote as they read the source files, and what the `validate` verb wrote where")
	m.line("it has been run against this database. Publishing writes none of its own, so")
	m.line("an empty table means no such row was written here, not that the data was")
	m.line("assessed at build time.")
	m.blank()
	rows := make([][]string, 0, len(w.BySeverity))
	for _, s := range w.BySeverity {
		rows = append(rows, []string{s.Severity, formatInt(s.Rows)})
	}
	m.table([]string{"severity", "rows"}, rows)
	m.line("By source — the producer prefix of source_file: `validate` for the validate")
	m.line("verb's rules, the snapshot kind directory's path (conagua-raw/<date>/<kind>)")
	m.line("for ingest's parsers, so two snapshot dates read as two rows:")
	m.blank()
	rows = rows[:0]
	for _, s := range w.BySource {
		rows = append(rows, []string{mdString(s.Source), formatInt(s.Warn), formatInt(s.Error)})
	}
	m.table([]string{"source", "warn", "error"}, rows)
}
