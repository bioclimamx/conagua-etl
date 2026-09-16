package validate_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

func f64(v float64) *float64 { return &v }

// openTempDB creates a fresh schema DB under the spec's temp dir through
// the one writer entry point, schedules its close, and returns the
// handle with its path.
func openTempDB() (*sql.DB, string) {
	GinkgoHelper()
	path := filepath.Join(GinkgoT().TempDir(), "validate.db")
	db, err := schema.Open(path)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = db.Close() })
	return db, path
}

func mustExec(db *sql.DB, query string, args ...any) {
	GinkgoHelper()
	_, err := db.ExecContext(context.Background(), query, args...)
	Expect(err).NotTo(HaveOccurred(), query)
}

// upsertStation seeds one stations row through the ingest writer (the
// only production path that creates stations) and returns its surrogate.
func upsertStation(db *sql.DB, s ingest.StationUpsert) int64 {
	GinkgoHelper()
	tx, err := db.BeginTx(context.Background(), nil)
	Expect(err).NotTo(HaveOccurred())
	id, err := ingest.UpsertStation(context.Background(), tx, s)
	Expect(err).NotTo(HaveOccurred())
	Expect(tx.Commit()).To(Succeed())
	return id
}

// station seeds a CONAGUA-conventional station by external id, name,
// and coordinates (nil = NULL).
func station(db *sql.DB, ext, name string, lat, lon *float64) int64 {
	GinkgoHelper()
	return upsertStation(db, ingest.StationUpsert{
		Source: ingest.SourceConaguaConventional, ExternalID: ext, Name: name, Lat: lat, Lon: lon,
	})
}

func insertIngestRun(db *sql.DB, status, startedAt string) int64 {
	GinkgoHelper()
	var id int64
	Expect(db.QueryRowContext(context.Background(), `
INSERT INTO ingest_runs (started_at, snapshot_date, sink_kind, status)
VALUES (?, '2026-06-08', 'local', ?) RETURNING id`, startedAt, status).Scan(&id)).To(Succeed())
	return id
}

func insertPowerRun(db *sql.DB, status, mode string, startYear, endYear int64) int64 {
	GinkgoHelper()
	var id int64
	Expect(db.QueryRowContext(context.Background(), `
INSERT INTO power_runs (started_at, status, endpoint_url, parameters, community,
    period_start_year, period_end_year, grid_resolution, solar_conversion, temporal_mode)
VALUES ('2026-06-09T10:00:00Z', ?, 'u', 'p', 'AG', ?, ?, '0.5x0.625', ?, ?) RETURNING id`,
		status, startYear, endYear, power.SolarMJpm2dToWm2, mode).Scan(&id)).To(Succeed())
	return id
}

func insertCell(db *sql.DB, cellID string) {
	GinkgoHelper()
	mustExec(db, `INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution)
	  VALUES (?, 21.5, -89.375, '0.5x0.625')`, cellID)
}

func insertStationCell(db *sql.DB, stationID int64, cellID string) {
	GinkgoHelper()
	mustExec(db, `INSERT INTO station_power_cell (station_id, cell_id, distance_km) VALUES (?, ?, 3.2)`,
		stationID, cellID)
}

// registryColumns is the 31 supplement value columns in registry order.
func registryColumns() []string {
	cols := make([]string, len(power.Registry))
	for i, p := range power.Registry {
		cols[i] = p.Column
	}
	return cols
}

// supplementValues renders the 31 value columns: fill for every column,
// overridden per column by values (nil = NULL).
func supplementValues(fill any, values map[string]any) []any {
	out := make([]any, 0, len(power.Registry))
	for _, p := range power.Registry {
		if v, ok := values[p.Column]; ok {
			out = append(out, v)
			continue
		}
		out = append(out, fill)
	}
	return out
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

// insertMonthly seeds one monthly_supplement row with every value
// column set to 1.5 bar the overrides in values.
func insertMonthly(db *sql.DB, cellID, period string, month int, runID any, values map[string]any) {
	GinkgoHelper()
	cols := registryColumns()
	mustExec(db, `INSERT INTO monthly_supplement (cell_id, period, month, `+strings.Join(cols, ", ")+
		`, power_run_id) VALUES (?, ?, ?, `+placeholders(len(cols))+`, ?)`,
		append(append([]any{cellID, period, month}, supplementValues(1.5, values)...), runID)...)
}

// insertDaily seeds one daily_supplement row with every value column set
// to 1.5 bar the overrides in values.
func insertDaily(db *sql.DB, cellID, date string, runID any, values map[string]any) {
	GinkgoHelper()
	cols := registryColumns()
	mustExec(db, `INSERT INTO daily_supplement (cell_id, date, `+strings.Join(cols, ", ")+
		`, power_run_id) VALUES (?, ?, `+placeholders(len(cols))+`, ?)`,
		append(append([]any{cellID, date}, supplementValues(1.5, values)...), runID)...)
}

func insertObservation(db *sql.DB, stationID int64, date string) {
	GinkgoHelper()
	mustExec(db, `INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
	  VALUES (?, ?, 30.5, 18.0, 0.0, 4.2)`, stationID, date)
}

func insertNormals(db *sql.DB, stationID int64, period string, month int) {
	GinkgoHelper()
	mustExec(db, `INSERT INTO monthly_normals (station_id, period, month, tmax, tmin, tmean, precip, evap)
	  VALUES (?, ?, ?, 33.0, 18.3, 25.8, 28.3, 150.25)`, stationID, period, month)
}

func insertExtras(db *sql.DB, stationID int64, period string, month int) {
	GinkgoHelper()
	mustExec(db, `INSERT INTO monthly_normals_extras (station_id, period, month, tmax_years_with_data)
	  VALUES (?, ?, ?, 28)`, stationID, period, month)
}

func insertWarning(db *sql.DB, stationID any, sourceFile string) {
	GinkgoHelper()
	mustExec(db, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
	  VALUES (?, ?, 3, 'warn', 'blank month')`, stationID, sourceFile)
}

// The clean fixture's keys.
const (
	cellA = "21.5N_89.3750W"
	cellB = "21.0N_89.6250W"
)

// cleanSeed is what seedClean created, so a spec can corrupt one thing.
type cleanSeed struct {
	merida, progreso int64
	ingestRun        int64
	monthlyRun       int64
	dailyRun         int64
}

// seedClean populates a fully consistent DB: a complete ingest run, a
// complete monthly and a complete daily power run, two in-bbox stations
// each tied to its own cell, supplement rows for both runs with every
// value column populated, observed rows in the three CONAGUA tables,
// and two parsing_warnings — one anchored to a station, one file-level
// with a NULL station_id. Every gate rule is silent on it.
func seedClean(db *sql.DB) cleanSeed {
	GinkgoHelper()
	var s cleanSeed
	s.ingestRun = insertIngestRun(db, "complete", "2026-06-08T10:00:00Z")
	s.monthlyRun = insertPowerRun(db, "complete", "monthly", 1991, 2020)
	s.dailyRun = insertPowerRun(db, "complete", "daily", 1981, 2025)

	s.merida = station(db, "31019", "Mérida", f64(20.98), f64(-89.65))
	s.progreso = station(db, "31023", "Progreso", f64(21.28), f64(-89.66))
	insertCell(db, cellA)
	insertCell(db, cellB)
	insertStationCell(db, s.merida, cellA)
	insertStationCell(db, s.progreso, cellB)

	insertMonthly(db, cellA, "1991-2020", 1, s.monthlyRun, map[string]any{"wd2m_deg": 360.0, "wd10m_deg": 0.0})
	insertMonthly(db, cellB, "1991-2020", 1, s.monthlyRun, nil)
	insertDaily(db, cellA, "2020-01-01", s.dailyRun, map[string]any{"wd2m_deg": 0.0, "wd10m_deg": 360.0})
	insertDaily(db, cellB, "2020-01-01", s.dailyRun, map[string]any{"t2m_c": nil})

	insertObservation(db, s.merida, "2020-01-01")
	insertObservation(db, s.merida, "2020-01-02")
	insertObservation(db, s.progreso, "2020-01-01")
	insertNormals(db, s.merida, "1991-2020", 1)
	insertExtras(db, s.merida, "1991-2020", 1)
	insertWarning(db, s.merida, "conagua-raw/2026-06-08/normals/31019.txt")
	insertWarning(db, nil, "conagua-raw/2026-06-08/catalog.html")
	return s
}

// ruleByID looks a gate rule up by id through the exported rule set.
func ruleByID(id string) validate.Rule {
	GinkgoHelper()
	for _, r := range validate.GateRules() {
		if r.ID == id {
			return r
		}
	}
	Fail(fmt.Sprintf("no gate rule %q", id))
	return validate.Rule{}
}

// runRule evaluates one gate rule over db.
func runRule(db *sql.DB, id string) validate.RuleResult {
	GinkgoHelper()
	res, err := ruleByID(id).Run(context.Background(), db)
	Expect(err).NotTo(HaveOccurred())
	return res
}

// issues extracts the Issue texts of findings, in order.
func issues(fs []validate.Finding) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Issue
	}
	return out
}

// severities extracts the Severity of findings, in order.
func severities(fs []validate.Finding) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Severity
	}
	return out
}

func itoa(n int64) string { return fmt.Sprint(n) }
