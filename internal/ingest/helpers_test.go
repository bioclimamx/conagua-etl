package ingest_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

func f64(v float64) *float64 { return &v }

// openDB opens (or creates) the SQLite file at path through the one
// writer entry point and schedules its close with the spec teardown.
func openDB(path string) *sql.DB {
	GinkgoHelper()
	db, err := schema.Open(path)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = db.Close() })
	return db
}

// beginTx opens a tx that is rolled back at spec teardown unless the
// spec committed it first (double-Rollback on a committed tx is a
// harmless ErrTxDone).
func beginTx(db *sql.DB) *sql.Tx {
	GinkgoHelper()
	tx, err := db.BeginTx(context.Background(), nil)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = tx.Rollback() })
	return tx
}

// writeIndexFile marshals a terminal-manifest Progress for the given
// stations to the canonical _index.json path under metaRoot.
func writeIndexFile(metaRoot, date string, stations []snapshot.StationProgress) {
	GinkgoHelper()
	p := snapshot.Progress{
		SnapshotDate:  date,
		SchemaVersion: snapshot.ProgressSchemaVersion,
		Stations:      stations,
	}
	data, err := json.Marshal(p)
	Expect(err).NotTo(HaveOccurred())
	dir := filepath.Join(metaRoot, "conagua-raw", date)
	Expect(os.MkdirAll(dir, 0o755)).To(Succeed())
	Expect(os.WriteFile(filepath.Join(dir, "_index.json"), data, 0o644)).To(Succeed())
}

// recordingSink wraps a Sink and counts Get calls, so specs can prove a
// code path never touched the sink (e.g. pull-errored files).
type recordingSink struct {
	inner    snapshot.Sink
	getCalls int
}

func (r *recordingSink) Exists(ctx context.Context, addr snapshot.Address) (bool, error) {
	return r.inner.Exists(ctx, addr)
}

func (r *recordingSink) Put(ctx context.Context, addr snapshot.Address, rd io.Reader) (snapshot.PutResult, error) {
	return r.inner.Put(ctx, addr, rd)
}

func (r *recordingSink) Get(ctx context.Context, addr snapshot.Address) (io.ReadCloser, error) {
	r.getCalls++
	return r.inner.Get(ctx, addr)
}

// erroringSink wraps a primary Sink and returns a fixed error from Get
// for one specific (kind, stationID) pair — the fault injector for the
// per-station rollback specs.
type erroringSink struct {
	inner          snapshot.Sink
	errForStation  string
	errForKind     conagua.Kind
	errToReturn    error
	callsIntercept int
}

func (e *erroringSink) Exists(ctx context.Context, addr snapshot.Address) (bool, error) {
	return e.inner.Exists(ctx, addr)
}

func (e *erroringSink) Put(ctx context.Context, addr snapshot.Address, r io.Reader) (snapshot.PutResult, error) {
	return e.inner.Put(ctx, addr, r)
}

func (e *erroringSink) Get(ctx context.Context, addr snapshot.Address) (io.ReadCloser, error) {
	if addr.StationID == e.errForStation && addr.Kind == e.errForKind {
		e.callsIntercept++
		return nil, e.errToReturn
	}
	return e.inner.Get(ctx, addr)
}

// indexFetcherSink wraps a Sink and adds the optional FetchIndex
// capability, so the R2 index-bootstrap path runs without a real R2.
type indexFetcherSink struct {
	snapshot.Sink
	indexBody []byte
	calls     int
}

func (s *indexFetcherSink) FetchIndex(context.Context, string) ([]byte, error) {
	s.calls++
	return s.indexBody, nil
}

// stationRow is the full read-back of one stations row (every column
// the CONAGUA ingest writes, key and values alike).
type stationRow struct {
	Source       string
	ExternalID   string
	Name         string
	State        sql.NullString
	Municipality sql.NullString
	Status       sql.NullString
	Lat          sql.NullFloat64
	Lon          sql.NullFloat64
	AltitudeM    sql.NullFloat64
	FirstYear    sql.NullInt64
	LastYear     sql.NullInt64
}

func selectStation(db *sql.DB, source, externalID string) stationRow {
	GinkgoHelper()
	var r stationRow
	err := db.QueryRow(`
SELECT source, external_id, name, state, municipality, status,
       lat, lon, altitude_m, first_year, last_year
  FROM stations WHERE source = ? AND external_id = ?`, source, externalID).
		Scan(&r.Source, &r.ExternalID, &r.Name, &r.State, &r.Municipality, &r.Status,
			&r.Lat, &r.Lon, &r.AltitudeM, &r.FirstYear, &r.LastYear)
	Expect(err).NotTo(HaveOccurred())
	return r
}

func stationID(db *sql.DB, source, externalID string) int64 {
	GinkgoHelper()
	var id int64
	err := db.QueryRow(`SELECT id FROM stations WHERE source = ? AND external_id = ?`,
		source, externalID).Scan(&id)
	Expect(err).NotTo(HaveOccurred())
	return id
}

// dailyRow is the value read-back of one daily_observations row.
type dailyRow struct {
	Tmax   sql.NullFloat64
	Tmin   sql.NullFloat64
	Precip sql.NullFloat64
	Evap   sql.NullFloat64
}

func selectDaily(db *sql.DB, stationID int64, date string) dailyRow {
	GinkgoHelper()
	var r dailyRow
	err := db.QueryRow(`
SELECT tmax, tmin, precip, evap FROM daily_observations
 WHERE station_id = ? AND date = ?`, stationID, date).
		Scan(&r.Tmax, &r.Tmin, &r.Precip, &r.Evap)
	Expect(err).NotTo(HaveOccurred())
	return r
}

// normalsRow is the value read-back of one monthly_normals row.
type normalsRow struct {
	Tmax   sql.NullFloat64
	Tmin   sql.NullFloat64
	Tmean  sql.NullFloat64
	Precip sql.NullFloat64
	Evap   sql.NullFloat64
}

func selectNormals(db *sql.DB, stationID int64, period string, month int) normalsRow {
	GinkgoHelper()
	var r normalsRow
	err := db.QueryRow(`
SELECT tmax, tmin, tmean, precip, evap FROM monthly_normals
 WHERE station_id = ? AND period = ? AND month = ?`, stationID, period, month).
		Scan(&r.Tmax, &r.Tmin, &r.Tmean, &r.Precip, &r.Evap)
	Expect(err).NotTo(HaveOccurred())
	return r
}

// extrasRow is the value read-back of one monthly_normals_extras row —
// all 19 value columns in schema order.
type extrasRow struct {
	TmaxMonthlyExtreme       sql.NullFloat64
	TmaxMonthlyExtremeYear   sql.NullInt64
	TmaxDailyExtreme         sql.NullFloat64
	TmaxDailyExtremeDate     sql.NullString
	TminMonthlyExtreme       sql.NullFloat64
	TminMonthlyExtremeYear   sql.NullInt64
	TminDailyExtreme         sql.NullFloat64
	TminDailyExtremeDate     sql.NullString
	PrecipMonthlyExtreme     sql.NullFloat64
	PrecipMonthlyExtremeYear sql.NullInt64
	PrecipDailyExtreme       sql.NullFloat64
	PrecipDailyExtremeDate   sql.NullString
	TmaxYearsWithData        sql.NullInt64
	TminYearsWithData        sql.NullInt64
	TmeanYearsWithData       sql.NullInt64
	PrecipYearsWithData      sql.NullInt64
	EvapYearsWithData        sql.NullInt64
	RainDays                 sql.NullFloat64
	RainDaysYearsWithData    sql.NullInt64
}

func selectExtras(db *sql.DB, stationID int64, period string, month int) extrasRow {
	GinkgoHelper()
	var r extrasRow
	err := db.QueryRow(`
SELECT tmax_monthly_extreme, tmax_monthly_extreme_year, tmax_daily_extreme, tmax_daily_extreme_date,
       tmin_monthly_extreme, tmin_monthly_extreme_year, tmin_daily_extreme, tmin_daily_extreme_date,
       precip_monthly_extreme, precip_monthly_extreme_year, precip_daily_extreme, precip_daily_extreme_date,
       tmax_years_with_data, tmin_years_with_data, tmean_years_with_data,
       precip_years_with_data, evap_years_with_data,
       rain_days, rain_days_years_with_data
  FROM monthly_normals_extras
 WHERE station_id = ? AND period = ? AND month = ?`, stationID, period, month).
		Scan(&r.TmaxMonthlyExtreme, &r.TmaxMonthlyExtremeYear, &r.TmaxDailyExtreme, &r.TmaxDailyExtremeDate,
			&r.TminMonthlyExtreme, &r.TminMonthlyExtremeYear, &r.TminDailyExtreme, &r.TminDailyExtremeDate,
			&r.PrecipMonthlyExtreme, &r.PrecipMonthlyExtremeYear, &r.PrecipDailyExtreme, &r.PrecipDailyExtremeDate,
			&r.TmaxYearsWithData, &r.TminYearsWithData, &r.TmeanYearsWithData,
			&r.PrecipYearsWithData, &r.EvapYearsWithData,
			&r.RainDays, &r.RainDaysYearsWithData)
	Expect(err).NotTo(HaveOccurred())
	return r
}

// wmoRow is the read-back of the eight wmo_completeness_* columns.
type wmoRow struct {
	Bin61, Bin71, Bin81, Bin91     sql.NullFloat64
	Cont61, Cont71, Cont81, Cont91 sql.NullFloat64
}

func selectWMO(db *sql.DB, stationID int64) wmoRow {
	GinkgoHelper()
	var r wmoRow
	err := db.QueryRow(`
SELECT wmo_completeness_bin_1961_1990, wmo_completeness_bin_1971_2000,
       wmo_completeness_bin_1981_2010, wmo_completeness_bin_1991_2020,
       wmo_completeness_cont_1961_1990, wmo_completeness_cont_1971_2000,
       wmo_completeness_cont_1981_2010, wmo_completeness_cont_1991_2020
  FROM stations WHERE id = ?`, stationID).
		Scan(&r.Bin61, &r.Bin71, &r.Bin81, &r.Bin91,
			&r.Cont61, &r.Cont71, &r.Cont81, &r.Cont91)
	Expect(err).NotTo(HaveOccurred())
	return r
}

// warningRow is the value read-back of one parsing_warnings row.
type warningRow struct {
	StationID  sql.NullInt64
	SourceFile string
	Line       sql.NullInt64
	Severity   string
	Issue      string
}

func selectWarnings(db *sql.DB) []warningRow {
	GinkgoHelper()
	rows, err := db.Query(`
SELECT station_id, source_file, line, severity, issue
  FROM parsing_warnings ORDER BY id`)
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close() //nolint:errcheck // read-side close in a spec
	var out []warningRow
	for rows.Next() {
		var w warningRow
		Expect(rows.Scan(&w.StationID, &w.SourceFile, &w.Line, &w.Severity, &w.Issue)).To(Succeed())
		out = append(out, w)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

// runRow is the full read-back of one ingest_runs row.
type runRow struct {
	StartedAt    string
	FinishedAt   sql.NullString
	SnapshotDate string
	SinkKind     string
	ETLGitSHA    sql.NullString
	Status       string
	Attempted    sql.NullInt64
	Succeeded    sql.NullInt64
	Failed       sql.NullInt64
	DailyRows    sql.NullInt64
	NormalsRows  sql.NullInt64
	ExtrasRows   sql.NullInt64
	Warnings     sql.NullInt64
}

func selectRun(db *sql.DB, id int64) runRow {
	GinkgoHelper()
	var r runRow
	err := db.QueryRow(`
SELECT started_at, finished_at, snapshot_date, sink_kind, etl_git_sha, status,
       stations_attempted, stations_succeeded, stations_failed,
       daily_rows, normals_rows, extras_rows, warnings_total
  FROM ingest_runs WHERE id = ?`, id).
		Scan(&r.StartedAt, &r.FinishedAt, &r.SnapshotDate, &r.SinkKind, &r.ETLGitSHA, &r.Status,
			&r.Attempted, &r.Succeeded, &r.Failed,
			&r.DailyRows, &r.NormalsRows, &r.ExtrasRows, &r.Warnings)
	Expect(err).NotTo(HaveOccurred())
	return r
}

func countRows(db *sql.DB, query string, args ...any) int {
	GinkgoHelper()
	var n int
	Expect(db.QueryRow(query, args...).Scan(&n)).To(Succeed())
	return n
}

func validInt(v int64) sql.NullInt64       { return sql.NullInt64{Int64: v, Valid: true} }
func validFloat(v float64) sql.NullFloat64 { return sql.NullFloat64{Float64: v, Valid: true} }
func validStr(s string) sql.NullString     { return sql.NullString{String: s, Valid: true} }

// runOpts builds the common Options shape for a synthetic-snapshot Run.
func runOpts(sink snapshot.Sink, metaRoot, date, dbPath string) ingest.Options {
	return ingest.Options{
		Sink:         sink,
		SinkKind:     "local",
		MetadataRoot: metaRoot,
		SnapshotDate: date,
		DBPath:       dbPath,
	}
}
