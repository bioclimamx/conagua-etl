package publish_test

// Specs for QA-REPORT.md: every number of every section round-tripped
// against a seeded DB, the expected values computed here with plain
// SQL or written out from the seed; the Markdown rendered
// deterministically with every section in order and no timestamp; the
// gate section listing every error and capping warn samples; and the
// seam to the real gate over the same DB.

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// seedQA builds the QA fixture over the shared station and run seeds:
// two more stations for the explicit-null shapes (a NULL state, a code
// with no official name), a station → cell link, supplement rows with
// values in a spread of columns (one with no run, one with a dangling
// run), and parsing_warnings rows from every producer (ingest keys of
// two kinds, validate namespaces, a NULL source_file).
func seedQA(db *sql.DB) (stations, runs map[string]int64) {
	GinkgoHelper()
	stations = seedTwoStates(db)
	runs = seedRuns(db)
	seedRunEdges(db)
	stations["ema/99"] = upsertStation(db, ingest.StationUpsert{
		Source: ingest.SourceConaguaEMA, ExternalID: "99", Name: "No state",
	})
	stations["ema/98"] = upsertStation(db, ingest.StationUpsert{
		Source: ingest.SourceConaguaEMA, ExternalID: "98", Name: "Odd state", State: "ZZZ", Status: "operating",
	})
	mustExec(db, `INSERT INTO station_power_cell (station_id, cell_id, distance_km) VALUES (?, 'n21.75_w89.375', 3.2)`,
		stations["conv/31001"])
	mustExec(db, `INSERT INTO station_power_cell (station_id, cell_id, distance_km) VALUES (?, 'n20.75_w88.125', 11.9)`,
		stations["conv/3101"])
	mustExec(db, `INSERT INTO monthly_supplement (cell_id, period, month, t2m_c, rh2m_pct, wd10m_deg, gwet_prof, power_run_id)
	  VALUES ('n20.75_w88.125', '1991-2020', 1, 26.1, 71.5, 360.0, 0.42, ?)`, runs["power-monthly-1981-2010"])
	mustExec(db, `INSERT INTO daily_supplement (cell_id, date, rh2m_pct, wd2m_deg, ps_kpa, power_run_id)
	  VALUES ('n21.75_w89.375', '2019-12-31', 80.25, 0.0, 101.2, ?)`, runs["power-daily-1981-2026"])
	mustExec(db, `INSERT INTO daily_supplement (cell_id, date, t2m_c, power_run_id)
	  VALUES ('n21.75_w89.375', '2019-12-30', 22.0, 9999)`)

	warn := func(stationID any, sourceFile any, severity, issue string) {
		mustExec(db, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue) VALUES (?, ?, NULL, ?, ?)`,
			stationID, sourceFile, severity, issue)
	}
	daily := "conagua-raw/2026-06-08/daily/31001.txt"
	normals := "conagua-raw/2026-06-08/normals_1991_2020/31001.txt"
	warn(stations["conv/31001"], daily, "warn", "line 12: unparseable tmax")
	warn(stations["conv/31001"], daily, "warn", "line 40: duplicate date")
	warn(stations["conv/31001"], normals, "warn", "missing evap section")
	warn(stations["conv/31001"], normals, "error", "header mismatch")
	warn(nil, "validate:bbox", "error", "station conagua_ema/99 (\"No state\") at impossible lat=0.0000 lon=0.0000")
	warn(nil, "validate:daily-sanity", "warn", "tmax < tmin on 3 days")
	warn(nil, "validate:daily-sanity", "warn", "precip > 500 on 1 day")
	warn(nil, "validate:daily-sanity", "warn", "evap > 30 on 2 days")
	warn(nil, nil, "warn", "orphan warning with no source")
	return stations, runs
}

// loadQAInputs resolves what publish resolves before LoadQA: the
// provenance runs, the table counts, and the state list.
func loadQAInputs(db *sql.DB) (publish.Runs, publish.TableCounts, []publish.State) {
	GinkgoHelper()
	ctx := context.Background()
	runs, err := publish.LoadRuns(ctx, db)
	Expect(err).NotTo(HaveOccurred())
	counts, err := publish.LoadCounts(ctx, db)
	Expect(err).NotTo(HaveOccurred())
	states, err := publish.LoadStates(ctx, db)
	Expect(err).NotTo(HaveOccurred())
	return runs, counts, states
}

func qaMeta(gitSHA string, runs publish.Runs) publish.ProfileMeta {
	return publish.ProfileMeta{
		SchemaVersion: schema.Version, ETLGitSHA: gitSHA, SnapshotDate: "2026-06-08",
		Runs: runs, Dataset: publish.DatasetMetadata(seededSnapshot, ""),
	}
}

func renderQA(q *publish.QAReport, meta publish.ProfileMeta) string {
	GinkgoHelper()
	var buf bytes.Buffer
	Expect(publish.RenderQA(&buf, q, meta)).To(Succeed())
	return buf.String()
}

// countSQL runs a single-value COUNT query — the plain-SQL oracle the
// loader's combined aggregates are held to.
func countSQL(db *sql.DB, query string, args ...any) int64 {
	GinkgoHelper()
	var n int64
	Expect(db.QueryRowContext(context.Background(), query, args...).Scan(&n)).To(Succeed(), query)
	return n
}

func pct(nonNull, rows int64) *float64 {
	p := float64(nonNull) * 100 / float64(rows)
	return &p
}

// powerColumnExtentsSQL is daily_supplement's per-column extent from
// plain SQL — one query per POWER column, each reading only the rows
// that carry that column — the oracle the loader's single
// conditional-aggregate pass is held to.
func powerColumnExtentsSQL(db *sql.DB) []publish.QAColumnExtent {
	GinkgoHelper()
	out := make([]publish.QAColumnExtent, 0, len(powerValueColumns))
	for _, col := range powerValueColumns {
		e := publish.QAColumnExtent{Column: col}
		var first, last sql.NullString
		Expect(db.QueryRowContext(context.Background(),
			"SELECT COUNT("+col+"), MIN(date), MAX(date) FROM daily_supplement WHERE "+col+" IS NOT NULL").
			Scan(&e.NonNull, &first, &last)).To(Succeed(), col)
		if first.Valid {
			e.FirstDate = str(first.String)
		}
		if last.Valid {
			e.LastDate = str(last.String)
		}
		out = append(out, e)
	}
	return out
}

// The value columns of the five null / gap tables, written out from
// the DDL; the POWER-31 list is the registry's, which the power suite
// holds equal to the DDL.
var (
	dailyValueColumns   = []string{"tmax", "tmin", "precip", "evap"}
	normalsValueColumns = []string{"tmax", "tmin", "tmean", "precip", "evap"}
	extrasValueColumns  = []string{
		"tmax_monthly_extreme", "tmax_monthly_extreme_year", "tmax_daily_extreme", "tmax_daily_extreme_date",
		"tmin_monthly_extreme", "tmin_monthly_extreme_year", "tmin_daily_extreme", "tmin_daily_extreme_date",
		"precip_monthly_extreme", "precip_monthly_extreme_year", "precip_daily_extreme", "precip_daily_extreme_date",
		"tmax_years_with_data", "tmin_years_with_data", "tmean_years_with_data", "precip_years_with_data",
		"evap_years_with_data", "rain_days", "rain_days_years_with_data",
	}
	powerValueColumns = func() []string {
		cols := make([]string, len(power.Registry))
		for i, p := range power.Registry {
			cols[i] = p.Column
		}
		return cols
	}()
)

// timestampRe matches the date part of an RFC 3339 stamp — what a
// wall-clock leak, or a run's started_at, would look like.
var timestampRe = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T`)

// recordingDriver wraps the registered sqlite driver so that every
// statement a connection prepares is captured by text. database/sql
// routes a query through Prepare whenever the connection offers neither
// QueryerContext nor ExecerContext, and recordingConn deliberately
// promotes only driver.Conn's three methods, so the capture is
// complete: a loader cannot run a statement this recorder does not see.
type recordingDriver struct {
	inner driver.Driver
	mu    sync.Mutex
	stmts []string
}

func (d *recordingDriver) Open(name string) (driver.Conn, error) {
	c, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &recordingConn{Conn: c, d: d}, nil
}

func (d *recordingDriver) record(query string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stmts = append(d.stmts, query)
}

// reset clears the capture; statements returns what was captured since.
func (d *recordingDriver) reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stmts = nil
}

func (d *recordingDriver) statements() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.stmts...)
}

type recordingConn struct {
	driver.Conn
	d *recordingDriver
}

func (c *recordingConn) Prepare(query string) (driver.Stmt, error) {
	c.d.record(query)
	return c.Conn.Prepare(query)
}

func (c *recordingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.d.record(query)
	if p, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return p.PrepareContext(ctx, query)
	}
	return c.Conn.Prepare(query)
}

const qaRecordingDriverName = "recording-sqlite-publish-qa"

var (
	qaRecording    = &recordingDriver{}
	qaRegisterOnce sync.Once
)

// openRecordedQA seeds a fresh database through the writer, closes it,
// and reopens the same file through the recording driver on a single
// connection — so the statements a load prepares are one connection's,
// in order, and can be counted and re-planned.
func openRecordedQA(seed func(*sql.DB)) *sql.DB {
	GinkgoHelper()
	path := filepath.Join(GinkgoT().TempDir(), "qa-recorded.db")
	db, err := schema.Open(path)
	Expect(err).NotTo(HaveOccurred())
	seed(db)
	Expect(db.Close()).To(Succeed())
	qaRegisterOnce.Do(func() {
		probe, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		qaRecording.inner = probe.Driver()
		Expect(probe.Close()).To(Succeed())
		sql.Register(qaRecordingDriverName, qaRecording)
	})
	rec, err := sql.Open(qaRecordingDriverName, path)
	Expect(err).NotTo(HaveOccurred())
	rec.SetMaxOpenConns(1)
	DeferCleanup(func() { _ = rec.Close() })
	return rec
}

// gateScopeText is the scope statement §5 carries between the gate's
// totals and its per-rule table: what these rules check, and the name
// of what assesses the climatology they do not.
const gateScopeText = "These rules check referential and structural integrity and coordinate\n" +
	"plausibility; none of them assesses climatological plausibility. Within-month\n" +
	"completeness, daily-series sanity and cross-period consistency are the separate\n" +
	"`validate` verb's rules, evaluated there and not part of this gate.\n\n"

// warningsScopeText is §7's statement of what its counts are of: rows
// earlier runs wrote into this database, never an assessment publish
// made.
func warningsScopeText(total int64) string {
	return "The parsing_warnings rows the database holds (" + itoa64(total) + "): what ingest's parsers\n" +
		"wrote as they read the source files, and what the `validate` verb wrote where\n" +
		"it has been run against this database. Publishing writes none of its own, so\n" +
		"an empty table means no such row was written here, not that the data was\n" +
		"assessed at build time.\n"
}

// Every section heading, in report order.
var qaHeadings = []string{
	"## 1. Snapshot and runs",
	"## 2. Per-table row counts",
	"## 3. Coverage",
	"## 4. Null / gap summary",
	"## 5. Gate results",
	"## 6. Run counters vs live counts",
	"## 7. Parsing warnings",
}

var _ = Describe("LoadQA", func() {
	var (
		db     *sql.DB
		q      *publish.QAReport
		runs   publish.Runs
		counts publish.TableCounts
	)
	BeforeEach(func() {
		db = openTempDB()
		seedQA(db)
		var states []publish.State
		runs, counts, states = loadQAInputs(db)
		var err error
		q, err = publish.LoadQA(context.Background(), db, "2026-06-08", runs, counts, nil, states)
		Expect(err).NotTo(HaveOccurred())
	})

	It("carries the resolved inputs through untouched", func() {
		Expect(q.SnapshotDate).To(Equal("2026-06-08"))
		Expect(q.Runs).To(Equal(runs))
		Expect(q.Counts).To(Equal(counts))
		Expect(q.Gate).To(BeNil())
	})

	It("counts stations by status, NULL last as an explicit null", func() {
		Expect(q.Coverage.StationsByStatus).To(Equal([]publish.QAStatusCount{
			{Status: str("operating"), Stations: 3},
			{Status: str("suspended"), Stations: 1},
			{Status: nil, Stations: 3},
		}))
		Expect(countSQL(db, `SELECT COUNT(*) FROM stations WHERE status = 'operating'`)).To(Equal(int64(3)))
		Expect(countSQL(db, `SELECT COUNT(*) FROM stations WHERE status IS NULL`)).To(Equal(int64(3)))
	})

	It("counts stations by state with the official name; an unknown code and a NULL state stay honest", func() {
		Expect(q.Coverage.StationsByState).To(Equal([]publish.QAStateCount{
			{Code: str("AGS"), Name: str(conagua.StateCode("ags").DisplayName()), Stations: 1},
			{Code: str("YUC"), Name: str(conagua.StateCode("yuc").DisplayName()), Stations: 4},
			{Code: str("ZZZ"), Name: nil, Stations: 1},
			{Code: nil, Name: nil, Stations: 1},
		}))
		Expect(*q.Coverage.StationsByState[1].Name).To(Equal("Yucatán"))
		Expect(countSQL(db, `SELECT COUNT(*) FROM stations WHERE state = 'YUC'`)).To(Equal(int64(4)))
	})

	It("names a state from the states list before the code table", func() {
		states := []publish.State{{Code: "YUC", Slug: "yuc", Name: "Renamed"}}
		got, err := publish.LoadQA(context.Background(), db, "2026-06-08", runs, counts, nil, states)
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Coverage.StationsByState[1]).To(Equal(publish.QAStateCount{Code: str("YUC"), Name: str("Renamed"), Stations: 4}))
		// AGS is not in the list and falls back to the code table.
		Expect(got.Coverage.StationsByState[0].Name).To(Equal(str("Aguascalientes")))
	})

	It("measures the observed daily series: rows, stations, and date bounds", func() {
		Expect(q.Coverage.Daily).To(Equal(publish.QASeriesExtent{
			Rows: 6, Keys: 4, FirstDate: str("1975-06-15"), LastDate: str("2020-01-02"),
			ValueFirstDate: str("1975-06-15"), ValueLastDate: str("2020-01-02"),
		}))
		Expect(countSQL(db, `SELECT COUNT(DISTINCT station_id) FROM daily_observations`)).To(Equal(int64(4)))
	})

	It("lists every normals period, oldest first, with stations and rows per table", func() {
		Expect(q.Coverage.Normals).To(Equal([]publish.QAPeriodCoverage{
			{Period: "1961-1990", NormalsStations: 1, NormalsRows: 1, ExtrasStations: 1, ExtrasRows: 1},
			{Period: "1971-2000"},
			{Period: "1981-2010", NormalsStations: 3, NormalsRows: 4, ExtrasStations: 2, ExtrasRows: 3},
			{Period: "1991-2020", NormalsStations: 1, NormalsRows: 2, ExtrasStations: 1, ExtrasRows: 1},
		}))
		for _, p := range q.Coverage.Normals {
			Expect(p.NormalsStations).To(Equal(countSQL(db,
				`SELECT COUNT(DISTINCT station_id) FROM monthly_normals WHERE period = ?`, p.Period)), p.Period)
			Expect(p.NormalsRows).To(Equal(countSQL(db,
				`SELECT COUNT(*) FROM monthly_normals WHERE period = ?`, p.Period)), p.Period)
			Expect(p.ExtrasStations).To(Equal(countSQL(db,
				`SELECT COUNT(DISTINCT station_id) FROM monthly_normals_extras WHERE period = ?`, p.Period)), p.Period)
			Expect(p.ExtrasRows).To(Equal(countSQL(db,
				`SELECT COUNT(*) FROM monthly_normals_extras WHERE period = ?`, p.Period)), p.Period)
		}
	})

	It("measures the POWER footprint: cells, links, the daily extent, every monthly period", func() {
		Expect(q.Coverage.Power).To(Equal(publish.QAPowerCoverage{
			Cells: 2, LinkedStations: 2,
			Daily: publish.QASeriesExtent{Rows: 4, Keys: 2, FirstDate: str("2019-12-30"), LastDate: str("2020-01-02"),
				ValueFirstDate: str("2019-12-30"), ValueLastDate: str("2020-01-02")},
			DailyColumnExtents: powerColumnExtentsSQL(db),
			Monthly: []publish.QAPowerPeriod{
				{Period: "1961-1990"}, {Period: "1971-2000"},
				{Period: "1981-2010", Cells: 1, Rows: 2},
				{Period: "1991-2020", Cells: 1, Rows: 1},
			},
		}))
		Expect(q.Coverage.Power.Daily.Rows).To(Equal(counts.DailySupplement))
	})

	It("breaks the POWER daily series out per column: every POWER-31 listed, in registry order, with its own bounds", func() {
		got := q.Coverage.Power.DailyColumnExtents
		Expect(got).To(Equal(powerColumnExtentsSQL(db)))
		Expect(got).To(HaveLen(len(powerValueColumns)))
		for i, e := range got {
			Expect(e.Column).To(Equal(powerValueColumns[i]), e.Column)
		}
		// A column's non-null count is the null / gap summary's own
		// COUNT(col) for the same table, read across from one pass.
		for i, c := range q.Nulls[4].Columns {
			Expect(got[i].NonNull).To(Equal(c.NonNull), c.Column)
		}

		byColumn := map[string]publish.QAColumnExtent{}
		for _, e := range got {
			byColumn[e.Column] = e
		}
		// Anchors written out from the seed. t2m_c spans the whole table;
		// rh2m_pct holds one row, a day after the table's first date and
		// two before its last — the shape a whole-table MIN(date) hides,
		// and the reason the breakdown exists; a column no row carries has
		// no bounds at all rather than the table's.
		Expect(byColumn["t2m_c"]).To(Equal(publish.QAColumnExtent{
			Column: "t2m_c", NonNull: 3, FirstDate: str("2019-12-30"), LastDate: str("2020-01-02")}))
		Expect(byColumn["rh2m_pct"]).To(Equal(publish.QAColumnExtent{
			Column: "rh2m_pct", NonNull: 1, FirstDate: str("2019-12-31"), LastDate: str("2019-12-31")}))
		Expect(byColumn["ps_kpa"]).To(Equal(publish.QAColumnExtent{
			Column: "ps_kpa", NonNull: 1, FirstDate: str("2019-12-31"), LastDate: str("2019-12-31")}))
		Expect(byColumn["solar_ghi_wm2"]).To(Equal(publish.QAColumnExtent{Column: "solar_ghi_wm2"}))
		// The whole-table extent stays the rows', not any column's.
		Expect(q.Coverage.Power.Daily.FirstDate).To(Equal(str("2019-12-30")))
		Expect(q.Coverage.Power.Daily.LastDate).To(Equal(str("2020-01-02")))
	})

	It("reads daily_supplement once for the null summary, the extent, and every column's bounds", func() {
		rec := openRecordedQA(func(seeded *sql.DB) { seedQA(seeded) })
		runs, counts, states := loadQAInputs(rec)
		qaRecording.reset()
		got, err := publish.LoadQA(context.Background(), rec, "2026-06-08", runs, counts, nil, states)
		Expect(err).NotTo(HaveOccurred())
		// Captured before anything else queries the table — the oracle
		// below runs 31 statements of its own over it.
		loaded := qaRecording.statements()
		Expect(got.Coverage.Power.DailyColumnExtents).To(Equal(powerColumnExtentsSQL(rec)))

		var overTable []string
		for _, s := range loaded {
			if strings.Contains(s, "FROM daily_supplement") {
				overTable = append(overTable, s)
			}
		}
		// Two statements read the table and no more: the aggregate pass
		// (COUNT(*), the extent, the per-column bounds, every COUNT(col))
		// and the rows-per-run GROUP BY. The 31 columns' bounds cost no
		// pass of their own — 31 separate queries over the deposit's
		// largest table is what this rules out.
		Expect(overTable).To(HaveLen(2))
		agg := overTable[0]
		Expect(agg).To(HavePrefix("SELECT COUNT(*), COUNT(DISTINCT cell_id), MIN(date), MAX(date), "))
		Expect(overTable[1]).To(ContainSubstring("GROUP BY power_run_id"))
		for _, col := range powerValueColumns {
			Expect(agg).To(ContainSubstring("MIN(CASE WHEN "+col+" IS NOT NULL THEN date END)"), col)
			Expect(agg).To(ContainSubstring("MAX(CASE WHEN "+col+" IS NOT NULL THEN date END)"), col)
			Expect(agg).To(ContainSubstring("COUNT("+col+")"), col)
		}
		// And that one statement reads the table exactly once: a single
		// SCAN, the plan's other line being the temp b-tree the
		// pre-existing COUNT(DISTINCT cell_id) needs.
		var reads []string
		for _, line := range planDetails(rec, agg) {
			if strings.Contains(line, "daily_supplement") {
				reads = append(reads, line)
			}
		}
		Expect(reads).To(Equal([]string{"SCAN daily_supplement"}))
	})

	It("summarizes nulls per value column of the five tables, one non-null count and share each", func() {
		Expect(q.Nulls).To(HaveLen(5))
		want := []struct {
			table   string
			columns []string
		}{
			{"daily_observations", dailyValueColumns},
			{"monthly_normals", normalsValueColumns},
			{"monthly_normals_extras", extrasValueColumns},
			{"monthly_supplement", powerValueColumns},
			{"daily_supplement", powerValueColumns},
		}
		for i, w := range want {
			t := q.Nulls[i]
			Expect(t.Table).To(Equal(w.table))
			Expect(t.Rows).To(Equal(countSQL(db, "SELECT COUNT(*) FROM "+w.table)), w.table)
			Expect(t.Columns).To(HaveLen(len(w.columns)), w.table)
			for j, col := range w.columns {
				nonNull := countSQL(db, "SELECT COUNT("+col+") FROM "+w.table)
				Expect(t.Columns[j]).To(Equal(publish.QANullColumn{Column: col, NonNull: nonNull, Percent: pct(nonNull, t.Rows)}),
					w.table+"."+col)
			}
		}
		// Anchors written out from the seed, so the SQL oracle and the
		// loader cannot agree on a wrong seed.
		daily := q.Nulls[0]
		Expect(daily.Rows).To(Equal(int64(6)))
		Expect(daily.Columns[0]).To(Equal(publish.QANullColumn{Column: "tmax", NonNull: 5, Percent: pct(5, 6)}))
		Expect(daily.Columns[3]).To(Equal(publish.QANullColumn{Column: "evap", NonNull: 4, Percent: pct(4, 6)}))
		normals := q.Nulls[1]
		Expect(normals.Columns[2]).To(Equal(publish.QANullColumn{Column: "tmean", NonNull: 5, Percent: pct(5, 7)}))
		extras := q.Nulls[2]
		Expect(extras.Columns[0]).To(Equal(publish.QANullColumn{Column: "tmax_monthly_extreme", NonNull: 4, Percent: pct(4, 5)}))
		Expect(extras.Columns[17]).To(Equal(publish.QANullColumn{Column: "rain_days", NonNull: 3, Percent: pct(3, 5)}))
		monthly := q.Nulls[3]
		Expect(monthly.Rows).To(Equal(int64(3)))
		Expect(monthly.Columns[0]).To(Equal(publish.QANullColumn{Column: "t2m_c", NonNull: 3, Percent: pct(3, 3)}))
		Expect(monthly.Columns[14]).To(Equal(publish.QANullColumn{Column: "wd10m_deg", NonNull: 1, Percent: pct(1, 3)}))
		dailySupp := q.Nulls[4]
		Expect(dailySupp.Rows).To(Equal(int64(4)))
		Expect(dailySupp.Columns[8]).To(Equal(publish.QANullColumn{Column: "rh2m_pct", NonNull: 1, Percent: pct(1, 4)}))
		Expect(dailySupp.Columns[30]).To(Equal(publish.QANullColumn{Column: "gwet_prof", NonNull: 0, Percent: pct(0, 4)}))
	})

	It("counts each power run's live rows by label beside its stored counter, and the rows no run claims", func() {
		Expect(q.Counters).To(Equal(publish.QACounters{
			Power: []publish.QAPowerLive{
				{RunLabel: "power-daily-1981-2026", SupplementRows: nil, MonthlyRows: 0, DailyRows: 2},
				{RunLabel: "power-monthly-1981-2010", SupplementRows: i64(28800), MonthlyRows: 3, DailyRows: 0},
			},
			MonthlyRowsWithoutRun: 0,
			DailyRowsWithoutRun:   1,
		}))
		Expect(countSQL(db, `SELECT COUNT(*) FROM daily_supplement WHERE power_run_id IS NULL`)).To(Equal(int64(1)))
		// The dangling reference is counted under no label and is not a
		// row without a run.
		Expect(countSQL(db, `SELECT COUNT(*) FROM daily_supplement WHERE power_run_id = 9999`)).To(Equal(int64(1)))
	})

	It("counts parsing_warnings by severity and by producer, a NULL source last", func() {
		Expect(q.Warnings).To(Equal(publish.QAWarnings{
			Total: 9,
			BySeverity: []publish.QASeverityCount{
				{Severity: "error", Rows: 2},
				{Severity: "warn", Rows: 7},
			},
			BySource: []publish.QASourceCount{
				{Source: str("conagua-raw/2026-06-08/daily"), Warn: 2, Error: 0},
				{Source: str("conagua-raw/2026-06-08/normals_1991_2020"), Warn: 1, Error: 1},
				{Source: str("validate"), Warn: 3, Error: 1},
				{Source: nil, Warn: 1, Error: 0},
			},
		}))
		Expect(q.Warnings.Total).To(Equal(counts.ParsingWarnings))
		Expect(countSQL(db, `SELECT COUNT(*) FROM parsing_warnings WHERE source_file LIKE 'validate:%' AND severity = 'warn'`)).
			To(Equal(int64(3)))
	})

	It("marshals as JSON with explicit nulls and empty lists", func() {
		raw, err := json.Marshal(q)
		Expect(err).NotTo(HaveOccurred())
		var got map[string]any
		Expect(json.Unmarshal(raw, &got)).To(Succeed())
		Expect(got["gate"]).To(BeNil())
		byState := got["coverage"].(map[string]any)["stations_by_state"].([]any)
		Expect(byState[3]).To(Equal(map[string]any{"code": nil, "name": nil, "stations": float64(1)}))
		bySource := got["parsing_warnings"].(map[string]any)["by_source"].([]any)
		Expect(bySource[3]).To(Equal(map[string]any{"source": nil, "warn": float64(1), "error": float64(0)}))
		Expect(got["counters"].(map[string]any)["power"].([]any)[0].(map[string]any)["supplement_rows"]).To(BeNil())

		cols := got["coverage"].(map[string]any)["power"].(map[string]any)["daily_supplement_columns"].([]any)
		Expect(cols).To(HaveLen(len(powerValueColumns)))
		Expect(cols[0]).To(Equal(map[string]any{
			"column": "t2m_c", "non_null": float64(3), "first_date": "2019-12-30", "last_date": "2020-01-02"}))
		Expect(powerValueColumns[15]).To(Equal("solar_ghi_wm2"))
		Expect(cols[15]).To(Equal(map[string]any{
			"column": "solar_ghi_wm2", "non_null": float64(0), "first_date": nil, "last_date": nil}))

		var back publish.QAReport
		Expect(json.Unmarshal(raw, &back)).To(Succeed())
		Expect(back.Coverage.Power).To(Equal(q.Coverage.Power))
	})

	It("is refused by a cancelled context", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := publish.LoadQA(ctx, db, "2026-06-08", runs, counts, nil, nil)
		Expect(err).To(MatchError(ContainSubstring("context canceled")))
	})
})

var _ = Describe("LoadQA on an empty DB", func() {
	It("loads zeros and explicit nulls from a schema-only DB with one complete ingest run", func() {
		db := openTempDB()
		insertIngestRun(db, ingestRunSeed{
			startedAt: "2026-06-09T01:00:00Z", finishedAt: str("2026-06-09T03:00:00Z"),
			snapshotDate: "2026-06-08", sinkKind: "local", gitSHA: str("f1r5t"), status: "complete",
			counters: make([]*int64, 7),
		})
		runs, counts, states := loadQAInputs(db)
		Expect(states).To(BeEmpty())
		gate, err := validate.Gate(context.Background(), db, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(gate.Errors).To(BeZero())

		q, err := publish.LoadQA(context.Background(), db, "2026-06-08", runs, counts, gate, states)
		Expect(err).NotTo(HaveOccurred())
		Expect(q.Coverage.StationsByStatus).To(BeEmpty())
		Expect(q.Coverage.StationsByState).To(BeEmpty())
		Expect(q.Coverage.Daily).To(Equal(publish.QASeriesExtent{}))
		Expect(q.Coverage.Power.Daily).To(Equal(publish.QASeriesExtent{}))
		// Every POWER column is still listed, each with no bounds at all:
		// an empty table is described, never abbreviated away.
		Expect(q.Coverage.Power.DailyColumnExtents).To(HaveLen(len(powerValueColumns)))
		for i, e := range q.Coverage.Power.DailyColumnExtents {
			Expect(e).To(Equal(publish.QAColumnExtent{Column: powerValueColumns[i]}), e.Column)
		}
		for _, p := range q.Coverage.Normals {
			Expect(p).To(Equal(publish.QAPeriodCoverage{Period: p.Period}))
		}
		for _, t := range q.Nulls {
			Expect(t.Rows).To(BeZero(), t.Table)
			for _, c := range t.Columns {
				Expect(c.NonNull).To(BeZero(), t.Table+"."+c.Column)
				Expect(c.Percent).To(BeNil(), t.Table+"."+c.Column)
			}
		}
		Expect(q.Counters).To(Equal(publish.QACounters{Power: []publish.QAPowerLive{}}))
		Expect(q.Warnings).To(Equal(publish.QAWarnings{BySeverity: []publish.QASeverityCount{}, BySource: []publish.QASourceCount{}}))

		md := renderQA(q, qaMeta("abc123", runs))
		for _, h := range qaHeadings {
			Expect(md).To(ContainSubstring(h + "\n"))
		}
		Expect(md).To(ContainSubstring("| daily_observations | 0 |\n"))
		Expect(md).To(ContainSubstring("| tmax | 0 | n/a |\n"))
		Expect(md).To(ContainSubstring("- First date: null\n"))
		Expect(md).To(ContainSubstring("| 2026-06-08 | complete | local | f1r5t |\n"))
		Expect(md).To(ContainSubstring("Rules: 9. Error findings: 0. Warn findings: 0.\n"))
		Expect(timestampRe.FindString(md)).To(BeEmpty())

		raw, err := json.Marshal(q)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(raw)).To(ContainSubstring(`"first_date":null`))
		Expect(string(raw)).To(ContainSubstring(
			`"daily_supplement_columns":[{"column":"t2m_c","non_null":0,"first_date":null,"last_date":null}`))
		Expect(string(raw)).To(ContainSubstring(`"power":[]`))
		Expect(string(raw)).To(ContainSubstring(`"by_source":[]`))
		Expect(string(raw)).To(ContainSubstring(`"percent":null`))
	})
})

var _ = Describe("RenderQA", func() {
	var (
		db   *sql.DB
		q    *publish.QAReport
		md   string
		meta publish.ProfileMeta
	)
	BeforeEach(func() {
		db = openTempDB()
		seedQA(db)
		runs, counts, states := loadQAInputs(db)
		gate, err := validate.Gate(context.Background(), db, nil)
		Expect(err).NotTo(HaveOccurred())
		q, err = publish.LoadQA(context.Background(), db, "2026-06-08", runs, counts, gate, states)
		Expect(err).NotTo(HaveOccurred())
		meta = qaMeta("abc123", runs)
		md = renderQA(q, meta)
	})

	It("renders byte-identically twice, LF-only, ending in exactly one newline", func() {
		Expect(renderQA(q, meta)).To(Equal(md))
		Expect(md).NotTo(ContainSubstring("\r"))
		Expect(md).To(HaveSuffix("|\n"))
		Expect(md).NotTo(HaveSuffix("\n\n"))
		Expect(md).NotTo(ContainSubstring("ARCHITECTURE"))
		Expect(md).NotTo(ContainSubstring("C3"))
	})

	It("carries no timestamp outside the gate's verbatim finding texts", func() {
		// The seed strands a running ingest run, and the gate's finding
		// names its started_at — data the report lists verbatim; every
		// other section is free of any timestamp.
		gateStart, gateEnd := strings.Index(md, "\n## 5."), strings.Index(md, "\n## 6.")
		Expect(gateStart).To(BeNumerically("<", gateEnd))
		Expect(timestampRe.FindString(md[:gateStart] + md[gateEnd:])).To(BeEmpty())
		Expect(md).NotTo(ContainSubstring("started_at |"))
	})

	It("credits the binary this repo builds, and no other binary name", func() {
		Expect(md).To(ContainSubstring("Every number in this report is computed by `conagua-etl publish` from the\n" +
			"shipped database at build time."))
		Expect(md).NotTo(ContainSubstring("bioclima-etl"))
		// The data dictionary is generated by the same package for the
		// same deposit: the two must credit one tool, not two.
		dict, err := publish.BuildDictionary(meta)
		Expect(err).NotTo(HaveOccurred())
		var buf bytes.Buffer
		Expect(publish.RenderDictionaryMarkdown(&buf, dict)).To(Succeed())
		Expect(buf.String()).To(ContainSubstring("conagua-etl publish"))
		Expect(buf.String()).NotTo(ContainSubstring("bioclima-etl"))
	})

	It("opens with the dataset identity, the snapshot, the schema version, and the git SHA", func() {
		Expect(md).To(HavePrefix("# QA report — " + publish.DatasetTitle + "\n\nVersion 0.1. Snapshot `2026-06-08`, schema version " +
			fmt.Sprint(schema.Version) + ", ETL git SHA `abc123`.\n"))
		Expect(renderQA(q, qaMeta("", q.Runs))).To(ContainSubstring("ETL git SHA (none).\n"))
	})

	It("writes every section heading in order", func() {
		last := -1
		for _, h := range qaHeadings {
			i := strings.Index(md, "\n"+h+"\n")
			Expect(i).To(BeNumerically(">", last), h)
			last = i
		}
	})

	It("lists the runs by natural label with their spans", func() {
		Expect(md).To(ContainSubstring("| snapshot_date | status | sink_kind | etl_git_sha |\n|---|---|---|---|\n" +
			"| 2026-05-01 | complete | r2 | 0ld5ha |\n| 2026-06-08 | complete | local | null |\n"))
		Expect(md).To(ContainSubstring("| run_label | temporal_mode | span | status | etl_git_sha |\n|---|---|---|---|---|\n" +
			"| power-daily-1981-2026 | daily | 1981-01-01 to 2026-06-08 | complete | null |\n" +
			"| power-monthly-1981-2010 | monthly | 1981 to 2010 | complete | p0w3r |\n"))
	})

	It("tabulates the eleven table counts in DDL order", func() {
		c := q.Counts
		Expect(md).To(ContainSubstring("| table | rows |\n|---|---|\n" +
			"| stations | " + fmt.Sprint(c.Stations) + " |\n" +
			"| monthly_normals | " + fmt.Sprint(c.MonthlyNormals) + " |\n" +
			"| monthly_normals_extras | " + fmt.Sprint(c.MonthlyNormalsExtras) + " |\n" +
			"| daily_observations | 6 |\n" +
			"| parsing_warnings | 9 |\n" +
			"| ingest_runs | 5 |\n" +
			"| power_runs | 3 |\n" +
			"| nasa_power_grid_cells | 2 |\n" +
			"| station_power_cell | 2 |\n" +
			"| monthly_supplement | 3 |\n" +
			"| daily_supplement | 4 |\n"))
	})

	It("renders the coverage tables with nulls explicit and dates verbatim", func() {
		Expect(md).To(ContainSubstring("| status | stations |\n|---|---|\n| operating | 3 |\n| suspended | 1 |\n| null | 3 |\n"))
		Expect(md).To(ContainSubstring("| code | state | stations |\n|---|---|---|\n| AGS | Aguascalientes | 1 |\n" +
			"| YUC | Yucatán | 4 |\n| ZZZ | null | 1 |\n| null | null | 1 |\n"))
		Expect(md).To(ContainSubstring("### Observed daily series (daily_observations)\n\n- Rows: 6\n- Stations with rows: 4\n" +
			"- First date: 1975-06-15\n- Last date: 2020-01-02\n"))
		Expect(md).To(ContainSubstring("| 1981-2010 | 3 | 4 | 2 | 3 |\n"))
		Expect(md).To(ContainSubstring("- Grid cells registered: 2\n- Stations linked to a cell: 2\n"))
		Expect(md).To(ContainSubstring("- Rows: 4\n- Cells with rows: 2\n- First date: 2019-12-30\n- Last date: 2020-01-02\n"))
		Expect(md).To(ContainSubstring("| period | cells | rows |\n|---|---|---|\n| 1961-1990 | 0 | 0 |\n| 1971-2000 | 0 | 0 |\n" +
			"| 1981-2010 | 1 | 2 |\n| 1991-2020 | 1 | 1 |\n"))
	})

	It("renders the null summary with one-decimal shares", func() {
		Expect(md).To(ContainSubstring("A NULL mirrors a gap in the source; nothing is imputed.\n"))
		Expect(md).To(ContainSubstring("### daily_observations (6 rows)\n\n| column | non-null rows | % |\n|---|---|---|\n" +
			"| tmax | 5 | 83.3 |\n| tmin | 5 | 83.3 |\n| precip | 5 | 83.3 |\n| evap | 4 | 66.7 |\n"))
		Expect(md).To(ContainSubstring("### monthly_supplement (3 rows)\n"))
		Expect(md).To(ContainSubstring("| wd10m_deg | 1 | 33.3 |\n"))
		Expect(md).To(ContainSubstring("### daily_supplement (4 rows)\n"))
		Expect(md).To(ContainSubstring("| gwet_prof | 0 | 0.0 |\n"))
		// 31 value rows per supplement table, none missing.
		Expect(strings.Count(md[strings.Index(md, "### monthly_supplement ("):strings.Index(md, "## 5.")], "\n| ")).
			To(Equal(2 * (31 + 1)))
	})

	It("renders the gate: a row per rule in execution order, every error listed verbatim", func() {
		rules := validate.GateRules()
		var summary strings.Builder
		summary.WriteString("| rule | name | scanned | warn | error |\n|---|---|---|---|---|\n")
		for _, rr := range q.Gate.Rules {
			fmt.Fprintf(&summary, "| %s | %s | %d | %d | %d |\n", rr.ID, rr.Name, rr.Scanned, rr.Warnings, rr.Errors)
		}
		Expect(md).To(ContainSubstring(summary.String()))
		Expect(q.Gate.Rules).To(HaveLen(len(rules)))
		var errs int
		for i, rr := range q.Gate.Rules {
			Expect(rr.ID).To(Equal(rules[i].ID))
			Expect(rr.Name).To(Equal(rules[i].Name))
			Expect(rr.ErrorIssues).To(HaveLen(int(rr.Errors)), rr.ID)
			errs += len(rr.ErrorIssues)
			for _, issue := range rr.ErrorIssues {
				Expect(md).To(ContainSubstring("\n- "+issue+"\n"), rr.ID)
			}
		}
		Expect(q.Gate.Errors).To(BeNumerically(">", 0))
		Expect(int(q.Gate.Errors)).To(Equal(errs))
		Expect(md).To(ContainSubstring(fmt.Sprintf("Rules: %d. Error findings: %d. Warn findings: %d.\n",
			len(rules), q.Gate.Errors, q.Gate.Warnings)))
		Expect(md).To(ContainSubstring("findings are listed up to 20 per rule.\n"))
	})

	It("renders the counters beside the live counts, and the rows no run claims", func() {
		Expect(md).To(ContainSubstring("Run for snapshot `2026-06-08`:\n\n| counter | stored | live table | live rows |\n|---|---|---|---|\n" +
			"| stations_attempted | 5524 | stations | 7 |\n| stations_succeeded | 5523 | stations | 7 |\n" +
			"| stations_failed | 1 |  |  |\n| daily_rows | 71399000 | daily_observations | 6 |\n" +
			"| normals_rows | 250000 | monthly_normals | 7 |\n| extras_rows | 249988 | monthly_normals_extras | 5 |\n" +
			"| warnings_total | null | parsing_warnings | 9 |\n"))
		Expect(md).To(ContainSubstring("| run_label | supplement_rows (stored) | monthly_supplement rows | daily_supplement rows |\n" +
			"|---|---|---|---|\n| power-daily-1981-2026 | null | 0 | 2 |\n| power-monthly-1981-2010 | 28800 | 3 | 0 |\n"))
		Expect(md).To(ContainSubstring("- monthly_supplement rows with no run (power_run_id NULL): 0\n" +
			"- daily_supplement rows with no run (power_run_id NULL): 1\n"))
	})

	It("keeps section 3 on the whole-table extent, the per-column detail staying out of it", func() {
		section := md[strings.Index(md, "## 3. Coverage"):strings.Index(md, "## 4. Null / gap summary")]
		Expect(section).To(ContainSubstring("Daily reanalysis series (daily_supplement):\n\n" +
			"- Rows: 4\n- Cells with rows: 2\n- First date: 2019-12-30\n- Last date: 2020-01-02\n"))
		for _, col := range powerValueColumns {
			Expect(section).NotTo(ContainSubstring(col), col)
		}
	})

	It("renders the parsing warnings by severity and by source", func() {
		Expect(md).To(ContainSubstring(warningsScopeText(9)))
		Expect(md).NotTo(ContainSubstring("as written by ingest"))
		Expect(md).To(ContainSubstring("| severity | rows |\n|---|---|\n| error | 2 |\n| warn | 7 |\n"))
		Expect(md).To(ContainSubstring("| source | warn | error |\n|---|---|---|\n" +
			"| conagua-raw/2026-06-08/daily | 2 | 0 |\n| conagua-raw/2026-06-08/normals_1991_2020 | 1 | 1 |\n" +
			"| validate | 3 | 1 |\n| null | 1 | 0 |\n"))
	})

	It("escapes a pipe in a cell so a value cannot break its row", func() {
		mustExec(db, `UPDATE stations SET status = 'odd|status' WHERE status = 'suspended'`)
		runs, counts, states := loadQAInputs(db)
		got, err := publish.LoadQA(context.Background(), db, "2026-06-08", runs, counts, nil, states)
		Expect(err).NotTo(HaveOccurred())
		Expect(renderQA(got, meta)).To(ContainSubstring(`| odd\|status | 1 |` + "\n"))
	})
})

var _ = Describe("RenderQA gate section", func() {
	// A fixture gate: one rule with two errors and thirty warnings —
	// more than the report's sample cap, fewer than the gate's own —
	// and one quiet rule.
	fixture := func() *validate.GateReport {
		noisy := validate.RuleReport{ID: "bbox", Name: "Lat/lon plausibility", Scanned: 5524, Warnings: 30, Errors: 2}
		for i := 1; i <= 2; i++ {
			noisy.Findings = append(noisy.Findings, validate.Finding{
				RuleID: "bbox", Severity: validate.SeverityError, Issue: fmt.Sprintf("error finding %d | with a pipe", i),
			})
		}
		for i := 1; i <= 30; i++ {
			noisy.Findings = append(noisy.Findings, validate.Finding{
				RuleID: "bbox", Severity: validate.SeverityWarn, Issue: fmt.Sprintf("warn finding %d", i),
			})
		}
		quiet := validate.RuleReport{ID: "wind-range", Name: "Wind direction range", Scanned: 12}
		return &validate.GateReport{Rules: []validate.RuleReport{noisy, quiet}, Warnings: 30, Errors: 2}
	}

	It("reads the gate into the section: the totals, every error's text, the first 20 warnings, no timing", func() {
		g := publish.NewQAGate(fixture())
		Expect(g.Warnings).To(Equal(int64(30)))
		Expect(g.Errors).To(Equal(int64(2)))
		Expect(g.Rules).To(HaveLen(2))
		Expect(g.Rules[0].ID).To(Equal("bbox"))
		Expect(g.Rules[0].Name).To(Equal("Lat/lon plausibility"))
		Expect(g.Rules[0].Scanned).To(Equal(int64(5524)))
		Expect(g.Rules[0].Warnings).To(Equal(int64(30)))
		Expect(g.Rules[0].Errors).To(Equal(int64(2)))
		Expect(g.Rules[0].ErrorIssues).To(Equal([]string{"error finding 1 | with a pipe", "error finding 2 | with a pipe"}))
		Expect(g.Rules[0].WarnSamples).To(HaveLen(20))
		Expect(g.Rules[0].WarnSamples[0]).To(Equal("warn finding 1"))
		Expect(g.Rules[0].WarnSamples[19]).To(Equal("warn finding 20"))
		Expect(g.Rules[1]).To(Equal(publish.QAGateRule{
			ID: "wind-range", Name: "Wind direction range", Scanned: 12, ErrorIssues: []string{}, WarnSamples: []string{},
		}))
		Expect(publish.NewQAGate(nil)).To(BeNil())

		raw, err := json.Marshal(g)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(raw)).To(HavePrefix(`{"rules":[{"id":"bbox","name":"Lat/lon plausibility","scanned":5524,"warnings":30,"errors":2,` +
			`"error_issues":["error finding 1 | with a pipe","error finding 2 | with a pipe"],"warn_samples":["warn finding 1",`))
		Expect(string(raw)).To(HaveSuffix(`"error_issues":[],"warn_samples":[]}],"warnings":30,"errors":2}`))
		var back publish.QAGate
		Expect(json.Unmarshal(raw, &back)).To(Succeed())
		Expect(&back).To(Equal(g))
	})

	It("lists every error, the first 20 warnings with the cap stated, and no subsection for a quiet rule", func() {
		md := renderQA(&publish.QAReport{Gate: publish.NewQAGate(fixture())}, publish.ProfileMeta{})
		Expect(md).To(ContainSubstring("Rules: 2. Error findings: 2. Warn findings: 30.\n"))
		Expect(md).To(ContainSubstring("| bbox | Lat/lon plausibility | 5524 | 30 | 2 |\n| wind-range | Wind direction range | 12 | 0 | 0 |\n"))
		Expect(md).To(ContainSubstring("### bbox\n\nErrors (2):\n\n- error finding 1 | with a pipe\n- error finding 2 | with a pipe\n\n" +
			"Warnings (20 of 30):\n\n- warn finding 1\n"))
		Expect(md).To(ContainSubstring("- warn finding 20\n\n## 6."))
		Expect(md).NotTo(ContainSubstring("warn finding 21"))
		Expect(md).NotTo(ContainSubstring("### wind-range"))
		Expect(strings.Count(md, "\n- warn finding ")).To(Equal(20))
	})

	It("states the gate's scope right after its totals, and names what evaluates the rest", func() {
		md := renderQA(&publish.QAReport{Gate: publish.NewQAGate(fixture())}, publish.ProfileMeta{})
		Expect(md).To(ContainSubstring("Rules: 2. Error findings: 2. Warn findings: 30.\n\n" +
			gateScopeText + "| rule | name | scanned | warn | error |\n"))
		// The scope statement is true of the rule sets themselves: the
		// three named checks belong to the verb, and to no gate rule.
		gated := map[string]bool{}
		for _, r := range validate.GateRules() {
			gated[r.ID] = true
		}
		verb := map[string]bool{}
		for _, r := range validate.AllRules("") {
			verb[r.ID] = true
		}
		for _, id := range []string{"wmo-month-completeness", "daily-sanity", "cross-period"} {
			Expect(gated).NotTo(HaveKey(id))
			Expect(verb).To(HaveKey(id))
		}
	})

	It("says so when the report carries no gate", func() {
		md := renderQA(&publish.QAReport{}, publish.ProfileMeta{})
		Expect(md).To(ContainSubstring("## 5. Gate results\n\n"))
		Expect(md).To(ContainSubstring("The publish gate, evaluated read-only before any archive is\nwritten: "))
		Expect(md).To(ContainSubstring("The gate was not evaluated for this report.\n"))
	})
})
