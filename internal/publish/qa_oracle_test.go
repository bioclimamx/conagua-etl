package publish_test

// QA-REPORT.md held to plain SQL, section by section, over a fixture
// built for the purpose: two states; stations of every status shape,
// two of them with no daily rows; ingest runs whose stored counters
// differ from the live counts (and an aborted one that must not win);
// a power run with counters and one without; supplement rows some of
// which no run claims; row counts chosen so the null shares land on
// exact binary ties (6.25, 18.75), on repeating decimals (1/3, 1/7),
// and on the two ends (0, 100); and parsing_warnings of both severities
// from both producers plus one with no source. Every number in the
// struct and in the rendered Markdown is compared with a query of its
// own; the rounding is pinned as literal text; a second load renders
// the same bytes; and a schema-only database with no runs at all
// renders.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

const (
	oracleCellYUC = "21.5N_89.3750W"
	oracleCellAGS = "22.0N_102.5000W"
	oracleSnap    = "2026-06-08"
)

// oracleSeed is what seedQAOracle created.
type oracleSeed struct {
	stations   map[string]int64
	monthlyRun int64
	dailyRun   int64
}

// nullIf returns v when i < upTo and NULL otherwise — the per-column
// non-null pattern the null / gap fixture is built from.
func nullIf(i, upTo int, v any) any {
	if i < upTo {
		return v
	}
	return nil
}

// valueIn returns v when from <= i < to and NULL outside it — the
// pattern of a column whose first row is not the table's first row.
func valueIn(i, from, to int, v any) any {
	if i >= from && i < to {
		return v
	}
	return nil
}

func seedQAOracle(db *sql.DB) oracleSeed {
	GinkgoHelper()
	conv := ingest.SourceConaguaConventional
	s := oracleSeed{stations: map[string]int64{}}
	s.stations["31001"] = upsertStation(db, ingest.StationUpsert{
		Source: conv, ExternalID: "31001", Name: "Mérida", State: "YUC", Lat: f64(20.98), Lon: f64(-89.65), Status: "operating",
	})
	s.stations["31002"] = upsertStation(db, ingest.StationUpsert{
		Source: conv, ExternalID: "31002", Name: "Tizimín", State: "YUC", Status: "suspended",
	})
	s.stations["31003"] = upsertStation(db, ingest.StationUpsert{
		Source: conv, ExternalID: "31003", Name: "Progreso", State: "YUC", Lat: f64(21.28), Lon: f64(-89.66),
	})
	s.stations["1001"] = upsertStation(db, ingest.StationUpsert{
		Source: conv, ExternalID: "1001", Name: "Aguascalientes", State: "AGS", Lat: f64(21.88), Lon: f64(-102.3), Status: "operating",
	})
	s.stations["1002"] = upsertStation(db, ingest.StationUpsert{
		Source: conv, ExternalID: "1002", Name: "Calvillo", State: "AGS", Lat: f64(21.85), Lon: f64(-102.72),
	})

	insertIngestRun(db, ingestRunSeed{
		startedAt: "2026-05-01T10:00:00Z", finishedAt: str("2026-05-01T12:00:00Z"),
		snapshotDate: "2026-05-01", sinkKind: "r2", gitSHA: str("0ld5ha"), status: "complete",
		counters: []*int64{i64(4000), i64(3990), i64(10), i64(60000000), i64(190000), i64(190000), i64(30)},
	})
	insertIngestRun(db, ingestRunSeed{
		startedAt: "2026-06-10T01:00:00Z", finishedAt: str("2026-06-10T02:30:00Z"),
		snapshotDate: oracleSnap, sinkKind: "local", gitSHA: nil, status: "complete",
		counters: []*int64{i64(5524), i64(5523), i64(1), i64(71399000), i64(250000), i64(249988), nil},
	})
	insertIngestRun(db, ingestRunSeed{
		startedAt: "2026-06-11T01:00:00Z", finishedAt: str("2026-06-11T01:05:00Z"),
		snapshotDate: oracleSnap, sinkKind: "local", gitSHA: str("ab0rt"), status: "aborted",
		counters: []*int64{i64(100), i64(90), i64(10), i64(1), i64(1), i64(1), i64(1)},
	})

	s.monthlyRun = insertPowerRun(db, powerRunSeed{
		startedAt: "2026-07-01T00:00:00Z", finishedAt: str("2026-07-01T06:00:00Z"), status: "complete",
		endpoint: "https://power.larc.nasa.gov/api/temporal/monthly/point", parameters: "T2M,RH2M,WD10M",
		community: "AG", startYear: 1991, endYear: 2020, gridResolution: "0.5x0.625",
		unitConversions: str(conversionsText), temporalMode: "monthly", gitSHA: str("p0w3r"),
		counters: []*int64{i64(2), i64(2), i64(0), i64(1000)},
	})
	s.dailyRun = insertPowerRun(db, powerRunSeed{
		startedAt: "2026-07-02T00:00:00Z", status: "complete",
		endpoint: "https://power.larc.nasa.gov/api/temporal/daily/point", parameters: "T2M,WS2M,WD2M,GWETPROF",
		community: "AG", startYear: 2020, endYear: 2020, gridResolution: "0.5x0.625",
		temporalMode: "daily", startDate: str("2020-01-01"), endDate: str("2020-01-05"),
		counters: []*int64{i64(2), nil, nil, nil},
	})
	insertPowerRun(db, powerRunSeed{
		startedAt: "2026-07-03T00:00:00Z", finishedAt: str("2026-07-03T00:01:00Z"), status: "aborted",
		endpoint: "https://power.larc.nasa.gov/api/temporal/monthly/point", parameters: "T2M",
		community: "AG", startYear: 1961, endYear: 1990, gridResolution: "0.5x0.625",
		temporalMode: "monthly", counters: []*int64{i64(0), i64(0), i64(0), i64(0)},
	})

	insertCell(db, oracleCellYUC, 21.5, -89.375)
	insertCell(db, oracleCellAGS, 22.0, -102.5)
	insertStationCell(db, s.stations["31001"], oracleCellYUC, 3.2)
	insertStationCell(db, s.stations["1001"], oracleCellAGS, 11.9)

	// daily_observations: 16 rows over three stations — tmax in 1 row
	// (6.25 %), tmin in 3 (18.75 %), precip in 2 (12.5 %), evap in all.
	dailyKeys := make([]struct {
		station string
		date    string
	}, 0, 16)
	for d := 1; d <= 10; d++ {
		dailyKeys = append(dailyKeys, struct{ station, date string }{"31001", fmt.Sprintf("2020-01-%02d", d)})
	}
	for d := 11; d <= 14; d++ {
		dailyKeys = append(dailyKeys, struct{ station, date string }{"31003", fmt.Sprintf("2020-01-%02d", d)})
	}
	dailyKeys = append(dailyKeys,
		struct{ station, date string }{"1001", "2019-12-31"},
		struct{ station, date string }{"1001", "2020-01-15"})
	for i, k := range dailyKeys {
		insertDaily(db, s.stations[k.station], k.date, nullIf(i, 1, 30.5), nullIf(i, 3, 18.0), nullIf(i, 2, 1.5), 4.2)
	}

	// monthly_normals: 3 rows — tmax 1/3, tmin 2/3, tmean 0/3, precip
	// 3/3, evap 1/3.
	insertNormals(db, s.stations["31001"], "1991-2020", 1, 33.0, 18.3, nil, 28.3, 150.25)
	insertNormals(db, s.stations["31001"], "1991-2020", 2, nil, 18.0, nil, 20.0, nil)
	insertNormals(db, s.stations["1001"], "1981-2010", 1, nil, nil, nil, 5.0, nil)

	// monthly_normals_extras: 7 rows — tmax_monthly_extreme 1/7,
	// tmax_years_with_data 2/7, rain_days 6/7, rain_days_years_with_data
	// 7/7, every other column NULL.
	extrasKeys := []struct {
		station, period string
		month           int
	}{
		{"31001", "1991-2020", 1}, {"31001", "1991-2020", 2}, {"31001", "1991-2020", 3},
		{"31001", "1991-2020", 4}, {"31001", "1991-2020", 5},
		{"1001", "1981-2010", 1}, {"1001", "1981-2010", 2},
	}
	for i, k := range extrasKeys {
		values := make([]any, 19)
		values[0] = nullIf(i, 1, 38.5)
		values[12] = nullIf(i, 2, 28)
		values[17] = nullIf(i, 6, 4.6)
		values[18] = 29
		insertExtras(db, s.stations[k.station], k.period, k.month, values...)
	}

	// monthly_supplement: 6 rows — t2m_c 1/6, rh2m_pct 5/6, wd10m_deg
	// 3/6; the last row claimed by no run.
	monthlyKeys := []struct {
		cell, period string
		month        int
	}{
		{oracleCellYUC, "1991-2020", 1}, {oracleCellYUC, "1991-2020", 2}, {oracleCellYUC, "1991-2020", 3},
		{oracleCellYUC, "1991-2020", 4}, {oracleCellAGS, "1981-2010", 1}, {oracleCellAGS, "1981-2010", 2},
	}
	for i, k := range monthlyKeys {
		mustExec(db, `INSERT INTO monthly_supplement (cell_id, period, month, t2m_c, rh2m_pct, wd10m_deg, power_run_id)
		  VALUES (?, ?, ?, ?, ?, ?, ?)`,
			k.cell, k.period, k.month, nullIf(i, 1, 26.1), nullIf(i, 5, 71.5), nullIf(i, 3, 180.0), nullIf(i, 5, s.monthlyRun))
	}

	// daily_supplement: 8 rows — t2m_c 1/8, ws2m_ms 7/8, wd2m_deg 3/8,
	// gwet_prof 5/8, and solar_ghi_wm2 2/8 on the fourth and fifth dates
	// alone (the production shape: the CERES-based streams begin years
	// after the MERRA-2 ones, so a column's first date is not the
	// table's); the last two rows claimed by no run.
	dailySuppKeys := []struct{ cell, date string }{
		{oracleCellYUC, "2020-01-01"}, {oracleCellYUC, "2020-01-02"}, {oracleCellYUC, "2020-01-03"},
		{oracleCellYUC, "2020-01-04"}, {oracleCellYUC, "2020-01-05"},
		{oracleCellAGS, "2020-01-01"}, {oracleCellAGS, "2020-01-02"}, {oracleCellAGS, "2020-01-03"},
	}
	for i, k := range dailySuppKeys {
		mustExec(db, `INSERT INTO daily_supplement
		  (cell_id, date, t2m_c, ws2m_ms, wd2m_deg, gwet_prof, solar_ghi_wm2, power_run_id)
		  VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			k.cell, k.date, nullIf(i, 1, 22.0), nullIf(i, 7, 3.1), nullIf(i, 3, 90.0), nullIf(i, 5, 0.42),
			valueIn(i, 3, 5, 250.5), nullIf(i, 6, s.dailyRun))
	}

	warn := func(stationID any, sourceFile any, severity, issue string) {
		mustExec(db, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue) VALUES (?, ?, NULL, ?, ?)`,
			stationID, sourceFile, severity, issue)
	}
	daily := "conagua-raw/2026-06-08/daily/31001.txt"
	normals := "conagua-raw/2026-06-08/normals_1991_2020/31001.txt"
	warn(s.stations["31001"], daily, "warn", "line 12: unparseable tmax")
	warn(s.stations["31001"], daily, "warn", "line 40: duplicate date")
	warn(s.stations["31001"], normals, "error", "header mismatch")
	warn(s.stations["31001"], normals, "warn", "missing evap section")
	warn(nil, "validate:bbox", "error", "station conagua_conventional/9 at impossible lat=0.0000 lon=0.0000")
	warn(nil, "validate:daily-sanity", "warn", "tmax < tmin on 3 days")
	warn(nil, "validate:daily-sanity", "warn", "precip > 500 on 1 day")
	warn(nil, nil, "warn", "orphan warning with no source")
	return s
}

// row renders cells as one of the report's Markdown table rows.
func row(cells ...string) string {
	return "| " + strings.Join(cells, " | ") + " |\n"
}

// table renders a whole Markdown table as the report writes it.
func table(header []string, rows ...[]string) string {
	var b strings.Builder
	b.WriteString(row(header...))
	b.WriteString("|" + strings.Repeat("---|", len(header)) + "\n")
	for _, r := range rows {
		b.WriteString(row(r...))
	}
	return b.String()
}

func itoa64(n int64) string { return strconv.FormatInt(n, 10) }

// pctText is the one-decimal share the report prints for nonNull of
// rows, from the standard library's formatter.
func pctText(nonNull, rows int64) string {
	return strconv.FormatFloat(float64(nonNull)*100/float64(rows), 'f', 1, 64)
}

// nullableSQL runs a single-value query whose result may be NULL and
// renders it as the report does: the digits, or "null".
func nullableSQL(db *sql.DB, query string, args ...any) string {
	GinkgoHelper()
	var v sql.NullInt64
	Expect(db.QueryRowContext(context.Background(), query, args...).Scan(&v)).To(Succeed(), query)
	if !v.Valid {
		return "null"
	}
	return itoa64(v.Int64)
}

// stringSQL runs a single-value TEXT query; NULL renders as "null".
// anyValueWhere is the plain-SQL predicate "the row carries at least one
// of columns", the oracle for the value-bearing date bounds.
func anyValueWhere(columns ...string) string {
	return " WHERE " + strings.Join(columns, " IS NOT NULL OR ") + " IS NOT NULL"
}

func stringSQL(db *sql.DB, query string, args ...any) string {
	GinkgoHelper()
	var v sql.NullString
	Expect(db.QueryRowContext(context.Background(), query, args...).Scan(&v)).To(Succeed(), query)
	if !v.Valid {
		return "null"
	}
	return v.String
}

// The eleven tables in the DDL's order, as the report lists them.
var qaTables = []string{
	"stations", "monthly_normals", "monthly_normals_extras", "daily_observations", "parsing_warnings",
	"ingest_runs", "power_runs", "nasa_power_grid_cells", "station_power_cell", "monthly_supplement", "daily_supplement",
}

// latestCompleteSQL selects one column of the latest complete ingest
// run for a snapshot date — the oracle for the counters section's
// stored values.
func latestCompleteSQL(column string) string {
	return "SELECT " + column + ` FROM ingest_runs WHERE snapshot_date = ? AND status = 'complete'
	 ORDER BY started_at DESC, id DESC LIMIT 1`
}

var _ = Describe("QA report oracle", func() {
	var (
		db     *sql.DB
		seed   oracleSeed
		q      *publish.QAReport
		md     string
		gate   *validate.GateReport
		runs   publish.Runs
		counts publish.TableCounts
	)

	BeforeEach(func() {
		db = openTempDB()
		seed = seedQAOracle(db)
		var states []publish.State
		runs, counts, states = loadQAInputs(db)
		var err error
		gate, err = validate.Gate(context.Background(), db, nil)
		Expect(err).NotTo(HaveOccurred())
		q, err = publish.LoadQA(context.Background(), db, oracleSnap, runs, counts, gate, states)
		Expect(err).NotTo(HaveOccurred())
		md = renderQA(q, qaMeta("abc123", runs))
	})

	It("section 1: the snapshot, the latest complete ingest run per snapshot date, and the referenced power runs by label", func() {
		Expect(md).To(ContainSubstring("## 1. Snapshot and runs\n\nSnapshot date: `" + oracleSnap + "`.\n"))
		var ingestRows [][]string
		for _, snap := range []string{"2026-05-01", oracleSnap} {
			ingestRows = append(ingestRows, []string{
				snap, "complete", stringSQL(db, latestCompleteSQL("sink_kind"), snap), stringSQL(db, latestCompleteSQL("etl_git_sha"), snap),
			})
		}
		Expect(ingestRows).To(Equal([][]string{{"2026-05-01", "complete", "r2", "0ld5ha"}, {oracleSnap, "complete", "local", "null"}}))
		Expect(md).To(ContainSubstring("### Ingest runs\n\n" +
			"The latest complete ingest run per snapshot date. Run timestamps are\n" +
			"provenance, not QA, and ride in manifest.json and provenance/.\n\n" +
			table([]string{"snapshot_date", "status", "sink_kind", "etl_git_sha"}, ingestRows...) + "\n### Power runs\n"))
		Expect(countSQL(db, `SELECT COUNT(DISTINCT snapshot_date) FROM ingest_runs WHERE status = 'complete'`)).To(Equal(int64(2)))
		Expect(q.Runs.Ingest).To(HaveLen(2))
		// The aborted run for the snapshot must not surface anywhere.
		Expect(md).NotTo(ContainSubstring("ab0rt"))
		Expect(md).NotTo(ContainSubstring("aborted"))

		referenced := countSQL(db, `SELECT COUNT(*) FROM power_runs WHERE id IN (
		  SELECT power_run_id FROM monthly_supplement UNION SELECT power_run_id FROM daily_supplement)`)
		Expect(referenced).To(Equal(int64(2)))
		Expect(q.Runs.Power).To(HaveLen(2))
		Expect(md).To(ContainSubstring("Every power run at least one supplement row references, by run label.\n\n" +
			table([]string{"run_label", "temporal_mode", "span", "status", "etl_git_sha"},
				[]string{"power-daily-2020-2020", "daily",
					stringSQL(db, `SELECT period_start_date FROM power_runs WHERE id = ?`, seed.dailyRun) + " to " +
						stringSQL(db, `SELECT period_end_date FROM power_runs WHERE id = ?`, seed.dailyRun),
					"complete", stringSQL(db, `SELECT etl_git_sha FROM power_runs WHERE id = ?`, seed.dailyRun)},
				[]string{"power-monthly-1991-2020", "monthly", "1991 to 2020", "complete",
					stringSQL(db, `SELECT etl_git_sha FROM power_runs WHERE id = ?`, seed.monthlyRun)},
			) + "\n## 2. Per-table row counts\n"))
		Expect(md).To(ContainSubstring(row("power-daily-2020-2020", "daily", "2020-01-01 to 2020-01-05", "complete", "null")))
		Expect(md).To(ContainSubstring(row("power-monthly-1991-2020", "monthly", "1991 to 2020", "complete", "p0w3r")))
	})

	It("section 2: COUNT(*) of all eleven tables, in DDL order", func() {
		rows := make([][]string, 0, len(qaTables))
		for _, t := range qaTables {
			rows = append(rows, []string{t, itoa64(countSQL(db, "SELECT COUNT(*) FROM "+t))})
		}
		Expect(md).To(ContainSubstring("## 2. Per-table row counts\n\n" + table([]string{"table", "rows"}, rows...) + "\n## 3. Coverage\n"))
		// Anchors written out from the seed.
		Expect(rows).To(ContainElements(
			[]string{"stations", "5"}, []string{"daily_observations", "16"}, []string{"monthly_normals", "3"},
			[]string{"monthly_normals_extras", "7"}, []string{"monthly_supplement", "6"}, []string{"daily_supplement", "8"},
			[]string{"parsing_warnings", "8"}, []string{"ingest_runs", "3"}, []string{"power_runs", "3"},
		))
		Expect(q.Counts).To(Equal(counts))
	})

	It("section 3: stations by status and by state, the daily extent, normals and POWER per period", func() {
		status := func(where string) string { return itoa64(countSQL(db, "SELECT COUNT(*) FROM stations WHERE "+where)) }
		observedValues := anyValueWhere("tmax", "tmin", "precip", "evap")
		powerValues := anyValueWhere(powerValueColumns...)
		Expect(md).To(ContainSubstring("### Stations by status\n\n" + table([]string{"status", "stations"},
			[]string{"operating", status("status = 'operating'")},
			[]string{"suspended", status("status = 'suspended'")},
			[]string{"null", status("status IS NULL")},
		) + "\n### Stations by state\n"))
		Expect(status("status IS NULL")).To(Equal("2"))
		Expect(md).To(ContainSubstring("### Stations by state\n\n" + table([]string{"code", "state", "stations"},
			[]string{"AGS", conagua.StateCode("ags").DisplayName(), status("state = 'AGS'")},
			[]string{"YUC", conagua.StateCode("yuc").DisplayName(), status("state = 'YUC'")},
		) + "\n### Observed daily series (daily_observations)\n"))
		Expect(md).To(ContainSubstring(row("YUC", "Yucatán", "3")))

		Expect(md).To(ContainSubstring("### Observed daily series (daily_observations)\n\n" +
			"- Rows: " + itoa64(countSQL(db, `SELECT COUNT(*) FROM daily_observations`)) + "\n" +
			"- Stations with rows: " + itoa64(countSQL(db, `SELECT COUNT(DISTINCT station_id) FROM daily_observations`)) + "\n" +
			"- First date: " + stringSQL(db, `SELECT MIN(date) FROM daily_observations`) + "\n" +
			"- Last date: " + stringSQL(db, `SELECT MAX(date) FROM daily_observations`) + "\n" +
			"- First date with a value: " + stringSQL(db, `SELECT MIN(date) FROM daily_observations`+observedValues) + "\n" +
			"- Last date with a value: " + stringSQL(db, `SELECT MAX(date) FROM daily_observations`+observedValues) + "\n\n" +
			"### Normals by reference period\n"))
		Expect(md).To(ContainSubstring("- Rows: 16\n- Stations with rows: 3\n- First date: 2019-12-31\n- Last date: 2020-01-15\n"))
		// Two stations hold no daily row and are not "stations with rows".
		Expect(countSQL(db, `SELECT COUNT(*) FROM stations WHERE id NOT IN (SELECT station_id FROM daily_observations)`)).To(Equal(int64(2)))

		var normals [][]string
		for _, p := range []string{"1961-1990", "1971-2000", "1981-2010", "1991-2020"} {
			normals = append(normals, []string{p,
				itoa64(countSQL(db, `SELECT COUNT(DISTINCT station_id) FROM monthly_normals WHERE period = ?`, p)),
				itoa64(countSQL(db, `SELECT COUNT(*) FROM monthly_normals WHERE period = ?`, p)),
				itoa64(countSQL(db, `SELECT COUNT(DISTINCT station_id) FROM monthly_normals_extras WHERE period = ?`, p)),
				itoa64(countSQL(db, `SELECT COUNT(*) FROM monthly_normals_extras WHERE period = ?`, p)),
			})
		}
		Expect(md).To(ContainSubstring("### Normals by reference period\n\n" + table([]string{
			"period", "monthly_normals stations", "monthly_normals rows", "monthly_normals_extras stations", "monthly_normals_extras rows",
		}, normals...) + "\n### NASA POWER\n"))
		Expect(normals[2]).To(Equal([]string{"1981-2010", "1", "1", "1", "2"}))
		Expect(normals[3]).To(Equal([]string{"1991-2020", "1", "2", "1", "5"}))

		var monthly [][]string
		for _, p := range []string{"1961-1990", "1971-2000", "1981-2010", "1991-2020"} {
			monthly = append(monthly, []string{p,
				itoa64(countSQL(db, `SELECT COUNT(DISTINCT cell_id) FROM monthly_supplement WHERE period = ?`, p)),
				itoa64(countSQL(db, `SELECT COUNT(*) FROM monthly_supplement WHERE period = ?`, p)),
			})
		}
		Expect(md).To(ContainSubstring("### NASA POWER\n\n" +
			"- Grid cells registered: " + itoa64(countSQL(db, `SELECT COUNT(*) FROM nasa_power_grid_cells`)) + "\n" +
			"- Stations linked to a cell: " + itoa64(countSQL(db, `SELECT COUNT(*) FROM station_power_cell`)) + "\n\n" +
			"Daily reanalysis series (daily_supplement):\n\n" +
			"- Rows: " + itoa64(countSQL(db, `SELECT COUNT(*) FROM daily_supplement`)) + "\n" +
			"- Cells with rows: " + itoa64(countSQL(db, `SELECT COUNT(DISTINCT cell_id) FROM daily_supplement`)) + "\n" +
			"- First date: " + stringSQL(db, `SELECT MIN(date) FROM daily_supplement`) + "\n" +
			"- Last date: " + stringSQL(db, `SELECT MAX(date) FROM daily_supplement`) + "\n" +
			"- First date with a value: " + stringSQL(db, `SELECT MIN(date) FROM daily_supplement`+powerValues) + "\n" +
			"- Last date with a value: " + stringSQL(db, `SELECT MAX(date) FROM daily_supplement`+powerValues) + "\n\n" +
			"Monthly climatology (monthly_supplement) by reference period:\n\n" +
			table([]string{"period", "cells", "rows"}, monthly...) + "\n## 4. Null / gap summary\n"))
		Expect(md).To(ContainSubstring("- Grid cells registered: 2\n- Stations linked to a cell: 2\n"))
		Expect(md).To(ContainSubstring("- Rows: 8\n- Cells with rows: 2\n- First date: 2020-01-01\n- Last date: 2020-01-05\n"))
		Expect(monthly).To(Equal([][]string{{"1961-1990", "0", "0"}, {"1971-2000", "0", "0"}, {"1981-2010", "1", "2"}, {"1991-2020", "1", "4"}}))
	})

	It("the POWER daily series per column: each column's own MIN/MAX date, a later-starting stream not read as the table's", func() {
		Expect(q.Coverage.Power.DailyColumnExtents).To(Equal(powerColumnExtentsSQL(db)))
		byColumn := map[string]publish.QAColumnExtent{}
		for _, e := range q.Coverage.Power.DailyColumnExtents {
			byColumn[e.Column] = e
		}
		// The defect this exists for: the table begins on 2020-01-01 and
		// solar_ghi_wm2 three days later, which the whole-table extent
		// cannot say. Both bounds are read from SQL as well as written out.
		Expect(stringSQL(db, `SELECT MIN(date) FROM daily_supplement`)).To(Equal("2020-01-01"))
		Expect(stringSQL(db, `SELECT MIN(date) FROM daily_supplement WHERE solar_ghi_wm2 IS NOT NULL`)).To(Equal("2020-01-04"))
		Expect(byColumn["solar_ghi_wm2"]).To(Equal(publish.QAColumnExtent{
			Column: "solar_ghi_wm2", NonNull: 2, FirstDate: str("2020-01-04"), LastDate: str("2020-01-05")}))
		// A column that ends early, one that spans the table, and one no
		// row carries at all.
		Expect(byColumn["t2m_c"]).To(Equal(publish.QAColumnExtent{
			Column: "t2m_c", NonNull: 1, FirstDate: str("2020-01-01"), LastDate: str("2020-01-01")}))
		Expect(byColumn["wd2m_deg"]).To(Equal(publish.QAColumnExtent{
			Column: "wd2m_deg", NonNull: 3, FirstDate: str("2020-01-01"), LastDate: str("2020-01-03")}))
		Expect(byColumn["ws2m_ms"]).To(Equal(publish.QAColumnExtent{
			Column: "ws2m_ms", NonNull: 7, FirstDate: str("2020-01-01"), LastDate: str("2020-01-05")}))
		Expect(byColumn["uva_wm2"]).To(Equal(publish.QAColumnExtent{Column: "uva_wm2"}))
		// The whole-table extent the report prints is unchanged by any of
		// it; its value bounds are the union of the columns' own.
		Expect(q.Coverage.Power.Daily).To(Equal(publish.QASeriesExtent{
			Rows: 8, Keys: 2, FirstDate: str("2020-01-01"), LastDate: str("2020-01-05"),
			ValueFirstDate: str(stringSQL(db, `SELECT MIN(date) FROM daily_supplement`+anyValueWhere(powerValueColumns...))),
			ValueLastDate:  str(stringSQL(db, `SELECT MAX(date) FROM daily_supplement`+anyValueWhere(powerValueColumns...))),
		}))
	})

	It("section 4: COUNT(col) and its one-decimal share for every value column of the five tables, the rounding pinned", func() {
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
		Expect(q.Nulls).To(HaveLen(len(want)))
		for i, w := range want {
			rows := countSQL(db, "SELECT COUNT(*) FROM "+w.table)
			t := q.Nulls[i]
			Expect(t.Table).To(Equal(w.table))
			Expect(t.Rows).To(Equal(rows))
			var mdRows [][]string
			for j, col := range w.columns {
				nonNull := countSQL(db, "SELECT COUNT("+col+") FROM "+w.table)
				Expect(t.Columns[j]).To(Equal(publish.QANullColumn{Column: col, NonNull: nonNull, Percent: pct(nonNull, rows)}), w.table+"."+col)
				mdRows = append(mdRows, []string{col, itoa64(nonNull), pctText(nonNull, rows)})
			}
			Expect(md).To(ContainSubstring(fmt.Sprintf("### %s (%d rows)\n\n", w.table, rows) +
				table([]string{"column", "non-null rows", "%"}, mdRows...) + "\n"))
		}
		// The rounding, as literal text: exact binary ties round half to
		// even (6.25 → 6.2, 18.75 → 18.8), repeating decimals to the
		// nearest tenth, and the ends print with their decimal.
		Expect(md).To(ContainSubstring("### daily_observations (16 rows)\n\n| column | non-null rows | % |\n|---|---|---|\n" +
			row("tmax", "1", "6.2") + row("tmin", "3", "18.8") + row("precip", "2", "12.5") + row("evap", "16", "100.0") + "\n"))
		Expect(md).To(ContainSubstring("### monthly_normals (3 rows)\n\n| column | non-null rows | % |\n|---|---|---|\n" +
			row("tmax", "1", "33.3") + row("tmin", "2", "66.7") + row("tmean", "0", "0.0") + row("precip", "3", "100.0") + row("evap", "1", "33.3") + "\n"))
		Expect(md).To(ContainSubstring(row("tmax_monthly_extreme", "1", "14.3")))
		Expect(md).To(ContainSubstring(row("tmax_years_with_data", "2", "28.6")))
		Expect(md).To(ContainSubstring(row("rain_days", "6", "85.7")))
		Expect(md).To(ContainSubstring(row("rain_days_years_with_data", "7", "100.0")))
		Expect(md).To(ContainSubstring(row("t2m_c", "1", "16.7")))
		Expect(md).To(ContainSubstring(row("rh2m_pct", "5", "83.3")))
		Expect(md).To(ContainSubstring(row("wd10m_deg", "3", "50.0")))
		Expect(md).To(ContainSubstring(row("t2m_c", "1", "12.5")))
		Expect(md).To(ContainSubstring(row("ws2m_ms", "7", "87.5")))
		Expect(md).To(ContainSubstring(row("wd2m_deg", "3", "37.5")))
		Expect(md).To(ContainSubstring(row("gwet_prof", "5", "62.5")))
		// 31 value rows per supplement table, none missing, none extra.
		suppSection := md[strings.Index(md, "### monthly_supplement ("):strings.Index(md, "## 5.")]
		Expect(strings.Count(suppSection, "\n| ")).To(Equal(2 * (31 + 1)))
	})

	It("section 5: the gate's own per-rule counts and every error finding, agreeing with an independent evaluation", func() {
		again, err := validate.Gate(context.Background(), db, nil)
		Expect(err).NotTo(HaveOccurred())
		var rows [][]string
		for _, rr := range again.Rules {
			rows = append(rows, []string{rr.ID, rr.Name, strconv.Itoa(rr.Scanned), strconv.Itoa(rr.Warnings), strconv.Itoa(rr.Errors)})
		}
		Expect(md).To(ContainSubstring(fmt.Sprintf("Rules: %d. Error findings: %d. Warn findings: %d.\n\n",
			len(again.Rules), again.Errors, again.Warnings) + gateScopeText +
			table([]string{"rule", "name", "scanned", "warn", "error"}, rows...)))
		// The scans, written out from the seed: 5 stations; 3 ledger
		// rows; 16 + 3 + 7 + 2 + 4 station references; 6 + 8 + 2 cell
		// references; 14 supplement rows for the run, fill, and wind
		// anchors; 2 referenced runs.
		Expect(rows).To(Equal([][]string{
			{"bbox", "Lat/lon plausibility", "5", "1", "0"},
			{"runs-in-flight", "Runs in flight / stranded", "0", "0", "0"},
			{"ingest-complete", "Complete ingest run", "3", "0", "0"},
			{"station-refs", "Station references", "32", "0", "0"},
			{"cell-refs", "POWER cell references", "16", "0", "0"},
			{"run-refs", "POWER run references", "14", "0", "2"},
			{"run-label-unique", "POWER run_label uniqueness", "2", "0", "0"},
			{"fill-leak", "POWER fill-value leak", "14", "0", "0"},
			{"wind-range", "Wind direction range", "14", "0", "0"},
		}))
		Expect(md).To(ContainSubstring("### bbox\n\nWarnings (1 of 1):\n\n" +
			`- station conagua_conventional/31002 ("Tizimín") has NULL lat and lon` + "\n\n"))
		Expect(md).To(ContainSubstring("### run-refs\n\nErrors (2):\n\n" +
			"- monthly_supplement: 1 row with a NULL power_run_id (" + oracleCellAGS + "/1981-2010/2)\n" +
			"- daily_supplement: 2 rows with a NULL power_run_id (" + oracleCellAGS + "/2020-01-02, " + oracleCellAGS + "/2020-01-03)\n\n" +
			"## 6. Run counters vs live counts\n"))
		Expect(countSQL(db, `SELECT COUNT(*) FROM monthly_supplement WHERE power_run_id IS NULL`)).To(Equal(int64(1)))
		Expect(countSQL(db, `SELECT COUNT(*) FROM daily_supplement WHERE power_run_id IS NULL`)).To(Equal(int64(2)))

		// The typed section carries the same evaluation, field for field.
		Expect(q.Gate.Warnings).To(Equal(int64(again.Warnings)))
		Expect(q.Gate.Errors).To(Equal(int64(again.Errors)))
		Expect(q.Gate.Rules).To(HaveLen(len(again.Rules)))
		for i, rr := range again.Rules {
			got := q.Gate.Rules[i]
			Expect(got.ID).To(Equal(rr.ID))
			Expect(got.Name).To(Equal(rr.Name))
			Expect(got.Scanned).To(Equal(int64(rr.Scanned)))
			Expect(got.Warnings).To(Equal(int64(rr.Warnings)))
			Expect(got.Errors).To(Equal(int64(rr.Errors)))
			var errIssues, warnIssues []string
			for _, f := range rr.Findings {
				if f.Severity == validate.SeverityError {
					errIssues = append(errIssues, f.Issue)
				} else {
					warnIssues = append(warnIssues, f.Issue)
				}
			}
			Expect(got.ErrorIssues).To(Equal(append([]string{}, errIssues...)), rr.ID)
			Expect(got.WarnSamples).To(Equal(append([]string{}, warnIssues...)), rr.ID)
		}
	})

	It("section 6: each run's stored counters beside the live counts, and the supplement rows no run claims", func() {
		live := func(t string) string { return itoa64(countSQL(db, "SELECT COUNT(*) FROM "+t)) }
		var ingest strings.Builder
		ingest.WriteString("### Ingest runs\n\n")
		for _, snap := range []string{"2026-05-01", oracleSnap} {
			stored := func(col string) string { return nullableSQL(db, latestCompleteSQL(col), snap) }
			ingest.WriteString("Run for snapshot `" + snap + "`:\n\n" + table([]string{"counter", "stored", "live table", "live rows"},
				[]string{"stations_attempted", stored("stations_attempted"), "stations", live("stations")},
				[]string{"stations_succeeded", stored("stations_succeeded"), "stations", live("stations")},
				[]string{"stations_failed", stored("stations_failed"), "", ""},
				[]string{"daily_rows", stored("daily_rows"), "daily_observations", live("daily_observations")},
				[]string{"normals_rows", stored("normals_rows"), "monthly_normals", live("monthly_normals")},
				[]string{"extras_rows", stored("extras_rows"), "monthly_normals_extras", live("monthly_normals_extras")},
				[]string{"warnings_total", stored("warnings_total"), "parsing_warnings", live("parsing_warnings")},
			) + "\n")
		}
		ingest.WriteString("### Power runs\n")
		Expect(md).To(ContainSubstring(ingest.String()))
		// Stored and live differ, by design, and the difference is
		// reported rather than refused.
		Expect(md).To(ContainSubstring(row("stations_attempted", "5524", "stations", "5")))
		Expect(md).To(ContainSubstring(row("daily_rows", "71399000", "daily_observations", "16")))
		Expect(md).To(ContainSubstring(row("warnings_total", "null", "parsing_warnings", "8")))
		Expect(md).To(ContainSubstring(row("warnings_total", "30", "parsing_warnings", "8")))

		perRun := func(t string, id int64) string {
			return itoa64(countSQL(db, "SELECT COUNT(*) FROM "+t+" WHERE power_run_id = ?", id))
		}
		Expect(md).To(ContainSubstring("Stored supplement_rows beside the rows referencing the run in each table.\n\n" +
			table([]string{"run_label", "supplement_rows (stored)", "monthly_supplement rows", "daily_supplement rows"},
				[]string{"power-daily-2020-2020", nullableSQL(db, `SELECT supplement_rows FROM power_runs WHERE id = ?`, seed.dailyRun),
					perRun("monthly_supplement", seed.dailyRun), perRun("daily_supplement", seed.dailyRun)},
				[]string{"power-monthly-1991-2020", nullableSQL(db, `SELECT supplement_rows FROM power_runs WHERE id = ?`, seed.monthlyRun),
					perRun("monthly_supplement", seed.monthlyRun), perRun("daily_supplement", seed.monthlyRun)},
			) + "\n" +
			"- monthly_supplement rows with no run (power_run_id NULL): " +
			itoa64(countSQL(db, `SELECT COUNT(*) FROM monthly_supplement WHERE power_run_id IS NULL`)) + "\n" +
			"- daily_supplement rows with no run (power_run_id NULL): " +
			itoa64(countSQL(db, `SELECT COUNT(*) FROM daily_supplement WHERE power_run_id IS NULL`)) + "\n\n## 7. Parsing warnings\n"))
		Expect(md).To(ContainSubstring(row("power-daily-2020-2020", "null", "0", "6")))
		Expect(md).To(ContainSubstring(row("power-monthly-1991-2020", "1000", "5", "0")))
		Expect(q.Counters).To(Equal(publish.QACounters{
			Power: []publish.QAPowerLive{
				{RunLabel: "power-daily-2020-2020", SupplementRows: nil, MonthlyRows: 0, DailyRows: 6},
				{RunLabel: "power-monthly-1991-2020", SupplementRows: i64(1000), MonthlyRows: 5, DailyRows: 0},
			},
			MonthlyRowsWithoutRun: 1, DailyRowsWithoutRun: 2,
		}))
	})

	It("section 7: parsing_warnings by severity and by producer prefix, both kinds of each", func() {
		bySeverity := func(sev string) string {
			return itoa64(countSQL(db, `SELECT COUNT(*) FROM parsing_warnings WHERE severity = ?`, sev))
		}
		Expect(md).To(ContainSubstring(warningsScopeText(countSQL(db, `SELECT COUNT(*) FROM parsing_warnings`)) + "\n" +
			table([]string{"severity", "rows"}, []string{"error", bySeverity("error")}, []string{"warn", bySeverity("warn")}) + "\n"))
		Expect(bySeverity("error")).To(Equal("2"))
		Expect(bySeverity("warn")).To(Equal("6"))

		bySource := func(like string, sev string) string {
			if like == "" {
				return itoa64(countSQL(db, `SELECT COUNT(*) FROM parsing_warnings WHERE source_file IS NULL AND severity = ?`, sev))
			}
			return itoa64(countSQL(db, `SELECT COUNT(*) FROM parsing_warnings WHERE source_file LIKE ? AND severity = ?`, like, sev))
		}
		Expect(md).To(ContainSubstring(table([]string{"source", "warn", "error"},
			[]string{"conagua-raw/2026-06-08/daily", bySource("conagua-raw/2026-06-08/daily/%", "warn"), bySource("conagua-raw/2026-06-08/daily/%", "error")},
			[]string{"conagua-raw/2026-06-08/normals_1991_2020", bySource("conagua-raw/2026-06-08/normals_1991_2020/%", "warn"),
				bySource("conagua-raw/2026-06-08/normals_1991_2020/%", "error")},
			[]string{"validate", bySource("validate:%", "warn"), bySource("validate:%", "error")},
			[]string{"null", bySource("", "warn"), bySource("", "error")},
		)))
		Expect(md).To(HaveSuffix(row("validate", "2", "1") + row("null", "1", "0")))
		Expect(q.Warnings.Total).To(Equal(int64(8)))
	})

	It("renders the same bytes from a second load, and the struct marshals identically", func() {
		runs2, counts2, states2 := loadQAInputs(db)
		gate2, err := validate.Gate(context.Background(), db, nil)
		Expect(err).NotTo(HaveOccurred())
		q2, err := publish.LoadQA(context.Background(), db, oracleSnap, runs2, counts2, gate2, states2)
		Expect(err).NotTo(HaveOccurred())
		Expect(renderQA(q2, qaMeta("abc123", runs2))).To(Equal(md))
		// The two gate evaluations differ only in their timings, which the
		// report leaves behind at load: the structs marshal identically,
		// gate section included.
		Expect(q.Gate).NotTo(BeNil())
		a, err := json.Marshal(q)
		Expect(err).NotTo(HaveOccurred())
		b, err := json.Marshal(q2)
		Expect(err).NotTo(HaveOccurred())
		Expect(a).To(Equal(b))
		Expect(string(a)).To(ContainSubstring(`"gate":{"rules":[{"id":"bbox",`))
		Expect(timestampRe.FindString(md)).To(BeEmpty())
		Expect(md).To(HaveSuffix("|\n"))
		Expect(md).NotTo(HaveSuffix("\n\n"))
	})
})

var _ = Describe("QA report over a schema-only database with no runs", func() {
	It("renders every section with zeros, explicit nulls, n/a shares, and the gate's own refusal, byte-identically", func() {
		db := openTempDB()
		runs, counts, states := loadQAInputs(db)
		Expect(runs).To(Equal(publish.Runs{Ingest: []publish.IngestRunRef{}, Power: []publish.PowerRunRef{}}))
		Expect(counts).To(Equal(publish.TableCounts{}))
		Expect(states).To(BeEmpty())
		gate, err := validate.Gate(context.Background(), db, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(gate.Errors).To(Equal(1))

		q, err := publish.LoadQA(context.Background(), db, "", runs, counts, gate, states)
		Expect(err).NotTo(HaveOccurred())
		meta := publish.ProfileMeta{SchemaVersion: schema.Version, Runs: runs, Dataset: publish.DatasetMetadata(time.Time{}, "")}
		md := renderQA(q, meta)
		Expect(renderQA(q, meta)).To(Equal(md))

		Expect(md).To(HavePrefix("# QA report — " + publish.DatasetTitle + "\n\nVersion 0.1. Snapshot (none), schema version " +
			strconv.Itoa(schema.Version) + ", ETL git SHA (none).\n"))
		for _, h := range qaHeadings {
			Expect(md).To(ContainSubstring("\n" + h + "\n"))
		}
		Expect(md).To(ContainSubstring("Snapshot date: (none).\n"))
		Expect(md).To(ContainSubstring(table([]string{"snapshot_date", "status", "sink_kind", "etl_git_sha"}) + "\n### Power runs\n"))
		Expect(md).To(ContainSubstring(table([]string{"run_label", "temporal_mode", "span", "status", "etl_git_sha"}) + "\n## 2."))
		var zeros [][]string
		for _, t := range qaTables {
			zeros = append(zeros, []string{t, "0"})
		}
		Expect(md).To(ContainSubstring(table([]string{"table", "rows"}, zeros...)))
		Expect(md).To(ContainSubstring(table([]string{"status", "stations"}) + "\n### Stations by state\n\n" +
			table([]string{"code", "state", "stations"}) + "\n### Observed daily series (daily_observations)\n\n" +
			"- Rows: 0\n- Stations with rows: 0\n- First date: null\n- Last date: null\n"))
		Expect(md).To(ContainSubstring(row("1961-1990", "0", "0", "0", "0") + row("1971-2000", "0", "0", "0", "0") +
			row("1981-2010", "0", "0", "0", "0") + row("1991-2020", "0", "0", "0", "0")))
		Expect(md).To(ContainSubstring("- Grid cells registered: 0\n- Stations linked to a cell: 0\n"))
		Expect(md).To(ContainSubstring("- Rows: 0\n- Cells with rows: 0\n- First date: null\n- Last date: null\n"))
		Expect(strings.Count(md, " | 0 | n/a |\n")).To(Equal(4 + 5 + 19 + 31 + 31))
		for _, t := range []string{"daily_observations", "monthly_normals", "monthly_normals_extras", "monthly_supplement", "daily_supplement"} {
			Expect(md).To(ContainSubstring("### " + t + " (0 rows)\n"))
		}
		Expect(md).To(ContainSubstring("Rules: 9. Error findings: 1. Warn findings: 0.\n"))
		Expect(md).To(ContainSubstring(row("ingest-complete", "Complete ingest run", "0", "0", "1")))
		Expect(md).To(ContainSubstring("### ingest-complete\n\nErrors (1):\n\n" +
			"- ingest_runs has no complete row (0 rows in total): nothing to publish\n\n## 6."))
		Expect(md).NotTo(ContainSubstring("Run for snapshot"))
		Expect(md).To(ContainSubstring(table([]string{"run_label", "supplement_rows (stored)", "monthly_supplement rows", "daily_supplement rows"}) +
			"\n- monthly_supplement rows with no run (power_run_id NULL): 0\n- daily_supplement rows with no run (power_run_id NULL): 0\n"))
		Expect(md).To(ContainSubstring("The parsing_warnings rows the database holds (0)"))
		Expect(md).To(HaveSuffix(table([]string{"source", "warn", "error"})))
		Expect(timestampRe.FindString(md)).To(BeEmpty())

		var buf bytes.Buffer
		Expect(publish.RenderQA(&buf, q, meta)).To(Succeed())
		Expect(buf.String()).To(Equal(md))
	})
})
