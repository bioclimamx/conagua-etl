package validate_test

// Run's mutation semantics, each proven by reading the affected tables
// back whole: the clear touches exactly the 'validate:' namespace, the
// orphan-runs reconcile flips exactly the 'running' rows older than
// 24 h and nothing else on them, a repeated run converges to the same
// multiset even where a rule's emission order is map-driven, and a
// skipped rule — any of them, or all of them — leaves no trace.

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// fullWarningRow is one parsing_warnings row with its surrogate, so a
// survivor is proven untouched rather than merely present.
type fullWarningRow struct {
	ID int64
	warningRow
}

// readAllWarnings returns every parsing_warnings row in id order.
func readAllWarnings(db *sql.DB) []fullWarningRow {
	GinkgoHelper()
	rows, err := db.QueryContext(context.Background(), `
SELECT id, station_id, source_file, line, severity, issue FROM parsing_warnings ORDER BY id`)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	var out []fullWarningRow
	for rows.Next() {
		var r fullWarningRow
		Expect(rows.Scan(&r.ID, &r.StationID, &r.SourceFile, &r.Line, &r.Severity, &r.Issue)).To(Succeed())
		out = append(out, r)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

// insertRawWarning seeds one parsing_warnings row with every column as
// given (nil = NULL).
func insertRawWarning(db *sql.DB, stationID, sourceFile, line any, severity, issue string) {
	GinkgoHelper()
	mustExec(db, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue) VALUES (?, ?, ?, ?, ?)`,
		stationID, sourceFile, line, severity, issue)
}

// ledgerRow is one ingest_runs or power_runs row, every column, read
// through the table's own column list so no column is left out.
type ledgerRow struct {
	ID     int64
	Values []sql.NullString
}

func readLedger(db *sql.DB, table string) []ledgerRow {
	GinkgoHelper()
	rows, err := db.QueryContext(context.Background(), fmt.Sprintf(`SELECT * FROM %s ORDER BY id`, table))
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	Expect(err).NotTo(HaveOccurred())
	Expect(cols[0]).To(Equal("id"))
	var out []ledgerRow
	for rows.Next() {
		r := ledgerRow{Values: make([]sql.NullString, len(cols)-1)}
		dest := make([]any, len(cols))
		dest[0] = &r.ID
		for i := range r.Values {
			dest[i+1] = &r.Values[i]
		}
		Expect(rows.Scan(dest...)).To(Succeed())
		out = append(out, r)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

func ledgerColumns(db *sql.DB, table string) []string {
	GinkgoHelper()
	rows, err := db.QueryContext(context.Background(), fmt.Sprintf(`SELECT * FROM %s LIMIT 0`, table))
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	Expect(err).NotTo(HaveOccurred())
	return cols[1:]
}

// ago renders now − d as the ledgers' RFC 3339 UTC stamp.
func ago(d time.Duration) string {
	return time.Now().UTC().Add(-d).Format(time.RFC3339)
}

// sortedRows orders rows by (source_file, station, severity, issue) so
// two runs compare as multisets.
func sortedRows(rows []warningRow) []warningRow {
	out := slices.Clone(rows)
	slices.SortFunc(out, func(a, b warningRow) int {
		if c := strings.Compare(a.SourceFile.String, b.SourceFile.String); c != 0 {
			return c
		}
		if a.StationID.Int64 != b.StationID.Int64 {
			return int(a.StationID.Int64 - b.StationID.Int64)
		}
		if c := strings.Compare(a.Severity, b.Severity); c != 0 {
			return c
		}
		return strings.Compare(a.Issue, b.Issue)
	})
	return out
}

func runOK(db *sql.DB, opts validate.Options) *validate.Report {
	GinkgoHelper()
	report, err := validate.Run(context.Background(), db, opts)
	Expect(err).NotTo(HaveOccurred())
	return report
}

var _ = Describe("Run's clear", func() {
	It("deletes every row of the 'validate:' namespace — any rule id, even none — and leaves every other row byte for byte, ids included", func() {
		db, _ := openTempDB()
		s := seedVerb(db)
		// Ingest-shaped survivors of every shape the column admits.
		insertRawWarning(db, nil, nil, nil, validate.SeverityError, "no source at all")
		insertRawWarning(db, s.merida, "conagua-raw/2026-06-08/daily/validate:31019.txt", 12, validate.SeverityWarn, "a path that contains the namespace")
		insertRawWarning(db, nil, " validate:bbox", nil, validate.SeverityWarn, "a leading space is not the prefix")
		insertRawWarning(db, s.progreso, "xvalidate:bbox", 1, validate.SeverityWarn, "nor is a leading character")
		// Prior validate rows the run must clear.
		insertRawWarning(db, s.merida, "validate:bbox", nil, validate.SeverityWarn, "stale")
		insertRawWarning(db, nil, "validate:no-such-rule", nil, validate.SeverityError, "stale, unknown rule")
		insertRawWarning(db, nil, "validate:", 7, validate.SeverityWarn, "stale, empty rule id, a line")
		before := readAllWarnings(db)
		Expect(before).To(HaveLen(2 + 4 + 3))
		var survivors []fullWarningRow
		for _, r := range before {
			if !strings.HasPrefix(r.SourceFile.String, "validate:") {
				survivors = append(survivors, r)
			}
		}
		Expect(survivors).To(HaveLen(6))

		report := runOK(db, validate.Options{})

		after := readAllWarnings(db)
		Expect(after[:6]).To(Equal(survivors), "the survivors, in place, every column intact")
		var written []warningRow
		for _, r := range after[6:] {
			Expect(r.ID).To(BeNumerically(">", before[len(before)-1].ID), "the new rows are appended after every prior id")
			written = append(written, r.warningRow)
		}
		Expect(written).To(Equal(verbRows(s)))
		Expect(len(written)).To(Equal(report.WarningsTotal + report.ErrorsTotal))
	})

	It("matches the namespace case-insensitively, as SQLite's LIKE does", func() {
		db, _ := openTempDB()
		insertWarning(db, nil, "conagua-raw/2026-06-08/catalog.html")
		insertRawWarning(db, nil, "VALIDATE:bbox", nil, validate.SeverityWarn, "upper-case namespace")
		insertRawWarning(db, nil, "Validate:bbox", nil, validate.SeverityWarn, "mixed-case namespace")
		runOK(db, validate.Options{Skip: nativeAnchors()})
		Expect(readWarnings(db, "%")).To(Equal([]warningRow{ingestRow(nil, "conagua-raw/2026-06-08/catalog.html")}))
	})

	It("runs even when there is nothing to clear and nothing to write, leaving the table as it was", func() {
		db, _ := openTempDB()
		insertWarning(db, nil, "conagua-raw/2026-06-08/catalog.html")
		insertRawWarning(db, nil, nil, nil, validate.SeverityError, "no source at all")
		before := readAllWarnings(db)
		report := runOK(db, validate.Options{Skip: nativeAnchors()})
		Expect(ruleIDs(report.PerRule)).To(Equal(coreRules))
		Expect(report.WarningsTotal + report.ErrorsTotal).To(BeZero())
		Expect(readAllWarnings(db)).To(Equal(before))
	})
})

var _ = Describe("orphan-runs' reconcile", func() {
	It("flips a 'running' row one minute past 24 h and leaves one a minute short of it, in both ledgers", func() {
		db, _ := openTempDB()
		justOver := insertIngestRun(db, "running", ago(24*time.Hour+time.Minute))
		justUnder := insertIngestRun(db, "running", ago(24*time.Hour-time.Minute))
		powOver := insertPowerRunAt(db, "running", ago(24*time.Hour+time.Minute))
		powUnder := insertPowerRunAt(db, "running", ago(24*time.Hour-time.Minute))

		res := runVerbRule(db, "", "orphan-runs")
		Expect(res.Scanned).To(Equal(2))
		Expect(res.Findings).To(HaveLen(2))
		Expect(res.Findings[0].Issue).To(HavePrefix(fmt.Sprintf("ingest_runs.id=%d started ", justOver)))
		Expect(res.Findings[1].Issue).To(HavePrefix(fmt.Sprintf("power_runs.id=%d started ", powOver)))

		for _, tc := range []struct {
			table  string
			id     int64
			status string
		}{
			{"ingest_runs", justOver, "aborted"}, {"ingest_runs", justUnder, "running"},
			{"power_runs", powOver, "aborted"}, {"power_runs", powUnder, "running"},
		} {
			status, finished := runStatus(db, tc.table, tc.id)
			Expect(status).To(Equal(tc.status), "%s id=%d", tc.table, tc.id)
			Expect(finished.Valid).To(Equal(tc.status == "aborted"), "%s id=%d", tc.table, tc.id)
		}
	})

	It("changes only status and finished_at on a flipped row, and no column of any other row, whatever its age", func() {
		db, _ := openTempDB()
		s := seedClean(db)
		// Every non-running status, old enough to be an orphan if status
		// were ignored.
		insertIngestRun(db, "aborted", ago(100*time.Hour))
		insertIngestRun(db, "complete", ago(100*time.Hour))
		insertPowerRunAt(db, "aborted", ago(100*time.Hour))
		insertPowerRunAt(db, "complete", ago(100*time.Hour))
		// The two orphans, with every nullable column populated so the
		// flip is seen against a full row.
		var stranded, powStranded int64
		Expect(db.QueryRow(`
INSERT INTO ingest_runs (started_at, snapshot_date, sink_kind, etl_git_sha, status,
    stations_attempted, stations_succeeded, stations_failed, daily_rows, normals_rows, extras_rows, warnings_total)
VALUES (?, '2026-06-08', 'r2', 'abc123', 'running', 10, 9, 1, 1000, 12, 12, 3) RETURNING id`,
			ago(48*time.Hour)).Scan(&stranded)).To(Succeed())
		Expect(db.QueryRow(`
INSERT INTO power_runs (started_at, status, endpoint_url, parameters, community, period_start_year,
    period_end_year, grid_resolution, solar_conversion, unit_conversions, temporal_mode)
VALUES (?, 'running', 'https://power.example/api', 'T2M,RH2M', 'AG', 1991, 2020, '0.5x0.625', 11.574, '[]', 'monthly') RETURNING id`,
			ago(48*time.Hour)).Scan(&powStranded)).To(Succeed())

		ingestBefore := readLedger(db, "ingest_runs")
		powerBefore := readLedger(db, "power_runs")
		Expect(ingestBefore).To(HaveLen(4), "seedClean's complete run, the two finished ones, the orphan")
		Expect(powerBefore).To(HaveLen(5), "seedClean's two complete runs, the two finished ones, the orphan")
		Expect(s.ingestRun).NotTo(Equal(stranded))

		lower := time.Now().UTC().Truncate(time.Second)
		res := runVerbRule(db, "", "orphan-runs")
		Expect(res.Findings).To(HaveLen(2))

		ingestCols := ledgerColumns(db, "ingest_runs")
		powerCols := ledgerColumns(db, "power_runs")
		expectFlipOnly := func(cols []string, before, after []ledgerRow, flipped int64) {
			GinkgoHelper()
			Expect(after).To(HaveLen(len(before)))
			statusCol := slices.Index(cols, "status")
			finishedCol := slices.Index(cols, "finished_at")
			for i := range before {
				Expect(after[i].ID).To(Equal(before[i].ID))
				if before[i].ID != flipped {
					Expect(after[i]).To(Equal(before[i]), "row id=%d must be untouched", before[i].ID)
					continue
				}
				for c, col := range cols {
					switch c {
					case statusCol:
						Expect(before[i].Values[c].String).To(Equal("running"))
						Expect(after[i].Values[c].String).To(Equal("aborted"))
					case finishedCol:
						Expect(before[i].Values[c].Valid).To(BeFalse())
						Expect(after[i].Values[c].Valid).To(BeTrue())
						stamp, err := time.Parse(time.RFC3339, after[i].Values[c].String)
						Expect(err).NotTo(HaveOccurred())
						Expect(stamp).To(BeTemporally(">=", lower))
						Expect(stamp).To(BeTemporally("<=", time.Now().UTC()))
					default:
						Expect(after[i].Values[c]).To(Equal(before[i].Values[c]), col)
					}
				}
			}
		}
		expectFlipOnly(ingestCols, ingestBefore, readLedger(db, "ingest_runs"), stranded)
		expectFlipOnly(powerCols, powerBefore, readLedger(db, "power_runs"), powStranded)
	})
})

var _ = Describe("Run's convergence", func() {
	// seedMapOrdered makes daily-sanity emit several buckets — three
	// stations, tmax and tmin outliers in two calendar months, several
	// diurnal offenders — so its map-driven emission order can differ
	// between runs while the multiset cannot.
	seedMapOrdered := func(db *sql.DB) {
		GinkgoHelper()
		seedClean(db)
		for i, ext := range []string{"m1", "m2", "m3"} {
			sid := station(db, ext, "map-ordered "+ext, f64(19.0+float64(i)), f64(-99.0))
			for _, month := range []int{3, 7} {
				for d := 1; d <= 30; d++ {
					insertObs(db, sid, 1995, month, d, 20.0+float64(i), 10.0)
				}
				// A hot tmax and a cold tmin, each a diurnal offender too.
				insertObs(db, sid, 1996, month, 1, 45.0, 10.0)
				insertObs(db, sid, 1996, month, 2, 20.0+float64(i), -15.0)
			}
			insertNormalsTemps(db, sid, "1981-2010", 1, 25.0, nil, nil)
			insertNormalsTemps(db, sid, "1991-2020", 1, 30.0, nil, nil)
		}
	}

	It("yields the same multiset of rows and the same per-rule counts on every run, even where a rule's emission order is map-driven", func() {
		db, _ := openTempDB()
		seedMapOrdered(db)

		var reports []*validate.Report
		var runs [][]warningRow
		for range 3 {
			reports = append(reports, runOK(db, validate.Options{}))
			runs = append(runs, readWarnings(db, "validate:%"))
		}
		daily := reports[0].PerRule[3]
		Expect(daily.ID).To(Equal("daily-sanity"))
		Expect(daily.Warnings).To(Equal(3+3*2*2), "3 diurnal + (3 stations × 2 months × 2 variables) z-score buckets")
		Expect(reports[0].PerRule[4].Warnings).To(Equal(3), "cross-period")

		for i := 1; i < len(runs); i++ {
			Expect(runs[i]).To(HaveLen(len(runs[0])))
			Expect(runs[i]).To(ConsistOf(runs[0]))
			Expect(sortedRows(runs[i])).To(Equal(sortedRows(runs[0])))
			for r := range reports[i].PerRule {
				a, b := reports[0].PerRule[r], reports[i].PerRule[r]
				Expect([]any{b.ID, b.Scanned, b.Warnings, b.Errors}).To(Equal([]any{a.ID, a.Scanned, a.Warnings, a.Errors}))
			}
			Expect(reports[i].WarningsTotal).To(Equal(reports[0].WarningsTotal))
			Expect(reports[i].ErrorsTotal).To(Equal(reports[0].ErrorsTotal))
		}
		Expect(countWarnings(db, "%")).To(Equal(2+len(runs[0])), "replaced, never accumulated")
	})
})

var _ = Describe("Run's Skip", func() {
	// rowsOf is verbRows(s) minus one rule's rows.
	rowsOf := func(s verbSeed, without string) []warningRow {
		var out []warningRow
		for _, r := range verbRows(s) {
			if r.SourceFile.String != "validate:"+without {
				out = append(out, r)
			}
		}
		return out
	}

	var entries []TableEntry
	for _, r := range validate.AllRules("") {
		entries = append(entries, Entry(r.ID, r.ID))
	}

	DescribeTable("omits the one rule named and runs every other: no hook, no report slot, no row under its namespace, the others' rows exactly",
		func(id string) {
			db, _ := openTempDB()
			s := seedVerb(db)
			var started, done []string
			report := runOK(db, validate.Options{
				Skip:        map[string]bool{id: true},
				OnRuleStart: func(rid, _ string) { started = append(started, rid) },
				OnRuleDone:  func(rid string, _ validate.RuleResult, _ time.Duration) { done = append(done, rid) },
			})
			want := slices.DeleteFunc(slices.Clone(verbOrder), func(x string) bool { return x == id })
			Expect(started).To(Equal(want))
			Expect(done).To(Equal(want))
			Expect(ruleIDs(report.PerRule)).To(Equal(want))
			Expect(countWarnings(db, "validate:"+id)).To(BeZero())
			Expect(readWarnings(db, "validate:%")).To(Equal(rowsOf(s, id)))
			Expect(report.WarningsTotal + report.ErrorsTotal).To(Equal(len(rowsOf(s, id))))

			status, finished := runStatus(db, "ingest_runs", s.stranded)
			if id == "orphan-runs" {
				Expect(status).To(Equal("running"), "a skipped orphan-runs reconciles nothing")
				Expect(finished.Valid).To(BeFalse())
			} else {
				Expect(status).To(Equal("aborted"))
				Expect(finished.Valid).To(BeTrue())
			}
		},
		entries,
	)

	It("with every rule skipped runs nothing, clears its prior rows all the same, and writes none", func() {
		db, _ := openTempDB()
		s := seedVerb(db)
		insertRawWarning(db, s.merida, "validate:bbox", nil, validate.SeverityWarn, "stale")
		all := map[string]bool{}
		for _, r := range validate.AllRules("") {
			all[r.ID] = true
		}
		before := time.Now().UTC().Truncate(time.Second)
		report := runOK(db, validate.Options{
			Skip:        all,
			OnRuleStart: func(string, string) { Fail("no rule may start") },
			OnRuleDone:  func(string, validate.RuleResult, time.Duration) { Fail("no rule may finish") },
		})
		Expect(report.PerRule).To(BeEmpty())
		Expect(report.WarningsTotal).To(BeZero())
		Expect(report.ErrorsTotal).To(BeZero())
		Expect(report.Period).To(Equal(validate.DefaultPeriod))
		Expect(report.StartedAt).To(BeTemporally(">=", before))
		Expect(report.FinishedAt).To(BeTemporally(">=", report.StartedAt))
		Expect(countWarnings(db, "validate:%")).To(BeZero())
		Expect(readWarnings(db, "conagua-raw/%")).To(Equal([]warningRow{
			ingestRow(i64(s.merida), "conagua-raw/2026-06-08/normals/31019.txt"),
			ingestRow(nil, "conagua-raw/2026-06-08/catalog.html"),
		}))
		status, _ := runStatus(db, "ingest_runs", s.stranded)
		Expect(status).To(Equal("running"))
	})

	It("ignores an id that names no rule: nothing is skipped and nothing is refused", func() {
		db, _ := openTempDB()
		s := seedVerb(db)
		report := runOK(db, validate.Options{Skip: map[string]bool{"runs-in-flight": true, "nope": true, "bbox": false}})
		Expect(ruleIDs(report.PerRule)).To(Equal(verbOrder))
		Expect(readWarnings(db, "validate:%")).To(Equal(verbRows(s)))
	})
})
