package cmd_test

// The validate e2e story on the real binary against a seeded DB: the
// cmd→validate→schema seam — flag
// wiring, the writer open, the per-rule progress lines, the summary,
// the atomic HTML report, the exit-code contract (0 clean / 1 on an
// error-severity finding or a run-level failure) — with the DB read
// back to prove the rules wrote through the whole stack: the verb's own
// prior rows replaced, ingest's rows untouched, the stranded power run
// reconciled with its warn row, every finding's columns round-tripped.
// Rule correctness at value level is the validate package's and the
// parity gate's; here each rule fires once so its row and its period
// scoping are observable.

import (
	"database/sql"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/bioclimamx/conagua-etl/internal/schema"
)

var (
	validateStartRE = regexp.MustCompile(`(?m)^\[\d{2}:\d{2}:\d{2}\] (\S+) +starting \((.+)\)$`)
	validateDoneRE  = regexp.MustCompile(`(?m)^\[\d{2}:\d{2}:\d{2}\] (\S+) +done · scanned=(\d+) warn=(\d+) error=(\d+) · \S+$`)
	validateRowRE   = regexp.MustCompile(`(?m)^  (\S+) +(\d+) +(\d+) +(\d+)  \S+$`)
)

// validateRuleOrder is the verb's rule set in execution order: the five
// QC rules, then the gate's integrity anchors.
var validateRuleOrder = []string{
	"orphan-runs", "bbox", "wmo-month-completeness", "daily-sanity", "cross-period",
	"ingest-complete", "station-refs", "cell-refs", "run-refs", "run-label-unique", "fill-leak", "wind-range",
}

// validateCounts is one rule's scanned / warn / error triple as the
// done line and the summary row report it.
type validateCounts struct {
	rule                string
	scanned, warn, errs int
}

// wantSeededCounts is the seeded DB's outcome under the default period,
// written by hand from the seed: the stranded power run; two stations
// with plausible coordinates; the 1991 and 1992 January groups, the
// 1992 one eleven days short; no daily-sanity row (both groups are
// constant, so no variance, and no diurnal outlier inside the window);
// the one normals period pair, 5 °C apart. Then the anchors: one
// ingest run; the station references — 105 daily rows, 2 normals, and
// the one ingest warning left once the verb's prior rows are cleared
// (its own findings land after the anchors ran) — every one resolvable;
// no supplement row, cell link, or referenced power run at all.
var wantSeededCounts = []validateCounts{
	{"orphan-runs", 1, 1, 0},
	{"bbox", 2, 0, 0},
	{"wmo-month-completeness", 2, 1, 0},
	{"daily-sanity", 0, 0, 0},
	{"cross-period", 1, 1, 0},
	{"ingest-complete", 1, 0, 0},
	{"station-refs", 108, 0, 0},
	{"cell-refs", 0, 0, 0},
	{"run-refs", 0, 0, 0},
	{"run-label-unique", 0, 0, 0},
	{"fill-leak", 0, 0, 0},
	{"wind-range", 0, 0, 0},
}

// validateWarningRow is one parsing_warnings row, every column.
type validateWarningRow struct {
	station    sql.NullString // external_id through the stations join
	sourceFile string
	line       sql.NullInt64
	severity   string
	issue      string
}

// validateSeed is a seeded DB and the values its expected findings
// quote back.
type validateSeed struct {
	path       string
	strandedAt string // the stranded power run's started_at
}

const (
	validateWMOIssue1991   = "WMO §4.4.1 fail in period 1991-2020, calendar month=01: 1 year(s) violate (≥11 missing or ≥5 consecutive); top: 1992 (11/31 missing, gap=11)"
	validateWMOIssue1981   = "WMO §4.4.1 fail in period 1981-2010, calendar month=02: 1 year(s) violate (≥11 missing or ≥5 consecutive); top: 1986 (5/28 missing, gap=5)"
	validateWMOIssue1985   = "WMO §4.4.1 fail in period 1985-1986, calendar month=02: 1 year(s) violate (≥11 missing or ≥5 consecutive); top: 1986 (5/28 missing, gap=5)"
	validateCrossIssue     = "tmax cross-period delta exceeds ±3.0°C in 1 (month, period-pair)(s); top: m=01 1981-2010 vs 1991-2020: 25.0 vs 30.0 (Δ-5.0°C)"
	validateDiurnalIssue   = "diurnal range > 25°C on 1 day(s); top: 1985-01-31 (35.0°C)"
	validateStaleIssue     = "stale finding from a prior run"
	validateIngestIssue    = "unparseable TMAX"
	validateImpossibleText = `station conagua_conventional/9999 ("BROKEN") at impossible lat=0.0000 lon=0.0000 — error blocks publish`
)

func (s validateSeed) orphanIssue() string {
	return fmt.Sprintf("power_runs.id=1 started %s, status='running' beyond 24h0m0s — reconciled to 'aborted'", s.strandedAt)
}

// seedValidateDB builds the seeded DB in a fresh temp dir through the
// writer opener and applies any extra statements, returning it with
// the stranded run's timestamp.
func seedValidateDB(extra ...string) validateSeed {
	dir := GinkgoT().TempDir()
	path := filepath.Join(dir, "bioclima.db")
	db, err := schema.Open(path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	exec := func(query string, args ...any) {
		_, err := db.Exec(query, args...)
		ExpectWithOffset(2, err).NotTo(HaveOccurred(), query)
	}
	daily := func(station int64, year, month, from, to int, tmax, tmin float64) {
		for d := from; d <= to; d++ {
			exec(`INSERT INTO daily_observations (station_id, date, tmax, tmin) VALUES (?, ?, ?, ?)`,
				station, fmt.Sprintf("%04d-%02d-%02d", year, month, d), tmax, tmin)
		}
	}

	exec(`INSERT INTO stations (id, source, external_id, name, state, lat, lon)
	  VALUES (1, 'conagua_conventional', '1001', 'AGUASCALIENTES (OBS)', 'AGS', 21.85027778, -102.2908333)`)
	exec(`INSERT INTO stations (id, source, external_id, name, state, lat, lon)
	  VALUES (2, 'conagua_conventional', '1002', 'CALVILLO', 'AGS', 21.85, -102.72)`)

	// Station 1002's daily series: January 1991 complete; January 1992
	// eleven days short (the 1991-2020 WMO finding); January 1985 with a
	// 35 °C diurnal swing on its last day (the 1981-2010 daily-sanity
	// findings); February 1986 with a five-day gap (the 1981-2010 WMO
	// finding).
	daily(2, 1991, 1, 1, 31, 25.0, 10.0)
	daily(2, 1992, 1, 1, 20, 25.0, 10.0)
	daily(2, 1985, 1, 1, 30, 22.0, 8.0)
	daily(2, 1985, 1, 31, 31, 35.0, 0.0)
	daily(2, 1986, 2, 1, 11, 22.0, 8.0)
	daily(2, 1986, 2, 17, 28, 22.0, 8.0)

	// Station 1001's normals: the same month 5 °C apart across two
	// periods (the cross-period finding).
	exec(`INSERT INTO monthly_normals (station_id, period, month, tmax) VALUES (1, '1981-2010', 1, 25.0), (1, '1991-2020', 1, 30.0)`)

	exec(`INSERT INTO ingest_runs (id, started_at, finished_at, snapshot_date, sink_kind, status)
	  VALUES (1, '2026-07-18T10:00:00Z', '2026-07-18T10:30:00Z', '2026-07-18', 'local', 'complete')`)
	strandedAt := time.Now().UTC().Add(-72 * time.Hour).Format(time.RFC3339)
	exec(`INSERT INTO power_runs (id, started_at, status, endpoint_url, parameters, community,
	    period_start_year, period_end_year, grid_resolution, solar_conversion)
	  VALUES (1, ?, 'running', 'u', 'p', 'AG', 1991, 2020, '0.5x0.625', 11.574)`, strandedAt)
	exec(`INSERT INTO power_runs (id, started_at, finished_at, status, endpoint_url, parameters, community,
	    period_start_year, period_end_year, grid_resolution, solar_conversion)
	  VALUES (2, '2026-07-19T00:00:00Z', '2026-07-19T01:00:00Z', 'complete', 'u', 'p', 'AG', 1991, 2020, '0.5x0.625', 11.574)`)

	// A prior validate finding the run must replace, and an ingest
	// warning it must leave alone.
	exec(`INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
	  VALUES (1, 'validate:bbox', NULL, 'warn', ?)`, validateStaleIssue)
	exec(`INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
	  VALUES (2, 'daily/1002.txt', 57, 'warn', ?)`, validateIngestIssue)

	for _, s := range extra {
		exec(s)
	}
	ExpectWithOffset(1, db.Close()).To(Succeed())
	return validateSeed{path: path, strandedAt: strandedAt}
}

// startValidate launches the verb on dbPath and returns the live
// session; the caller decides how it ends.
func startValidate(dbPath, report string, extra ...string) *gexec.Session {
	args := append([]string{"validate", "--db", dbPath, "--report", report}, extra...)
	session, err := gexec.Start(exec.Command(binPath, args...), GinkgoWriter, GinkgoWriter)
	ExpectWithOffset(2, err).NotTo(HaveOccurred())
	return session
}

func runValidateToExit(dbPath, report string, extra ...string) *gexec.Session {
	session := startValidate(dbPath, report, extra...)
	EventuallyWithOffset(1, session, 60*time.Second).Should(gexec.Exit())
	return session
}

// readValidateRows returns every parsing_warnings row, every column,
// ordered by source_file then issue.
func readValidateRows(db *sql.DB, where string, args ...any) []validateWarningRow {
	q := `SELECT s.external_id, w.source_file, w.line, w.severity, w.issue
	  FROM parsing_warnings w LEFT JOIN stations s ON s.id = w.station_id`
	if where != "" {
		q += " WHERE " + where
	}
	rows, err := db.Query(q+" ORDER BY w.source_file, w.issue", args...)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer rows.Close() //nolint:errcheck // spec teardown
	var out []validateWarningRow
	for rows.Next() {
		var r validateWarningRow
		ExpectWithOffset(1, rows.Scan(&r.station, &r.sourceFile, &r.line, &r.severity, &r.issue)).To(Succeed())
		out = append(out, r)
	}
	ExpectWithOffset(1, rows.Err()).NotTo(HaveOccurred())
	return out
}

// warnRow builds an expected validate row: station-anchored (or NULL
// when ext is empty), the rule's namespace, no line.
func warnRow(ext, rule, severity, issue string) validateWarningRow {
	return validateWarningRow{
		station:    sql.NullString{String: ext, Valid: ext != ""},
		sourceFile: "validate:" + rule,
		severity:   severity,
		issue:      issue,
	}
}

// sortedRows orders expected rows the way readValidateRows does.
func sortedRows(rows []validateWarningRow) []validateWarningRow {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].sourceFile != rows[j].sourceFile {
			return rows[i].sourceFile < rows[j].sourceFile
		}
		return rows[i].issue < rows[j].issue
	})
	return rows
}

// expectDoneLines asserts stderr's per-rule lines: one starting and one
// done line per rule in want's order, the done line's counts exact.
func expectDoneLines(stderr string, want []validateCounts) {
	ExpectWithOffset(1, startedRules(stderr)).To(Equal(ruleNames(want)))
	expectDoneCounts(stderr, want)
}

// expectDoneCounts asserts stderr's done lines alone: one per rule in
// want's order, the counts exact.
func expectDoneCounts(stderr string, want []validateCounts) {
	dones := validateDoneRE.FindAllStringSubmatch(stderr, -1)
	ExpectWithOffset(2, dones).To(HaveLen(len(want)))
	for i, w := range want {
		ExpectWithOffset(2, dones[i][1:]).To(Equal([]string{
			w.rule, strconv.Itoa(w.scanned), strconv.Itoa(w.warn), strconv.Itoa(w.errs),
		}), w.rule)
	}
}

// startedRules lists the rules stderr's starting lines name, in order.
func startedRules(stderr string) []string {
	var out []string
	for _, m := range validateStartRE.FindAllStringSubmatch(stderr, -1) {
		out = append(out, m[1])
	}
	return out
}

// ruleNames lists want's rules in order.
func ruleNames(want []validateCounts) []string {
	var out []string
	for _, w := range want {
		out = append(out, w.rule)
	}
	return out
}

// expectSummaryRows asserts stdout's per-rule table rows in order.
func expectSummaryRows(stdout string, want []validateCounts) {
	rows := validateRowRE.FindAllStringSubmatch(stdout, -1)
	ExpectWithOffset(1, rows).To(HaveLen(len(want)))
	for i, w := range want {
		ExpectWithOffset(1, rows[i][1:]).To(Equal([]string{
			w.rule, strconv.Itoa(w.scanned), strconv.Itoa(w.warn), strconv.Itoa(w.errs),
		}), w.rule)
	}
}

// withCounts is want with one rule's counts replaced.
func withCounts(want []validateCounts, rule string, scanned, warn, errs int) []validateCounts {
	out := make([]validateCounts, len(want))
	copy(out, want)
	for i := range out {
		if out[i].rule == rule {
			out[i] = validateCounts{rule, scanned, warn, errs}
		}
	}
	return out
}

// withoutRule is want minus one rule.
func withoutRule(want []validateCounts, rule string) []validateCounts {
	var out []validateCounts
	for _, w := range want {
		if w.rule != rule {
			out = append(out, w)
		}
	}
	return out
}

// expectSelfContainedReport holds the HTML report to its contract: a
// complete document with no external asset, the period and every rule
// that ran named, and the given fragments present.
func expectSelfContainedReport(path, period string, rules []string, fragments ...string) string {
	html := string(readBytes(path))
	ExpectWithOffset(1, html).To(HavePrefix("<!DOCTYPE html>"))
	ExpectWithOffset(1, html).To(HaveSuffix("</html>\n"))
	ExpectWithOffset(1, html).To(ContainSubstring("Validate report"))
	ExpectWithOffset(1, html).To(ContainSubstring(period))
	for _, r := range rules {
		ExpectWithOffset(1, html).To(ContainSubstring(r))
	}
	for _, f := range fragments {
		ExpectWithOffset(1, html).To(ContainSubstring(f), f)
	}
	for _, external := range []string{"<link", "<script", "http://", "https://", "src="} {
		ExpectWithOffset(1, html).NotTo(ContainSubstring(external), "external asset reference")
	}
	expectNoTempResidue(filepath.Dir(path))
	return html
}

var _ = Describe("conagua-etl validate end-to-end", func() {
	var seed validateSeed
	var reportPath string

	BeforeEach(func() {
		seed = seedValidateDB()
		reportPath = filepath.Join(GinkgoT().TempDir(), "out", "validate-report.html")
	})

	It("validates the seeded DB: exit 0, one line per rule, the summary table, the findings written with prior validate rows replaced and ingest's kept, the stranded run reconciled, the self-contained report written", func() {
		session := runValidateToExit(seed.path, reportPath)
		Expect(session.ExitCode()).To(Equal(0))
		stderr := string(session.Err.Contents())
		stdout := string(session.Out.Contents())

		Expect(stderr).To(ContainSubstring(fmt.Sprintf("db=%s (writer)  period=1991-2020  skip=none  report=%s\n", seed.path, reportPath)))
		expectDoneLines(stderr, wantSeededCounts)
		Expect(stderr).NotTo(ContainSubstring("error:"))

		Expect(stdout).To(ContainSubstring("\nvalidate complete\n"))
		Expect(reportNum(stdout, "warnings")).To(Equal(3))
		Expect(reportNum(stdout, "errors")).To(Equal(0))
		Expect(stdout).To(ContainSubstring("  period            : 1991-2020\n"))
		Expect(stdout).To(MatchRegexp(`(?m)^  elapsed           : \S+$`))
		Expect(stdout).To(ContainSubstring("  rule                     scanned   warn   error  elapsed\n"))
		expectSummaryRows(stdout, wantSeededCounts)
		Expect(stdout).To(HaveSuffix("\nHTML report: " + reportPath + "\n"))

		db := openRO(seed.path)
		Expect(readValidateRows(db, "w.source_file LIKE 'validate:%'")).To(Equal(sortedRows([]validateWarningRow{
			warnRow("", "orphan-runs", "warn", seed.orphanIssue()),
			warnRow("1002", "wmo-month-completeness", "warn", validateWMOIssue1991),
			warnRow("1001", "cross-period", "warn", validateCrossIssue),
		})))
		// The prior validate row is gone; ingest's row is untouched, its
		// line included.
		Expect(readValidateRows(db, "w.source_file NOT LIKE 'validate:%'")).To(Equal([]validateWarningRow{{
			station: sql.NullString{String: "1002", Valid: true}, sourceFile: "daily/1002.txt",
			line: sql.NullInt64{Int64: 57, Valid: true}, severity: "warn", issue: validateIngestIssue,
		}}))
		Expect(countRows(db, `SELECT COUNT(*) FROM parsing_warnings`)).To(Equal(4))

		// The stranded run is aborted with a finish stamp inside the
		// run's window; the complete run and the ingest run are as seeded.
		var status string
		var finishedAt sql.NullString
		Expect(db.QueryRow(`SELECT status, finished_at FROM power_runs WHERE id = 1`).Scan(&status, &finishedAt)).To(Succeed())
		Expect(status).To(Equal("aborted"))
		Expect(finishedAt.Valid).To(BeTrue())
		stamp, err := time.Parse(time.RFC3339, finishedAt.String)
		Expect(err).NotTo(HaveOccurred())
		Expect(stamp).To(BeTemporally("~", time.Now().UTC(), 2*time.Minute))
		Expect(queryStrings(db, `SELECT status || ' ' || finished_at FROM power_runs WHERE id = 2`)).
			To(Equal([]string{"complete 2026-07-19T01:00:00Z"}))
		Expect(queryStrings(db, `SELECT status || ' ' || finished_at FROM ingest_runs`)).
			To(Equal([]string{"complete 2026-07-18T10:30:00Z"}))

		expectSelfContainedReport(reportPath, "1991-2020", validateRuleOrder,
			"WMO §4.4.1 fail in period 1991-2020", "reconciled to", "tmax cross-period delta")
	})

	It("re-runs idempotently: the findings are replaced, not accumulated, and the reconciled run is no longer stranded", func() {
		first := runValidateToExit(seed.path, reportPath)
		Expect(first.ExitCode()).To(Equal(0))
		rerun := runValidateToExit(seed.path, reportPath)
		Expect(rerun.ExitCode()).To(Equal(0))

		expectDoneLines(string(rerun.Err.Contents()), withCounts(wantSeededCounts, "orphan-runs", 0, 0, 0))
		Expect(reportNum(string(rerun.Out.Contents()), "warnings")).To(Equal(2))

		db := openRO(seed.path)
		Expect(readValidateRows(db, "w.source_file LIKE 'validate:%'")).To(Equal(sortedRows([]validateWarningRow{
			warnRow("1002", "wmo-month-completeness", "warn", validateWMOIssue1991),
			warnRow("1001", "cross-period", "warn", validateCrossIssue),
		})))
		Expect(countRows(db, `SELECT COUNT(*) FROM parsing_warnings`)).To(Equal(3))
		Expect(countRows(db, `SELECT COUNT(*) FROM power_runs WHERE status = 'running'`)).To(BeZero())
		expectSelfContainedReport(reportPath, "1991-2020", validateRuleOrder)
	})

	It("exits 1 on an impossible coordinate — the error line names the count, the finding is written, the report still lands", func() {
		seed = seedValidateDB(`INSERT INTO stations (id, source, external_id, name, lat, lon)
		  VALUES (3, 'conagua_conventional', '9999', 'BROKEN', 0, 0)`)

		session := runValidateToExit(seed.path, reportPath)
		Expect(session.ExitCode()).To(Equal(1))
		stderr := string(session.Err.Contents())
		stdout := string(session.Out.Contents())

		Expect(stderr).To(HaveSuffix("error: 1 validation error(s) — publish refuses this DB\n"))
		expectDoneLines(stderr, withCounts(wantSeededCounts, "bbox", 3, 0, 1))
		Expect(reportNum(stdout, "warnings")).To(Equal(3))
		Expect(reportNum(stdout, "errors")).To(Equal(1))
		expectSummaryRows(stdout, withCounts(wantSeededCounts, "bbox", 3, 0, 1))
		Expect(stdout).To(HaveSuffix("\nHTML report: " + reportPath + "\n"))

		db := openRO(seed.path)
		Expect(readValidateRows(db, "w.source_file = 'validate:bbox'")).To(Equal([]validateWarningRow{
			warnRow("9999", "bbox", "error", validateImpossibleText),
		}))
		Expect(countRows(db, `SELECT COUNT(*) FROM parsing_warnings WHERE severity = 'error'`)).To(Equal(1))
		expectSelfContainedReport(reportPath, "1991-2020", validateRuleOrder, "impossible lat=0.0000 lon=0.0000")
	})

	It("omits a skipped rule under --skip: no lines, no summary row, no rows", func() {
		session := runValidateToExit(seed.path, reportPath, "--skip", "cross-period")
		Expect(session.ExitCode()).To(Equal(0))
		stderr := string(session.Err.Contents())
		stdout := string(session.Out.Contents())

		Expect(stderr).To(ContainSubstring("  skip=cross-period  "))
		want := withoutRule(wantSeededCounts, "cross-period")
		expectDoneLines(stderr, want)
		expectSummaryRows(stdout, want)
		Expect(reportNum(stdout, "warnings")).To(Equal(2))

		db := openRO(seed.path)
		Expect(countRows(db, `SELECT COUNT(*) FROM parsing_warnings WHERE source_file = 'validate:cross-period'`)).To(BeZero())
		Expect(countRows(db, `SELECT COUNT(*) FROM parsing_warnings WHERE source_file LIKE 'validate:%'`)).To(Equal(2))
		html := expectSelfContainedReport(reportPath, "1991-2020", withoutRuleIDs(validateRuleOrder, "cross-period"))
		Expect(html).NotTo(ContainSubstring("cross-period"))
	})

	It("accepts --skip as a comma-separated list and repeats, rendered sorted", func() {
		session := runValidateToExit(seed.path, reportPath, "--skip", "daily-sanity,cross-period", "--skip", "orphan-runs")
		Expect(session.ExitCode()).To(Equal(0))
		stderr := string(session.Err.Contents())

		Expect(stderr).To(ContainSubstring("  skip=cross-period,daily-sanity,orphan-runs  "))
		expectDoneLines(stderr, withoutRule(withoutRule(withoutRule(wantSeededCounts, "cross-period"), "daily-sanity"), "orphan-runs"))

		// The skipped orphan-runs rule reconciled nothing.
		db := openRO(seed.path)
		Expect(queryStrings(db, `SELECT status FROM power_runs WHERE id = 1`)).To(Equal([]string{"running"}))
	})

	It("scopes the two period rules to --period, observable in the issue texts", func() {
		session := runValidateToExit(seed.path, reportPath, "--period", "1981-2010")
		Expect(session.ExitCode()).To(Equal(0))
		stderr := string(session.Err.Contents())
		stdout := string(session.Out.Contents())

		Expect(stderr).To(ContainSubstring("  period=1981-2010  "))
		// The 1981-2010 window holds the 1985 and 1986 groups and, since
		// the periods overlap, the 1991 and 1992 ones: two WMO findings,
		// each quoting this window; the 1985 outlier day now in scope.
		want := withCounts(withCounts(wantSeededCounts, "wmo-month-completeness", 4, 2, 0), "daily-sanity", 3, 3, 0)
		expectDoneLines(stderr, want)
		expectSummaryRows(stdout, want)
		Expect(stdout).To(ContainSubstring("  period            : 1981-2010\n"))
		Expect(reportNum(stdout, "warnings")).To(Equal(7))

		db := openRO(seed.path)
		rows := readValidateRows(db, "w.source_file LIKE 'validate:%'")
		Expect(rows).To(HaveLen(7))
		Expect(rows).To(ContainElements(
			warnRow("1002", "wmo-month-completeness", "warn", validateWMOIssue1981),
			warnRow("1002", "wmo-month-completeness", "warn", strings.Replace(validateWMOIssue1991, "1991-2020", "1981-2010", 1)),
			warnRow("1002", "daily-sanity", "warn", validateDiurnalIssue),
			warnRow("1001", "cross-period", "warn", validateCrossIssue),
			warnRow("", "orphan-runs", "warn", seed.orphanIssue()),
		))
		Expect(rows).NotTo(ContainElement(warnRow("1002", "wmo-month-completeness", "warn", validateWMOIssue1991)))
		var zscore []string
		for _, r := range rows {
			if r.sourceFile == "validate:daily-sanity" && strings.Contains(r.issue, "outliers") {
				zscore = append(zscore, r.issue)
				Expect(r.station.String).To(Equal("1002"))
			}
		}
		Expect(zscore).To(HaveLen(2))
		Expect(strings.Join(zscore, "\n")).To(ContainSubstring("tmax outliers > 3.0σ in calendar month=01: 1 day(s)"))
		Expect(strings.Join(zscore, "\n")).To(ContainSubstring("tmin outliers > 3.0σ in calendar month=01: 1 day(s)"))
		Expect(strings.Join(zscore, "\n")).To(ContainSubstring("1985-01-31 tmax=35.0"))
		Expect(strings.Join(zscore, "\n")).To(ContainSubstring("1985-01-31 tmin=0.0"))

		expectSelfContainedReport(reportPath, "1981-2010", validateRuleOrder,
			"WMO §4.4.1 fail in period 1981-2010", "diurnal range")
	})

	It("refuses an unknown --skip before the DB is opened, listing the rule ids", func() {
		dbPath := filepath.Join(GinkgoT().TempDir(), "never-created.db")
		session := runValidateToExit(dbPath, reportPath, "--skip", "nope")
		Expect(session.ExitCode()).To(Equal(1))
		stderr := string(session.Err.Contents())
		Expect(stderr).To(HaveSuffix(fmt.Sprintf("error: --skip: unknown rule \"nope\" (valid: %s)\n", strings.Join(validateRuleOrder, ", "))))
		Expect(validateDoneRE.FindAllString(stderr, -1)).To(BeEmpty())
		Expect(dbPath).NotTo(BeAnExistingFile())
		Expect(reportPath).NotTo(BeAnExistingFile())
	})

	It("scopes the two period rules to a window outside the CONAGUA normals set", func() {
		session := runValidateToExit(seed.path, reportPath, "--period", "1985-1986")
		Expect(session.ExitCode()).To(Equal(0))
		stderr := string(session.Err.Contents())
		stdout := string(session.Out.Contents())

		Expect(stderr).To(ContainSubstring("  period=1985-1986  "))
		// The window holds the 1985 January group (complete, with the
		// outlier day) and the 1986 February gap; the 1991 and 1992
		// groups fall outside it.
		want := withCounts(withCounts(wantSeededCounts, "wmo-month-completeness", 2, 1, 0), "daily-sanity", 3, 3, 0)
		expectDoneLines(stderr, want)
		expectSummaryRows(stdout, want)
		Expect(stdout).To(ContainSubstring("  period            : 1985-1986\n"))
		Expect(reportNum(stdout, "warnings")).To(Equal(6))

		db := openRO(seed.path)
		rows := readValidateRows(db, "w.source_file LIKE 'validate:%'")
		Expect(rows).To(HaveLen(6))
		Expect(rows).To(ContainElements(
			warnRow("1002", "wmo-month-completeness", "warn", validateWMOIssue1985),
			warnRow("1002", "daily-sanity", "warn", validateDiurnalIssue),
			warnRow("1001", "cross-period", "warn", validateCrossIssue),
			warnRow("", "orphan-runs", "warn", seed.orphanIssue()),
		))
		Expect(rows).NotTo(ContainElement(warnRow("1002", "wmo-month-completeness", "warn", validateWMOIssue1991)))
		expectSelfContainedReport(reportPath, "1985-1986", validateRuleOrder,
			"WMO §4.4.1 fail in period 1985-1986", "diurnal range")
	})

	It("exits 1 on a run-level failure — a column a rule reads is gone — printing the rules that completed under an aborted heading and writing no report", func() {
		// A dropped table would come back empty through the writer's
		// idempotent DDL; a dropped column stays gone.
		seed = seedValidateDB(`ALTER TABLE monthly_normals DROP COLUMN tmean`)
		session := runValidateToExit(seed.path, reportPath)
		Expect(session.ExitCode()).To(Equal(1))
		stderr := string(session.Err.Contents())
		stdout := string(session.Out.Contents())

		Expect(stderr).To(MatchRegexp(`(?m)^error: rule cross-period: .*tmean`))
		// The failing rule started and never finished; the rules after
		// it never started.
		completed := wantSeededCounts[:4]
		Expect(startedRules(stderr)).To(Equal(validateRuleOrder[:5]))
		expectDoneCounts(stderr, completed)
		Expect(stdout).To(ContainSubstring("\nvalidate aborted\n"))
		expectSummaryRows(stdout, completed)
		Expect(stdout).NotTo(ContainSubstring("HTML report:"))
		Expect(reportPath).NotTo(BeAnExistingFile())
		Expect(filepath.Dir(reportPath)).NotTo(BeADirectory())
	})
})

// withoutRuleIDs is ids minus one.
func withoutRuleIDs(ids []string, drop string) []string {
	var out []string
	for _, id := range ids {
		if id != drop {
			out = append(out, id)
		}
	}
	return out
}

var _ = Describe("conagua-etl validate --help", func() {
	It("documents that the verb mutates the database, the rules, and the exit codes", func() {
		out, err := exec.Command(binPath, "validate", "--help").CombinedOutput()
		Expect(err).NotTo(HaveOccurred())
		help := string(out)
		Expect(help).To(ContainSubstring("This verb MUTATES the database"))
		Expect(help).To(ContainSubstring("flipped to 'aborted'"))
		for _, id := range validateRuleOrder {
			Expect(help).To(ContainSubstring(id))
		}
		Expect(help).To(ContainSubstring("Exit codes: 0"))
		Expect(help).To(ContainSubstring("--db string"))
		Expect(help).To(ContainSubstring("--period string"))
		Expect(help).To(ContainSubstring("--skip strings"))
		Expect(help).To(ContainSubstring("--report string"))
		// --db carries no default — a bare invocation must never name the
		// production database — and the help documents the copy-first
		// workflow and the WAL sidecars the writer open keeps beside the file.
		Expect(help).NotTo(ContainSubstring(`(default "./bioclima.db")`))
		Expect(help).To(ContainSubstring("--db is required and has no default"))
		Expect(help).To(ContainSubstring("  sqlite3 \"file:bioclima.db?mode=ro\" \"VACUUM INTO 'copy.db'\"\n  conagua-etl validate --db copy.db\n"))
		Expect(help).To(ContainSubstring("required, must exist. Opened for writing"))
		Expect(help).To(ContainSubstring("A WAL-mode database keeps its -shm/-wal sidecars beside it, so the directory must be writable."))
		Expect(help).To(ContainSubstring(`(default "1991-2020")`))
		Expect(help).To(ContainSubstring(`(default "./validate-report.html")`))
	})
})
