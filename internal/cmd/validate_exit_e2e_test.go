package cmd_test

// The validate verb's flag-validation table and exit-code contract on
// the real binary (exit 0 ok, 1 failure; the verb has
// no degraded outcome, so 2 never occurs). Every --period of the
// YYYY-YYYY shape — the CONAGUA normals periods and any other window —
// and every rule id the registry lists is accepted; every other
// spelling is refused at the flag, before the database is opened, with
// the message naming the fault; the period check precedes the skip
// check, a bare invocation is refused at the flag (--db is required,
// with no default), and a --db that does not exist is refused without
// creating it. The exit code follows the findings — 0 with warnings only, 1 with
// an error-severity finding, 1 on a run-level failure — and the DB
// mutation is committed by the time the exit code is decided, so a
// report that cannot be written still leaves the findings behind.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/bioclimamx/conagua-etl/internal/cmd"
)

// allSkipped is every verb rule as one --skip value, so a run over any
// period is instant and writes nothing.
func allSkipped() string { return strings.Join(validateRuleOrder, ",") }

var _ = Describe("conagua-etl validate flag validation", func() {
	var seed validateSeed
	var reportPath string

	BeforeEach(func() {
		seed = seedValidateDB()
		reportPath = filepath.Join(GinkgoT().TempDir(), "validate-report.html")
	})

	periodEntries := func() []TableEntry {
		var out []TableEntry
		for _, p := range append(conaguaPeriods(), "2001-2020", "1990-2019") {
			out = append(out, Entry(p, p))
		}
		return out
	}

	DescribeTable("accepts each CONAGUA normals period and any other YYYY-YYYY window, stamping it on the run",
		func(period string) {
			session := runValidateToExit(seed.path, reportPath, "--period", period, "--skip", allSkipped())
			Expect(session.ExitCode()).To(Equal(0))
			Expect(string(session.Err.Contents())).To(ContainSubstring("  period=" + period + "  "))
			Expect(string(session.Out.Contents())).To(ContainSubstring("  period            : " + period + "\n"))
			Expect(reportPath).To(BeAnExistingFile())
			Expect(string(readBytes(reportPath))).To(ContainSubstring("· period " + period))
		},
		periodEntries(),
	)

	DescribeTable("refuses a --period that is not a YYYY-YYYY window before the DB is touched, naming the fault",
		func(period, want string) {
			dbPath := filepath.Join(GinkgoT().TempDir(), "never-created.db")
			session := runValidateToExit(dbPath, reportPath, "--period", period)
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(Equal("error: " + want + "\n"))
			Expect(string(session.Out.Contents())).To(BeEmpty())
			Expect(dbPath).NotTo(BeAnExistingFile())
			Expect(reportPath).NotTo(BeAnExistingFile())
		},
		Entry("a single year", "1991", `--period: "1991" is not a YYYY-YYYY window`),
		Entry("a trailing token", "1991-2020-x", `--period: "1991-2020-x" is not a YYYY-YYYY window`),
		Entry("no hyphen", "19912020", `--period: "19912020" is not a YYYY-YYYY window`),
		Entry("two-digit years", "91-20", `--period: "91-20" is not a YYYY-YYYY window`),
		Entry("a leading space", " 1991-2020", `--period: " 1991-2020" is not a YYYY-YYYY window`),
		Entry("a trailing space", "1991-2020 ", `--period: "1991-2020 " is not a YYYY-YYYY window`),
		Entry("an en dash", "1991–2020", `--period: "1991–2020" is not a YYYY-YYYY window`),
		Entry("empty", "", `--period: "" is not a YYYY-YYYY window`),
		Entry("an inverted window", "2020-1991", `--period: 2020-1991 ends before it starts`),
	)

	skipEntries := func() []TableEntry {
		var out []TableEntry
		for _, id := range validateRuleOrder {
			out = append(out, Entry(id, id))
		}
		return out
	}

	DescribeTable("accepts each registry rule id under --skip and omits exactly that rule",
		func(id string) {
			session := runValidateToExit(seed.path, reportPath, "--skip", id)
			Expect(session.ExitCode()).To(Equal(0))
			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring("  skip=" + id + "  "))
			want := withoutRule(wantSeededCounts, id)
			expectDoneLines(stderr, want)
			expectSummaryRows(string(session.Out.Contents()), want)
			db := openRO(seed.path)
			Expect(countRows(db, `SELECT COUNT(*) FROM parsing_warnings WHERE source_file = ?`, "validate:"+id)).To(BeZero())
		},
		skipEntries(),
	)

	DescribeTable("refuses a --skip that names no verb rule before the DB exists, listing the rule ids",
		func(skip string, bad string) {
			dbPath := filepath.Join(GinkgoT().TempDir(), "never-created.db")
			session := runValidateToExit(dbPath, reportPath, "--skip", skip)
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(Equal(fmt.Sprintf(
				"error: --skip: unknown rule %q (valid: %s)\n", bad, strings.Join(validateRuleOrder, ", "))))
			Expect(dbPath).NotTo(BeAnExistingFile())
			Expect(reportPath).NotTo(BeAnExistingFile())
		},
		Entry("a gate-only rule", "runs-in-flight", "runs-in-flight"),
		Entry("upper case", "BBOX", "BBOX"),
		Entry("a typo", "daily-sanity,cross-perod", "cross-perod"),
		Entry("a rule name that is not an id", "Lat/lon plausibility", "Lat/lon plausibility"),
		Entry("the namespace form", "validate:bbox", "validate:bbox"),
	)

	It("tolerates whitespace, empty entries, and repeats in --skip, rendering the set sorted and once", func() {
		session := runValidateToExit(seed.path, reportPath, "--skip", " bbox , cross-period,,bbox", "--skip", "", "--skip", "cross-period")
		Expect(session.ExitCode()).To(Equal(0))
		stderr := string(session.Err.Contents())
		Expect(stderr).To(ContainSubstring("  skip=bbox,cross-period  "))
		expectDoneLines(stderr, withoutRule(withoutRule(wantSeededCounts, "bbox"), "cross-period"))
	})

	It("renders an empty --skip as none", func() {
		session := runValidateToExit(seed.path, reportPath, "--skip", "")
		Expect(session.ExitCode()).To(Equal(0))
		Expect(string(session.Err.Contents())).To(ContainSubstring("  skip=none  "))
		expectDoneLines(string(session.Err.Contents()), wantSeededCounts)
	})

	It("checks --period, then --skip, then --db when all three are refused", func() {
		dbPath := filepath.Join(GinkgoT().TempDir(), "never-created.db")
		session := runValidateToExit(dbPath, reportPath, "--period", "1991", "--skip", "nope")
		Expect(session.ExitCode()).To(Equal(1))
		Expect(string(session.Err.Contents())).To(HavePrefix(`error: --period: "1991" is not a YYYY-YYYY window`))
		Expect(dbPath).NotTo(BeAnExistingFile())

		session = runValidateToExit(dbPath, reportPath, "--skip", "nope")
		Expect(session.ExitCode()).To(Equal(1))
		Expect(string(session.Err.Contents())).To(HavePrefix(`error: --skip: unknown rule "nope"`))
		Expect(dbPath).NotTo(BeAnExistingFile())
	})
})

var _ = Describe("conagua-etl validate exit codes", func() {
	var seed validateSeed
	var reportPath string

	BeforeEach(func() {
		seed = seedValidateDB()
		reportPath = filepath.Join(GinkgoT().TempDir(), "validate-report.html")
	})

	It("maps the verb's outcomes to 0 and 1 only: no degraded outcome exists", func() {
		// The one error the verb returns on findings is a plain error —
		// never DegradedError — so exit 2 is unreachable.
		Expect(cmd.ExitCode(errors.New("1 validation error(s) — publish refuses this DB"))).To(Equal(1))
		Expect(cmd.ExitCode(nil)).To(BeZero())
	})

	It("exits 0 with warnings only: warnings never fail the run", func() {
		session := runValidateToExit(seed.path, reportPath)
		Expect(session.ExitCode()).To(Equal(0))
		Expect(reportNum(string(session.Out.Contents()), "warnings")).To(Equal(3))
		Expect(reportNum(string(session.Out.Contents()), "errors")).To(Equal(0))
		Expect(string(session.Err.Contents())).NotTo(ContainSubstring("error:"))
	})

	It("exits 0 when the one error-producing rule is skipped, though the impossible coordinate is still there", func() {
		seed = seedValidateDB(`INSERT INTO stations (id, source, external_id, name, lat, lon)
		  VALUES (3, 'conagua_conventional', '9999', 'BROKEN', 0, 0)`)
		refused := runValidateToExit(seed.path, reportPath)
		Expect(refused.ExitCode()).To(Equal(1))
		Expect(string(refused.Err.Contents())).To(HaveSuffix("error: 1 validation error(s) — publish refuses this DB\n"))

		skipped := runValidateToExit(seed.path, reportPath, "--skip", "bbox")
		Expect(skipped.ExitCode()).To(Equal(0))
		Expect(reportNum(string(skipped.Out.Contents()), "errors")).To(Equal(0))
		db := openRO(seed.path)
		Expect(countRows(db, `SELECT COUNT(*) FROM parsing_warnings WHERE source_file = 'validate:bbox'`)).To(BeZero(),
			"the prior run's error row is cleared and bbox never re-judged it")
	})

	It("refuses a bare invocation — --db is required, no default — with exit 1 before anything is opened or created", func() {
		session, err := gexec.Start(exec.Command(binPath, "validate", "--report", reportPath), GinkgoWriter, GinkgoWriter)
		Expect(err).NotTo(HaveOccurred())
		Eventually(session, 60*time.Second).Should(gexec.Exit(1))
		Expect(string(session.Err.Contents())).To(Equal("error: required flag(s) \"db\" not set\n"))
		Expect(string(session.Out.Contents())).To(BeEmpty())
		Expect(reportPath).NotTo(BeAnExistingFile())
		// No default path was opened: the working directory gained no
		// database and no WAL sidecar.
		stray, err := filepath.Glob("bioclima.db*")
		Expect(err).NotTo(HaveOccurred())
		Expect(stray).To(BeEmpty())
	})

	It("refuses an absent --db before anything is created: exit 1, no database, no report, no rule run", func() {
		dbPath := filepath.Join(GinkgoT().TempDir(), "fresh", "bioclima.db")
		Expect(os.MkdirAll(filepath.Dir(dbPath), 0o755)).To(Succeed())
		session := runValidateToExit(dbPath, reportPath)
		Expect(session.ExitCode()).To(Equal(1))
		Expect(string(session.Err.Contents())).To(Equal(
			"error: --db: " + dbPath + " does not exist (validate never creates a database)\n"))
		Expect(string(session.Out.Contents())).To(BeEmpty())
		Expect(dbPath).NotTo(BeAnExistingFile())
		entries, err := os.ReadDir(filepath.Dir(dbPath))
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(BeEmpty(), "no database, WAL, or shm file created")
		Expect(reportPath).NotTo(BeAnExistingFile())
	})

	It("exits 1 on an empty DB: every rule runs and the missing ingest run is the error", func() {
		dbPath := filepath.Join(GinkgoT().TempDir(), "empty.db")
		Expect(os.WriteFile(dbPath, nil, 0o644)).To(Succeed())
		session := runValidateToExit(dbPath, reportPath)
		Expect(session.ExitCode()).To(Equal(1))
		stderr := string(session.Err.Contents())
		Expect(stderr).To(HaveSuffix("error: 1 validation error(s) — publish refuses this DB\n"))
		expectDoneLines(stderr, []validateCounts{
			{"orphan-runs", 0, 0, 0}, {"bbox", 0, 0, 0}, {"wmo-month-completeness", 0, 0, 0},
			{"daily-sanity", 0, 0, 0}, {"cross-period", 0, 0, 0}, {"ingest-complete", 0, 0, 1},
			{"station-refs", 0, 0, 0}, {"cell-refs", 0, 0, 0}, {"run-refs", 0, 0, 0},
			{"run-label-unique", 0, 0, 0}, {"fill-leak", 0, 0, 0}, {"wind-range", 0, 0, 0},
		})
		db := openRO(dbPath)
		Expect(readValidateRows(db, "")).To(Equal([]validateWarningRow{
			warnRow("", "ingest-complete", "error", "ingest_runs has no complete row (0 rows in total): nothing to publish"),
		}))
		Expect(reportPath).To(BeAnExistingFile())
	})

	It("exits 1 when --db cannot be opened, before any rule runs", func() {
		dir := GinkgoT().TempDir()
		session := runValidateToExit(dir, reportPath)
		Expect(session.ExitCode()).To(Equal(1))
		stderr := string(session.Err.Contents())
		Expect(stderr).To(HavePrefix("error: open " + dir + ": "))
		Expect(validateDoneRE.FindAllString(stderr, -1)).To(BeEmpty())
		Expect(reportPath).NotTo(BeAnExistingFile())
	})

	It("exits 1 when the report cannot be written, the findings already committed and the summary printed", func() {
		blocker := filepath.Join(GinkgoT().TempDir(), "report.html")
		Expect(os.Mkdir(blocker, 0o755)).To(Succeed())
		session := runValidateToExit(seed.path, blocker)
		Expect(session.ExitCode()).To(Equal(1))
		stderr := string(session.Err.Contents())
		stdout := string(session.Out.Contents())
		Expect(stderr).To(HavePrefix("db=" + seed.path + " (writer)"))
		Expect(stderr).To(MatchRegexp(`(?m)^error: write report: html report: write file ` + regexp.QuoteMeta(blocker)))
		expectDoneLines(stderr, wantSeededCounts)
		Expect(stdout).To(ContainSubstring("\nvalidate complete\n"))
		Expect(stdout).NotTo(ContainSubstring("HTML report:"))

		db := openRO(seed.path)
		Expect(readValidateRows(db, "w.source_file LIKE 'validate:%'")).To(Equal(sortedRows([]validateWarningRow{
			warnRow("", "orphan-runs", "warn", seed.orphanIssue()),
			warnRow("1002", "wmo-month-completeness", "warn", validateWMOIssue1991),
			warnRow("1001", "cross-period", "warn", validateCrossIssue),
		})))
		Expect(queryStrings(db, `SELECT status FROM power_runs WHERE id = 1`)).To(Equal([]string{"aborted"}))
		entries, err := os.ReadDir(filepath.Dir(blocker))
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(HaveLen(1), "no temp sibling left beside the blocker")
	})
})
