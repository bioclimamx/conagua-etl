package cmd_test

// The cancellation path on the real binary (SIGINT and SIGTERM cancel):
// a validate interrupted between or inside its rules
// exits 1 with the aborted summary, writes no report and no temp file,
// and leaves the database in the one state the verb's write discipline
// allows — its prior findings cleared (the clear precedes the first
// rule) and the new batch absent (it lands only after the last rule):
// never a partial set. The seed carries enough daily rows that the two
// period-scoped rules cannot complete before the signal lands, so the
// interrupt is mid-run by construction rather than by timing luck.

import (
	"database/sql"
	"fmt"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

// The heavy validate fixture: heavyValidateStations stations with
// plausible coordinates, each carrying heavyValidateDays consecutive
// days from 1991-01-01 — inside the default period, complete months,
// so the rows scan heavy and judge clean. The two period-scoped rules
// are what the signal must land in, and their cost grows with the
// station count more than with the row count: at this size they hold
// close to three seconds of work after the run's first starting line
// (wmo-month-completeness ~0.5 s, daily-sanity ~2 s on the reference
// machine), against a signal that lands within tens of milliseconds.
const (
	heavyValidateStations = 40
	heavyValidateDays     = 10_000
)

// heavyValidateSeed is the extra statements that add the fixture through
// two recursive CTEs.
var heavyValidateSeed = []string{
	fmt.Sprintf(`WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM seq WHERE n + 1 < %d)
	 INSERT INTO stations (id, source, external_id, name, state, lat, lon)
	 SELECT 100 + n, 'conagua_conventional', '9' || (100 + n), 'HEAVY ' || n, 'AGS', 21.85, -102.29 FROM seq`,
		heavyValidateStations),
	fmt.Sprintf(`WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM seq WHERE n + 1 < %d)
	 INSERT INTO daily_observations (station_id, date, tmax, tmin)
	 SELECT 100 + n %% %d, date('1991-01-01', '+' || (n / %d) || ' days'), 25.0 + n %% 7, 10.0 + n %% 5 FROM seq`,
		heavyValidateStations*heavyValidateDays, heavyValidateStations, heavyValidateStations),
}

var _ = Describe("conagua-etl validate interrupted mid-run", func() {
	It("exits 1 on SIGINT with the aborted summary: no report, no temp residue, the prior findings cleared and the new batch never written", func() {
		seed := seedValidateDB(heavyValidateSeed...)
		reportPath := filepath.Join(GinkgoT().TempDir(), "validate-report.html")

		session := startValidate(seed.path, reportPath)
		Eventually(func() string { return string(session.Err.Contents()) }, interruptWait, interruptPoll).
			Should(MatchRegexp(validateStartRE.String()))
		session.Interrupt()
		Eventually(session, interruptWait).Should(gexec.Exit(1))

		stderr := string(session.Err.Contents())
		stdout := string(session.Out.Contents())

		// The rules that started form a prefix of the execution order and
		// stop short of the set; the rule in flight at the signal, if any,
		// never printed its done line; the run-level error names the
		// cancellation through the rule it stopped at.
		started := startedRules(stderr)
		Expect(started).NotTo(BeEmpty())
		Expect(len(started)).To(BeNumerically("<", len(validateRuleOrder)))
		Expect(started).To(Equal(validateRuleOrder[:len(started)]))
		dones := validateDoneRE.FindAllStringSubmatch(stderr, -1)
		Expect(len(dones)).To(BeNumerically("<=", len(started)))
		Expect(stderr).To(MatchRegexp(`(?m)^error: rule \S+: .*(context canceled|interrupted)`))

		Expect(stdout).To(ContainSubstring("\nvalidate aborted\n"))
		Expect(stdout).To(ContainSubstring("  period            : 1991-2020\n"))
		Expect(validateRowRE.FindAllString(stdout, -1)).To(HaveLen(len(dones)))
		Expect(stdout).NotTo(ContainSubstring("HTML report:"))
		Expect(reportPath).NotTo(BeAnExistingFile())
		expectNoTempResidue(filepath.Dir(reportPath))

		// The database: the verb's prior row is gone (the clear ran before
		// the first rule started) and no new row landed (the batch is
		// written after the last rule), ingest's row is untouched, and
		// the seeded data is whole.
		db := openRO(seed.path)
		Expect(readValidateRows(db, "w.source_file LIKE 'validate:%'")).To(BeEmpty())
		Expect(readValidateRows(db, "w.source_file NOT LIKE 'validate:%'")).To(Equal([]validateWarningRow{{
			station: sql.NullString{String: "1002", Valid: true}, sourceFile: "daily/1002.txt",
			line: sql.NullInt64{Int64: 57, Valid: true}, severity: "warn", issue: validateIngestIssue,
		}}))
		Expect(countRows(db, `SELECT COUNT(*) FROM parsing_warnings`)).To(Equal(1))
		Expect(countRows(db, `SELECT COUNT(*) FROM daily_observations`)).To(Equal(105 + heavyValidateStations*heavyValidateDays))
		Expect(countRows(db, `SELECT COUNT(*) FROM stations`)).To(Equal(2 + heavyValidateStations))

		// The stranded run — orphan-runs is the rule the signal races —
		// is reconciled whole or not at all: 'running' with no finish
		// stamp, or 'aborted' with one; the other runs are as seeded.
		var status string
		var finishedAt sql.NullString
		Expect(db.QueryRow(`SELECT status, finished_at FROM power_runs WHERE id = 1`).Scan(&status, &finishedAt)).To(Succeed())
		Expect(finishedAt.Valid).To(Equal(status == "aborted"), status)
		Expect(status).To(Or(Equal("running"), Equal("aborted")))
		Expect(queryStrings(db, `SELECT status || ' ' || finished_at FROM power_runs WHERE id = 2`)).
			To(Equal([]string{"complete 2026-07-19T01:00:00Z"}))
		Expect(queryStrings(db, `SELECT status || ' ' || finished_at FROM ingest_runs`)).
			To(Equal([]string{"complete 2026-07-18T10:30:00Z"}))
	})
})
