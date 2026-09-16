package validate_test

// The gate over each of the schema package's two handles: the
// read-only handle publish uses — proven read-only in the same spec by
// the writes it refuses — evaluates every rule to completion with
// findings of every severity; and a writer handle,
// though it could write, leaves the file byte for byte as it found it
// once the gate has run over a DB that tempts every write path (a
// stranded run to reconcile, warnings to record).

import (
	"context"
	"database/sql"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// seedTempting seeds a clean DB plus one finding of each severity from
// a rule that would write on the validate verb's path: a station
// outside MX (warn), a stranded ingest run (error), and a leaked fill
// (error). Returns the stranded run's id.
func seedTempting(db *sql.DB) int64 {
	GinkgoHelper()
	s := seedClean(db)
	station(db, "outside", "London", f64(51.5), f64(-0.1))
	stranded := insertIngestRun(db, "running", "2020-01-01T00:00:00Z")
	insertDaily(db, cellA, "2020-01-02", s.dailyRun, map[string]any{"t2m_c": -999.0})
	return stranded
}

// temptingErrors is the error-finding text seedTempting yields, in rule
// order.
func temptingErrors(stranded int64) []string {
	return []string{
		fmt.Sprintf("ingest_runs.id=%d started 2020-01-01T00:00:00Z, status='running' — a run in flight or stranded "+
			"cannot be vouched for (finish it, or reconcile through validate)", stranded),
		"daily_supplement.t2m_c: 1 row holding POWER's -999 fill (" + cellA + "/2020-01-02)",
	}
}

var _ = Describe("Gate over each handle", func() {
	It("evaluates every rule over the read-only handle that refuses a write, query_only on", func() {
		db, path := openTempDB()
		stranded := seedTempting(db)
		Expect(db.Close()).To(Succeed())

		ro, err := schema.OpenReadOnly(path)
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(ro.Close()).To(Succeed()) }()
		var queryOnly int
		Expect(ro.QueryRow(`PRAGMA query_only`).Scan(&queryOnly)).To(Succeed())
		Expect(queryOnly).To(Equal(1))
		// The writes the validate verb would make are exactly what this
		// handle refuses.
		_, err = ro.Exec(`INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
		  VALUES (NULL, 'validate:bbox', NULL, 'warn', 'x')`)
		Expect(err).To(MatchError(ContainSubstring("readonly")))
		_, err = ro.Exec(`UPDATE ingest_runs SET status = 'aborted' WHERE id = ?`, stranded)
		Expect(err).To(MatchError(ContainSubstring("readonly")))

		report, err := validate.Gate(context.Background(), ro, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Rules).To(HaveLen(len(validate.GateRules())))
		Expect(report.Warnings).To(Equal(1))
		Expect(report.Errors).To(Equal(2))
		Expect(issues(report.ErrorFindings())).To(Equal(temptingErrors(stranded)))
		Expect(issues(report.Rules[0].Findings)).To(Equal([]string{
			`station conagua_conventional/outside ("London") at lat=51.5000 lon=-0.1000 outside MX bbox [14.5,32.8]×[-118.5,-86.7] — likely typo`,
		}))

		// Still nothing written, through this handle or any other.
		var n int
		Expect(ro.QueryRow(`SELECT COUNT(*) FROM parsing_warnings WHERE source_file LIKE 'validate:%'`).Scan(&n)).To(Succeed())
		Expect(n).To(BeZero())
		var status string
		Expect(ro.QueryRow(`SELECT status FROM ingest_runs WHERE id = ?`, stranded).Scan(&status)).To(Succeed())
		Expect(status).To(Equal("running"))
	})

	It("leaves a database opened through schema.Open byte-identical after the gate", func() {
		db, path := openTempDB()
		stranded := seedTempting(db)
		Expect(db.Close()).To(Succeed())
		Expect(path+"-wal").NotTo(BeAnExistingFile(), "the writer's close checkpoints and removes the WAL")
		before := sha256Of(path)

		w, err := schema.Open(path)
		Expect(err).NotTo(HaveOccurred())
		report, err := validate.Gate(context.Background(), w, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(w.Close()).To(Succeed())
		Expect(report.Warnings).To(Equal(1))
		Expect(report.Errors).To(Equal(2))
		Expect(issues(report.ErrorFindings())).To(Equal(temptingErrors(stranded)))

		Expect(path + "-wal").NotTo(BeAnExistingFile())
		Expect(sha256Of(path)).To(Equal(before))
	})
})
