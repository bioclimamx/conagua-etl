package validate_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// The orphan-runs cases — a 'running' row older than the
// threshold is reconciled, a recent one is left alone, an old finished
// one is ignored — over both ledgers, with the finding text pinned as
// a fixed string and the reconciled rows read back in full.
var _ = Describe("orphan-runs", func() {
	long := func() string { return time.Now().UTC().Add(-72 * time.Hour).Format(time.RFC3339) }
	short := func() string { return time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339) }

	It("reconciles a 'running' row older than 24 h in either ledger, leaves a recent or finished one alone, and records each", func() {
		db, _ := openTempDB()
		oldStart := long()
		old := insertIngestRun(db, "running", oldStart)
		recent := insertIngestRun(db, "running", short())
		done := insertIngestRun(db, "complete", oldStart)
		pow := insertPowerRunAt(db, "running", oldStart)
		powRecent := insertPowerRunAt(db, "running", short())
		powDone := insertPowerRunAt(db, "aborted", oldStart)

		before := time.Now().UTC().Truncate(time.Second)
		res := runVerbRule(db, "", "orphan-runs")
		Expect(res.Scanned).To(Equal(2))
		Expect(issues(res.Findings)).To(Equal([]string{
			fmt.Sprintf("ingest_runs.id=%d started %s, status='running' beyond 24h0m0s — reconciled to 'aborted'", old, oldStart),
			fmt.Sprintf("power_runs.id=%d started %s, status='running' beyond 24h0m0s — reconciled to 'aborted'", pow, oldStart),
		}))
		for _, f := range res.Findings {
			Expect(f.RuleID).To(Equal("orphan-runs"))
			Expect(f.Severity).To(Equal(validate.SeverityWarn))
			Expect(f.StationID).To(BeNil())
		}

		for _, tc := range []struct {
			table string
			id    int64
		}{{"ingest_runs", old}, {"power_runs", pow}} {
			status, finished := runStatus(db, tc.table, tc.id)
			Expect(status).To(Equal("aborted"), tc.table)
			Expect(finished.Valid).To(BeTrue(), tc.table)
			stamp, err := time.Parse(time.RFC3339, finished.String)
			Expect(err).NotTo(HaveOccurred())
			Expect(stamp).To(BeTemporally(">=", before))
			Expect(stamp).To(BeTemporally("<=", time.Now().UTC()))
		}
		for _, tc := range []struct {
			table  string
			id     int64
			status string
		}{
			{"ingest_runs", recent, "running"}, {"ingest_runs", done, "complete"},
			{"power_runs", powRecent, "running"}, {"power_runs", powDone, "aborted"},
		} {
			status, finished := runStatus(db, tc.table, tc.id)
			Expect(status).To(Equal(tc.status), tc.table)
			Expect(finished.Valid).To(BeFalse(), tc.table)
		}
	})

	It("finds nothing on a second pass: the reconcile is idempotent", func() {
		db, _ := openTempDB()
		old := insertIngestRun(db, "running", long())
		first := runVerbRule(db, "", "orphan-runs")
		Expect(first.Findings).To(HaveLen(1))
		_, finished := runStatus(db, "ingest_runs", old)

		second := runVerbRule(db, "", "orphan-runs")
		Expect(second.Findings).To(BeEmpty())
		Expect(second.Scanned).To(BeZero())
		status, again := runStatus(db, "ingest_runs", old)
		Expect(status).To(Equal("aborted"))
		Expect(again).To(Equal(finished), "the first reconcile's stamp stands")
	})

	It("is silent when no run is stranded, whatever the age of the finished ones", func() {
		db, _ := openTempDB()
		seedClean(db)
		insertIngestRun(db, "aborted", "2020-01-01T00:00:00Z")
		insertIngestRun(db, "running", short())
		res := runVerbRule(db, "", "orphan-runs")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(BeZero())
	})
})
