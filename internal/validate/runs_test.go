package validate_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

var _ = Describe("runs-in-flight", func() {
	It("is silent when no run is running, whatever the age of the finished ones", func() {
		db, _ := openTempDB()
		seedClean(db)
		insertIngestRun(db, "aborted", "2020-01-01T00:00:00Z")
		insertPowerRun(db, "aborted", "monthly", 1961, 1990)
		res := runRule(db, "runs-in-flight")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(0))
	})

	It("flags every running row of both ledgers as an error with no age threshold", func() {
		db, _ := openTempDB()
		seedClean(db)
		old := insertIngestRun(db, "running", "2020-01-01T00:00:00Z")
		recent := insertIngestRun(db, "running", "2026-06-09T09:59:00Z")
		pow := insertPowerRun(db, "running", "daily", 2026, 2026)

		res := runRule(db, "runs-in-flight")
		Expect(res.Scanned).To(Equal(3))
		Expect(severities(res.Findings)).To(Equal([]string{
			validate.SeverityError, validate.SeverityError, validate.SeverityError,
		}))
		Expect(issues(res.Findings)).To(Equal([]string{
			"ingest_runs.id=" + itoa(old) + " started 2020-01-01T00:00:00Z, status='running' — a run in flight or stranded cannot be vouched for (finish it, or reconcile through validate)",
			"ingest_runs.id=" + itoa(recent) + " started 2026-06-09T09:59:00Z, status='running' — a run in flight or stranded cannot be vouched for (finish it, or reconcile through validate)",
			"power_runs.id=" + itoa(pow) + " started 2026-06-09T10:00:00Z, status='running' — a run in flight or stranded cannot be vouched for (finish it, or reconcile through validate)",
		}))
		for _, f := range res.Findings {
			Expect(f.RuleID).To(Equal("runs-in-flight"))
			Expect(f.StationID).To(BeNil())
		}
	})
})

var _ = Describe("ingest-complete", func() {
	It("is silent with a complete run and counts the ledger", func() {
		db, _ := openTempDB()
		seedClean(db)
		insertIngestRun(db, "aborted", "2026-06-07T10:00:00Z")
		res := runRule(db, "ingest-complete")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(2))
	})

	It("errors on an empty ledger", func() {
		db, _ := openTempDB()
		res := runRule(db, "ingest-complete")
		Expect(res.Scanned).To(Equal(0))
		Expect(res.Findings).To(HaveLen(1))
		Expect(res.Findings[0].Severity).To(Equal(validate.SeverityError))
		Expect(res.Findings[0].RuleID).To(Equal("ingest-complete"))
		Expect(res.Findings[0].Issue).To(Equal("ingest_runs has no complete row (0 rows in total): nothing to publish"))
	})

	It("errors when every run is aborted or running", func() {
		db, _ := openTempDB()
		insertIngestRun(db, "aborted", "2026-06-07T10:00:00Z")
		insertIngestRun(db, "running", "2026-06-08T10:00:00Z")
		res := runRule(db, "ingest-complete")
		Expect(res.Scanned).To(Equal(2))
		Expect(issues(res.Findings)).To(Equal([]string{
			"ingest_runs has no complete row (2 rows in total): nothing to publish",
		}))
	})
})
