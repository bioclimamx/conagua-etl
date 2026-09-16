package validate_test

import (
	"context"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// The gate over a real database, read-only — the national DB in
// practice — reporting each rule's scan count, findings, and wall time
// to the Ginkgo writer (run with -v to see it). Skipped unless
// VALIDATE_GATE_DB names the file; asserts only that every rule
// evaluates, since the findings are the DB's to have.
var _ = Describe("Gate over VALIDATE_GATE_DB", func() {
	It("evaluates every rule read-only and reports", func() {
		path := os.Getenv("VALIDATE_GATE_DB")
		if path == "" {
			Skip("VALIDATE_GATE_DB not set")
		}
		db, err := schema.OpenReadOnly(path)
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(db.Close()).To(Succeed()) }()

		report, err := validate.Gate(context.Background(), db, func(rr validate.RuleReport) {
			GinkgoWriter.Printf("%-18s scanned=%-10d warn=%-4d error=%-4d %s\n",
				rr.ID, rr.Scanned, rr.Warnings, rr.Errors, rr.Elapsed.Round(1e6))
			for _, f := range rr.Findings {
				GinkgoWriter.Printf("    [%s] %s\n", f.Severity, f.Issue)
			}
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Rules).To(HaveLen(len(validate.GateRules())))
		GinkgoWriter.Printf("total warn=%d error=%d elapsed=%s\n",
			report.Warnings, report.Errors, report.FinishedAt.Sub(report.StartedAt).Round(1e6))
	})
})
