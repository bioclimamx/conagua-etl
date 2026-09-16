package parity_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// The env-guarded validate parity harness: both builds must write the
// same parsing_warnings set — counts per rule plus issue text. It runs
// only when VALIDATE_PARITY_BASE_DB points at a copy the reference
// validate build already ran on and VALIDATE_PARITY_NEW_DB at an
// untouched copy of the same database; the spec runs this repo's
// validate on the new copy first — through schema.Open, since the verb
// writes — then compares the two copies' validate rows. The markdown
// report lands at VALIDATE_PARITY_REPORT_PATH (default
// ./validate-parity.md).
//
// Same input ⇒ identical output for the five reference rules, so every
// reference rule is hard-asserted with zero tolerance: identical
// multisets and equal per-rule counts. The native rules (no reference
// counterpart) are reported by row count only. Both copies are
// throwaways by contract: the new one is mutated here, and the
// orphan-runs rule is wall-clock dependent — it reports a stranded run
// on the pass that reconciles it, so each copy must see exactly one
// pass.
var _ = Describe("Validate parity harness", func() {
	It("runs this repo's validate on the new copy and compares its warnings to the reference build's", func(ctx SpecContext) {
		basePath := os.Getenv("VALIDATE_PARITY_BASE_DB")
		newPath := os.Getenv("VALIDATE_PARITY_NEW_DB")
		if basePath == "" || newPath == "" {
			Skip("VALIDATE_PARITY_BASE_DB / VALIDATE_PARITY_NEW_DB not set — validate parity harness skipped")
		}

		newDB, err := schema.Open(newPath)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(newDB.Close()).To(Succeed()) })

		report, err := validate.Run(ctx, newDB, validate.Options{
			OnRuleStart: func(id, name string) {
				GinkgoWriter.Printf("[%s] %-24s starting (%s)\n", time.Now().Format("15:04:05"), id, name)
			},
			OnRuleDone: func(id string, res validate.RuleResult, elapsed time.Duration) {
				warns, errs := 0, 0
				for _, f := range res.Findings {
					switch f.Severity {
					case validate.SeverityWarn:
						warns++
					case validate.SeverityError:
						errs++
					}
				}
				GinkgoWriter.Printf("[%s] %-24s done · scanned=%d warn=%d error=%d · %s\n",
					time.Now().Format("15:04:05"), id, res.Scanned, warns, errs, elapsed.Round(time.Millisecond))
			},
		})
		Expect(err).NotTo(HaveOccurred())
		GinkgoWriter.Printf("validate: period %s · %d warnings · %d errors · %s\n",
			report.Period, report.WarningsTotal, report.ErrorsTotal,
			report.FinishedAt.Sub(report.StartedAt).Round(time.Millisecond))

		baseDB, err := schema.OpenReadOnly(basePath)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(baseDB.Close()).To(Succeed()) })

		cmp, err := parity.CompareValidate(ctx, baseDB, newDB)
		Expect(err).NotTo(HaveOccurred())
		cmp.BaseLabel = basePath
		cmp.NewLabel = newPath
		for _, rr := range report.PerRule {
			cmp.NewSummary = append(cmp.NewSummary, parity.ValidateRuleSummary{
				ID: rr.ID, Scanned: rr.Scanned, Warnings: rr.Warnings, Errors: rr.Errors,
			})
		}

		// Write the report before any assertion so a failing gate still
		// leaves the full evidence on disk.
		reportPath := os.Getenv("VALIDATE_PARITY_REPORT_PATH")
		if reportPath == "" {
			reportPath = "validate-parity.md"
		}
		Expect(os.MkdirAll(filepath.Dir(reportPath), 0o755)).To(Succeed())
		Expect(os.WriteFile(reportPath, []byte(cmp.Markdown()), 0o644)).To(Succeed())
		GinkgoWriter.Printf("validate parity report: %s\n", reportPath)

		for _, r := range cmp.Rules {
			GinkgoWriter.Printf("%s: base %d / new %d rows; %d identical / %d base-only / %d new-only rows; "+
				"warn %d vs %d, error %d vs %d\n",
				r.ID, r.BaseRows, r.NewRows, r.Identical, r.BaseOnlyRows, r.NewOnlyRows,
				r.BaseWarnings, r.NewWarnings, r.BaseErrors, r.NewErrors)
		}
		for _, n := range cmp.Native {
			GinkgoWriter.Printf("%s: native rule — base %d / new %d rows (reported, not compared)\n",
				n.ID, n.BaseRows, n.NewRows)
		}

		// Vacuous-pass guard: a base copy without reference validate rows
		// means the env points at the wrong database — the gate must
		// compare the reference build's real output, never trivially pass.
		Expect(cmp.BaseRows()).To(BeNumerically(">", 0),
			"no reference validate rows in %s — did the reference validate run on it?", basePath)

		for _, r := range cmp.Rules {
			Expect(r.BaseOnlyRows).To(BeZero(),
				"%s: %d row(s) only in base (first: %s) — full report: %s",
				r.ID, r.BaseOnlyRows, firstSample(r.BaseOnly), reportPath)
			Expect(r.NewOnlyRows).To(BeZero(),
				"%s: %d row(s) only in new (first: %s) — full report: %s",
				r.ID, r.NewOnlyRows, firstSample(r.NewOnly), reportPath)
			Expect(r.NewRows).To(Equal(r.BaseRows),
				"%s: row count differs — full report: %s", r.ID, reportPath)
			Expect(r.Identical).To(Equal(r.BaseRows),
				"%s: rows vs identical mismatch — full report: %s", r.ID, reportPath)
			Expect([]int{r.NewWarnings, r.NewErrors}).To(Equal([]int{r.BaseWarnings, r.BaseErrors}),
				"%s: per-rule warn/error counts differ — full report: %s", r.ID, reportPath)

			// The verb's own summary must describe the rows it wrote —
			// the seam between Run's Report and the DB is this repo's.
			s, ok := cmp.Summary(r.ID)
			Expect(ok).To(BeTrue(), "%s: no Run summary — was the rule skipped?", r.ID)
			Expect([]int{s.Warnings, s.Errors}).To(Equal([]int{r.NewWarnings, r.NewErrors}),
				"%s: Run's report disagrees with the rows written", r.ID)
		}
		assertValidateRowsAnchor(newDB, report)
	})
})

// assertValidateRowsAnchor checks the verb's totals against the rows it
// left: every validate row on the new side, native rules included, is
// accounted for by the Report's totals.
func assertValidateRowsAnchor(db *sql.DB, report *validate.Report) {
	GinkgoHelper()
	var rows int
	Expect(db.QueryRow(`SELECT COUNT(*) FROM parsing_warnings WHERE source_file LIKE 'validate:%'`).
		Scan(&rows)).To(Succeed())
	Expect(rows).To(Equal(report.WarningsTotal+report.ErrorsTotal),
		"validate rows written vs the Report's totals")
}
