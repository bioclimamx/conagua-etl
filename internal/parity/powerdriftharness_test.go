package parity_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// The env-guarded supplement drift harness. It runs only when
// POWER_DRIFT_BASE_DB and POWER_DRIFT_NEW_DB point at two power
// databases (typically a reference database and a re-pull of it);
// the markdown report lands at POWER_DRIFT_REPORT_PATH (default: a
// temp dir).
//
// The comparison is evidence, never a gate: the base supplement rows came from live pulls of a
// re-versioning reanalysis product, so classified drift is upstream
// movement — the spec asserts only that the comparison ran and the
// report exists on disk. Both databases open through
// schema.OpenReadOnly.
var _ = Describe("Power supplement drift harness", func() {
	It("classifies supplement drift between the two databases", func(ctx SpecContext) {
		basePath := os.Getenv("POWER_DRIFT_BASE_DB")
		newPath := os.Getenv("POWER_DRIFT_NEW_DB")
		if basePath == "" || newPath == "" {
			Skip("POWER_DRIFT_BASE_DB / POWER_DRIFT_NEW_DB not set — drift harness skipped")
		}

		baseDB, err := schema.OpenReadOnly(basePath)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(baseDB.Close()).To(Succeed()) })
		newDB, err := schema.OpenReadOnly(newPath)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(newDB.Close()).To(Succeed()) })

		drift, err := parity.ComparePowerDrift(ctx, baseDB, newDB)
		Expect(err).NotTo(HaveOccurred())
		drift.BaseLabel = basePath
		drift.NewLabel = newPath

		reportPath := os.Getenv("POWER_DRIFT_REPORT_PATH")
		if reportPath == "" {
			reportPath = filepath.Join(GinkgoT().TempDir(), "power-drift-report.md")
		}
		Expect(os.MkdirAll(filepath.Dir(reportPath), 0o755)).To(Succeed())
		Expect(os.WriteFile(reportPath, []byte(drift.Markdown()), 0o644)).To(Succeed())
		GinkgoWriter.Printf("power drift report: %s\n", reportPath)

		for _, t := range drift.Tables() {
			GinkgoWriter.Printf(
				"%s: %d base / %d new rows; %d aligned / %d base-only / %d new-only; "+
					"values %d identical / %d revised / %d appended / %d removed\n",
				t.Table, t.BaseRows(), t.NewRows(), t.RowsAligned,
				t.BaseOnly.Total, t.NewOnly.Total,
				t.Values.Identical, t.Values.Revised, t.Values.Appended, t.Values.Removed)
		}

		// The only assertion: the comparison completed and its evidence
		// is durable. Drift itself is reported, never asserted.
		info, err := os.Stat(reportPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size()).To(BeNumerically(">", 0))
	})
})
