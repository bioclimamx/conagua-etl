package parity_test

import (
	"fmt"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
)

// The env-guarded cross-snapshot harness. It runs only when both
// PARITY_BASE_DIR and PARITY_NEW_DIR point at snapshot date-dirs; the
// markdown report lands at PARITY_REPORT_PATH (default: a temp dir).
//
// Hard assertions cover only what this repo controls: our parsers must
// parse every file on both sides, and the station universes per
// value-compared kind must match. Data drift between snapshots
// (appended/revised values) is upstream reality, not our test subject —
// it is reported, never asserted.
var _ = Describe("Cross-snapshot parity harness", func() {
	It("value-compares the base and new snapshots", func(ctx SpecContext) {
		baseDir := os.Getenv("PARITY_BASE_DIR")
		newDir := os.Getenv("PARITY_NEW_DIR")
		if baseDir == "" || newDir == "" {
			Skip("PARITY_BASE_DIR / PARITY_NEW_DIR not set — cross-snapshot harness skipped")
		}

		report, err := parity.CompareSnapshots(ctx, baseDir, newDir)
		Expect(err).NotTo(HaveOccurred())

		// Write the report before any assertion so a failing gate still
		// leaves the full evidence on disk.
		reportPath := os.Getenv("PARITY_REPORT_PATH")
		if reportPath == "" {
			reportPath = filepath.Join(GinkgoT().TempDir(), "report.md")
		}
		Expect(os.MkdirAll(filepath.Dir(reportPath), 0o755)).To(Succeed())
		Expect(os.WriteFile(reportPath, []byte(report.Markdown()), 0o644)).To(Succeed())
		GinkgoWriter.Printf("parity report: %s\n", reportPath)

		for _, kr := range report.Kinds {
			if !kr.ValueCompared {
				GinkgoWriter.Printf("%s: presence-only (no parser) — base %d / new %d files\n",
					kr.Kind, kr.BaseFiles, kr.NewFiles)
				continue
			}

			GinkgoWriter.Printf(
				"%s: %d both / %d base-only / %d new-only; %d identical, %d revised stations; "+
					"rows %d identical / %d appended / %d removed / %d revised; "+
					"%d field revisions, %d header diffs, %d warning-count diffs\n",
				kr.Kind, kr.StationsBoth, kr.BaseOnly.Total, kr.NewOnly.Total,
				kr.StationsIdentical, kr.StationsRevised,
				kr.RowsIdentical, kr.RowsAppended.Total, kr.RowsRemoved.Total, kr.RowsRevised,
				kr.Revisions.Total, kr.HeaderDiffs.Total, kr.WarningDiffs.Total)

			// Hard gate 1: our parsers parse every national file on
			// both sides — the pull parity claim.
			Expect(kr.ParseErrors.Total).To(BeZero(),
				"kind %s: %d parse error(s); first: %s — full report: %s",
				kr.Kind, kr.ParseErrors.Total, firstFinding(kr.ParseErrors), reportPath)

			// Hard gate 2: identical station universes per compared kind.
			Expect(kr.BaseOnly.Total).To(BeZero(),
				"kind %s: %d station(s) only in base (first: %s) — full report: %s",
				kr.Kind, kr.BaseOnly.Total, firstFinding(kr.BaseOnly), reportPath)
			Expect(kr.NewOnly.Total).To(BeZero(),
				"kind %s: %d station(s) only in new (first: %s) — full report: %s",
				kr.Kind, kr.NewOnly.Total, firstFinding(kr.NewOnly), reportPath)
		}
	})
})

// firstFinding renders the first retained example for an assertion
// message; safe when the category is empty (the message goes unused).
func firstFinding[T any](c parity.Capped[T]) string {
	if len(c.Examples) == 0 {
		return "none"
	}
	return fmt.Sprintf("%+v", c.Examples[0])
}
