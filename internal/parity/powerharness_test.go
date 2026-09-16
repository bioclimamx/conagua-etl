package parity_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// The env-guarded power parity harness. It runs only when
// PARITY_POWER_DB points at a reference ingest database with stored
// cells, links, and power runs; the markdown report lands at PARITY_POWER_REPORT_PATH
// (default: a temp dir).
//
// Everything here is offline and this repo's responsibility, so every
// comparison is hard-asserted with zero tolerance: the ported cell
// math must reproduce all stored nasa_power_grid_cells and
// station_power_cell rows float-exactly (609 cells and 5,524 links
// nationally), and every complete power_runs manifest must equal the
// pinned constants, the parameter registry, and a BuildURL
// round-trip. The DB opens through schema.OpenReadOnly — the reference
// database must never be written.
var _ = Describe("Power parity harness", func() {
	It("reproduces the reference DB's cell math and run manifests", func(ctx SpecContext) {
		dbPath := os.Getenv("PARITY_POWER_DB")
		if dbPath == "" {
			Skip("PARITY_POWER_DB not set — power parity harness skipped")
		}

		db, err := schema.OpenReadOnly(dbPath)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(db.Close()).To(Succeed()) })

		cmp, err := parity.ComparePower(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		cmp.Label = dbPath

		// Write the report before any assertion so a failing gate still
		// leaves the full evidence on disk.
		reportPath := os.Getenv("PARITY_POWER_REPORT_PATH")
		if reportPath == "" {
			reportPath = filepath.Join(GinkgoT().TempDir(), "power-report.md")
		}
		Expect(os.MkdirAll(filepath.Dir(reportPath), 0o755)).To(Succeed())
		Expect(os.WriteFile(reportPath, []byte(cmp.Markdown()), 0o644)).To(Succeed())
		GinkgoWriter.Printf("power parity report: %s\n", reportPath)

		GinkgoWriter.Printf("stations: %d; runs checked: %d, findings: %d\n",
			cmp.StationsCompared, len(cmp.Runs), cmp.ManifestFindings.Total)
		for _, t := range cmp.Tables() {
			GinkgoWriter.Printf("%s: derived %d / stored %d; %d compared / %d identical / %d value diffs\n",
				t.Table, t.DerivedRows(), t.StoredRows(),
				t.RowsCompared, t.RowsIdentical, t.ValueDiffs.Total)
		}

		// Vacuous-pass guard: an empty station, cell, or run universe
		// means the env points at the wrong database — the gate must
		// compare the real national data, never trivially pass.
		Expect(cmp.StationsCompared).To(BeNumerically(">", 0),
			"no stations with coordinates in %s — wrong DB?", dbPath)
		Expect(cmp.Cells.StoredRows()).To(BeNumerically(">", 0),
			"no nasa_power_grid_cells rows in %s — wrong DB?", dbPath)
		Expect(cmp.Runs).NotTo(BeEmpty(),
			"no complete power_runs rows in %s — wrong DB?", dbPath)

		for _, t := range cmp.Tables() {
			Expect(t.DerivedOnly.Total).To(BeZero(),
				"%s: %d derived key(s) missing from the DB (first: %s) — full report: %s",
				t.Table, t.DerivedOnly.Total, firstSample(t.DerivedOnly), reportPath)
			Expect(t.StoredOnly.Total).To(BeZero(),
				"%s: %d stored row(s) not derivable from any station (first: %s) — full report: %s",
				t.Table, t.StoredOnly.Total, firstSample(t.StoredOnly), reportPath)
			Expect(t.ValueDiffs.Total).To(BeZero(),
				"%s: %d value diff(s) (first: %s) — full report: %s",
				t.Table, t.ValueDiffs.Total, firstSample(t.ValueDiffs), reportPath)
			Expect(t.RowsIdentical).To(Equal(t.RowsCompared),
				"%s: compared vs identical mismatch — full report: %s", t.Table, reportPath)
		}

		Expect(cmp.ManifestFindings.Total).To(BeZero(),
			"%d manifest finding(s) (first: %s) — full report: %s",
			cmp.ManifestFindings.Total, firstSample(cmp.ManifestFindings), reportPath)
	})
})
