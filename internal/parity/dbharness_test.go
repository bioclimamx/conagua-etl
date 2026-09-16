package parity_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// The env-guarded ingest-DB parity harness. It runs only when PARITY_BASE_DB and PARITY_NEW_DB point at
// ingest databases built from the *same* snapshot; the markdown report
// lands at PARITY_DB_REPORT_PATH (default: a temp dir).
//
// Same input ⇒ identical output, so unlike the cross-snapshot harness
// every comparison is hard-asserted with zero tolerance: identical
// station universes, zero value diffs and zero one-side-only rows in
// stations/normals/extras/daily, and identical warnings multisets —
// plus, per side, the counters anchor: the latest ingest_runs row must
// be self-consistent with that side's live tables.
// Both databases open through schema.OpenReadOnly — the base is the
// reference ingest database and must never be written.
var _ = Describe("Ingest DB parity harness", func() {
	It("compares two same-snapshot ingest databases row for row", func(ctx SpecContext) {
		basePath := os.Getenv("PARITY_BASE_DB")
		newPath := os.Getenv("PARITY_NEW_DB")
		if basePath == "" || newPath == "" {
			Skip("PARITY_BASE_DB / PARITY_NEW_DB not set — ingest DB harness skipped")
		}

		baseDB, err := schema.OpenReadOnly(basePath)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(baseDB.Close()).To(Succeed()) })
		newDB, err := schema.OpenReadOnly(newPath)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(newDB.Close()).To(Succeed()) })

		cmp, err := parity.CompareDBs(ctx, baseDB, newDB)
		Expect(err).NotTo(HaveOccurred())
		cmp.BaseLabel = basePath
		cmp.NewLabel = newPath

		// Write the report before any assertion so a failing gate still
		// leaves the full evidence on disk.
		reportPath := os.Getenv("PARITY_DB_REPORT_PATH")
		if reportPath == "" {
			reportPath = filepath.Join(GinkgoT().TempDir(), "db-report.md")
		}
		Expect(os.MkdirAll(filepath.Dir(reportPath), 0o755)).To(Succeed())
		Expect(os.WriteFile(reportPath, []byte(cmp.Markdown()), 0o644)).To(Succeed())
		GinkgoWriter.Printf("ingest DB parity report: %s\n", reportPath)

		for _, t := range cmp.Tables() {
			GinkgoWriter.Printf("%s: %d compared / %d identical / %d value diffs / %d base-only / %d new-only\n",
				t.Table, t.RowsCompared, t.RowsIdentical,
				t.ValueDiffs.Total, t.BaseOnly.Total, t.NewOnly.Total)

			Expect(t.BaseOnly.Total).To(BeZero(),
				"%s: %d row(s) only in base (first: %s) — full report: %s",
				t.Table, t.BaseOnly.Total, firstSample(t.BaseOnly), reportPath)
			Expect(t.NewOnly.Total).To(BeZero(),
				"%s: %d row(s) only in new (first: %s) — full report: %s",
				t.Table, t.NewOnly.Total, firstSample(t.NewOnly), reportPath)
			Expect(t.ValueDiffs.Total).To(BeZero(),
				"%s: %d value diff(s) (first: %s) — full report: %s",
				t.Table, t.ValueDiffs.Total, firstSample(t.ValueDiffs), reportPath)
			Expect(t.RowsIdentical).To(Equal(t.RowsCompared),
				"%s: compared vs identical mismatch — full report: %s", t.Table, reportPath)
		}

		// Counters anchor: each side's latest ingest_runs row must be
		// self-consistent with that side's live tables — the run
		// manifest is provenance, so a counter that disagrees with the
		// rows it claims to describe fails the gate on either side.
		assertRunCountersAnchor(ctx, baseDB, basePath)
		assertRunCountersAnchor(ctx, newDB, newPath)
	})
})

// assertRunCountersAnchor checks one side's latest ingest_runs row
// against its live tables: per-row counters equal COUNT(*) of the table
// each summarizes, and the station tally is internally consistent
// (attempted = succeeded + failed). A side with no runs skips cleanly.
func assertRunCountersAnchor(ctx context.Context, db *sql.DB, label string) {
	GinkgoHelper()
	var (
		runID                                             int64
		attempted, succeeded, failed                      sql.NullInt64
		dailyRows, normalsRows, extrasRows, warningsTotal sql.NullInt64
	)
	err := db.QueryRowContext(ctx, `
SELECT id, stations_attempted, stations_succeeded, stations_failed,
       daily_rows, normals_rows, extras_rows, warnings_total
  FROM ingest_runs ORDER BY id DESC LIMIT 1`).
		Scan(&runID, &attempted, &succeeded, &failed,
			&dailyRows, &normalsRows, &extrasRows, &warningsTotal)
	if errors.Is(err, sql.ErrNoRows) {
		GinkgoWriter.Printf("%s: no ingest_runs rows — counters anchor skipped\n", label)
		return
	}
	Expect(err).NotTo(HaveOccurred(), "%s: read latest ingest_runs row", label)

	count := func(table string) sql.NullInt64 {
		var n int64
		Expect(db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&n)).
			To(Succeed(), "%s: count %s", label, table)
		return sql.NullInt64{Int64: n, Valid: true}
	}
	// Comparing sql.NullInt64 values asserts validity too: a NULL
	// counter on the latest run (e.g. a never-closed row) fails here.
	Expect(dailyRows).To(Equal(count("daily_observations")),
		"%s: ingest_runs %d daily_rows vs daily_observations", label, runID)
	Expect(normalsRows).To(Equal(count("monthly_normals")),
		"%s: ingest_runs %d normals_rows vs monthly_normals", label, runID)
	Expect(extrasRows).To(Equal(count("monthly_normals_extras")),
		"%s: ingest_runs %d extras_rows vs monthly_normals_extras", label, runID)
	Expect(warningsTotal).To(Equal(count("parsing_warnings")),
		"%s: ingest_runs %d warnings_total vs parsing_warnings", label, runID)
	Expect(succeeded.Valid && failed.Valid).To(BeTrue(),
		"%s: ingest_runs %d station tallies must be non-NULL", label, runID)
	Expect(attempted).To(Equal(sql.NullInt64{Int64: succeeded.Int64 + failed.Int64, Valid: true}),
		"%s: ingest_runs %d stations_attempted vs succeeded+failed", label, runID)
}

// firstSample renders the first retained sample for an assertion
// message; safe when the category is empty (the message goes unused).
func firstSample[T any](s parity.Sampled[T]) string {
	if len(s.Samples) == 0 {
		return "none"
	}
	return fmt.Sprintf("%+v", s.Samples[0])
}
