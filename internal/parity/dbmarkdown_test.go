package parity_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
)

var _ = Describe("DBComparison.Markdown", func() {
	It("round-trips every findings category into its section table", func() {
		cmp := &parity.DBComparison{
			BaseLabel: "/db/base.db",
			NewLabel:  "/db/new.db",
			Stations: parity.TableComparison{
				Table:         "stations",
				RowsCompared:  3,
				RowsIdentical: 2,
				ValueDiffs: parity.Sampled[parity.DBDiff]{Total: 1, Samples: []parity.DBDiff{
					{Key: dbKey(""), Column: "name", Base: "OLD | NAME", New: "NEW NAME"},
				}},
				BaseOnly: parity.Sampled[parity.DBKey]{Total: 1, Samples: []parity.DBKey{
					{Source: seedSource, ExternalID: "2002"},
				}},
				NewOnly: parity.Sampled[parity.DBKey]{Total: 2, Samples: []parity.DBKey{
					{Source: seedSource, ExternalID: "3003"},
				}},
			},
			Normals: parity.TableComparison{
				Table:         "monthly_normals",
				RowsCompared:  24,
				RowsIdentical: 23,
				ValueDiffs: parity.Sampled[parity.DBDiff]{Total: 1, Samples: []parity.DBDiff{
					{Key: dbKey("1961-1990/m01"), Column: "tmax", Base: "25.5", New: "NULL"},
				}},
			},
			Extras: parity.TableComparison{Table: "monthly_normals_extras", RowsCompared: 12, RowsIdentical: 12},
			Daily:  parity.TableComparison{Table: "daily_observations", RowsCompared: 1000, RowsIdentical: 1000},
			Warnings: parity.TableComparison{
				Table:         "parsing_warnings",
				RowsCompared:  2,
				RowsIdentical: 1,
				ValueDiffs: parity.Sampled[parity.DBDiff]{Total: 1, Samples: []parity.DBDiff{
					{Key: dbKey(`daily/1001.txt:57 warn "unparseable TMAX"`), Column: "count", Base: "2", New: "1"},
				}},
			},
			BaseRun: seededRunCounters(2),
			NewRun: &parity.RunCounters{
				ID:           5,
				StartedAt:    "2026-07-19T08:00:00Z",
				SnapshotDate: "2026-06-08",
				SinkKind:     "r2",
				Status:       "running",
			},
		}

		md := cmp.Markdown()

		Expect(md).To(ContainSubstring("# Ingest DB parity report"))
		Expect(md).To(ContainSubstring("- Base DB: `/db/base.db`"))
		Expect(md).To(ContainSubstring("- New DB: `/db/new.db`"))

		// Summary rows carry the exact tallies, in column order; the
		// per-side row totals derive from compared + one-side-only.
		Expect(md).To(ContainSubstring("| stations | 4 | 5 | 3 | 2 | 1 | 1 | 2 |"))
		Expect(md).To(ContainSubstring("| monthly_normals | 24 | 24 | 24 | 23 | 1 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("| monthly_normals_extras | 12 | 12 | 12 | 12 | 0 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("| daily_observations | 1000 | 1000 | 1000 | 1000 | 0 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("| parsing_warnings | 2 | 2 | 2 | 1 | 1 | 0 | 0 |"))

		// The run section renders both sides' columns verbatim,
		// including the NULL columns of the unfinished new-side run.
		Expect(md).To(ContainSubstring("## Latest ingest run (reported, not compared)"))
		Expect(md).To(ContainSubstring("| id | 2 | 5 |"))
		Expect(md).To(ContainSubstring("| started_at | 2026-07-18T10:00:00Z | 2026-07-19T08:00:00Z |"))
		Expect(md).To(ContainSubstring("| finished_at | 2026-07-18T10:05:00Z | NULL |"))
		Expect(md).To(ContainSubstring("| snapshot_date | 2026-06-08 | 2026-06-08 |"))
		Expect(md).To(ContainSubstring("| sink_kind | local | r2 |"))
		Expect(md).To(ContainSubstring("| etl_git_sha | abc1234 | NULL |"))
		Expect(md).To(ContainSubstring("| status | complete | running |"))
		Expect(md).To(ContainSubstring("| stations_attempted | 3 | NULL |"))
		Expect(md).To(ContainSubstring("| stations_succeeded | 2 | NULL |"))
		Expect(md).To(ContainSubstring("| stations_failed | 1 | NULL |"))
		Expect(md).To(ContainSubstring("| daily_rows | 100 | NULL |"))
		Expect(md).To(ContainSubstring("| normals_rows | 24 | NULL |"))
		Expect(md).To(ContainSubstring("| extras_rows | 24 | NULL |"))
		Expect(md).To(ContainSubstring("| warnings_total | 3 | NULL |"))

		Expect(md).To(ContainSubstring("## stations"))
		Expect(md).To(ContainSubstring("### Value diffs (1)"))
		Expect(md).To(ContainSubstring(`| conagua_conventional/1001 | name | OLD \| NAME | NEW NAME |`))
		Expect(md).To(ContainSubstring("### Base-only rows (1)"))
		Expect(md).To(ContainSubstring("| conagua_conventional/2002 |"))
		Expect(md).To(ContainSubstring("### New-only rows (2)"))
		Expect(md).To(ContainSubstring("| conagua_conventional/3003 |"))
		Expect(md).To(ContainSubstring("_showing first 1 of 2_"))

		// Row discriminators render inside the key cell.
		Expect(md).To(ContainSubstring("| conagua_conventional/1001 1961-1990/m01 | tmax | 25.5 | NULL |"))
		Expect(md).To(ContainSubstring(`| conagua_conventional/1001 daily/1001.txt:57 warn "unparseable TMAX" | count | 2 | 1 |`))

		// Clean tables render no-drift prose instead of sections.
		Expect(md).To(ContainSubstring("No drift: all 12 compared rows identical."))
		Expect(md).To(ContainSubstring("No drift: all 1000 compared rows identical."))
	})

	It("renders a fully clean comparison with no findings sections", func(ctx SpecContext) {
		baseDB, newDB := seedPair()
		cmp := mustCompare(ctx, baseDB, newDB)
		cmp.BaseLabel = "/db/base.db"
		cmp.NewLabel = "/db/new.db"

		md := cmp.Markdown()

		Expect(md).To(ContainSubstring("| stations | 1 | 1 | 1 | 1 | 0 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("| monthly_normals | 2 | 2 | 2 | 2 | 0 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("| monthly_normals_extras | 1 | 1 | 1 | 1 | 0 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("| daily_observations | 3 | 3 | 3 | 3 | 0 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("| parsing_warnings | 2 | 2 | 2 | 2 | 0 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("No drift: all 3 compared rows identical."))
		Expect(md).NotTo(ContainSubstring("### "), "a clean comparison must render no findings sections")
	})

	It("says so when neither side carries a run row", func() {
		cmp := &parity.DBComparison{}
		Expect(cmp.Markdown()).To(ContainSubstring("Neither database carries an ingest_runs row."))
	})

	It("marks a missing single side's run explicitly", func() {
		cmp := &parity.DBComparison{BaseRun: seededRunCounters(1)}
		Expect(cmp.Markdown()).To(ContainSubstring("| id | 1 | — (no run) |"))
	})
})

var _ = Describe("Sampled counters", func() {
	It("retains the first DBSampleCap samples and keeps the total exact", func() {
		var s parity.Sampled[int]
		for i := range parity.DBSampleCap + 3 {
			s.Add(i)
		}
		Expect(s.Total).To(Equal(parity.DBSampleCap + 3))
		want := make([]int, parity.DBSampleCap)
		for i := range want {
			want[i] = i
		}
		Expect(s.Samples).To(Equal(want))
		Expect(s.Truncated()).To(BeTrue())
	})

	It("is not truncated while under the cap", func() {
		var s parity.Sampled[string]
		s.Add("x")
		Expect(s.Total).To(Equal(1))
		Expect(s.Samples).To(Equal([]string{"x"}))
		Expect(s.Truncated()).To(BeFalse())
	})
})

var _ = Describe("DBKey.String", func() {
	It("renders station-only, station+row, and stationless keys", func() {
		Expect(parity.DBKey{Source: "s", ExternalID: "e"}.String()).To(Equal("s/e"))
		Expect(parity.DBKey{Source: "s", ExternalID: "e", Row: "r"}.String()).To(Equal("s/e r"))
		Expect(parity.DBKey{Row: "r"}.String()).To(Equal("r"))
	})
})

var _ = Describe("TableComparison", func() {
	It("derives per-side row totals from compared plus one-side-only", func() {
		t := parity.TableComparison{RowsCompared: 5}
		for range 2 {
			t.BaseOnly.Add(parity.DBKey{})
		}
		t.NewOnly.Add(parity.DBKey{})
		Expect(t.BaseRows()).To(Equal(7))
		Expect(t.NewRows()).To(Equal(6))
		Expect(t.Clean()).To(BeFalse())
	})

	It("is clean only with zero findings in every category", func() {
		Expect(parity.TableComparison{RowsCompared: 5, RowsIdentical: 5}.Clean()).To(BeTrue())
		diff := parity.TableComparison{}
		diff.ValueDiffs.Add(parity.DBDiff{})
		Expect(diff.Clean()).To(BeFalse())
	})
})
