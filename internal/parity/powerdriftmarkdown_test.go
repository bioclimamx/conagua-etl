package parity_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
)

var _ = Describe("PowerDrift.Markdown", func() {
	It("round-trips the summary, per-parameter tallies, and samples", func() {
		d := &parity.PowerDrift{
			BaseLabel: "/db/base.db",
			NewLabel:  "/db/new.db",
			Monthly: parity.DriftTable{
				Table:       "monthly_supplement",
				RowsAligned: 5,
				BaseOnly: parity.Sampled[parity.DBKey]{Total: 1, Samples: []parity.DBKey{
					{Row: "19.5N_99.3750W 1991-2020/m02"},
				}},
				NewOnly: parity.Sampled[parity.DBKey]{Total: 1, Samples: []parity.DBKey{
					{Row: "25.0N_100.6250W 1981-2010/m03"},
				}},
				Values: parity.DriftCounts{Identical: 100, Revised: 2, Appended: 3, Removed: 4},
				Columns: []parity.DriftColumn{
					{Column: "t2m_c", DriftCounts: parity.DriftCounts{Identical: 5, Revised: 2}},
				},
				RevisedSamples: parity.Sampled[parity.DBDiff]{Total: 2, Samples: []parity.DBDiff{
					{Key: parity.DBKey{Row: "19.5N_99.3750W 1991-2020/m01"}, Column: "t2m_c", Base: "1.5", New: "2.5"},
				}},
			},
			Daily: parity.DriftTable{Table: "daily_supplement"},
		}

		md := d.Markdown()

		Expect(md).To(ContainSubstring("# POWER supplement drift report"))
		Expect(md).To(ContainSubstring("- Base DB: `/db/base.db`"))
		Expect(md).To(ContainSubstring("- New DB: `/db/new.db`"))
		Expect(md).To(ContainSubstring("Report-only evidence"))

		// Summary rows: base/new totals derive from aligned + one-side-only.
		Expect(md).To(ContainSubstring("| monthly_supplement | 6 | 6 | 5 | 1 | 1 | 100 | 2 | 3 | 4 |"))
		Expect(md).To(ContainSubstring("| daily_supplement | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |"))

		Expect(md).To(ContainSubstring("### Per-parameter drift"))
		Expect(md).To(ContainSubstring("| t2m_c | 5 | 2 | 0 | 0 |"))

		Expect(md).To(ContainSubstring("### Revised samples (2)"))
		Expect(md).To(ContainSubstring("| 19.5N_99.3750W 1991-2020/m01 | t2m_c | 1.5 | 2.5 |"))
		Expect(md).To(ContainSubstring("_showing first 1 of 2_"))

		Expect(md).To(ContainSubstring("### Base-only rows (1)"))
		Expect(md).To(ContainSubstring("| 19.5N_99.3750W 1991-2020/m02 |"))
		Expect(md).To(ContainSubstring("### New-only rows (1)"))
		Expect(md).To(ContainSubstring("| 25.0N_100.6250W 1981-2010/m03 |"))

		// The empty daily table renders its no-rows prose.
		Expect(md).To(ContainSubstring("No rows on either side."))
	})

	It("renders two empty databases without findings sections", func(ctx SpecContext) {
		drift, err := parity.ComparePowerDrift(ctx, openIngestDB(), openIngestDB())
		Expect(err).NotTo(HaveOccurred())
		drift.BaseLabel = "/db/base.db"
		drift.NewLabel = "/db/new.db"

		md := drift.Markdown()
		Expect(md).To(ContainSubstring("| monthly_supplement | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("No rows on either side."))
		Expect(md).NotTo(ContainSubstring("### "), "empty tables must render no findings sections")
	})
})
