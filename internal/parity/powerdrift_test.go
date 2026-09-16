package parity_test

import (
	"database/sql"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
	"github.com/bioclimamx/conagua-etl/internal/power"
)

// driftColumn finds one column's tally by name.
func driftColumn(t parity.DriftTable, name string) parity.DriftColumn {
	for _, c := range t.Columns {
		if c.Column == name {
			return c
		}
	}
	Fail("column " + name + " not present in DriftTable")
	return parity.DriftColumn{}
}

var _ = Describe("ComparePowerDrift", func() {
	var (
		baseDB, newDB *sql.DB
		cellA, cellB  string
	)

	// The seeded scenario exercises every drift class in both tables:
	// an aligned row mixing identical / revised / appended / removed
	// values, a base-only row, and a new-only row in a second cell (so
	// the per-cell streaming walks a cell union, not one side's list).
	BeforeEach(func() {
		baseDB = openIngestDB()
		newDB = openIngestDB()
		cellA = powerCellShared().ID
		cellB = powerCellSolo().ID

		insertMonthlySupplementRow(baseDB, cellA, "1991-2020", 1,
			map[string]float64{"t2m_c": 20.5, "rh2m_pct": 50.25, "ws2m_ms": 3.5})
		insertMonthlySupplementRow(baseDB, cellA, "1991-2020", 2,
			map[string]float64{"t2m_c": 19})
		insertMonthlySupplementRow(newDB, cellA, "1991-2020", 1,
			map[string]float64{"t2m_c": 20.5, "rh2m_pct": 51.5, "precip_mmpd": 1.25})
		insertMonthlySupplementRow(newDB, cellB, "1981-2010", 3,
			map[string]float64{"evland_mmpd": 2.5, "gwet_top": 0.5})

		insertDailySupplementRow(baseDB, cellA, "1985-01-01",
			map[string]float64{"t2m_c": 15.5})
		insertDailySupplementRow(baseDB, cellB, "1985-01-05",
			map[string]float64{"ws10m_ms": 4.5})
		insertDailySupplementRow(newDB, cellA, "1985-01-01",
			map[string]float64{"t2m_c": 15.75, "cloud_amt_pct": 60.5})
		insertDailySupplementRow(newDB, cellA, "1985-01-02",
			map[string]float64{"t2m_c": 16})
	})

	It("classifies every monthly_supplement value per row and column", func(ctx SpecContext) {
		drift, err := parity.ComparePowerDrift(ctx, baseDB, newDB)
		Expect(err).NotTo(HaveOccurred())

		m := drift.Monthly
		Expect(m.Table).To(Equal("monthly_supplement"))
		Expect(m.RowsAligned).To(Equal(1))
		Expect(m.BaseRows()).To(Equal(2))
		Expect(m.NewRows()).To(Equal(2))
		Expect(m.BaseOnly.Total).To(Equal(1))
		Expect(m.BaseOnly.Samples[0]).To(Equal(parity.DBKey{Row: cellA + " 1991-2020/m02"}))
		Expect(m.NewOnly.Total).To(Equal(1))
		Expect(m.NewOnly.Samples[0]).To(Equal(parity.DBKey{Row: cellB + " 1981-2010/m03"}))

		Expect(m.Values).To(Equal(parity.DriftCounts{
			Identical: 28, Revised: 1, Appended: 3, Removed: 2,
		}))
		Expect(driftColumn(m, "t2m_c").DriftCounts).To(Equal(parity.DriftCounts{Identical: 1, Removed: 1}))
		Expect(driftColumn(m, "rh2m_pct").DriftCounts).To(Equal(parity.DriftCounts{Revised: 1}))
		Expect(driftColumn(m, "ws2m_ms").DriftCounts).To(Equal(parity.DriftCounts{Removed: 1}))
		Expect(driftColumn(m, "precip_mmpd").DriftCounts).To(Equal(parity.DriftCounts{Appended: 1}))
		// The aligned row holds a both-NULL (identical) value for these
		// two, on top of the new-only row's appended one.
		Expect(driftColumn(m, "evland_mmpd").DriftCounts).To(Equal(parity.DriftCounts{Identical: 1, Appended: 1}))
		Expect(driftColumn(m, "gwet_top").DriftCounts).To(Equal(parity.DriftCounts{Identical: 1, Appended: 1}))
		Expect(driftColumn(m, "solar_ghi_wm2").DriftCounts).To(Equal(parity.DriftCounts{Identical: 1}))

		Expect(m.RevisedSamples.Total).To(Equal(1))
		Expect(m.RevisedSamples.Samples[0]).To(Equal(parity.DBDiff{
			Key:    parity.DBKey{Row: cellA + " 1991-2020/m01"},
			Column: "rh2m_pct", Base: "50.25", New: "51.5",
		}))
	})

	It("classifies every daily_supplement value per row and column", func(ctx SpecContext) {
		drift, err := parity.ComparePowerDrift(ctx, baseDB, newDB)
		Expect(err).NotTo(HaveOccurred())

		d := drift.Daily
		Expect(d.Table).To(Equal("daily_supplement"))
		Expect(d.RowsAligned).To(Equal(1))
		Expect(d.BaseOnly.Total).To(Equal(1))
		Expect(d.BaseOnly.Samples[0]).To(Equal(parity.DBKey{Row: cellB + " 1985-01-05"}))
		Expect(d.NewOnly.Total).To(Equal(1))
		Expect(d.NewOnly.Samples[0]).To(Equal(parity.DBKey{Row: cellA + " 1985-01-02"}))

		Expect(d.Values).To(Equal(parity.DriftCounts{
			Identical: 29, Revised: 1, Appended: 2, Removed: 1,
		}))
		Expect(driftColumn(d, "t2m_c").DriftCounts).To(Equal(parity.DriftCounts{Revised: 1, Appended: 1}))
		Expect(driftColumn(d, "cloud_amt_pct").DriftCounts).To(Equal(parity.DriftCounts{Appended: 1}))
		Expect(driftColumn(d, "ws10m_ms").DriftCounts).To(Equal(parity.DriftCounts{Identical: 1, Removed: 1}))

		Expect(d.RevisedSamples.Total).To(Equal(1))
		Expect(d.RevisedSamples.Samples[0]).To(Equal(parity.DBDiff{
			Key:    parity.DBKey{Row: cellA + " 1985-01-01"},
			Column: "t2m_c", Base: "15.5", New: "15.75",
		}))
	})

	It("carries one tally per registry column, in registry order", func(ctx SpecContext) {
		drift, err := parity.ComparePowerDrift(ctx, baseDB, newDB)
		Expect(err).NotTo(HaveOccurred())

		for _, t := range drift.Tables() {
			Expect(t.Columns).To(HaveLen(len(power.Registry)))
			for i, p := range power.Registry {
				Expect(t.Columns[i].Column).To(Equal(p.Column))
			}
		}
	})

	It("reports two empty databases as fully clean", func(ctx SpecContext) {
		drift, err := parity.ComparePowerDrift(ctx, openIngestDB(), openIngestDB())
		Expect(err).NotTo(HaveOccurred())
		for _, t := range drift.Tables() {
			Expect(t.RowsAligned).To(BeZero())
			Expect(t.BaseOnly.Total).To(BeZero())
			Expect(t.NewOnly.Total).To(BeZero())
			Expect(t.Values).To(Equal(parity.DriftCounts{}))
		}
	})
})
