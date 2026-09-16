package publish

// In-package specs for the annual slot (index 12 of a month series): the
// three aggregations, the all-twelve-or-null rule, the closed circular
// boundary, and the dispatch table's coverage of every month-series
// variable that carries a derived annual — the five CONAGUA normals and
// POWER-31 — exactly once, with the extras columns absent.

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/power"
)

// twelve pins exactly twelve months (a shorter list panics on the index,
// which is what a fixture typo should do).
func twelve(vals ...float64) [12]*float64 {
	var out [12]*float64
	for i := range out {
		out[i] = &vals[i]
	}
	return out
}

func withNil(months [12]*float64, slots ...int) [12]*float64 {
	for _, s := range slots {
		months[s] = nil
	}
	return months
}

// The 1991-2020 normals of station 1001 as CONAGUA publishes them
// (internal/conagua/testdata/normals/real_1991_2020_01001.txt), whose
// annual column reads 27.4 for tmax and 431.9 for precip.
var (
	fixtureTmax   = twelve(23, 25.5, 27.9, 30.4, 32, 30.5, 28.4, 28.2, 27.2, 26.8, 25.1, 23.4)
	fixturePrecip = twelve(7.1, 10.8, 5.4, 2.2, 14.9, 81.6, 101.5, 84.7, 78.2, 28, 10.1, 7.4)
)

var _ = ginkgo.Describe("the derived annual aggregations", func() {
	ginkgo.It("means the twelve months unweighted, CONAGUA's own convention", func() {
		got := meanAnnual(fixtureTmax)
		Expect(got).NotTo(BeNil())
		Expect(formatReal(*got, 1)).To(Equal("27.4"))
		Expect(*got).To(BeNumerically("~", 328.4/12, 1e-12))
	})

	ginkgo.It("sums a monthly total, never means it", func() {
		got := sumAnnual(fixturePrecip)
		Expect(got).NotTo(BeNil())
		Expect(formatReal(*got, 1)).To(Equal("431.9"))
		mean := meanAnnual(fixturePrecip)
		Expect(formatReal(*mean, 1)).NotTo(Equal(formatReal(*got, 1)))
	})

	ginkgo.It("takes the circular mean of directions through power's function, closed boundary included", func() {
		got := circularAnnual(twelve(350, 10, 350, 10, 350, 10, 350, 10, 350, 10, 350, 10))
		Expect(got).NotTo(BeNil())
		Expect(*got).To(Equal(360.0))
		Expect(*got).To(Equal(power.CircularMeanDegrees([]float64{350, 10, 350, 10, 350, 10, 350, 10, 350, 10, 350, 10})))
		Expect(formatReal(*got, 1)).To(Equal("360.0"))

		interior := circularAnnual(twelve(80, 100, 80, 100, 80, 100, 80, 100, 80, 100, 80, 100))
		Expect(*interior).To(BeNumerically("~", 90.0, 1e-9))
	})

	ginkgo.DescribeTable("yields null unless all twelve months are present (slot 12 null under a partial year)",
		func(kind annualKind) {
			full := twelve(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
			Expect(kind.fold(full)).NotTo(BeNil())
			Expect(kind.fold(withNil(full, 0))).To(BeNil())
			Expect(kind.fold(withNil(full, 11))).To(BeNil())
			Expect(kind.fold(withNil(full, 5))).To(BeNil())
			Expect(kind.fold([12]*float64{})).To(BeNil())
		},
		ginkgo.Entry("mean", annualMean),
		ginkgo.Entry("sum", annualSum),
		ginkgo.Entry("circular", annualCircular),
	)

	ginkgo.It("folds through the kind", func() {
		full := twelve(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
		Expect(*annualSum.fold(full)).To(Equal(78.0))
		Expect(*annualMean.fold(full)).To(Equal(6.5))
		Expect(*annualCircular.fold(full)).To(Equal(power.CircularMeanDegrees([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})))
	})
})

var _ = ginkgo.Describe("the derived annual dispatch table", func() {
	ginkgo.It("covers every normals variable and every POWER-31 column exactly once, and nothing else", func() {
		var want []string
		for _, c := range valueColumns(MonthlyNormals) {
			want = append(want, c.Name)
		}
		for _, p := range power.Registry {
			want = append(want, p.Column)
		}
		Expect(want).To(HaveLen(5 + 31))
		var got []string
		for name := range annualBy {
			got = append(got, name)
		}
		Expect(got).To(ConsistOf(want))
	})

	ginkgo.It("sums CONAGUA precip_mm and evap_mm, means the temperatures", func() {
		Expect(annualBy).To(HaveKeyWithValue("precip_mm", annualSum))
		Expect(annualBy).To(HaveKeyWithValue("evap_mm", annualSum))
		Expect(annualBy).To(HaveKeyWithValue("tmax_c", annualMean))
		Expect(annualBy).To(HaveKeyWithValue("tmin_c", annualMean))
		Expect(annualBy).To(HaveKeyWithValue("tmean_c", annualMean))
	})

	ginkgo.It("takes the circular mean for exactly the registry's angles and the mean for every other POWER column", func() {
		for _, p := range power.Registry {
			if p.Circular {
				Expect(annualBy).To(HaveKeyWithValue(p.Column, annualCircular), p.Column)
			} else {
				Expect(annualBy).To(HaveKeyWithValue(p.Column, annualMean), p.Column)
			}
		}
		Expect(annualBy["wd2m_deg"]).To(Equal(annualCircular))
		Expect(annualBy["wd10m_deg"]).To(Equal(annualCircular))
		Expect(annualBy["precip_mmpd"]).To(Equal(annualMean))
	})

	ginkgo.It("names no monthly_normals_extras column, so their slot 12 stays null", func() {
		for _, c := range valueColumns(MonthlyNormalsExtras) {
			Expect(annualBy).NotTo(HaveKey(c.Name))
		}
	})
})
