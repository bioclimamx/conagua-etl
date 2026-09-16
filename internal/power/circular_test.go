package power_test

import (
	"math"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/power"
)

var _ = Describe("CircularMeanDegrees", func() {
	It("vector-averages across the 0/360° boundary and lands on the closed upper edge", func() {
		// 5° and 355° cancel in sine to a negative residue so small that
		// the fold to [0, 360) yields exactly 360.0 — the closed boundary
		// the rollup pins bit for bit; never the arithmetic 180.
		Expect(power.CircularMeanDegrees([]float64{5, 355})).To(Equal(360.0))
		Expect(power.CircularMeanDegrees([]float64{350, 10, 350, 10, 350, 10, 350, 10, 350, 10, 350, 10})).To(Equal(360.0))
	})

	It("averages interior angles arithmetically and passes a single angle through", func() {
		Expect(power.CircularMeanDegrees([]float64{80, 100})).To(BeNumerically("~", 90.0, 1e-9))
		Expect(power.CircularMeanDegrees([]float64{270})).To(BeNumerically("~", 270.0, 1e-9))
		Expect(power.CircularMeanDegrees([]float64{90, 90})).To(BeNumerically("~", 90.0, 1e-9))
	})

	It("never leaves [0, 360]", func() {
		for _, in := range [][]float64{{0, 0}, {359.9, 0.1}, {180, 180}, {90, 270, 0}, {45, 135, 225, 315, 1}} {
			got := power.CircularMeanDegrees(in)
			Expect(got).To(SatisfyAll(BeNumerically(">=", 0.0), BeNumerically("<=", 360.0)), "%v", in)
		}
	})

	It("yields NaN on an empty input, which callers guard", func() {
		Expect(math.IsNaN(power.CircularMeanDegrees(nil))).To(BeTrue())
	})

	It("is the mean the monthly rollup writes, bit for bit", func() {
		got := power.RollUp(powerResponse(map[string]map[string]float64{
			"WD2M": {"199101": 5.0, "199201": 355.0, "199102": 80.0, "199202": 100.0, "199103": 270.0},
		}), 1991, 2020)["WD2M"]
		Expect(got[1].Mean).To(Equal(360.0))
		Expect(got[2].Mean).To(Equal(power.CircularMeanDegrees([]float64{80, 100})))
		Expect(got[3].Mean).To(Equal(power.CircularMeanDegrees([]float64{270})))
	})
})
