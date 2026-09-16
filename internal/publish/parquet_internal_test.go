package publish

// In-package specs for the Parquet value path: parquetReal is the one
// fixed-decimal formatter's result parsed back to a double, over a
// scratch buffer it hands back, and costs no allocation per cell.

import (
	"math"
	"testing"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("parquetReal", func() {
	col := Column{Name: "solar_ghi_wm2", DB: "solar_ghi_wm2", Kind: KindReal, Decimals: 2}

	ginkgo.It("is the nearest double of the decimal rounded to the fixed export precision, positive zero for a value that rounds to zero", func() {
		v, buf, err := parquetReal(col, 228.472222222222, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(math.Float64bits(v)).To(Equal(math.Float64bits(228.47)))
		Expect(string(buf)).To(Equal("228.47"))

		v, buf, err = parquetReal(col, -0.004, buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(0.0))
		Expect(math.Signbit(v)).To(BeFalse())
		Expect(string(buf)).To(Equal("0.00"))
	})

	ginkgo.It("refuses a non-finite value by column name, handing the scratch back", func() {
		buf := make([]byte, 0, 32)
		_, got, err := parquetReal(col, math.Inf(1), buf)
		Expect(err).To(MatchError("column solar_ghi_wm2: non-finite value +Inf"))
		Expect(cap(got)).To(Equal(cap(buf)))
	})

	ginkgo.It("does not allocate per cell once the scratch has room", func() {
		buf := make([]byte, 0, 32)
		var err error
		Expect(testing.AllocsPerRun(1000, func() {
			_, buf, err = parquetReal(col, -228.472222222222, buf)
		})).To(BeZero())
		Expect(err).NotTo(HaveOccurred())
		Expect(string(buf)).To(Equal("-228.47"))
	})
})
