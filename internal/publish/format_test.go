package publish

import (
	"database/sql"
	"math"
	"testing"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("formatReal", func() {
	ginkgo.DescribeTable("writes fixed decimals per strconv 'f' semantics, with negative zero normalized",
		func(v float64, decimals int, want string) {
			Expect(formatReal(v, decimals)).To(Equal(want))
		},
		ginkgo.Entry("POWER temperature at 2", 24.475, 2, "24.48"),
		ginkgo.Entry("solar conversion tail rounded away", 228.472222222222, 2, "228.47"),
		ginkgo.Entry("DMS-derived station latitude at 6", 21.85027778, 6, "21.850278"),
		ginkgo.Entry("exact grid centroid padded to 6", -89.375, 6, "-89.375000"),
		ginkgo.Entry("CONAGUA one-decimal value verbatim", 110.3, 1, "110.3"),
		ginkgo.Entry("integral REAL padded", 9.0, 1, "9.0"),
		ginkgo.Entry("exact binary tie rounds half to even", 8.25, 1, "8.2"),
		ginkgo.Entry("inexact near-tie rounds by its true value", 0.45, 1, "0.5"),
		ginkgo.Entry("negative rounds away from zero as a decimal", -3.35, 1, "-3.4"),
		ginkgo.Entry("completeness fraction at 4", 35.0/36.0, 4, "0.9722"),
		ginkgo.Entry("smallest cont step survives at 4", 1.0/1080.0, 4, "0.0009"),
		ginkgo.Entry("zero decimals", 2.5, 0, "2"),
		ginkgo.Entry("zero", 0.0, 1, "0.0"),
		ginkgo.Entry("negative zero literal", math.Copysign(0, -1), 1, "0.0"),
		ginkgo.Entry("sub-precision negative at 1", -0.04, 1, "0.0"),
		ginkgo.Entry("sub-precision negative at 4", -0.00001, 4, "0.0000"),
		ginkgo.Entry("sub-precision negative at 0", -0.4, 0, "0"),
		ginkgo.Entry("negative that survives rounding keeps its sign", -0.05, 1, "-0.1"),
	)
})

// inf is +Inf, spelled so the literal 9e999 the DB specs seed and the
// value the formatter refuses are one thing.
func inf() float64 { return math.Inf(1) }

var _ = ginkgo.Describe("appendReal", func() {
	ginkgo.It("is formatReal's implementation: the same bytes appended after the buffer's contents", func() {
		for _, c := range []struct {
			v        float64
			decimals int
		}{{24.475, 2}, {228.472222222222, 2}, {-89.375, 6}, {8.25, 1}, {-0.04, 1}, {-0.00001, 4}, {-0.4, 0}, {-0.05, 1}, {0, 1}} {
			want := formatReal(c.v, c.decimals)
			Expect(string(appendReal(nil, c.v, c.decimals))).To(Equal(want))
			Expect(string(appendReal([]byte("x,"), c.v, c.decimals))).To(Equal("x," + want))
		}
	})

	ginkgo.It("normalizes a negative zero on the appended tail only, leaving a leading minus in the buffer alone", func() {
		Expect(string(appendReal([]byte("-1.0,"), -0.04, 1))).To(Equal("-1.0,0.0"))
		Expect(string(appendReal([]byte("-"), math.Copysign(0, -1), 2))).To(Equal("-0.00"))
	})

	ginkgo.It("does not allocate when the buffer has room", func() {
		buf := make([]byte, 0, 64)
		Expect(testing.AllocsPerRun(100, func() { buf = appendReal(buf[:0], -228.472222222222, 2) })).To(BeZero())
		Expect(string(buf)).To(Equal("-228.47"))
	})
})

var _ = ginkgo.Describe("checkFinite", func() {
	ginkgo.It("passes a finite value and refuses ±Inf and NaN by column name", func() {
		Expect(checkFinite("lat", 21.85)).To(Succeed())
		Expect(checkFinite("lat", math.Inf(1))).To(MatchError("column lat: non-finite value +Inf"))
		Expect(checkFinite("lat", math.Inf(-1))).To(MatchError("column lat: non-finite value -Inf"))
		Expect(checkFinite("lat", math.NaN())).To(MatchError("column lat: non-finite value NaN"))
	})
})

var _ = ginkgo.Describe("formatInt", func() {
	ginkgo.DescribeTable("writes base-10 integers",
		func(v int64, want string) {
			Expect(formatInt(v)).To(Equal(want))
		},
		ginkgo.Entry("zero", int64(0), "0"),
		ginkgo.Entry("a year", int64(1998), "1998"),
		ginkgo.Entry("negative", int64(-5), "-5"),
		ginkgo.Entry("max", int64(math.MaxInt64), "9223372036854775807"),
	)
})

var _ = ginkgo.Describe("scanTarget", func() {
	ginkgo.DescribeTable("hands out a nullable destination per Kind",
		func(kind Kind, want any) {
			Expect(scanTarget(Column{Kind: kind})).To(BeAssignableToTypeOf(want))
		},
		ginkgo.Entry("text", KindText, new(sql.NullString)),
		ginkgo.Entry("date", KindDate, new(sql.NullString)),
		ginkgo.Entry("period", KindPeriod, new(sql.NullString)),
		ginkgo.Entry("int", KindInt, new(sql.NullInt64)),
		ginkgo.Entry("real", KindReal, new(sql.NullFloat64)),
	)
})

var _ = ginkgo.Describe("formatCell", func() {
	ginkgo.DescribeTable("renders every Kind, NULL as the empty field",
		func(c Column, target any, want string) {
			got, err := formatCell(c, target)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(want))
		},
		ginkgo.Entry("text NULL", Column{Name: "name", Kind: KindText}, &sql.NullString{}, ""),
		ginkgo.Entry("text value verbatim", Column{Name: "name", Kind: KindText},
			&sql.NullString{String: `Mérida, "La Plancha"`, Valid: true}, `Mérida, "La Plancha"`),
		ginkgo.Entry("date NULL", Column{Name: "date", Kind: KindDate}, &sql.NullString{}, ""),
		ginkgo.Entry("date verbatim", Column{Name: "date", Kind: KindDate},
			&sql.NullString{String: "1998-05-14", Valid: true}, "1998-05-14"),
		ginkgo.Entry("period NULL", Column{Name: "period", Kind: KindPeriod}, &sql.NullString{}, ""),
		ginkgo.Entry("period verbatim", Column{Name: "period", Kind: KindPeriod},
			&sql.NullString{String: "1981-2010", Valid: true}, "1981-2010"),
		ginkgo.Entry("int NULL", Column{Name: "month", Kind: KindInt}, &sql.NullInt64{}, ""),
		ginkgo.Entry("int value", Column{Name: "month", Kind: KindInt}, &sql.NullInt64{Int64: 12, Valid: true}, "12"),
		ginkgo.Entry("real NULL", Column{Name: "tmax_c", Kind: KindReal, Decimals: 1}, &sql.NullFloat64{}, ""),
		ginkgo.Entry("real at the column's decimals", Column{Name: "lat", Kind: KindReal, Decimals: 6},
			&sql.NullFloat64{Float64: 21.85027778, Valid: true}, "21.850278"),
		ginkgo.Entry("real negative zero normalized", Column{Name: "tmin_c", Kind: KindReal, Decimals: 1},
			&sql.NullFloat64{Float64: -0.04, Valid: true}, "0.0"),
	)

	ginkgo.It("refuses a non-finite REAL as a corruption signal", func() {
		c := Column{Name: "lat", Kind: KindReal, Decimals: 6}
		_, err := formatCell(c, &sql.NullFloat64{Float64: math.Inf(1), Valid: true})
		Expect(err).To(MatchError(ContainSubstring("column lat: non-finite value +Inf")))
		_, err = formatCell(c, &sql.NullFloat64{Float64: math.NaN(), Valid: true})
		Expect(err).To(MatchError(ContainSubstring("non-finite")))
	})

	ginkgo.It("refuses a scan target it did not hand out", func() {
		_, err := formatCell(Column{Name: "x", Kind: KindText}, new(string))
		Expect(err).To(MatchError(ContainSubstring("column x: unsupported scan target *string")))
	})
})
