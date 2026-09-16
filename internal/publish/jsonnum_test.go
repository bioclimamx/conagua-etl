package publish

// In-package specs for the profile's scalar cells and the ordered month
// block: each cell renders the fixed-decimal literal the CSV writer
// emits or an explicit null, a non-finite REAL is refused, HTML is never
// escaped under the profile encoder, and a block's keys follow its
// spec's column order.

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"math"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// encodeNoEscape renders v the way the profile writer does, compact.
func encodeNoEscape(v any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "", err
	}
	return string(bytes.TrimSuffix(buf.Bytes(), []byte("\n"))), nil
}

func mustEncode(v any) string {
	ginkgo.GinkgoHelper()
	s, err := encodeNoEscape(v)
	Expect(err).NotTo(HaveOccurred())
	return s
}

var _ = ginkgo.Describe("jsonReal", func() {
	ginkgo.DescribeTable("marshals the fixed-decimal literal formatReal gives",
		func(v float64, decimals int, want string) {
			Expect(mustEncode(realPtr(&v, decimals))).To(Equal(want))
			Expect(mustEncode(realOf(sql.NullFloat64{Float64: v, Valid: true}, decimals))).To(Equal(want))
		},
		ginkgo.Entry("one decimal, integral", 31.0, 1, "31.0"),
		ginkgo.Entry("two decimals rounded", 24.475, 2, "24.48"),
		ginkgo.Entry("solar tail rounded away", 228.472222222222, 2, "228.47"),
		ginkgo.Entry("six decimals", 20.65, 6, "20.650000"),
		ginkgo.Entry("negative", -3.35, 1, "-3.4"),
		ginkgo.Entry("sub-precision negative is positive zero", -0.04, 1, "0.0"),
		ginkgo.Entry("four decimals", 35.0/36.0, 4, "0.9722"),
	)

	ginkgo.It("marshals null for an absent value and never omits the key", func() {
		Expect(mustEncode(realPtr(nil, 2))).To(Equal("null"))
		Expect(mustEncode(realOf(sql.NullFloat64{}, 2))).To(Equal("null"))
		Expect(mustEncode(struct {
			V jsonReal `json:"v"`
		}{})).To(Equal(`{"v":null}`))
	})

	ginkgo.It("refuses a non-finite value", func() {
		for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
			_, err := encodeNoEscape(realPtr(&v, 1))
			Expect(err).To(MatchError(ContainSubstring("non-finite value")))
		}
	})

	ginkgo.It("renders the same literal inside a struct, a pointer, and an interface slot", func() {
		v := 9.0
		r := realPtr(&v, 1)
		Expect(mustEncode(struct{ V jsonReal }{r})).To(Equal(`{"V":9.0}`))
		Expect(mustEncode(&r)).To(Equal("9.0"))
		Expect(mustEncode([]json.Marshaler{r})).To(Equal("[9.0]"))
	})
})

var _ = ginkgo.Describe("jsonInt and jsonText", func() {
	ginkgo.It("marshals digits or null", func() {
		Expect(mustEncode(jsonInt{v: sql.NullInt64{Int64: 1998, Valid: true}})).To(Equal("1998"))
		Expect(mustEncode(jsonInt{v: sql.NullInt64{Int64: -7, Valid: true}})).To(Equal("-7"))
		Expect(mustEncode(jsonInt{})).To(Equal("null"))
	})

	ginkgo.It("marshals the string verbatim or null, with no HTML escaping", func() {
		Expect(mustEncode(jsonText{v: sql.NullString{String: "1998-05-14", Valid: true}})).To(Equal(`"1998-05-14"`))
		Expect(mustEncode(jsonText{v: sql.NullString{String: `Mérida, "La Plancha" <&>`, Valid: true}})).
			To(Equal(`"Mérida, \"La Plancha\" <&>"`))
		Expect(mustEncode(jsonText{})).To(Equal("null"))
	})
})

var _ = ginkgo.Describe("cellOf and nullCell", func() {
	ginkgo.It("converts each scan target kind into the cell that renders it, copying the value", func() {
		real := Column{Name: "tmax_c", Kind: KindReal, Decimals: 1}
		f := sql.NullFloat64{Float64: 30.55, Valid: true}
		cell, err := cellOf(real, &f)
		Expect(err).NotTo(HaveOccurred())
		f.Float64 = 0
		Expect(mustEncode(cell)).To(Equal("30.6"))

		i := sql.NullInt64{Int64: 3, Valid: true}
		cell, err = cellOf(Column{Name: "n", Kind: KindInt}, &i)
		Expect(err).NotTo(HaveOccurred())
		Expect(mustEncode(cell)).To(Equal("3"))

		s := sql.NullString{String: "x", Valid: true}
		cell, err = cellOf(Column{Name: "t", Kind: KindText}, &s)
		Expect(err).NotTo(HaveOccurred())
		Expect(mustEncode(cell)).To(Equal(`"x"`))

		_, err = cellOf(real, new(string))
		Expect(err).To(MatchError(ContainSubstring("column tmax_c: unsupported scan target")))
	})

	ginkgo.It("accepts exactly what scanTarget yields", func() {
		for _, c := range []Column{{Name: "a", Kind: KindReal}, {Name: "b", Kind: KindInt}, {Name: "c", Kind: KindText}, {Name: "d", Kind: KindDate}} {
			cell, err := cellOf(c, scanTarget(c))
			Expect(err).NotTo(HaveOccurred())
			Expect(mustEncode(cell)).To(Equal("null"))
			Expect(mustEncode(nullCell(c))).To(Equal("null"))
		}
	})
})

var _ = ginkgo.Describe("monthBlock", func() {
	cols := []Column{
		{Name: "tmax_c", Kind: KindReal, Decimals: 1},
		{Name: "year", Kind: KindInt},
		{Name: "date", Kind: KindDate},
	}

	ginkgo.It("starts with every slot an explicit null and keeps the key order of its columns", func() {
		b := newMonthBlock(cols)
		Expect(mustEncode(b)).To(Equal(`{"tmax_c":[null,null,null,null,null,null,null,null,null,null,null,null,null],` +
			`"year":[null,null,null,null,null,null,null,null,null,null,null,null,null],` +
			`"date":[null,null,null,null,null,null,null,null,null,null,null,null,null]}`))
	})

	ginkgo.It("renders each slot through its cell and a nil block as null", func() {
		b := newMonthBlock(cols)
		v := 31.0
		b.series[0][0] = realPtr(&v, 1)
		b.series[1][11] = jsonInt{v: sql.NullInt64{Int64: 1998, Valid: true}}
		b.series[2][12] = jsonText{v: sql.NullString{String: "1998-05-14", Valid: true}}
		Expect(mustEncode(b)).To(Equal(`{"tmax_c":[31.0,null,null,null,null,null,null,null,null,null,null,null,null],` +
			`"year":[null,null,null,null,null,null,null,null,null,null,null,1998,null],` +
			`"date":[null,null,null,null,null,null,null,null,null,null,null,null,"1998-05-14"]}`))
		Expect(mustEncode(map[string]*monthBlock{"1961-1990": nil, "1991-2020": b})).
			To(HavePrefix(`{"1961-1990":null,"1991-2020":{"tmax_c":[31.0,`))
	})

	ginkgo.It("derives slot 12 per the dispatch and leaves an unlisted series null", func() {
		b := newMonthBlock([]Column{
			{Name: "precip_mm", Kind: KindReal, Decimals: 1},
			{Name: "tmax_c", Kind: KindReal, Decimals: 1},
			{Name: "rain_days", Kind: KindReal, Decimals: 1},
		})
		for m := range 12 {
			for i := range b.cols {
				v := float64(m + 1)
				b.series[i][m] = realPtr(&v, 1)
			}
		}
		b.deriveAnnual()
		Expect(mustEncode(b.series[0][12])).To(Equal("78.0"))
		Expect(mustEncode(b.series[1][12])).To(Equal("6.5"))
		Expect(mustEncode(b.series[2][12])).To(Equal("null"))
	})

	ginkgo.It("names the column when a slot cannot be rendered", func() {
		b := newMonthBlock(cols)
		inf := math.Inf(1)
		b.series[0][3] = realPtr(&inf, 1)
		_, err := b.MarshalJSON()
		Expect(err).To(MatchError(ContainSubstring("column tmax_c")))
		Expect(err).To(MatchError(ContainSubstring("non-finite")))
	})
})
