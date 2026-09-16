package publish

import (
	"database/sql"
	"testing"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/power"
)

var _ = ginkgo.Describe("appendJSONQuoted", func() {
	ginkgo.DescribeTable("quotes with RFC 8259's minimal escaping and passes UTF-8 and HTML characters through",
		func(in, want string) {
			Expect(string(appendJSONQuoted(nil, in))).To(Equal(want))
			Expect(string(appendJSONQuoted([]byte("x"), in))).To(Equal("x" + want))
		},
		ginkgo.Entry("a date", "1967-05-19", `"1967-05-19"`),
		ginkgo.Entry("empty", "", `""`),
		ginkgo.Entry("quote and backslash", `a"b\c`, `"a\"b\\c"`),
		ginkgo.Entry("newline, return, tab", "a\nb\rc\td", `"a\nb\rc\td"`),
		ginkgo.Entry("other control characters as \\u00XX", "\x00\x01\x1f", `"\u0000\u0001\u001f"`),
		ginkgo.Entry("UTF-8 verbatim", "Mérida, Yucatán", `"Mérida, Yucatán"`),
		ginkgo.Entry("HTML characters verbatim", "<a&b>", `"<a&b>"`),
		ginkgo.Entry("DEL verbatim", "\x7f", "\"\x7f\""),
	)
})

var _ = ginkgo.Describe("the daily.json row shape", func() {
	ginkgo.It("is derived from CombinedDaily: the date, the observed block, POWER-31, with the CSV's names and decimals", func() {
		Expect(dailyJSON.dateKey).To(Equal(`"date":`))
		Expect(dailyJSON.observed).To(HaveLen(4))
		Expect(dailyJSON.reanalysis).To(HaveLen(len(power.Registry)))
		Expect(dailyJSON.columns).To(HaveLen(1 + 4 + len(power.Registry)))

		var wantObserved, wantPower []Column
		for _, c := range CombinedDaily.Columns {
			switch {
			case c.DB == "date":
				Expect(dailyJSON.columns[0]).To(Equal(c))
			case CombinedDaily.Source(c) == DailyObservations.Table && !c.Key:
				wantObserved = append(wantObserved, c)
			case CombinedDaily.Source(c) == PowerDaily.Table:
				wantPower = append(wantPower, c)
			}
		}
		Expect(dailyJSON.observed).To(Equal(wantObserved))
		Expect(dailyJSON.reanalysis).To(Equal(wantPower))
		Expect(dailyJSON.columns[1:5]).To(Equal(wantObserved))
		Expect(dailyJSON.columns[5:]).To(Equal(wantPower))

		// The observed block is daily_observations under the export
		// names, at the two decimals CONAGUA's daily products actually
		// carry: trace ("inappreciable") rain publishes as 0.01 mm, so one decimal
		// would render a trace day as a dry 0.0 and contradict the SQLite
		// artifact shipped beside this file in the same deposit.
		var observedNames []string
		for i, c := range dailyJSON.observed {
			Expect(dailyJSON.observedKeys[i]).To(Equal(`"` + c.Name + `":`))
			Expect(c.Decimals).To(Equal(2), c.Name)
			observedNames = append(observedNames, c.Name)
		}
		Expect(observedNames).To(Equal([]string{"tmax_c", "tmin_c", "precip_mm", "evap_mm"}))
		for i, c := range dailyJSON.reanalysis {
			Expect(dailyJSON.reanalysisKeys[i]).To(Equal(`"` + c.Name + `":`))
			Expect(c.Name).To(Equal(power.Registry[i].Column))
		}
	})
})

var _ = ginkgo.Describe("the daily.json row assembler", func() {
	// A full row: every observed value and every POWER value present, so
	// the assembler renders 35 numbers through appendReal.
	fullRow := func() (observed, reanalysis []sql.NullFloat64) {
		observed = make([]sql.NullFloat64, len(dailyJSON.observed))
		for i := range observed {
			observed[i] = sql.NullFloat64{Float64: 30.5 + float64(i), Valid: true}
		}
		reanalysis = make([]sql.NullFloat64, len(dailyJSON.reanalysis))
		for i := range reanalysis {
			reanalysis[i] = sql.NullFloat64{Float64: -0.004 + 228.472222222222*float64(i%3), Valid: true}
		}
		return observed, reanalysis
	}

	ginkgo.It("allocates nothing per row once the buffer has grown: a full row appends into the reused buffer", func() {
		observed, reanalysis := fullRow()
		buf := make([]byte, 0, 4096)
		allocs := testing.AllocsPerRun(100, func() {
			var err error
			buf, err = dailyJSON.appendRow(buf[:0], "2020-01-01", observed, reanalysis, true)
			if err != nil {
				panic(err)
			}
		})
		Expect(allocs).To(BeZero())
		Expect(string(buf)).To(HavePrefix(`{"date":"2020-01-01","observed":{"tmax_c":30.50,"tmin_c":31.50,`))
		Expect(string(buf)).To(ContainSubstring(`"reanalysis":{"t2m_c":0.00,"t2m_max_c":228.47,`))
		Expect(string(buf)).NotTo(ContainSubstring("-0.00"))
	})

	ginkgo.It("renders the same literals the CSV formatter does, value for value", func() {
		observed, reanalysis := fullRow()
		observed[1].Valid = false
		reanalysis[3].Valid = false
		buf, err := dailyJSON.appendRow(nil, "2020-01-01", observed, reanalysis, true)
		Expect(err).NotTo(HaveOccurred())
		want := `{"date":"2020-01-01","observed":{`
		for i, c := range dailyJSON.observed {
			cell, err := formatCell(c, &observed[i])
			Expect(err).NotTo(HaveOccurred())
			if cell == "" {
				cell = "null"
			}
			if i > 0 {
				want += ","
			}
			want += `"` + c.Name + `":` + cell
		}
		want += `},"reanalysis":{`
		for i, c := range dailyJSON.reanalysis {
			cell, err := formatCell(c, &reanalysis[i])
			Expect(err).NotTo(HaveOccurred())
			if cell == "" {
				cell = "null"
			}
			if i > 0 {
				want += ","
			}
			want += `"` + c.Name + `":` + cell
		}
		want += "}}"
		Expect(string(buf)).To(Equal(want))
	})

	ginkgo.It("keeps a trace precipitation day at 0.01 rather than flatten it to a dry 0.0", func() {
		observed, reanalysis := fullRow()
		// CONAGUA publishes "inappreciable" rain as 0.01 mm. The second
		// decimal is the whole value here: at one decimal the assembler
		// would write 0.0, a dry day, for a day it rained.
		observed[2] = sql.NullFloat64{Float64: 0.01, Valid: true}
		Expect(dailyJSON.observed[2].Name).To(Equal("precip_mm"))
		buf, err := dailyJSON.appendRow(nil, "2020-01-01", observed, reanalysis, true)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(buf)).To(ContainSubstring(`"precip_mm":0.01,`))
		Expect(string(buf)).NotTo(ContainSubstring(`"precip_mm":0.0,`))
		Expect(string(buf)).NotTo(ContainSubstring(`"precip_mm":0.00,`))
	})

	ginkgo.It("refuses a date that is not valid UTF-8 rather than write it", func() {
		observed, reanalysis := fullRow()
		_, err := dailyJSON.appendRow(nil, "2020-01-0\xff", observed, reanalysis, true)
		Expect(err).To(MatchError(`date "2020-01-0\xff" is not valid UTF-8`))
	})

	ginkgo.It("refuses a non-finite value by column name, as the CSV does", func() {
		observed, reanalysis := fullRow()
		reanalysis[25].Float64 = inf()
		_, err := dailyJSON.appendRow(nil, "2020-01-01", observed, reanalysis, true)
		Expect(err).To(MatchError("column " + dailyJSON.reanalysis[25].Name + ": non-finite value +Inf"))
	})
})
