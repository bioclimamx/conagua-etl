package conagua

import (
	"bytes"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Expected parse of testdata/normals/real_1991_2020_01001.txt — a
// golden fixture (real station 01001, period 1991-2020). Every value is
// a recorded reference parse of the same fixture, so these tables pin
// ParseNormalsFile's output value for value. The file carries no HUMEDAD RELATIVA section,
// mirroring the whole CONAGUA conventional archive (no observed RH).
var wantReal01001Rows = []MonthlyNormalsRow{
	{Month: 1, Tmax: fp(23), Tmin: fp(4.9), Tmean: fp(13.9), Precip: fp(7.1), Evap: fp(98.5)},
	{Month: 2, Tmax: fp(25.5), Tmin: fp(6.3), Tmean: fp(15.9), Precip: fp(10.8), Evap: fp(121.4)},
	{Month: 3, Tmax: fp(27.9), Tmin: fp(8), Tmean: fp(17.9), Precip: fp(5.4), Evap: fp(184.7)},
	{Month: 4, Tmax: fp(30.4), Tmin: fp(10.5), Tmean: fp(20.4), Precip: fp(2.2), Evap: fp(213.1)},
	{Month: 5, Tmax: fp(32), Tmin: fp(13.4), Tmean: fp(22.7), Precip: fp(14.9), Evap: fp(221.4)},
	{Month: 6, Tmax: fp(30.5), Tmin: fp(15.6), Tmean: fp(23), Precip: fp(81.6), Evap: fp(191.1)},
	{Month: 7, Tmax: fp(28.4), Tmin: fp(14.9), Tmean: fp(21.7), Precip: fp(101.5), Evap: fp(156.4)},
	{Month: 8, Tmax: fp(28.2), Tmin: fp(14.9), Tmean: fp(21.5), Precip: fp(84.7), Evap: fp(149.1)},
	{Month: 9, Tmax: fp(27.2), Tmin: fp(14.4), Tmean: fp(20.8), Precip: fp(78.2), Evap: fp(117.6)},
	{Month: 10, Tmax: fp(26.8), Tmin: fp(11.7), Tmean: fp(19.3), Precip: fp(28), Evap: fp(117.7)},
	{Month: 11, Tmax: fp(25.1), Tmin: fp(7.7), Tmean: fp(16.4), Precip: fp(10.1), Evap: fp(92.3)},
	{Month: 12, Tmax: fp(23.4), Tmin: fp(5.2), Tmean: fp(14.3), Precip: fp(7.4), Evap: fp(87.1)},
}

var wantReal01001Extras = []MonthlyNormalsExtrasRow{
	{
		Month:              1,
		TmaxMonthlyExtreme: fp(24.9), TmaxMonthlyExtremeYear: ip(2017), TmaxDailyExtreme: fp(29.2), TmaxDailyExtremeDate: sp("2018-01-13"),
		TminMonthlyExtreme: fp(2.5), TminMonthlyExtremeYear: ip(1999), TminDailyExtreme: fp(-9), TminDailyExtremeDate: sp("2016-01-17"),
		PrecipMonthlyExtreme: fp(42.6), PrecipMonthlyExtremeYear: ip(2010), PrecipDailyExtreme: fp(20.3), PrecipDailyExtremeDate: sp("2020-01-02"),
		TmaxYearsWithData: ip(30), TminYearsWithData: ip(30), TmeanYearsWithData: ip(30), PrecipYearsWithData: ip(30), EvapYearsWithData: ip(28),
		RainDays: fp(2.4), RainDaysYearsWithData: ip(30),
	},
	{
		Month:              2,
		TmaxMonthlyExtreme: fp(27.4), TmaxMonthlyExtremeYear: ip(2019), TmaxDailyExtreme: fp(31.6), TmaxDailyExtremeDate: sp("2019-02-28"),
		TminMonthlyExtreme: fp(3.9), TminMonthlyExtremeYear: ip(1998), TminDailyExtreme: fp(-1.4), TminDailyExtremeDate: sp("2004-02-02"),
		PrecipMonthlyExtreme: fp(102.8), PrecipMonthlyExtremeYear: ip(2010), PrecipDailyExtreme: fp(38.3), PrecipDailyExtremeDate: sp("2010-02-02"),
		TmaxYearsWithData: ip(30), TminYearsWithData: ip(30), TmeanYearsWithData: ip(30), PrecipYearsWithData: ip(30), EvapYearsWithData: ip(29),
		RainDays: fp(1.9), RainDaysYearsWithData: ip(30),
	},
	{
		Month:              3,
		TmaxMonthlyExtreme: fp(30.7), TmaxMonthlyExtremeYear: ip(2018), TmaxDailyExtreme: fp(38), TmaxDailyExtremeDate: sp("2000-03-28"),
		TminMonthlyExtreme: fp(5.9), TminMonthlyExtremeYear: ip(1993), TminDailyExtreme: fp(-1), TminDailyExtremeDate: sp("2013-03-03"),
		PrecipMonthlyExtreme: fp(93.3), PrecipMonthlyExtremeYear: ip(2015), PrecipDailyExtreme: fp(42.3), PrecipDailyExtremeDate: sp("2015-03-15"),
		TmaxYearsWithData: ip(29), TminYearsWithData: ip(29), TmeanYearsWithData: ip(30), PrecipYearsWithData: ip(29), EvapYearsWithData: ip(29),
		RainDays: fp(1.6), RainDaysYearsWithData: ip(29),
	},
	{
		Month:              4,
		TmaxMonthlyExtreme: fp(33), TmaxMonthlyExtremeYear: ip(2020), TmaxDailyExtreme: fp(36.2), TmaxDailyExtremeDate: sp("2020-04-21"),
		TminMonthlyExtreme: fp(8.6), TminMonthlyExtremeYear: ip(1992), TminDailyExtreme: fp(2), TminDailyExtremeDate: sp("2000-04-06"),
		PrecipMonthlyExtreme: fp(30.4), PrecipMonthlyExtremeYear: ip(1994), PrecipDailyExtreme: fp(18.3), PrecipDailyExtremeDate: sp("1994-04-16"),
		TmaxYearsWithData: ip(30), TminYearsWithData: ip(30), TmeanYearsWithData: ip(30), PrecipYearsWithData: ip(30), EvapYearsWithData: ip(29),
		RainDays: fp(0.8), RainDaysYearsWithData: ip(30),
	},
	{
		Month:              5,
		TmaxMonthlyExtreme: fp(34.7), TmaxMonthlyExtremeYear: ip(1998), TmaxDailyExtreme: fp(37.4), TmaxDailyExtremeDate: sp("2016-05-25"),
		TminMonthlyExtreme: fp(11.2), TminMonthlyExtremeYear: ip(1993), TminDailyExtreme: fp(6.2), TminDailyExtremeDate: sp("2013-05-01"),
		PrecipMonthlyExtreme: fp(91.4), PrecipMonthlyExtremeYear: ip(2018), PrecipDailyExtreme: fp(38), PrecipDailyExtremeDate: sp("2018-05-05"),
		TmaxYearsWithData: ip(28), TminYearsWithData: ip(28), TmeanYearsWithData: ip(29), PrecipYearsWithData: ip(29), EvapYearsWithData: ip(28),
		RainDays: fp(3.8), RainDaysYearsWithData: ip(29),
	},
	{
		Month:              6,
		TmaxMonthlyExtreme: fp(33.2), TmaxMonthlyExtremeYear: ip(2005), TmaxDailyExtreme: fp(39), TmaxDailyExtremeDate: sp("2018-06-19"),
		TminMonthlyExtreme: fp(13.8), TminMonthlyExtremeYear: ip(1992), TminDailyExtreme: fp(9.4), TminDailyExtremeDate: sp("1992-06-14"),
		PrecipMonthlyExtreme: fp(293.4), PrecipMonthlyExtremeYear: ip(2007), PrecipDailyExtreme: fp(71.6), PrecipDailyExtremeDate: sp("2007-06-19"),
		TmaxYearsWithData: ip(30), TminYearsWithData: ip(30), TmeanYearsWithData: ip(30), PrecipYearsWithData: ip(29), EvapYearsWithData: ip(29),
		RainDays: fp(9.7), RainDaysYearsWithData: ip(29),
	},
	{
		Month:              7,
		TmaxMonthlyExtreme: fp(30.4), TmaxMonthlyExtremeYear: ip(2009), TmaxDailyExtreme: fp(35), TmaxDailyExtremeDate: sp("2000-07-23"),
		TminMonthlyExtreme: fp(14), TminMonthlyExtremeYear: ip(1994), TminDailyExtreme: fp(10.2), TminDailyExtremeDate: sp("1994-07-25"),
		PrecipMonthlyExtreme: fp(247.2), PrecipMonthlyExtremeYear: ip(2003), PrecipDailyExtreme: fp(68.6), PrecipDailyExtremeDate: sp("2003-07-06"),
		TmaxYearsWithData: ip(29), TminYearsWithData: ip(29), TmeanYearsWithData: ip(29), PrecipYearsWithData: ip(29), EvapYearsWithData: ip(28),
		RainDays: fp(12.7), RainDaysYearsWithData: ip(29),
	},
	{
		Month:              8,
		TmaxMonthlyExtreme: fp(30.3), TmaxMonthlyExtremeYear: ip(2019), TmaxDailyExtreme: fp(33), TmaxDailyExtremeDate: sp("2009-08-02"),
		TminMonthlyExtreme: fp(13.7), TminMonthlyExtremeYear: ip(1993), TminDailyExtreme: fp(3), TminDailyExtremeDate: sp("1994-08-28"),
		PrecipMonthlyExtreme: fp(259.5), PrecipMonthlyExtremeYear: ip(2008), PrecipDailyExtreme: fp(48), PrecipDailyExtremeDate: sp("2008-08-28"),
		TmaxYearsWithData: ip(29), TminYearsWithData: ip(29), TmeanYearsWithData: ip(29), PrecipYearsWithData: ip(29), EvapYearsWithData: ip(29),
		RainDays: fp(12.4), RainDaysYearsWithData: ip(29),
	},
	{
		Month:              9,
		TmaxMonthlyExtreme: fp(30.2), TmaxMonthlyExtremeYear: ip(2020), TmaxDailyExtreme: fp(32), TmaxDailyExtremeDate: sp("2000-09-10"),
		TminMonthlyExtreme: fp(13.3), TminMonthlyExtremeYear: ip(1994), TminDailyExtreme: fp(0), TminDailyExtremeDate: sp("1993-09-24"),
		PrecipMonthlyExtreme: fp(199.6), PrecipMonthlyExtremeYear: ip(2003), PrecipDailyExtreme: fp(57.9), PrecipDailyExtremeDate: sp("2018-09-10"),
		TmaxYearsWithData: ip(30), TminYearsWithData: ip(30), TmeanYearsWithData: ip(30), PrecipYearsWithData: ip(30), EvapYearsWithData: ip(30),
		RainDays: fp(9.8), RainDaysYearsWithData: ip(30),
	},
	{
		Month:              10,
		TmaxMonthlyExtreme: fp(29.7), TmaxMonthlyExtremeYear: ip(2020), TmaxDailyExtreme: fp(34.6), TmaxDailyExtremeDate: sp("2004-10-03"),
		TminMonthlyExtreme: fp(8), TminMonthlyExtremeYear: ip(2010), TminDailyExtreme: fp(0), TminDailyExtremeDate: sp("1992-10-04"),
		PrecipMonthlyExtreme: fp(111.6), PrecipMonthlyExtremeYear: ip(2006), PrecipDailyExtreme: fp(42.6), PrecipDailyExtremeDate: sp("2006-10-12"),
		TmaxYearsWithData: ip(30), TminYearsWithData: ip(30), TmeanYearsWithData: ip(30), PrecipYearsWithData: ip(30), EvapYearsWithData: ip(30),
		RainDays: fp(4.9), RainDaysYearsWithData: ip(30),
	},
	{
		Month:              11,
		TmaxMonthlyExtreme: fp(28), TmaxMonthlyExtremeYear: ip(2020), TmaxDailyExtreme: fp(31.8), TmaxDailyExtremeDate: sp("2010-11-02"),
		TminMonthlyExtreme: fp(4.7), TminMonthlyExtremeYear: ip(2010), TminDailyExtreme: fp(-4.2), TminDailyExtremeDate: sp("2011-11-29"),
		PrecipMonthlyExtreme: fp(88.1), PrecipMonthlyExtremeYear: ip(2018), PrecipDailyExtreme: fp(43.9), PrecipDailyExtremeDate: sp("2018-11-04"),
		TmaxYearsWithData: ip(30), TminYearsWithData: ip(30), TmeanYearsWithData: ip(30), PrecipYearsWithData: ip(30), EvapYearsWithData: ip(30),
		RainDays: fp(1.8), RainDaysYearsWithData: ip(30),
	},
	{
		Month:              12,
		TmaxMonthlyExtreme: fp(25.1), TmaxMonthlyExtremeYear: ip(2007), TmaxDailyExtreme: fp(28.8), TmaxDailyExtremeDate: sp("2019-12-01"),
		TminMonthlyExtreme: fp(1.7), TminMonthlyExtremeYear: ip(2010), TminDailyExtreme: fp(-5), TminDailyExtremeDate: sp("1997-12-14"),
		PrecipMonthlyExtreme: fp(74.5), PrecipMonthlyExtremeYear: ip(2013), PrecipDailyExtreme: fp(24.1), PrecipDailyExtremeDate: sp("2015-12-11"),
		TmaxYearsWithData: ip(30), TminYearsWithData: ip(30), TmeanYearsWithData: ip(30), PrecipYearsWithData: ip(30), EvapYearsWithData: ip(29),
		RainDays: fp(1.6), RainDaysYearsWithData: ip(30),
	},
}

// Expected parse of testdata/normals/rh_section_ignored.txt — the
// synthetic fixture that wedges a HUMEDAD RELATIVA section (NORMAL and
// AÑOS CON DATOS rows) between TEMPERATURA MEDIA and PRECIPITACIÓN.
// Every RH value in the fixture is globally unique (floats 69.3–80.1
// with distinctive decimals; year counts 11–23), so any leak into a
// neighboring section's fields is unmistakable.
var wantRHIgnoredRows = []MonthlyNormalsRow{
	{Month: 1, Tmax: fp(25), Tmin: fp(18), Tmean: fp(21.5), Precip: fp(30), Evap: fp(100)},
	{Month: 2, Tmax: fp(26), Tmin: fp(19), Tmean: fp(22.5), Precip: fp(20), Evap: fp(110)},
	{Month: 3, Tmax: fp(27), Tmin: fp(20), Tmean: fp(23.5), Precip: fp(15), Evap: fp(130)},
	{Month: 4, Tmax: fp(28), Tmin: fp(21), Tmean: fp(24.5), Precip: fp(35), Evap: fp(150)},
	{Month: 5, Tmax: fp(29), Tmin: fp(22), Tmean: fp(25.5), Precip: fp(50), Evap: fp(160)},
	{Month: 6, Tmax: fp(30), Tmin: fp(23), Tmean: fp(26.5), Precip: fp(200), Evap: fp(155)},
	{Month: 7, Tmax: fp(31), Tmin: fp(24), Tmean: fp(27.5), Precip: fp(250), Evap: fp(140)},
	{Month: 8, Tmax: fp(30), Tmin: fp(23), Tmean: fp(26.5), Precip: fp(270), Evap: fp(145)},
	{Month: 9, Tmax: fp(29), Tmin: fp(22), Tmean: fp(25.5), Precip: fp(300), Evap: fp(135)},
	{Month: 10, Tmax: fp(28), Tmin: fp(21), Tmean: fp(24.5), Precip: fp(150), Evap: fp(120)},
	{Month: 11, Tmax: fp(27), Tmin: fp(20), Tmean: fp(23.5), Precip: fp(60), Evap: fp(115)},
	{Month: 12, Tmax: fp(26), Tmin: fp(19), Tmean: fp(22.5), Precip: fp(40), Evap: fp(105)},
}

// wantRHIgnoredExtras: the fixture's only extras data are TEMPERATURA
// MEDIA's AÑOS CON DATOS (25..30 cycling — which RH's must not
// overwrite) and NÚMERO DE DÍAS CON LLUVIA's NORMAL row.
var wantRHIgnoredExtras = func() []MonthlyNormalsExtrasRow {
	tmeanYears := []int{25, 26, 27, 28, 29, 30, 25, 26, 27, 28, 29, 30}
	rainDays := []float64{3, 2, 2, 4, 5, 15, 6, 22, 10, 9, 4, 7}
	out := make([]MonthlyNormalsExtrasRow, 12)
	for i := range out {
		out[i] = MonthlyNormalsExtrasRow{
			Month:              i + 1,
			TmeanYearsWithData: ip(tmeanYears[i]),
			RainDays:           fp(rainDays[i]),
		}
	}
	return out
}()

// rhFixtureFloats / rhFixtureYears are every value carried by the
// HUMEDAD RELATIVA section of rh_section_ignored.txt (including the
// annual-total column). None of them may surface anywhere in the result.
var (
	rhFixtureFloats = []float64{80.1, 79.2, 77.4, 76.5, 75.6, 74.7, 73.8, 72.9, 71.1, 70.2, 69.3, 74.9}
	rhFixtureYears  = []int{11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23}
)

// collectFloats gathers every non-nil float64 field across the parse
// result, so specs can assert a value appears nowhere at all.
func collectFloats(rows []MonthlyNormalsRow, extras []MonthlyNormalsExtrasRow) []float64 {
	var out []float64
	add := func(ps ...*float64) {
		for _, p := range ps {
			if p != nil {
				out = append(out, *p)
			}
		}
	}
	for _, r := range rows {
		add(r.Tmax, r.Tmin, r.Tmean, r.Precip, r.Evap)
	}
	for _, e := range extras {
		add(e.TmaxMonthlyExtreme, e.TmaxDailyExtreme,
			e.TminMonthlyExtreme, e.TminDailyExtreme,
			e.PrecipMonthlyExtreme, e.PrecipDailyExtreme,
			e.RainDays)
	}
	return out
}

// collectInts gathers every non-nil int field across the extras rows.
func collectInts(extras []MonthlyNormalsExtrasRow) []int {
	var out []int
	add := func(ps ...*int) {
		for _, p := range ps {
			if p != nil {
				out = append(out, *p)
			}
		}
	}
	for _, e := range extras {
		add(e.TmaxMonthlyExtremeYear, e.TminMonthlyExtremeYear, e.PrecipMonthlyExtremeYear,
			e.TmaxYearsWithData, e.TminYearsWithData, e.TmeanYearsWithData,
			e.PrecipYearsWithData, e.EvapYearsWithData, e.RainDaysYearsWithData)
	}
	return out
}

var _ = Describe("ParseNormalsFile", func() {
	Context("with the real 1991-2020 file for station 01001", func() {
		var (
			header Header
			rows   []MonthlyNormalsRow
			extras []MonthlyNormalsExtrasRow
		)

		BeforeEach(func() {
			var warnings []Warning
			var err error
			header, rows, extras, warnings, err = ParseNormalsFile(
				bytes.NewReader(readFixture("normals", "real_1991_2020_01001.txt")), "1991-2020")
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("round-trips every header field", func() {
			Expect(header).To(Equal(Header{
				ExternalID:   "1001",
				Name:         "AGUASCALIENTES (OBS)",
				State:        "AGUASCALIENTES",
				Municipality: "AGUASCALIENTES",
				Status:       StatusOperating,
				CVEOMM:       "76571",
				Lat:          fp(21.85027778),
				Lon:          fp(-102.2908333),
				AltitudeM:    fp(1890.8),
			}))
		})

		It("round-trips every NORMAL value for all 12 months", func() {
			Expect(rows).To(HaveLen(12))
			for i, want := range wantReal01001Rows {
				Expect(rows[i]).To(Equal(want), "month %d", want.Month)
			}
		})

		It("round-trips every extras field for all 12 months", func() {
			Expect(extras).To(HaveLen(12))
			for i, want := range wantReal01001Extras {
				Expect(extras[i]).To(Equal(want), "month %d", want.Month)
			}
		})
	})

	Context("with a HUMEDAD RELATIVA section between recognized sections", func() {
		// The section-isolation guard. The parser does not extract RH,
		// but "HUMEDAD RELATIVA" must stay a *recognized*
		// title: if it went unrecognized, the state machine would still be
		// inside the preceding section (TEMPERATURA MEDIA) when RH's
		// NORMAL / AÑOS CON DATOS rows arrive, and those values would
		// silently overwrite Tmean and TmeanYearsWithData.
		var (
			rows   []MonthlyNormalsRow
			extras []MonthlyNormalsExtrasRow
		)

		BeforeEach(func() {
			var warnings []Warning
			var err error
			_, rows, extras, warnings, err = ParseNormalsFile(
				bytes.NewReader(readFixture("normals", "rh_section_ignored.txt")), "1991-2020")
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("leaves the preceding TEMPERATURA MEDIA section untouched — no leaked rows", func() {
			Expect(rows).To(HaveLen(12))
			for i, want := range wantRHIgnoredRows {
				Expect(rows[i]).To(Equal(want), "month %d", want.Month)
			}
			Expect(extras).To(HaveLen(12))
			for i, want := range wantRHIgnoredExtras {
				Expect(extras[i]).To(Equal(want), "month %d", want.Month)
			}
		})

		It("surfaces no RH value anywhere in the parse result", func() {
			floats := collectFloats(rows, extras)
			ints := collectInts(extras)
			for _, rh := range rhFixtureFloats {
				Expect(floats).NotTo(ContainElement(rh), "RH normal %v leaked", rh)
			}
			for _, rh := range rhFixtureYears {
				Expect(ints).NotTo(ContainElement(rh), "RH years-with-data %d leaked", rh)
			}
		})
	})

	Context("when the file-body period disagrees with the expected period", func() {
		It("fails the whole file, naming both periods", func() {
			_, _, _, _, err := ParseNormalsFile(
				bytes.NewReader(readFixture("normals", "wrong_period.txt")), "1991-2020")
			// Both the expected and the file-body period must appear so
			// operators can diagnose the drift quickly.
			Expect(err).To(MatchError(And(
				ContainSubstring("1981-2010"),
				ContainSubstring("1991-2020"),
			)))
		})
	})

	Context("when the file has no period banner at all", func() {
		It("returns an error", func() {
			_, _, _, _, err := ParseNormalsFile(
				strings.NewReader("not a normals file\n"), "1991-2020")
			Expect(err).To(MatchError(ContainSubstring("NORMAL CLIMATOL")))
		})
	})
})

var _ = DescribeTable("PeriodForKind",
	func(k Kind, want string, wantOK bool) {
		got, ok := PeriodForKind(k)
		Expect(got).To(Equal(want))
		Expect(ok).To(Equal(wantOK))
	},
	Entry("1961-1990", KindNormals1961_1990, "1961-1990", true),
	Entry("1971-2000", KindNormals1971_2000, "1971-2000", true),
	Entry("1981-2010", KindNormals1981_2010, "1981-2010", true),
	Entry("1991-2020", KindNormals1991_2020, "1991-2020", true),
	Entry("daily is not a normals kind", KindDaily, "", false),
	Entry("monthly is not a normals kind", KindMonthly, "", false),
	Entry("extremes is not a normals kind", KindExtremes, "", false),
)
