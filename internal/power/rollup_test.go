package power_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/power"
)

// powerResponse wraps parameter series in the Response envelope with
// POWER's conventional -999 fill sentinel — the shape RollUp and
// ExtractDaily consume.
func powerResponse(params map[string]map[string]float64) *power.Response {
	resp := &power.Response{}
	resp.Header.FillValue = -999
	resp.Properties.Parameter = params
	return resp
}

var _ = Describe("RollUp", func() {
	It("averages each calendar month across years and ignores annual keys", func() {
		got := power.RollUp(powerResponse(map[string]map[string]float64{
			"RH2M": {
				"199101": 60.0, // Jan 1991
				"199201": 70.0, // Jan 1992
				"199102": 50.0, // Feb 1991
				"ANN":    65.0, // overall annual — must be ignored
				"1991":   62.0, // per-year annual — must be ignored
			},
		}), 1991, 2020)
		// Full-map equality also proves no other month was emitted.
		Expect(got["RH2M"]).To(Equal(map[int]power.MonthlyValue{
			1: {Mean: 65.0, Years: 2},
			2: {Mean: 50.0, Years: 1},
		}))
	})

	It("drops values equal to the response's fill sentinel", func() {
		got := power.RollUp(powerResponse(map[string]map[string]float64{
			"WS10M": {
				"199101": 3.0,
				"199201": -999, // fill — POWER's missing-data sentinel
				"199301": 5.0,
			},
		}), 1991, 2020)
		Expect(got["WS10M"]).To(Equal(map[int]power.MonthlyValue{
			1: {Mean: 4.0, Years: 2},
		}))
	})

	It("clips years outside the requested window", func() {
		// Defensive — if POWER ever ignored our start/end query params
		// and returned more years than asked, the climatology must stay
		// bounded to the requested period.
		got := power.RollUp(powerResponse(map[string]map[string]float64{
			"T2M_MAX": {
				"199001": 10.0, // before window
				"199101": 20.0, // in window
				"202101": 30.0, // after window
			},
		}), 1991, 2020)
		Expect(got["T2M_MAX"]).To(Equal(map[int]power.MonthlyValue{
			1: {Mean: 20.0, Years: 1},
		}))
	})

	It("accepts December of the window's final year", func() {
		got := power.RollUp(powerResponse(map[string]map[string]float64{
			"T2M": {"202012": 7.0},
		}), 1991, 2020)
		Expect(got["T2M"]).To(Equal(map[int]power.MonthlyValue{
			12: {Mean: 7.0, Years: 1},
		}))
	})

	It("returns an empty rollup for a nil response", func() {
		Expect(power.RollUp(nil, 1991, 2020)).To(BeEmpty())
	})

	// The parseYYYYMM boundary classes, asserted through the
	// exported surface: a malformed key carries a poison value that must
	// never reach any month's mean.
	DescribeTable("discards series keys that are not calendar months",
		func(key string) {
			got := power.RollUp(powerResponse(map[string]map[string]float64{
				"T2M": {"199106": 10.0, key: 999.0},
			}), 1991, 2020)
			Expect(got["T2M"]).To(Equal(map[int]power.MonthlyValue{
				6: {Mean: 10.0, Years: 1},
			}))
		},
		Entry("month 00", "199100"),
		Entry("month 13 — POWER's per-year annual mean", "199113"),
		Entry("overall annual key", "ANN"),
		Entry("bare per-year key", "1991"),
		Entry("five digits", "19911"),
		Entry("non-numeric", "abcdef"),
		Entry("partial-numeric", "1991AA"),
	)

	It("keeps POWER's YYYY13 annual means out of the scalar mean", func() {
		got := power.RollUp(powerResponse(map[string]map[string]float64{
			"T2M": {
				// Real per-month values for Jan across 3 years.
				"199101": 20.0,
				"199201": 22.0,
				"199301": 24.0,
				// POWER's per-year annual means, keyed as month 13. A bug
				// that folded these in would shift Jan's mean toward 18
				// and double the years count.
				"199113": 18.0,
				"199213": 18.0,
				"199313": 18.0,
			},
		}), 1991, 2020)
		Expect(got["T2M"]).To(Equal(map[int]power.MonthlyValue{
			1: {Mean: 22.0, Years: 3},
		}))
	})

	Describe("wind direction (circular parameters)", func() {
		It("vector-averages across the 0/360° boundary", func() {
			got := power.RollUp(powerResponse(map[string]map[string]float64{
				"WD2M": {
					// January: 5° and 355° → vector mean ≈ 0°.
					"199101": 5.0,
					"199201": 355.0,
					// February: 80° and 100° → interior mean ≈ 90°.
					"199102": 80.0,
					"199202": 100.0,
					// March: a single value passes through unchanged.
					"199103": 270.0,
				},
			}), 1991, 2020)
			wd := got["WD2M"]
			Expect(wd).To(HaveLen(3))

			// Jan: the two boundary values must bracket 0° via the vector
			// mean. If this lands at 180°, arithmetic averaging crept
			// back in. The fold remaps negative atan2 results into
			// [0°, 360°), but a near-perfect sin cancellation (5° vs
			// 355°) yields a negative angle so tiny that adding 360.0
			// rounds to exactly 360.0 — the implementation emits that
			// as-is, pinned float-exact, so the upper edge here is
			// closed.
			Expect(wd[1].Years).To(Equal(2))
			Expect(wd[1].Mean).To(SatisfyAll(
				BeNumerically(">=", 0.0), BeNumerically("<=", 360.0)))
			Expect(wd[1].Mean).To(SatisfyAny(
				BeNumerically("<", 1.0), BeNumerically(">", 359.0)))

			Expect(wd[2].Years).To(Equal(2))
			Expect(wd[2].Mean).To(BeNumerically("~", 90.0, 0.5))

			Expect(wd[3].Years).To(Equal(1))
			Expect(wd[3].Mean).To(BeNumerically("~", 270.0, 1e-6))
		})

		It("drops fill values and out-of-window years in the circular path", func() {
			got := power.RollUp(powerResponse(map[string]map[string]float64{
				"WD10M": {
					"199001": 90.0,   // before window — drop
					"199101": 90.0,   // in window
					"199201": -999.0, // fill — drop
					"199301": 90.0,   // in window
					"202101": 270.0,  // after window — drop
				},
			}), 1991, 2020)
			wd := got["WD10M"]
			Expect(wd).To(HaveLen(1))
			Expect(wd[1].Years).To(Equal(2))
			Expect(wd[1].Mean).To(BeNumerically("~", 90.0, 1e-6))
		})

		It("keeps POWER's YYYY13 annual means out of the vector sum", func() {
			got := power.RollUp(powerResponse(map[string]map[string]float64{
				"WD2M": {
					"199101": 80.0,
					"199201": 90.0,
					"199301": 100.0,
					// Per-year annual means at a wildly different angle so
					// a leak would drag Jan's mean toward ~135°.
					"199113": 180.0,
					"199213": 180.0,
					"199313": 180.0,
				},
			}), 1991, 2020)
			wd := got["WD2M"]
			Expect(wd).To(HaveLen(1))
			Expect(wd[1].Years).To(Equal(3))
			Expect(wd[1].Mean).To(BeNumerically("~", 90.0, 0.5))
		})
	})
})

var _ = Describe("ExtractDaily", func() {
	It("extracts in-window days and drops fills and strays", func() {
		got := power.ExtractDaily(powerResponse(map[string]map[string]float64{
			"T2M": {
				"20200101": 20.0,
				"20200102": -999.0, // fill — drop
				"20200103": 22.0,
				"20191231": 19.0, // before window — drop
				"20200201": 24.0, // after window — drop
				"BADKEY":   99.0, // unparseable — drop
			},
		}), "2020-01-01", "2020-01-31")
		Expect(got["T2M"]).To(Equal(map[string]float64{
			"2020-01-01": 20.0,
			"2020-01-03": 22.0,
		}))
	})

	It("returns an empty extract for a nil response", func() {
		Expect(power.ExtractDaily(nil, "2020-01-01", "2020-12-31")).To(BeEmpty())
	})

	It("maps 8-digit keys to ISO dates at the calendar boundaries", func() {
		got := power.ExtractDaily(powerResponse(map[string]map[string]float64{
			"T2M": {
				"19810101": 1.0, // POWER daily's first available date
				"20200115": 3.0,
				"20201231": 2.0,
			},
		}), "1900-01-01", "2100-12-31")
		Expect(got["T2M"]).To(Equal(map[string]float64{
			"1981-01-01": 1.0,
			"2020-01-15": 3.0,
			"2020-12-31": 2.0,
		}))
	})

	// The parseYYYYMMDD boundary classes: POWER daily series come
	// back exclusively as 8-digit numeric strings; anything else (a
	// stray YYYYMM, an old-style ANN, a YYYY13 leak from the monthly
	// endpoint) must be rejected. The window is deliberately wide so
	// only key validity is under test.
	DescribeTable("discards keys that are not wire-form calendar dates",
		func(key string) {
			got := power.ExtractDaily(powerResponse(map[string]map[string]float64{
				"T2M": {"20200115": 20.0, key: 999.0},
			}), "1900-01-01", "2100-12-31")
			Expect(got["T2M"]).To(Equal(map[string]float64{"2020-01-15": 20.0}))
		},
		Entry("month 00", "20200000"),
		Entry("month 13", "20201300"),
		Entry("day 32", "20200132"),
		Entry("day 00", "20200100"),
		Entry("monthly form", "199101"),
		Entry("ISO with dashes", "2020-01-15"),
		Entry("YYYY13 monthly annual-mean leak", "199113"),
		Entry("overall annual key", "ANN"),
		Entry("non-numeric", "abcdefgh"),
		Entry("partial-numeric", "2020010A"),
	)
})

var _ = Describe("ConvertSolar", func() {
	It("pins the exact MJ/m²/day → W/m² factor", func() {
		// The factor is part of the reproducibility manifest
		// (power_runs.solar_conversion), so it must hold float-exact.
		Expect(power.SolarMJpm2dToWm2).To(Equal(1e6 / 86400.0))
		Expect(power.ConvertSolar(1.0)).To(Equal(power.SolarMJpm2dToWm2))
	})

	It("matches the documented reference values", func() {
		Expect(power.ConvertSolar(1.0)).To(BeNumerically("~", 11.5740740740, 1e-6))
		// ~21 MJ/m²/day (clear summer day) → ~243 W/m² 24-h average.
		Expect(power.ConvertSolar(21.0)).To(BeNumerically("~", 243.055, 0.01))
	})
})
