package publish_test

// Specs for the derived annual slot at its numeric edges, through the
// profile from seeded rows: a sum whose twelve-fold accumulation
// carries a binary tail renders as the exact one-decimal literal; a mean
// is taken from the stored full-precision values, so its literal can
// differ from what the rendered months alone would suggest; a sum with
// exactly one NULL month is null while its complete sibling in the same
// period sums (the completeness rule is per variable); and the circular
// mean of twelve identical directions is that direction — 0 as 0.0 and
// 360 on the closed boundary as 360.0 — exactly what power's function
// yields.

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/power"
)

// constantPowerRow is fullPowerRow with the named columns overridden,
// every other column its fixture value.
func constantPowerRow(overrides map[string]float64) map[string]powerCase {
	row := withNulls(fullPowerRow)
	for col, v := range overrides {
		row[col] = powerCase{in: v}
	}
	return row
}

// sameTwelve is one direction twelve times.
func sameTwelve(v float64) []float64 {
	out := make([]float64, 12)
	for i := range out {
		out[i] = v
	}
	return out
}

var _ = Describe("the derived annual at its numeric edges, through the profile", func() {
	var got map[string]any

	BeforeEach(func() {
		db := openTempDB()
		id := upsertStation(db, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "31005", Name: "Ticul", State: "YUC",
		})
		for m := 12; m >= 1; m-- {
			// Twelve identical months per variable, each value chosen so the
			// twelve-fold accumulation or its division carries a binary tail.
			insertNormals(db, id, "1981-2010", m, 1.15, 0.1, 100.1, 0.1, 3.7)
			// precip NULL in December only; every other variable complete.
			var precip any = 0.05
			if m == 12 {
				precip = nil
			}
			insertNormals(db, id, "1991-2020", m, 20.0, 10.0, 15.0, precip, 100.1)
			// evap NULL in January only.
			var evap any = 1.15
			if m == 1 {
				evap = nil
			}
			insertNormals(db, id, "1971-2000", m, 20.0, 10.0, 15.0, 0.05, evap)
		}
		insertCell(db, cellSingle, 21.0, -89.625)
		insertStationCell(db, id, cellSingle, 5.5)
		for m := 12; m >= 1; m-- {
			insertPowerMonthly(db, cellSingle, "1981-2010", m, constantPowerRow(map[string]float64{
				"wd2m_deg": 90, "wd10m_deg": 360, "t2m_c": 3.7, "precip_mmpd": 1.15,
			}))
			insertPowerMonthly(db, cellSingle, "1991-2020", m, constantPowerRow(map[string]float64{
				"wd2m_deg": 0, "wd10m_deg": 270, "ts_c": 0.05, "precip_mmpd": 0.1,
			}))
		}
		got = decodeProfile(profileBytes(db, yucatan, "31005"))
	})

	It("sums twelve monthly totals to the exact one-decimal literal, the accumulation's binary tail rounded away", func() {
		p := obj(got, "normals", "periods", "1981-2010")
		Expect(p["precip_mm"].([]any)[:12]).To(Equal(months(func(int) any { return num("0.1") }, nil)[:12]))
		Expect(p["precip_mm"].([]any)[12]).To(Equal(num("1.2")))
		Expect(p["evap_mm"].([]any)[:12]).To(Equal(months(func(int) any { return num("3.7") }, nil)[:12]))
		Expect(p["evap_mm"].([]any)[12]).To(Equal(num("44.4")))
		// Never the mean of the totals.
		Expect(p["evap_mm"].([]any)[12]).NotTo(Equal(num("3.7")))

		Expect(obj(got, "normals", "periods", "1971-2000")["precip_mm"].([]any)[12]).To(Equal(num("0.6")))
		Expect(obj(got, "normals", "periods", "1991-2020")["evap_mm"].([]any)[12]).To(Equal(num("1201.2")))
	})

	It("means twelve months from the stored full-precision values, not from the months' rendered literals", func() {
		// 1.15 stored sits just under the one-decimal tie, so each month
		// renders 1.1; the twelve-fold sum lands just above 13.8 and the
		// mean just above 1.15, which rounds to 1.2 — a literal an annual
		// taken from the rendered 1.1s could never reach.
		p := obj(got, "normals", "periods", "1981-2010")
		Expect(p["tmax_c"].([]any)[:12]).To(Equal(months(func(int) any { return num("1.1") }, nil)[:12]))
		Expect(p["tmax_c"].([]any)[12]).To(Equal(num("1.2")))
		Expect(p["tmin_c"].([]any)[12]).To(Equal(num("0.1")))
		Expect(p["tmean_c"].([]any)[12]).To(Equal(num("100.1")))

		pow := obj(got, "power_monthly", "periods", "1981-2010")
		Expect(pow["t2m_c"].([]any)[12]).To(Equal(num("3.70")))
		Expect(pow["precip_mmpd"].([]any)[12]).To(Equal(num("1.15")))

		// The reverse case: 24.475 stored sits just above the two-decimal
		// tie and each month renders 24.48, while the mean of twelve lands
		// just below and renders 24.47.
		later := obj(got, "power_monthly", "periods", "1991-2020")
		Expect(later["t2m_c"].([]any)[:12]).To(Equal(months(func(int) any { return num("24.48") }, nil)[:12]))
		Expect(later["t2m_c"].([]any)[12]).To(Equal(num("24.47")))
		Expect(later["ts_c"].([]any)[12]).To(Equal(num("0.05")))
		Expect(later["precip_mmpd"].([]any)[12]).To(Equal(num("0.10")))
	})

	It("leaves slot 12 null for a sum with exactly one NULL month while its complete sibling in the period sums (slot 12 null under a partial year, per variable)", func() {
		later := obj(got, "normals", "periods", "1991-2020")
		Expect(later["precip_mm"].([]any)[:11]).To(Equal(months(func(int) any { return num("0.1") }, nil)[:11]))
		Expect(later["precip_mm"].([]any)[11]).To(BeNil())
		Expect(later["precip_mm"].([]any)[12]).To(BeNil())
		Expect(later["evap_mm"].([]any)[12]).To(Equal(num("1201.2")))
		Expect(later["tmax_c"].([]any)[12]).To(Equal(num("20.0")))
		Expect(later["tmin_c"].([]any)[12]).To(Equal(num("10.0")))
		Expect(later["tmean_c"].([]any)[12]).To(Equal(num("15.0")))

		earlier := obj(got, "normals", "periods", "1971-2000")
		Expect(earlier["evap_mm"].([]any)[0]).To(BeNil())
		Expect(earlier["evap_mm"].([]any)[1:12]).To(Equal(months(func(int) any { return num("1.1") }, nil)[1:12]))
		Expect(earlier["evap_mm"].([]any)[12]).To(BeNil())
		Expect(earlier["precip_mm"].([]any)[12]).To(Equal(num("0.6")))
	})

	It("takes the circular mean of twelve identical directions as that direction: 0 as 0.0, 360 on the closed boundary as 360.0", func() {
		pow := obj(got, "power_monthly", "periods", "1981-2010")
		Expect(pow["wd2m_deg"].([]any)[12]).To(Equal(num("90.0")))
		Expect(pow["wd10m_deg"].([]any)[12]).To(Equal(num("360.0")))
		Expect(pow["wd10m_deg"].([]any)[12]).NotTo(Equal(num("0.0")))
		later := obj(got, "power_monthly", "periods", "1991-2020")
		Expect(later["wd2m_deg"].([]any)[12]).To(Equal(num("0.0")))
		Expect(later["wd10m_deg"].([]any)[12]).To(Equal(num("270.0")))

		// Each is what power's own function yields, at one decimal.
		Expect(pow["wd2m_deg"].([]any)[12]).To(Equal(circularOf(sameTwelve(90)...)))
		Expect(pow["wd10m_deg"].([]any)[12]).To(Equal(circularOf(sameTwelve(360)...)))
		Expect(later["wd2m_deg"].([]any)[12]).To(Equal(circularOf(sameTwelve(0)...)))
		Expect(later["wd10m_deg"].([]any)[12]).To(Equal(circularOf(sameTwelve(270)...)))
		Expect(power.CircularMeanDegrees(sameTwelve(360))).To(Equal(360.0))
		Expect(power.CircularMeanDegrees(sameTwelve(0))).To(Equal(0.0))
	})

	It("means every other POWER column of twelve identical months to the month's own literal", func() {
		pow := obj(got, "power_monthly", "periods", "1981-2010")
		for _, p := range power.Registry {
			if p.Circular || p.Column == "t2m_c" || p.Column == "precip_mmpd" {
				continue
			}
			want := num(fullPowerRow[p.Column].want)
			Expect(pow[p.Column].([]any)[:12]).To(Equal(months(func(int) any { return want }, nil)[:12]), p.Column)
			Expect(pow[p.Column].([]any)[12]).To(Equal(want), p.Column)
		}
	})
})
