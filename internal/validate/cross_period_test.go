package validate_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// The cross-period fixture — a 5 °C tmax jump between two
// periods — with the finding text pinned, plus the tolerance edge, the
// NULL edge, and the per-(station, variable) aggregation and order.
var _ = Describe("cross-period", func() {
	It("reproduces the reference fixture: a 5 °C tmax jump between two periods is one tmax finding", func() {
		db, _ := openTempDB()
		sid := station(db, "x", "test", f64(19.4), f64(-99.2))
		insertNormalsTemps(db, sid, "1981-2010", 1, 25.0, nil, nil)
		insertNormalsTemps(db, sid, "1991-2020", 1, 30.0, nil, nil)

		res := runVerbRule(db, "", "cross-period")
		Expect(res.Scanned).To(Equal(1))
		Expect(issues(res.Findings)).To(Equal([]string{
			"tmax cross-period delta exceeds ±3.0°C in 1 (month, period-pair)(s); top: m=01 1981-2010 vs 1991-2020: 25.0 vs 30.0 (Δ-5.0°C)",
		}))
		Expect(res.Findings[0].RuleID).To(Equal("cross-period"))
		Expect(res.Findings[0].Severity).To(Equal(validate.SeverityWarn))
		Expect(res.Findings[0].StationID).To(HaveValue(Equal(sid)))
	})

	It("treats a delta of exactly 3.0 °C as within tolerance and a NULL on either side as no comparison", func() {
		db, _ := openTempDB()
		sid := station(db, "x", "test", f64(19.4), f64(-99.2))
		insertNormalsTemps(db, sid, "1981-2010", 1, 20.0, 10.0, nil)
		insertNormalsTemps(db, sid, "1991-2020", 1, 23.0, nil, 15.0)
		res := runVerbRule(db, "", "cross-period")
		Expect(res.Scanned).To(Equal(1))
		Expect(res.Findings).To(BeEmpty())
	})

	It("emits one finding per (station, variable) ordered by station then variable, sampling the first three pairs in period order", func() {
		db, _ := openTempDB()
		a := station(db, "a", "first", f64(19.4), f64(-99.2))
		b := station(db, "b", "second", f64(19.5), f64(-99.3))
		// b's rows first, so the ordering comes from the rule.
		insertNormalsTemps(db, b, "1971-2000", 6, nil, nil, 20.0)
		insertNormalsTemps(db, b, "1991-2020", 6, nil, nil, 15.0)
		// a: tmax climbs 4 °C a period (every pair over), tmin jumps only
		// at the last period (three pairs over), tmean is flat.
		insertNormalsTemps(db, a, "1961-1990", 1, 10.0, 5.0, 8.0)
		insertNormalsTemps(db, a, "1971-2000", 1, 14.0, 5.0, 8.0)
		insertNormalsTemps(db, a, "1981-2010", 1, 18.0, 5.0, 8.0)
		insertNormalsTemps(db, a, "1991-2020", 1, 22.0, 9.0, 8.0)

		res := runVerbRule(db, "", "cross-period")
		Expect(res.Scanned).To(Equal(7))
		Expect(issues(res.Findings)).To(Equal([]string{
			"tmax cross-period delta exceeds ±3.0°C in 6 (month, period-pair)(s); top: " +
				"m=01 1961-1990 vs 1971-2000: 10.0 vs 14.0 (Δ-4.0°C); " +
				"m=01 1961-1990 vs 1981-2010: 10.0 vs 18.0 (Δ-8.0°C); " +
				"m=01 1961-1990 vs 1991-2020: 10.0 vs 22.0 (Δ-12.0°C)",
			"tmin cross-period delta exceeds ±3.0°C in 3 (month, period-pair)(s); top: " +
				"m=01 1961-1990 vs 1991-2020: 5.0 vs 9.0 (Δ-4.0°C); " +
				"m=01 1971-2000 vs 1991-2020: 5.0 vs 9.0 (Δ-4.0°C); " +
				"m=01 1981-2010 vs 1991-2020: 5.0 vs 9.0 (Δ-4.0°C)",
			"tmean cross-period delta exceeds ±3.0°C in 1 (month, period-pair)(s); top: m=06 1971-2000 vs 1991-2020: 20.0 vs 15.0 (Δ+5.0°C)",
		}))
		Expect(res.Findings[0].StationID).To(HaveValue(Equal(a)))
		Expect(res.Findings[1].StationID).To(HaveValue(Equal(a)))
		Expect(res.Findings[2].StationID).To(HaveValue(Equal(b)))
	})

	It("compares only same-(station, month) rows", func() {
		db, _ := openTempDB()
		a := station(db, "a", "first", f64(19.4), f64(-99.2))
		b := station(db, "b", "second", f64(19.5), f64(-99.3))
		insertNormalsTemps(db, a, "1981-2010", 1, 10.0, nil, nil)
		insertNormalsTemps(db, a, "1991-2020", 2, 30.0, nil, nil)
		insertNormalsTemps(db, b, "1991-2020", 1, 30.0, nil, nil)
		res := runVerbRule(db, "", "cross-period")
		Expect(res.Scanned).To(BeZero())
		Expect(res.Findings).To(BeEmpty())
	})
})
