package validate_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// The diurnal-range fixture — thirty normal
// January days and one 35 °C swing — with every finding it yields
// pinned (the swing also sits at 5.5σ in both z-score passes; the
// z-score texts are held to SQLite's own arithmetic), plus the edges of
// each sub-check. Findings within a rule are emitted in map order, so
// specs with more than one bucket compare as a multiset.
var _ = Describe("daily-sanity", func() {
	It("reproduces the reference fixture: a 35 °C diurnal day trips the range check and, at 5.5σ, both z-score passes", func() {
		db, _ := openTempDB()
		sid := station(db, "x", "test", f64(19.4), f64(-99.2))
		for d := 1; d <= 30; d++ {
			insertObs(db, sid, 1991, 1, d, 22.0, 8.0)
		}
		insertObs(db, sid, 1991, 1, 31, 35.0, 0.0)

		res := runVerbRule(db, "1991-2020", "daily-sanity")
		Expect(res.Scanned).To(Equal(3))
		Expect(issues(res.Findings)).To(Equal([]string{
			"diurnal range > 25°C on 1 day(s); top: 1991-01-31 (35.0°C)",
			"tmax outliers > 3.0σ in calendar month=01: 1 day(s) (mean=22.4 σ=2.3); top: 1991-01-31 tmax=35.0 (+5.5σ)",
			"tmin outliers > 3.0σ in calendar month=01: 1 day(s) (mean=7.7 σ=1.4); top: 1991-01-31 tmin=0.0 (-5.5σ)",
		}))
		for _, f := range res.Findings {
			Expect(f.RuleID).To(Equal("daily-sanity"))
			Expect(f.Severity).To(Equal(validate.SeverityWarn))
			Expect(f.StationID).To(HaveValue(Equal(sid)))
		}
	})

	It("aggregates the diurnal check per station, inlining the three widest days widest-first, and flags strictly above 25 °C", func() {
		db, _ := openTempDB()
		a := station(db, "a", "first", f64(19.4), f64(-99.2))
		b := station(db, "b", "second", f64(19.5), f64(-99.3))
		for d, span := range map[int]float64{1: 26.0, 2: 40.0, 3: 30.0, 4: 35.0, 5: 27.0} {
			insertObs(db, a, 1991, 6, d, 10.0+span, 10.0)
		}
		insertObs(db, b, 1991, 6, 1, 35.5, 10.0) // 25.5 — flagged
		insertObs(db, b, 1991, 6, 2, 35.0, 10.0) // exactly 25.0 — not

		res := runVerbRule(db, "1991-2020", "daily-sanity")
		Expect(res.Scanned).To(Equal(6))
		Expect(issues(res.Findings)).To(ConsistOf(
			"diurnal range > 25°C on 5 day(s); top: 1991-06-02 (40.0°C); 1991-06-04 (35.0°C); 1991-06-03 (30.0°C)",
			"diurnal range > 25°C on 1 day(s); top: 1991-06-01 (25.5°C)",
		))
		byStation := map[int64]string{}
		for _, f := range res.Findings {
			byStation[*f.StationID] = f.Issue
		}
		Expect(byStation[a]).To(HavePrefix("diurnal range > 25°C on 5 day(s)"))
		Expect(byStation[b]).To(HavePrefix("diurnal range > 25°C on 1 day(s)"))
	})

	It("z-score: a (station, month) needs 30 readings and a positive variance", func() {
		db, _ := openTempDB()
		a := station(db, "a", "first", f64(19.4), f64(-99.2))
		b := station(db, "b", "second", f64(19.5), f64(-99.3))
		// a: 29 readings — one short of the floor — so the 40 °C day is
		// only a diurnal finding.
		for d := 1; d <= 28; d++ {
			insertObs(db, a, 1991, 1, d, 20.0, 10.0)
		}
		insertObs(db, a, 1991, 1, 29, 40.0, 10.0)
		// b: 31 constant readings — variance 0 — nothing to measure against.
		for d := 1; d <= 31; d++ {
			insertObs(db, b, 1991, 1, d, 20.0, 10.0)
		}
		res := runVerbRule(db, "1991-2020", "daily-sanity")
		Expect(res.Scanned).To(Equal(1))
		Expect(issues(res.Findings)).To(Equal([]string{
			"diurnal range > 25°C on 1 day(s); top: 1991-01-29 (30.0°C)",
		}))
		Expect(res.Findings[0].StationID).To(HaveValue(Equal(a)))
	})

	It("z-score: buckets by calendar month across years and names the variable that tripped", func() {
		db, _ := openTempDB()
		sid := station(db, "x", "test", f64(19.4), f64(-99.2))
		for d := 1; d <= 30; d++ {
			insertObs(db, sid, 1991, 1, d, 20.0, 10.0)
		}
		// The 31st January reading is a year later: same calendar-month
		// bucket. tmin stays constant, so only tmax trips.
		insertObs(db, sid, 1992, 1, 1, 40.0, 10.0)
		res := runVerbRule(db, "1991-2020", "daily-sanity")
		Expect(res.Scanned).To(Equal(2))
		Expect(issues(res.Findings)).To(Equal([]string{
			"diurnal range > 25°C on 1 day(s); top: 1992-01-01 (30.0°C)",
			"tmax outliers > 3.0σ in calendar month=01: 1 day(s) (mean=20.6 σ=3.5); top: 1992-01-01 tmax=40.0 (+5.5σ)",
		}))
	})

	It("ignores rows with a NULL in the pair for the diurnal check and rows outside the period", func() {
		db, _ := openTempDB()
		sid := station(db, "x", "test", f64(19.4), f64(-99.2))
		insertObs(db, sid, 1991, 1, 1, 40.0, nil)
		insertObs(db, sid, 1991, 1, 2, nil, 0.0)
		insertObs(db, sid, 1990, 12, 31, 40.0, 0.0)
		insertObs(db, sid, 2021, 1, 1, 40.0, 0.0)
		res := runVerbRule(db, "1991-2020", "daily-sanity")
		Expect(res.Scanned).To(BeZero())
		Expect(res.Findings).To(BeEmpty())
	})

	It("rejects a malformed period", func() {
		db, _ := openTempDB()
		_, err := verbRule("1991", "daily-sanity").Run(context.Background(), db)
		Expect(err).To(MatchError(`malformed period "1991"`))
	})
})
