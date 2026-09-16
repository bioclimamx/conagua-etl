package validate_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

const wmoIssueTail = " year(s) violate (≥11 missing or ≥5 consecutive); top: "

// The WMO month-completeness fixture — a full month, an
// 11-missing month, and a 5-day-gap month — with the finding texts
// pinned, plus the helpers' table cases.
var _ = Describe("wmo-month-completeness", func() {
	It("reproduces the reference fixture: a full month is silent, 11 missing and a 5-day gap each fail, one finding per (station, calendar month)", func() {
		db, _ := openTempDB()
		sid := station(db, "x", "test", f64(19.4), f64(-99.2))
		// Year 1991, Jan: all 31 days present → no finding.
		for d := 1; d <= 31; d++ {
			insertObs(db, sid, 1991, 1, d, 25.0, 10.0)
		}
		// Year 1992, Jan: 20 days present (11 missing) → warn.
		for d := 1; d <= 20; d++ {
			insertObs(db, sid, 1992, 1, d, 25.0, 10.0)
		}
		// Year 1991, Feb: days 1-11 and 17-28 — 23 of 28 present, a 5-day
		// gap → warn.
		for d := 1; d <= 11; d++ {
			insertObs(db, sid, 1991, 2, d, 22.0, 8.0)
		}
		for d := 17; d <= 28; d++ {
			insertObs(db, sid, 1991, 2, d, 22.0, 8.0)
		}

		res := runVerbRule(db, "1991-2020", "wmo-month-completeness")
		Expect(res.Scanned).To(Equal(3))
		Expect(issues(res.Findings)).To(Equal([]string{
			"WMO §4.4.1 fail in period 1991-2020, calendar month=01: 1" + wmoIssueTail + "1992 (11/31 missing, gap=11)",
			"WMO §4.4.1 fail in period 1991-2020, calendar month=02: 1" + wmoIssueTail + "1991 (5/28 missing, gap=5)",
		}))
		for _, f := range res.Findings {
			Expect(f.RuleID).To(Equal("wmo-month-completeness"))
			Expect(f.Severity).To(Equal(validate.SeverityWarn))
			Expect(f.StationID).To(HaveValue(Equal(sid)))
		}
	})

	It("summarizes the failing years in year order, inlining the first three, and orders findings by (station, month)", func() {
		db, _ := openTempDB()
		a := station(db, "a", "first", f64(19.4), f64(-99.2))
		b := station(db, "b", "second", f64(19.5), f64(-99.3))
		// b's data first, so the ordering comes from the rule, not insertion.
		for _, y := range []int{1995, 1993, 1991, 1997} {
			for d := 1; d <= 15; d++ {
				insertObs(db, b, y, 3, d, 25.0, 10.0)
			}
		}
		for d := 1; d <= 12; d++ {
			insertObs(db, a, 1991, 12, d, 25.0, 10.0)
		}

		res := runVerbRule(db, "1991-2020", "wmo-month-completeness")
		Expect(res.Scanned).To(Equal(5))
		Expect(issues(res.Findings)).To(Equal([]string{
			"WMO §4.4.1 fail in period 1991-2020, calendar month=12: 1" + wmoIssueTail + "1991 (19/31 missing, gap=19)",
			"WMO §4.4.1 fail in period 1991-2020, calendar month=03: 4" + wmoIssueTail +
				"1991 (16/31 missing, gap=16); 1993 (16/31 missing, gap=16); 1995 (16/31 missing, gap=16)",
		}))
		Expect(res.Findings[0].StationID).To(HaveValue(Equal(a)))
		Expect(res.Findings[1].StationID).To(HaveValue(Equal(b)))
	})

	It("counts only rows with a tmax and only dates inside the period", func() {
		db, _ := openTempDB()
		sid := station(db, "x", "test", f64(19.4), f64(-99.2))
		// 1990 is outside 1991-2020: a gappy month there is not even scanned.
		for d := 1; d <= 5; d++ {
			insertObs(db, sid, 1990, 1, d, 25.0, 10.0)
		}
		// 1991-03: 31 rows, but tmax NULL on 20 of them → 11 days of data.
		for d := 1; d <= 31; d++ {
			var tmax any = 25.0
			if d > 11 {
				tmax = nil
			}
			insertObs(db, sid, 1991, 3, d, tmax, 10.0)
		}
		res := runVerbRule(db, "1991-2020", "wmo-month-completeness")
		Expect(res.Scanned).To(Equal(1))
		Expect(issues(res.Findings)).To(Equal([]string{
			"WMO §4.4.1 fail in period 1991-2020, calendar month=03: 1" + wmoIssueTail + "1991 (20/31 missing, gap=20)",
		}))
	})

	It("scopes to the period given and names it in the finding", func() {
		db, _ := openTempDB()
		sid := station(db, "x", "test", f64(19.4), f64(-99.2))
		for d := 1; d <= 20; d++ {
			insertObs(db, sid, 1990, 1, d, 25.0, 10.0)
		}
		for d := 1; d <= 20; d++ {
			insertObs(db, sid, 1992, 1, d, 25.0, 10.0)
		}
		res := runVerbRule(db, "1961-1990", "wmo-month-completeness")
		Expect(res.Scanned).To(Equal(1))
		Expect(issues(res.Findings)).To(Equal([]string{
			"WMO §4.4.1 fail in period 1961-1990, calendar month=01: 1" + wmoIssueTail + "1990 (11/31 missing, gap=11)",
		}))
	})

	It("rejects a malformed period", func() {
		db, _ := openTempDB()
		_, err := verbRule("19912020", "wmo-month-completeness").Run(context.Background(), db)
		Expect(err).To(MatchError(`malformed period "19912020"`))
		_, err = verbRule("abcd-efgh", "wmo-month-completeness").Run(context.Background(), db)
		Expect(err).To(MatchError(ContainSubstring(`parse period years "abcd-efgh"`)))
	})
})

var _ = Describe("longestGap", func() {
	DescribeTable("returns the longest run of missing day-of-month values",
		func(dayList string, expected, want int) {
			Expect(validate.LongestGap(dayList, expected)).To(Equal(want))
		},
		Entry("all present", "01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31", 31, 0),
		Entry("missing first 5", "06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31", 31, 5),
		Entry("middle 5-day gap", "01,02,03,04,05,06,07,08,09,10,11,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31", 31, 5),
		Entry("trailing 3", "01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28", 31, 3),
		Entry("empty list", "", 31, 31),
		// Days [1, 15, 31] in a 31-day month: missing 2..14 (13 days) and
		// 16..30 (15 days). Out-of-order input, larger gap wins.
		Entry("unsorted input", "31,01,15", 31, 15),
		Entry("out-of-range and unparsable entries are dropped", "00,x,32,10", 31, 21),
		Entry("nothing parsable counts as all missing", "x,y", 31, 31),
		Entry("zero expected", "01", 0, 0),
	)
})

var _ = Describe("daysInMonth", func() {
	DescribeTable("returns the calendar length, 0 outside 1..12",
		func(y, m, want int) { Expect(validate.DaysInMonth(y, m)).To(Equal(want)) },
		Entry("leap February", 2020, 2, 29),
		Entry("February", 2021, 2, 28),
		Entry("April", 2020, 4, 30),
		Entry("December", 2020, 12, 31),
		Entry("month 0", 2020, 0, 0),
		Entry("month 13", 2020, 13, 0),
	)
})

var _ = Describe("period parsers", func() {
	It("turn a period into its ISO date bounds and its integer years", func() {
		start, end, err := validate.PeriodBounds("1991-2020")
		Expect(err).NotTo(HaveOccurred())
		Expect([]string{start, end}).To(Equal([]string{"1991-01-01", "2020-12-31"}))
		sy, ey, err := validate.PeriodYearBounds("1991-2020")
		Expect(err).NotTo(HaveOccurred())
		Expect([]int{sy, ey}).To(Equal([]int{1991, 2020}))
	})

	It("refuse any other shape, so a typo does not widen the scope", func() {
		for _, bad := range []string{"", "1991", "1991-2020-x", "1991_2020"} {
			_, _, err := validate.PeriodBounds(bad)
			Expect(err).To(MatchError(`malformed period "`+bad+`"`), bad)
			_, _, err = validate.PeriodYearBounds(bad)
			Expect(err).To(MatchError(`malformed period "`+bad+`"`), bad)
		}
		_, _, err := validate.PeriodYearBounds("19x1-2020")
		Expect(err).To(MatchError(ContainSubstring(`parse period years "19x1-2020"`)))
	})

	It("read the month of an ISO date without parsing it, 0 when too short", func() {
		Expect(validate.MonthFromDate("2020-07-15")).To(Equal(7))
		Expect(validate.MonthFromDate("1991-12-01")).To(Equal(12))
		Expect(validate.MonthFromDate("2020-1")).To(Equal(0))
	})
})
