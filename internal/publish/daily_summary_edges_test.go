package publish_test

// Specs for the profile's daily summary at the edges of its one date-ordered
// pass, each from a freshly seeded station whose rows are inserted in
// reverse date order: the longest dry run closing the series, a series
// whose only dry day is its first, two equal-length runs (the earliest
// wins), a later longer run replacing an earlier one, runs across a year
// boundary and a leap day (calendar-consecutive is what counts), a day
// whose only observation is a zero precip (dry, and an observed day), a
// trace rain day (0.01 mm is wet, and keeps its second decimal), a
// series with no dry day at all (dry_spell null while the records
// stand), and a tie on each record's value (the earliest date wins for
// the maximum, the minimum, and the 1-day precip alike). Every field of
// the block is asserted each time.

import (
	"slices"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
)

// dayRow is one seeded daily_observations row; nil seeds NULL.
type dayRow struct {
	date                     string
	tmax, tmin, precip, evap any
}

func day(date string, tmax, tmin, precip, evap any) dayRow {
	return dayRow{date: date, tmax: tmax, tmin: tmin, precip: precip, evap: evap}
}

// summaryOf seeds one station with rows — inserted last-first, so the
// summary's order comes from the date sort and never from insertion —
// and returns its profile's daily_summary block.
func summaryOf(rows ...dayRow) map[string]any {
	GinkgoHelper()
	db := openTempDB()
	id := upsertStation(db, ingest.StationUpsert{
		Source: ingest.SourceConaguaConventional, ExternalID: "31006", Name: "Motul", State: "YUC",
	})
	for _, r := range slices.Backward(rows) {
		insertDaily(db, id, r.date, r.tmax, r.tmin, r.precip, r.evap)
	}
	return obj(decodeProfile(profileBytes(db, yucatan, "31006")), "daily_summary")
}

// wantSummary is the block for a station with no cell: observed coverage,
// the three records, and the dry spell as given (nil for the explicit
// null), reanalysis coverage null.
func wantSummary(observed, tmax, tmin, precip, dry any) map[string]any {
	return map[string]any{
		"source":   "bioclima_derived",
		"coverage": map[string]any{"observed": observed, "reanalysis": nil},
		"extremes": map[string]any{
			"source": "conagua_observed", "record_tmax_c": tmax, "record_tmin_c": tmin, "record_precip_mm_1day": precip,
		},
		"dry_spell": dry,
	}
}

func coverageOf(first, last string, days, tmax, tmin, precip, evap int) map[string]any {
	return map[string]any{
		"first_date": first, "last_date": last, "days_with_obs": num(strconv.Itoa(days)),
		"days_by_variable": map[string]any{
			"tmax_c": num(strconv.Itoa(tmax)), "tmin_c": num(strconv.Itoa(tmin)),
			"precip_mm": num(strconv.Itoa(precip)), "evap_mm": num(strconv.Itoa(evap)),
		},
	}
}

func recordOf(value, date string) map[string]any {
	return map[string]any{"value": num(value), "date": date}
}

func dryOf(days int, start, end string) map[string]any {
	return map[string]any{"longest_dry_run_days": num(strconv.Itoa(days)), "start_date": start, "end_date": end}
}

var _ = DescribeTable("the daily summary at the edges of the date-ordered pass",
	func(rows []dayRow, want map[string]any) {
		Expect(summaryOf(rows...)).To(Equal(want))
	},
	Entry("the longest dry run closes the series: its end date is the last date",
		[]dayRow{
			day("2020-01-01", 30.0, 20.0, 5.0, 1.0),
			day("2020-01-02", 31.0, 19.0, 2.5, 1.1),
			day("2020-01-03", 29.0, 18.0, 0.0, 1.2),
			day("2020-01-04", 28.0, 17.0, 0.0, 1.3),
			day("2020-01-05", 27.0, 16.0, 0.0, 1.4),
		},
		wantSummary(coverageOf("2020-01-01", "2020-01-05", 5, 5, 5, 5, 5),
			recordOf("31.00", "2020-01-02"), recordOf("16.00", "2020-01-05"), recordOf("5.00", "2020-01-01"),
			dryOf(3, "2020-01-03", "2020-01-05"))),
	Entry("the only dry day is the first: a one-day run at the start, a NULL and wet days after it",
		[]dayRow{
			day("2020-01-01", 30.0, 20.0, 0.0, 1.0),
			day("2020-01-02", 31.0, 19.0, 3.0, nil),
			day("2020-01-03", 29.0, 18.0, nil, 1.2),
			day("2020-01-04", 28.0, 17.0, 1.0, 1.3),
		},
		wantSummary(coverageOf("2020-01-01", "2020-01-04", 4, 4, 4, 3, 3),
			recordOf("31.00", "2020-01-02"), recordOf("17.00", "2020-01-04"), recordOf("3.00", "2020-01-02"),
			dryOf(1, "2020-01-01", "2020-01-01"))),
	Entry("two runs of equal length: the earliest wins",
		[]dayRow{
			day("2020-01-01", 30.0, 20.0, 0.0, 1.0),
			day("2020-01-02", 30.0, 20.0, 0.0, 1.0),
			day("2020-01-03", 30.0, 20.0, 0.0, 1.0),
			day("2020-01-04", 30.0, 20.0, 0.2, 1.0),
			day("2020-01-05", 30.0, 20.0, 0.0, 1.0),
			day("2020-01-06", 30.0, 20.0, 0.0, 1.0),
			day("2020-01-07", 30.0, 20.0, 0.0, 1.0),
		},
		wantSummary(coverageOf("2020-01-01", "2020-01-07", 7, 7, 7, 7, 7),
			recordOf("30.00", "2020-01-01"), recordOf("20.00", "2020-01-01"), recordOf("0.20", "2020-01-04"),
			dryOf(3, "2020-01-01", "2020-01-03"))),
	Entry("a later, longer run replaces an earlier one",
		[]dayRow{
			day("2020-01-01", 30.0, 20.0, 0.0, 1.0),
			day("2020-01-02", 30.0, 20.0, 0.0, 1.0),
			day("2020-01-03", 30.0, 20.0, 4.0, 1.0),
			day("2020-01-04", 30.0, 20.0, 0.0, 1.0),
			day("2020-01-05", 30.0, 20.0, 0.0, 1.0),
			day("2020-01-06", 30.0, 20.0, 0.0, 1.0),
		},
		wantSummary(coverageOf("2020-01-01", "2020-01-06", 6, 6, 6, 6, 6),
			recordOf("30.00", "2020-01-01"), recordOf("20.00", "2020-01-01"), recordOf("4.00", "2020-01-03"),
			dryOf(3, "2020-01-04", "2020-01-06"))),
	Entry("a run spans a year boundary: calendar-consecutive dates continue it",
		[]dayRow{
			day("2019-12-30", 25.0, 15.0, 0.0, 2.0),
			day("2019-12-31", 25.0, 15.0, 0.0, 2.0),
			day("2020-01-01", 25.0, 15.0, 0.0, 2.0),
			day("2020-01-02", 25.0, 15.0, 0.0, 2.0),
		},
		wantSummary(coverageOf("2019-12-30", "2020-01-02", 4, 4, 4, 4, 4),
			recordOf("25.00", "2019-12-30"), recordOf("15.00", "2019-12-30"), recordOf("0.00", "2019-12-30"),
			dryOf(4, "2019-12-30", "2020-01-02"))),
	Entry("a run spans February's end in a common year and in a leap year: the leap day is one more consecutive date",
		[]dayRow{
			day("2019-02-28", 25.0, 15.0, 0.0, 2.0),
			day("2019-03-01", 25.0, 15.0, 0.0, 2.0),
			day("2020-02-28", 25.0, 15.0, 0.0, 2.0),
			day("2020-02-29", 25.0, 15.0, 0.0, 2.0),
			day("2020-03-01", 25.0, 15.0, 0.0, 2.0),
		},
		wantSummary(coverageOf("2019-02-28", "2020-03-01", 5, 5, 5, 5, 5),
			recordOf("25.00", "2019-02-28"), recordOf("15.00", "2019-02-28"), recordOf("0.00", "2019-02-28"),
			dryOf(3, "2020-02-28", "2020-03-01"))),
	Entry("a day whose only observation is a zero precip is dry and counts as observed; an all-NULL day ends the run and counts as nothing",
		[]dayRow{
			day("2020-01-01", nil, nil, 0.0, nil),
			day("2020-01-02", nil, nil, 0.0, nil),
			day("2020-01-03", nil, nil, nil, nil),
		},
		wantSummary(coverageOf("2020-01-01", "2020-01-03", 2, 0, 0, 2, 0),
			nil, nil, recordOf("0.00", "2020-01-01"),
			dryOf(2, "2020-01-01", "2020-01-02"))),
	// CONAGUA publishes "inappreciable" rain as 0.01 mm. It is a wet day
	// — isDry is a strict zero — and the record it sets must reach the
	// profile as 0.01, not as the dry 0.0 a one-decimal export would
	// print while the shipped SQLite held the trace.
	Entry("a trace rain day breaks the dry run and keeps its hundredths: 0.01 mm is wet, not a dry 0.0",
		[]dayRow{
			day("2020-01-01", 30.0, 20.0, 0.0, 1.0),
			day("2020-01-02", 30.0, 20.0, 0.01, 1.0),
			day("2020-01-03", 30.0, 20.0, 0.0, 1.0),
			day("2020-01-04", 30.0, 20.0, 0.0, 1.0),
		},
		wantSummary(coverageOf("2020-01-01", "2020-01-04", 4, 4, 4, 4, 4),
			recordOf("30.00", "2020-01-01"), recordOf("20.00", "2020-01-01"), recordOf("0.01", "2020-01-02"),
			dryOf(2, "2020-01-03", "2020-01-04"))),
	Entry("no dry day at all: dry_spell is null while the records stand",
		[]dayRow{
			day("2020-01-01", 30.0, 20.0, 1.5, 1.0),
			day("2020-01-02", 31.0, 19.0, nil, 1.1),
			day("2020-01-03", 29.0, 18.0, 0.1, 1.2),
		},
		wantSummary(coverageOf("2020-01-01", "2020-01-03", 3, 3, 3, 2, 3),
			recordOf("31.00", "2020-01-02"), recordOf("18.00", "2020-01-03"), recordOf("1.50", "2020-01-01"),
			nil)),
	Entry("a tie on each record's value: the earliest date wins for the maximum, the minimum, and the 1-day precip",
		[]dayRow{
			day("2020-02-01", 31.0, 0.5, 2.0, 1.0),
			day("2020-02-02", 30.0, -1.5, 3.0, 1.0),
			day("2020-02-03", 31.0, 0.0, 12.4, 1.0),
			day("2020-02-04", 25.0, -1.5, 12.4, 1.0),
		},
		wantSummary(coverageOf("2020-02-01", "2020-02-04", 4, 4, 4, 4, 4),
			recordOf("31.00", "2020-02-01"), recordOf("-1.50", "2020-02-02"), recordOf("12.40", "2020-02-03"),
			nil)),
)
