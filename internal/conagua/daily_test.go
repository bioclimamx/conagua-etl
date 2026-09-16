package conagua

import (
	"bufio"
	"bytes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// parseDailyFixture runs ParseDaily over a daily/ fixture, collecting
// rows and warnings. skipHeader composes ParseHeader first, the way
// ingest consumes a full station file.
func parseDailyFixture(name string, skipHeader bool, lineOffset int) ([]DailyRow, []Warning) {
	GinkgoHelper()
	br := bufio.NewReader(bytes.NewReader(readFixture("daily", name)))
	if skipHeader {
		_, _, err := ParseHeader(br)
		Expect(err).NotTo(HaveOccurred())
	}

	var rows []DailyRow
	var warnings []Warning
	err := ParseDaily(br, lineOffset,
		func(r DailyRow) { rows = append(rows, r) },
		func(w Warning) { warnings = append(warnings, w) },
	)
	Expect(err).NotTo(HaveOccurred())
	return rows, warnings
}

var _ = Describe("ParseDaily", func() {
	Context("composed with ParseHeader on a real file (station 01001)", func() {
		It("round-trips every row and field, with no warnings", func() {
			rows, warnings := parseDailyFixture("real_01001.txt", true, 0)
			Expect(warnings).To(BeEmpty())

			// The fixture holds the first 15 data rows of 01001.txt
			// (1878-01-01 onward): five observed-temperature days, then a
			// run of all-NULO temperature days — both shapes the national
			// archive is full of.
			want := []DailyRow{
				{Date: "1878-01-01", Tmax: fp(20.2), Tmin: fp(9.8), Precip: fp(0), Evap: nil},
				{Date: "1878-01-02", Tmax: fp(20.2), Tmin: fp(19), Precip: fp(0), Evap: nil},
				{Date: "1878-01-03", Tmax: fp(20), Tmin: fp(11), Precip: fp(0), Evap: nil},
				{Date: "1878-01-04", Tmax: fp(19.9), Tmin: fp(8.1), Precip: fp(0), Evap: nil},
				{Date: "1878-01-05", Tmax: fp(19.6), Tmin: fp(9.8), Precip: fp(0), Evap: nil},
				{Date: "1878-01-06", Tmax: nil, Tmin: nil, Precip: fp(0), Evap: nil},
				{Date: "1878-01-07", Tmax: nil, Tmin: nil, Precip: fp(0), Evap: nil},
				{Date: "1878-01-08", Tmax: nil, Tmin: nil, Precip: fp(0), Evap: nil},
				{Date: "1878-01-09", Tmax: nil, Tmin: nil, Precip: fp(0), Evap: nil},
				{Date: "1878-01-10", Tmax: nil, Tmin: nil, Precip: fp(0), Evap: nil},
				{Date: "1878-01-11", Tmax: nil, Tmin: nil, Precip: fp(0), Evap: nil},
				{Date: "1878-01-12", Tmax: nil, Tmin: nil, Precip: fp(0), Evap: nil},
				{Date: "1878-01-13", Tmax: nil, Tmin: nil, Precip: fp(0), Evap: nil},
				{Date: "1878-01-14", Tmax: nil, Tmin: nil, Precip: fp(0), Evap: nil},
				{Date: "1878-01-15", Tmax: nil, Tmin: nil, Precip: fp(0), Evap: nil},
			}
			Expect(rows).To(HaveLen(len(want)))
			for i, w := range want {
				Expect(rows[i]).To(Equal(w), "row %d (%s)", i, w.Date)
			}
		})
	})

	Context("with malformed values, sentinels, short rows, and blank lines", func() {
		It("round-trips the surviving rows field by field", func() {
			rows, _ := parseDailyFixture("edge_cases.txt", false, 0)

			want := []DailyRow{
				// 2020-01-01 happy path.
				{Date: "2020-01-01", Tmax: fp(20.2), Tmin: fp(9.8), Precip: fp(0), Evap: nil},
				// 2020-01-02 NULO temps.
				{Date: "2020-01-02", Tmax: nil, Tmin: nil, Precip: fp(1.5), Evap: fp(3)},
				// 2020-01-03: -999 sentinel for EVAP only.
				{Date: "2020-01-03", Tmax: fp(20), Tmin: fp(11), Precip: fp(0), Evap: nil},
				// 2020-01-04: bogus TMAX → nil + warning; other fields still parse.
				{Date: "2020-01-04", Tmax: nil, Tmin: fp(8.1), Precip: fp(0), Evap: nil},
				// 2020-01-05 short row is SKIPPED — not in this list.
				{Date: "2020-01-06", Tmax: fp(19.6), Tmin: fp(9.8), Precip: fp(0), Evap: nil},
			}
			Expect(rows).To(HaveLen(len(want)))
			for i, w := range want {
				Expect(rows[i]).To(Equal(w), "row %d (%s)", i, w.Date)
			}
		})

		It("emits one warning per issue, keyed to the source line", func() {
			_, warnings := parseDailyFixture("edge_cases.txt", false, 0)
			Expect(warnings).To(Equal([]Warning{
				{Line: 6, Message: `malformed TMAX "abc": strconv.ParseFloat: parsing "abc": invalid syntax`},
				{Line: 7, Message: "daily row has 3 fields, expected 5: \"2020-01-05\\tSHORT\\tROW\""},
			}))
		})

		It("shifts warning line numbers by lineOffset", func() {
			// ingest passes the count of header lines already consumed so
			// warnings point at positions in the full source file.
			_, warnings := parseDailyFixture("edge_cases.txt", false, 100)
			Expect(warnings).To(HaveLen(2))
			Expect(warnings[0].Line).To(Equal(106))
			Expect(warnings[1].Line).To(Equal(107))
		})
	})
})
