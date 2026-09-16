package parity_test

import (
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/parity"
)

// capDate spreads i over 28-day months so any number of generated ISO
// dates up to ~336 stays valid and sorts in generation order.
func capDate(i int) string {
	return fmt.Sprintf("1990-%02d-%02d", i/28+1, i%28+1)
}

var _ = Describe("Report.Markdown", func() {
	It("round-trips every findings category into its section table", func() {
		report := parity.Report{
			BaseDir: "/snap/base",
			NewDir:  "/snap/new",
			Kinds: []parity.KindReport{
				{
					Kind:          conagua.KindDaily,
					ValueCompared: true,
					BaseFiles:     5,
					NewFiles:      4,
					StationsBoth:  3,
					BaseOnly:      parity.Capped[string]{Total: 2, Examples: []string{"11111"}},
					NewOnly:       parity.Capped[string]{Total: 1, Examples: []string{"22222"}},
					ParseErrors: parity.Capped[parity.ParseError]{Total: 1, Examples: []parity.ParseError{
						{Station: "33333", Side: parity.SideBase, Err: "parse daily: bad | table"},
					}},
					StationsIdentical: 1,
					StationsRevised:   2,
					RowsIdentical:     7,
					RowsRevised:       1,
					RowsAppended: parity.Capped[parity.RowRef]{Total: 1, Examples: []parity.RowRef{
						{Station: "44444", Date: "1990-05-01"},
					}},
					RowsRemoved: parity.Capped[parity.RowRef]{Total: 1, Examples: []parity.RowRef{
						{Station: "44444", Date: "1990-05-02"},
					}},
					Revisions: parity.Capped[parity.Revision]{Total: 9, Examples: []parity.Revision{
						{Station: "44444", Date: "1990-05-03", Field: "Tmax", Base: "20.2", New: "NULL"},
					}},
					HeaderDiffs: parity.Capped[parity.Revision]{Total: 1, Examples: []parity.Revision{
						{Station: "44444", Field: "Header.Name", Base: "OLD NAME", New: "NEW NAME"},
					}},
					WarningDiffs: parity.Capped[parity.WarningCountDiff]{Total: 1, Examples: []parity.WarningCountDiff{
						{Station: "44444", Base: 3, New: 5},
					}},
				},
				{
					Kind:            conagua.KindNormals1991_2020,
					ValueCompared:   true,
					BaseFiles:       1,
					NewFiles:        1,
					StationsBoth:    1,
					StationsRevised: 1,
					Revisions: parity.Capped[parity.Revision]{Total: 1, Examples: []parity.Revision{
						{Station: "01001", Month: 2, Field: "Tmax", Base: "25.5", New: "26.1"},
					}},
				},
			},
		}

		md := report.Markdown()

		Expect(md).To(ContainSubstring("# Cross-snapshot parity report"))
		Expect(md).To(ContainSubstring("- Base snapshot: `/snap/base`"))
		Expect(md).To(ContainSubstring("- New snapshot: `/snap/new`"))

		// Summary rows carry the exact counters, in column order.
		Expect(md).To(ContainSubstring("| daily | value | 5 | 4 | 3 | 2 | 1 | 1 | 1 | 2 |"))
		Expect(md).To(ContainSubstring("| normals_1991_2020 | value | 1 | 1 | 1 | 0 | 0 | 0 | 0 | 1 |"))

		// Daily row-level accounting table.
		Expect(md).To(ContainSubstring("| 7 | 1 | 1 | 1 |"))

		Expect(md).To(ContainSubstring("### Parse errors (1)"))
		Expect(md).To(ContainSubstring(`| 33333 | base | parse daily: bad \| table |`))

		Expect(md).To(ContainSubstring("### Station universe drift"))
		Expect(md).To(ContainSubstring("- Base-only (2): `11111` … (+1 more)"))
		Expect(md).To(ContainSubstring("- New-only (1): `22222`"))

		Expect(md).To(ContainSubstring("### Field revisions (9)"))
		Expect(md).To(ContainSubstring("| 44444 | 1990-05-03 | Tmax | 20.2 | NULL |"))
		Expect(md).To(ContainSubstring("_showing first 1 of 9_"))

		Expect(md).To(ContainSubstring("### Appended rows (dates only in new) (1)"))
		Expect(md).To(ContainSubstring("| 44444 | 1990-05-01 |"))
		Expect(md).To(ContainSubstring("### Removed rows (dates only in base) (1)"))
		Expect(md).To(ContainSubstring("| 44444 | 1990-05-02 |"))

		Expect(md).To(ContainSubstring("### Header diffs (1)"))
		Expect(md).To(ContainSubstring("| 44444 | Header.Name | OLD NAME | NEW NAME |"))

		Expect(md).To(ContainSubstring("### Warning-count diffs (1)"))
		Expect(md).To(ContainSubstring("| 44444 | 3 | 5 |"))

		// Normals revisions key by month, not date.
		Expect(md).To(ContainSubstring("| Station | Month | Field | Base | New |"))
		Expect(md).To(ContainSubstring("| 01001 | 2 | Tmax | 25.5 | 26.1 |"))
	})

	It("renders a clean comparison as no-drift prose with presence-only summary rows", func(ctx SpecContext) {
		body := dailyBody(obsName, threeRows()...)
		report := runCompare(ctx,
			map[string]string{"daily/01001": body, "monthly/01001": "monthly A\n"},
			map[string]string{
				"daily/01001":   mustReplace(body, "17/04/2026", "01/06/2026"),
				"monthly/01001": "monthly B\n",
			},
		)

		md := report.Markdown()
		Expect(md).To(ContainSubstring("| daily | value | 1 | 1 | 1 | 0 | 0 | 0 | 1 | 0 |"))
		Expect(md).To(ContainSubstring("| monthly | presence-only | 1 | 1 | 1 | 0 | 0 | — | — | — |"))
		Expect(md).To(ContainSubstring("| extremes | presence-only | 0 | 0 | 0 | 0 | 0 | — | — | — |"))
		Expect(md).To(ContainSubstring("`monthly` and `extremes` are **presence-only**"))
		Expect(md).To(ContainSubstring("| 3 | 0 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("No drift: all 1 stations present on both sides parsed identically."))
		Expect(md).NotTo(ContainSubstring("### "), "a clean comparison must render no findings sections")
	})

	It("caps example lists at ExampleCap and marks the truncation explicitly", func(ctx SpecContext) {
		total := parity.ExampleCap + 5
		baseRows := make([]string, total)
		newRows := make([]string, total)
		for i := 0; i < total; i++ {
			baseRows[i] = capDate(i) + "\t0\tNULO\t20\t10"
			newRows[i] = capDate(i) + "\t0\tNULO\t21\t10"
		}
		report := runCompare(ctx,
			map[string]string{"daily/01001": dailyBody(obsName, baseRows...)},
			map[string]string{"daily/01001": dailyBody(obsName, newRows...)},
		)

		kr := kindReportFor(report, conagua.KindDaily)
		Expect(kr.Revisions.Total).To(Equal(total))
		Expect(kr.Revisions.Truncated()).To(BeTrue())
		want := make([]parity.Revision, parity.ExampleCap)
		for i := range want {
			want[i] = parity.Revision{Station: "01001", Date: capDate(i), Field: "Tmax", Base: "20", New: "21"}
		}
		Expect(kr.Revisions.Examples).To(Equal(want))

		md := report.Markdown()
		Expect(md).To(ContainSubstring(fmt.Sprintf("### Field revisions (%d)", total)))
		Expect(md).To(ContainSubstring(fmt.Sprintf("_showing first %d of %d_", parity.ExampleCap, total)))
		Expect(md).To(ContainSubstring("| daily | value | 1 | 1 | 1 | 0 | 0 | 0 | 0 | 1 |"))
		Expect(md).To(ContainSubstring(fmt.Sprintf("| 0 | 0 | 0 | %d |", total)))
		// Exactly the retained examples are rendered as table rows.
		Expect(strings.Count(md, "| 01001 | 1990-")).To(Equal(parity.ExampleCap))
	})
})
