package parity_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
)

var _ = Describe("ValidateComparison.Markdown", func() {
	It("renders every section: the summary, both tools' summaries, the native rules, and each rule's findings with truncation notes", func() {
		samples := make([]parity.ValidateRowDiff, parity.DBSampleCap)
		for i := range samples {
			samples[i] = parity.ValidateRowDiff{
				Warning: parity.ValidateWarning{Station: "2002", SourceFile: "validate:daily-sanity", Severity: "warn",
					Issue: fmt.Sprintf("tmax outliers > 3.0σ in calendar month=%02d", i+1)},
				New: 1,
			}
		}
		cmp := &parity.ValidateComparison{
			BaseLabel: "/db/base.db",
			NewLabel:  "/db/ours.db",
			Rules: []parity.ValidateRule{
				{ID: "orphan-runs", BaseRows: 1, NewRows: 1, Identical: 1, BaseWarnings: 1, NewWarnings: 1},
				{ID: "bbox", BaseRows: 2, NewRows: 2, Identical: 1,
					BaseOnly: parity.Sampled[parity.ValidateRowDiff]{Total: 1, Samples: []parity.ValidateRowDiff{{
						Warning: parity.ValidateWarning{Station: "2002", SourceFile: "validate:bbox", Severity: "error",
							Issue: `station conagua_conventional/2002 ("A | B") at impossible lat=0.0000 lon=0.0000`},
						Base: 1,
					}}},
					NewOnly: parity.Sampled[parity.ValidateRowDiff]{Total: 1, Samples: []parity.ValidateRowDiff{{
						Warning: parity.ValidateWarning{SourceFile: "validate:bbox", Severity: "warn", Issue: "moved to NULL"},
						New:     1,
					}}},
					BaseOnlyRows: 1, NewOnlyRows: 1,
					BaseWarnings: 1, BaseErrors: 1, NewWarnings: 2},
				{ID: "wmo-month-completeness", BaseRows: 3, NewRows: 3, Identical: 3, BaseWarnings: 3, NewWarnings: 3},
				{ID: "daily-sanity", BaseRows: 4, NewRows: 30, Identical: 4,
					NewOnly:     parity.Sampled[parity.ValidateRowDiff]{Total: 25, Samples: samples},
					NewOnlyRows: 26, BaseWarnings: 4, NewWarnings: 30},
				{ID: "cross-period", BaseRows: 5, NewRows: 4, Identical: 4,
					BaseOnly: parity.Sampled[parity.ValidateRowDiff]{Total: 1, Samples: []parity.ValidateRowDiff{{
						Warning: parity.ValidateWarning{Station: "1001", SourceFile: "validate:cross-period", Severity: "warn", Issue: "twice"},
						Base:    2, New: 1,
					}}},
					BaseOnlyRows: 1, BaseWarnings: 5, NewWarnings: 4},
			},
			Native: []parity.ValidateNativeRule{{ID: "station-refs", BaseRows: 0, NewRows: 2}},
			NewSummary: []parity.ValidateRuleSummary{
				{ID: "orphan-runs", Scanned: 1, Warnings: 1},
				{ID: "bbox", Scanned: 5524, Warnings: 2},
				{ID: "wmo-month-completeness", Scanned: 927354, Warnings: 3},
				{ID: "daily-sanity", Scanned: 1365692, Warnings: 30},
				// cross-period deliberately absent: a skipped rule has no summary.
			},
		}

		md := cmp.Markdown()

		Expect(md).To(ContainSubstring("# Validate parity report"))
		Expect(md).To(ContainSubstring("- Base DB (the reference validate ran on it): `/db/base.db`"))
		Expect(md).To(ContainSubstring("- New DB (this repo's validate ran on it): `/db/ours.db`"))
		Expect(md).To(ContainSubstring("`orphan-runs` is wall-clock dependent"))

		// Summary rows carry the exact tallies, in column order, and the
		// totals sum the reference rules.
		Expect(md).To(ContainSubstring("| Rule | Base rows | New rows | Identical | Base-only rows | New-only rows |"))
		Expect(md).To(ContainSubstring("| orphan-runs | 1 | 1 | 1 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("| bbox | 2 | 2 | 1 | 1 | 1 |"))
		Expect(md).To(ContainSubstring("| wmo-month-completeness | 3 | 3 | 3 | 0 | 0 |"))
		Expect(md).To(ContainSubstring("| daily-sanity | 4 | 30 | 4 | 0 | 26 |"))
		Expect(md).To(ContainSubstring("| cross-period | 5 | 4 | 4 | 1 | 0 |"))
		Expect(md).To(ContainSubstring("| **total** | 15 | 40 | 13 | 2 | 27 |"))

		// The tool summaries: base tallies from the rows, ours from the
		// supplied summaries, dashes for the rule without one.
		Expect(md).To(ContainSubstring("## Per-rule tool summaries"))
		Expect(md).To(ContainSubstring("| Rule | Base warn | Base error | Ours scanned | Ours warn | Ours error |"))
		Expect(md).To(ContainSubstring("| bbox | 1 | 1 | 5524 | 2 | 0 |"))
		Expect(md).To(ContainSubstring("| daily-sanity | 4 | 0 | 1365692 | 30 | 0 |"))
		Expect(md).To(ContainSubstring("| cross-period | 5 | 0 | — | — | — |"))

		Expect(md).To(ContainSubstring("## Native rules (reported, not compared)"))
		Expect(md).To(ContainSubstring("| station-refs | 0 | 2 |"))

		// Per-rule sections: clean rules say so; drifted rules table
		// their tuples with both multiplicities, the delimiter escaped,
		// a NULL station rendered as NULL, and truncation noted.
		Expect(md).To(ContainSubstring("## orphan-runs\n\nNo drift: all 1 rows identical."))
		Expect(md).To(ContainSubstring("## bbox\n\n### Base-only tuples (1, 1 rows)"))
		Expect(md).To(ContainSubstring(`| 2002 | error | station conagua_conventional/2002 ("A \| B") at impossible lat=0.0000 lon=0.0000 | 1 | 0 |`))
		Expect(md).To(ContainSubstring("### New-only tuples (1, 1 rows)"))
		Expect(md).To(ContainSubstring("| NULL | warn | moved to NULL | 0 | 1 |"))
		Expect(md).To(ContainSubstring("## daily-sanity\n\n### New-only tuples (25, 26 rows)"))
		Expect(md).To(ContainSubstring("| 2002 | warn | tmax outliers > 3.0σ in calendar month=20 | 0 | 1 |"))
		Expect(md).To(ContainSubstring("_showing first 20 of 25_"))
		Expect(md).To(ContainSubstring("## cross-period\n\n### Base-only tuples (1, 1 rows)"))
		Expect(md).To(ContainSubstring("| 1001 | warn | twice | 2 | 1 |"))
	})

	It("says so when no native rows exist and no summaries were supplied", func() {
		cmp := &parity.ValidateComparison{
			Rules: []parity.ValidateRule{{ID: "bbox", BaseRows: 1, NewRows: 1, Identical: 1}},
		}
		md := cmp.Markdown()
		Expect(md).To(ContainSubstring("No `validate:` rows outside the reference rule set on either side."))
		Expect(md).To(ContainSubstring("not supplied for this comparison"))
		Expect(md).To(ContainSubstring("| bbox | 0 | 0 | — | — | — |"))
	})
})
