package parity_test

// The validate comparator over one crafted pair carrying every mismatch
// class at once, the whole ValidateComparison asserted field for field:
// a NULL station re-anchored (orphan-runs); a changed issue, a changed
// severity, and a dangling surrogate against a NULL (bbox); a finding
// moved to another station and a new-only row (wmo-month-completeness);
// a multiplicity drop and a base-only row (daily-sanity); a rule empty
// on both sides (cross-period); native rules on each side; ingest's own
// rows on both. Then the tuple order the samples follow, and the
// Markdown the crafted pair renders.

import (
	"database/sql"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
)

// craftedPair builds the pair: the identical seed on both sides, then
// one mutation per class on the side named.
func craftedPair() (baseDB, newDB *sql.DB) {
	GinkgoHelper()
	b, n := seedValidatePair()

	// orphan-runs: NULL station on base, anchored to A on new.
	orphan := validateSeedRowsFor("orphan-runs")[0]
	execSQL(n, `UPDATE parsing_warnings SET station_id = ? WHERE issue = ?`, validateNewA, orphan.issue)

	// bbox: A's issue text changed on new; B's severity changed on new;
	// a dangling surrogate on base against a NULL station on new.
	bboxA, bboxB := validateSeedRowsFor("bbox")[0], validateSeedRowsFor("bbox")[1]
	execSQL(n, `UPDATE parsing_warnings SET issue = ? WHERE issue = ?`, bboxAChanged, bboxA.issue)
	execSQL(n, `UPDATE parsing_warnings SET severity = 'warn' WHERE issue = ?`, bboxB.issue)
	execSQL(b, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
		VALUES (999, 'validate:bbox', NULL, 'warn', 'dangling')`)
	execSQL(n, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
		VALUES (NULL, 'validate:bbox', NULL, 'warn', 'dangling')`)

	// wmo-month-completeness: A's finding moved to B on new; an extra
	// row on new.
	wmoA := validateSeedRowsFor("wmo-month-completeness")[0]
	execSQL(n, `UPDATE parsing_warnings SET station_id = ? WHERE issue = ?`, validateNewB, wmoA.issue)
	insertValidateRow(n, validateSeedRow{validateExtA, "wmo-month-completeness", "warn", wmoExtra})

	// daily-sanity: one of the duplicated diurnal rows dropped on new;
	// the z-score row dropped on new.
	diurnal, zscore := validateSeedRowsFor("daily-sanity")[0], validateSeedRowsFor("daily-sanity")[2]
	execSQL(n, `DELETE FROM parsing_warnings WHERE id = (SELECT MAX(id) FROM parsing_warnings WHERE issue = ?)`, diurnal.issue)
	execSQL(n, `DELETE FROM parsing_warnings WHERE issue = ?`, zscore.issue)

	// cross-period: no rows on either side.
	execSQL(b, `DELETE FROM parsing_warnings WHERE source_file = 'validate:cross-period'`)
	execSQL(n, `DELETE FROM parsing_warnings WHERE source_file = 'validate:cross-period'`)

	// Native rules, one per side.
	execSQL(b, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
		VALUES (NULL, 'validate:fill-leak', NULL, 'error', 'monthly_supplement.t2m_c: 1 row at the -999 fill')`)
	execSQL(n, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
		VALUES (NULL, 'validate:station-refs', NULL, 'error', 'daily_observations: 3 rows naming no station')`)
	execSQL(n, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
		VALUES (?, 'validate:station-refs', NULL, 'error', 'a second finding')`, validateNewA)
	return b, n
}

const (
	bboxAChanged = `station conagua_conventional/1001 ("AGUASCALIENTES (OBS)") has NULL lat and lon`
	wmoExtra     = "WMO §4.4.1 fail in period 1991-2020, calendar month=03: 1 year(s) violate (≥11 missing or ≥5 consecutive); top: 1999 (12/31 missing, gap=12)"
)

func tuple(station, rule, severity, issue string) parity.ValidateWarning {
	return parity.ValidateWarning{Station: station, SourceFile: "validate:" + rule, Severity: severity, Issue: issue}
}

func sampled(diffs ...parity.ValidateRowDiff) parity.Sampled[parity.ValidateRowDiff] {
	return parity.Sampled[parity.ValidateRowDiff]{Total: len(diffs), Samples: diffs}
}

// craftedRules is the comparison the crafted pair must yield, every
// field written by hand.
func craftedRules() []parity.ValidateRule {
	orphan := validateSeedRowsFor("orphan-runs")[0]
	bboxA, bboxB := validateSeedRowsFor("bbox")[0], validateSeedRowsFor("bbox")[1]
	wmoA := validateSeedRowsFor("wmo-month-completeness")[0]
	diurnal, zscore := validateSeedRowsFor("daily-sanity")[0], validateSeedRowsFor("daily-sanity")[2]
	return []parity.ValidateRule{
		{
			ID: "orphan-runs", BaseRows: 1, NewRows: 1, Identical: 0,
			BaseOnly:     sampled(parity.ValidateRowDiff{Warning: tuple("", "orphan-runs", "warn", orphan.issue), Base: 1, New: 0}),
			NewOnly:      sampled(parity.ValidateRowDiff{Warning: tuple(validateKeyA, "orphan-runs", "warn", orphan.issue), Base: 0, New: 1}),
			BaseOnlyRows: 1, NewOnlyRows: 1,
			BaseWarnings: 1, BaseErrors: 0, NewWarnings: 1, NewErrors: 0,
		},
		{
			ID: "bbox", BaseRows: 3, NewRows: 3, Identical: 0,
			BaseOnly: sampled(
				parity.ValidateRowDiff{Warning: tuple(validateKeyA, "bbox", "warn", bboxA.issue), Base: 1, New: 0},
				parity.ValidateRowDiff{Warning: tuple(validateKeyB, "bbox", "error", bboxB.issue), Base: 1, New: 0},
				parity.ValidateRowDiff{Warning: tuple("unresolved-station-id:999", "bbox", "warn", "dangling"), Base: 1, New: 0},
			),
			NewOnly: sampled(
				parity.ValidateRowDiff{Warning: tuple("", "bbox", "warn", "dangling"), Base: 0, New: 1},
				parity.ValidateRowDiff{Warning: tuple(validateKeyA, "bbox", "warn", bboxAChanged), Base: 0, New: 1},
				parity.ValidateRowDiff{Warning: tuple(validateKeyB, "bbox", "warn", bboxB.issue), Base: 0, New: 1},
			),
			BaseOnlyRows: 3, NewOnlyRows: 3,
			BaseWarnings: 2, BaseErrors: 1, NewWarnings: 3, NewErrors: 0,
		},
		{
			ID: "wmo-month-completeness", BaseRows: 2, NewRows: 3, Identical: 1,
			BaseOnly: sampled(parity.ValidateRowDiff{Warning: tuple(validateKeyA, "wmo-month-completeness", "warn", wmoA.issue), Base: 1, New: 0}),
			NewOnly: sampled(
				parity.ValidateRowDiff{Warning: tuple(validateKeyA, "wmo-month-completeness", "warn", wmoExtra), Base: 0, New: 1},
				parity.ValidateRowDiff{Warning: tuple(validateKeyB, "wmo-month-completeness", "warn", wmoA.issue), Base: 0, New: 1},
			),
			BaseOnlyRows: 1, NewOnlyRows: 2,
			BaseWarnings: 2, BaseErrors: 0, NewWarnings: 3, NewErrors: 0,
		},
		{
			ID: "daily-sanity", BaseRows: 3, NewRows: 1, Identical: 1,
			BaseOnly: sampled(
				parity.ValidateRowDiff{Warning: tuple(validateKeyB, "daily-sanity", "warn", diurnal.issue), Base: 2, New: 1},
				parity.ValidateRowDiff{Warning: tuple(validateKeyB, "daily-sanity", "warn", zscore.issue), Base: 1, New: 0},
			),
			BaseOnlyRows: 2, NewOnlyRows: 0,
			BaseWarnings: 3, BaseErrors: 0, NewWarnings: 1, NewErrors: 0,
		},
		{ID: "cross-period"},
	}
}

var _ = Describe("CompareValidate over the crafted pair", func() {
	It("tallies every mismatch class in one comparison, field for field", func(ctx SpecContext) {
		baseDB, newDB := craftedPair()
		cmp := mustCompareValidate(ctx, baseDB, newDB)

		Expect(cmp.Rules).To(Equal(craftedRules()))
		Expect(cmp.Native).To(Equal([]parity.ValidateNativeRule{
			{ID: "fill-leak", BaseRows: 1, NewRows: 0},
			{ID: "station-refs", BaseRows: 0, NewRows: 2},
		}))
		Expect(cmp.Clean()).To(BeFalse())
		Expect(cmp.BaseRows()).To(Equal(1 + 3 + 2 + 3 + 0))
		Expect(cmp.NewRows()).To(Equal(1 + 3 + 3 + 1 + 0))
		Expect(cmp.Identical()).To(Equal(0 + 0 + 1 + 1 + 0))
		Expect(cmp.NewSummary).To(BeNil())
		Expect(cmp.BaseLabel).To(BeEmpty())
		Expect(cmp.NewLabel).To(BeEmpty())

		cross, ok := cmp.Rule("cross-period")
		Expect(ok).To(BeTrue())
		Expect(cross.Clean()).To(BeTrue(), "a rule with no rows on either side is clean — and vacuous")
		Expect(cross.BaseRows).To(BeZero())
		_, ok = cmp.Rule("station-refs")
		Expect(ok).To(BeFalse(), "a native rule has no reference tally")
		_, ok = cmp.Summary("bbox")
		Expect(ok).To(BeFalse(), "no summary was supplied")
	})

	It("orders a rule's samples by station key — NULL first, then the unresolved surrogate last — severity, and issue", func(ctx SpecContext) {
		baseDB, newDB := seedValidatePair()
		execSQL(baseDB, `DELETE FROM parsing_warnings WHERE source_file = 'validate:cross-period'`)
		execSQL(newDB, `DELETE FROM parsing_warnings WHERE source_file = 'validate:cross-period'`)
		for _, r := range []validateSeedRow{
			{validateExtB, "cross-period", "warn", "b"},
			{validateExtA, "cross-period", "warn", "z"},
			{validateExtA, "cross-period", "warn", "a"},
			{validateExtA, "cross-period", "error", "z"},
			{"", "cross-period", "warn", "null station"},
		} {
			insertValidateRow(newDB, r)
		}
		execSQL(newDB, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
			VALUES (999, 'validate:cross-period', NULL, 'error', 'dangling')`)

		r := validateRule(mustCompareValidate(ctx, baseDB, newDB), "cross-period")
		var order []string
		for _, d := range r.NewOnly.Samples {
			order = append(order, d.Warning.String())
		}
		Expect(order).To(Equal([]string{
			`NULL warn "null station"`,
			validateKeyA + ` error "z"`,
			validateKeyA + ` warn "a"`,
			validateKeyA + ` warn "z"`,
			validateKeyB + ` warn "b"`,
			`unresolved-station-id:999 error "dangling"`,
		}))
		Expect(r.NewOnlyRows).To(Equal(6))
		Expect([]int{r.NewWarnings, r.NewErrors}).To(Equal([]int{4, 2}))
	})

	It("renders the crafted pair's Markdown with each class where it belongs", func(ctx SpecContext) {
		baseDB, newDB := craftedPair()
		cmp := mustCompareValidate(ctx, baseDB, newDB)
		cmp.BaseLabel, cmp.NewLabel = "/scratch/parity-base.db", "/scratch/parity-ours.db"
		cmp.NewSummary = []parity.ValidateRuleSummary{{ID: "bbox", Scanned: 4, Warnings: 3, Errors: 0}}
		md := cmp.Markdown()

		Expect(md).To(ContainSubstring("- Base DB (the reference validate ran on it): `/scratch/parity-base.db`"))
		Expect(md).To(ContainSubstring("- New DB (this repo's validate ran on it): `/scratch/parity-ours.db`"))
		Expect(md).To(ContainSubstring("| orphan-runs | 1 | 1 | 0 | 1 | 1 |\n" +
			"| bbox | 3 | 3 | 0 | 3 | 3 |\n" +
			"| wmo-month-completeness | 2 | 3 | 1 | 1 | 2 |\n" +
			"| daily-sanity | 3 | 1 | 1 | 2 | 0 |\n" +
			"| cross-period | 0 | 0 | 0 | 0 | 0 |\n" +
			"| **total** | 9 | 8 | 2 | 7 | 6 |\n"))
		Expect(md).To(ContainSubstring("| bbox | 2 | 1 | 4 | 3 | 0 |\n"))
		Expect(md).To(ContainSubstring("| daily-sanity | 3 | 0 | — | — | — |\n"))
		Expect(md).To(ContainSubstring("| fill-leak | 1 | 0 |\n| station-refs | 0 | 2 |\n"))
		Expect(md).To(ContainSubstring("## orphan-runs\n\n### Base-only tuples (1, 1 rows)\n\n"))
		Expect(md).To(ContainSubstring("| NULL | warn | " + validateSeedRowsFor("orphan-runs")[0].issue + " | 1 | 0 |\n"))
		Expect(md).To(ContainSubstring("| " + validateKeyA + " | warn | " + validateSeedRowsFor("orphan-runs")[0].issue + " | 0 | 1 |\n"))
		Expect(md).To(ContainSubstring("| unresolved-station-id:999 | warn | dangling | 1 | 0 |\n"))
		Expect(md).To(ContainSubstring("| NULL | warn | dangling | 0 | 1 |\n"))
		Expect(md).To(ContainSubstring("| " + validateKeyB + " | warn | " + validateSeedRowsFor("daily-sanity")[0].issue + " | 2 | 1 |\n"))
		Expect(md).To(ContainSubstring("## cross-period\n\nNo drift: all 0 rows identical.\n"))
		Expect(strings.Count(md, "_showing first")).To(BeZero(), "nothing is truncated")
	})
})
