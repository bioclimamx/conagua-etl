package parity_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
)

// The seeded baseline per reference rule: rows on each side. daily-sanity
// carries its diurnal tuple twice.
var validateSeedCounts = map[string]int{
	"orphan-runs":            1,
	"bbox":                   2,
	"wmo-month-completeness": 2,
	"daily-sanity":           3,
	"cross-period":           1,
}

var _ = Describe("CompareValidate", func() {
	It("reports identically-seeded DBs as identical per rule despite divergent surrogate ids, ignoring ingest's own warnings", func(ctx SpecContext) {
		baseDB, newDB := seedValidatePair()

		cmp := mustCompareValidate(ctx, baseDB, newDB)

		Expect(cmp.Clean()).To(BeTrue())
		ids := make([]string, 0, len(cmp.Rules))
		for _, r := range cmp.Rules {
			ids = append(ids, r.ID)
			expectValidateRuleClean(r, validateSeedCounts[r.ID])
		}
		Expect(ids).To(Equal(parity.ReferenceValidateRules), "one tally per reference rule, in execution order")
		Expect(cmp.BaseRows()).To(Equal(9))
		Expect(cmp.NewRows()).To(Equal(9))
		Expect(cmp.Identical()).To(Equal(9))
		Expect(cmp.Native).To(BeEmpty())

		// Severity tallies per side recover the tools' summary counts.
		bbox := validateRule(cmp, "bbox")
		Expect([]int{bbox.BaseWarnings, bbox.BaseErrors, bbox.NewWarnings, bbox.NewErrors}).To(Equal([]int{1, 1, 1, 1}))
		daily := validateRule(cmp, "daily-sanity")
		Expect([]int{daily.BaseWarnings, daily.BaseErrors, daily.NewWarnings, daily.NewErrors}).To(Equal([]int{3, 0, 3, 0}))
	})

	It("names the rule of a row missing from the new side, with the tuple and both multiplicities", func(ctx SpecContext) {
		baseDB, newDB := seedValidatePair()
		missing := validateSeedRowsFor("wmo-month-completeness")[1]
		execSQL(newDB, `DELETE FROM parsing_warnings WHERE issue = ?`, missing.issue)

		cmp := mustCompareValidate(ctx, baseDB, newDB)

		Expect(cmp.Clean()).To(BeFalse())
		r := validateRule(cmp, "wmo-month-completeness")
		Expect(r.BaseRows).To(Equal(2))
		Expect(r.NewRows).To(Equal(1))
		Expect(r.Identical).To(Equal(1))
		Expect(r.BaseOnly.Total).To(Equal(1))
		Expect(r.BaseOnlyRows).To(Equal(1))
		Expect(r.BaseOnly.Samples).To(Equal([]parity.ValidateRowDiff{
			{Warning: validateWarningFor(missing), Base: 1, New: 0},
		}))
		Expect(r.NewOnly.Total).To(BeZero())
		Expect(r.NewOnlyRows).To(BeZero())
		for _, other := range cmp.Rules {
			if other.ID != "wmo-month-completeness" {
				expectValidateRuleClean(other, validateSeedCounts[other.ID])
			}
		}
	})

	It("names the rule of an extra row on the new side", func(ctx SpecContext) {
		baseDB, newDB := seedValidatePair()
		extra := validateSeedRow{validateExtB, "cross-period", "warn",
			"tmin cross-period delta exceeds ±3.0°C in 1 (month, period-pair)(s); top: m=12 1961-1990 vs 1991-2020: 6.1 vs 2.6 (Δ+3.5°C)"}
		insertValidateRow(newDB, extra)

		cmp := mustCompareValidate(ctx, baseDB, newDB)

		Expect(cmp.Clean()).To(BeFalse())
		r := validateRule(cmp, "cross-period")
		Expect(r.BaseRows).To(Equal(1))
		Expect(r.NewRows).To(Equal(2))
		Expect(r.Identical).To(Equal(1))
		Expect(r.BaseOnly.Total).To(BeZero())
		Expect(r.NewOnly.Total).To(Equal(1))
		Expect(r.NewOnlyRows).To(Equal(1))
		Expect(r.NewOnly.Samples).To(Equal([]parity.ValidateRowDiff{
			{Warning: validateWarningFor(extra), Base: 0, New: 1},
		}))
		Expect(r.NewWarnings).To(Equal(2))
	})

	It("shows a changed issue text as one base-only and one new-only tuple of the same rule", func(ctx SpecContext) {
		baseDB, newDB := seedValidatePair()
		orig := validateSeedRowsFor("cross-period")[0]
		changed := orig
		changed.issue = "tmax cross-period delta exceeds ±3.0°C in 1 (month, period-pair)(s); top: m=01 1981-2010 vs 1991-2020: 25.0 vs 30.5 (Δ-5.5°C)"
		execSQL(newDB, `UPDATE parsing_warnings SET issue = ? WHERE issue = ?`, changed.issue, orig.issue)

		cmp := mustCompareValidate(ctx, baseDB, newDB)

		r := validateRule(cmp, "cross-period")
		Expect(r.BaseRows).To(Equal(1))
		Expect(r.NewRows).To(Equal(1))
		Expect(r.Identical).To(BeZero())
		Expect(r.BaseOnly.Samples).To(Equal([]parity.ValidateRowDiff{{Warning: validateWarningFor(orig), Base: 1, New: 0}}))
		Expect(r.NewOnly.Samples).To(Equal([]parity.ValidateRowDiff{{Warning: validateWarningFor(changed), Base: 0, New: 1}}))
		Expect(r.BaseOnlyRows).To(Equal(1))
		Expect(r.NewOnlyRows).To(Equal(1))
	})

	It("shows a changed severity as a tuple drift and moves the severity tallies", func(ctx SpecContext) {
		baseDB, newDB := seedValidatePair()
		orig := validateSeedRowsFor("bbox")[1]
		Expect(orig.severity).To(Equal("error"))
		changed := orig
		changed.severity = "warn"
		execSQL(newDB, `UPDATE parsing_warnings SET severity = 'warn' WHERE issue = ?`, orig.issue)

		cmp := mustCompareValidate(ctx, baseDB, newDB)

		r := validateRule(cmp, "bbox")
		Expect(r.Identical).To(Equal(1))
		Expect(r.BaseOnly.Samples).To(Equal([]parity.ValidateRowDiff{{Warning: validateWarningFor(orig), Base: 1, New: 0}}))
		Expect(r.NewOnly.Samples).To(Equal([]parity.ValidateRowDiff{{Warning: validateWarningFor(changed), Base: 0, New: 1}}))
		Expect([]int{r.BaseWarnings, r.BaseErrors, r.NewWarnings, r.NewErrors}).To(Equal([]int{1, 1, 2, 0}))
	})

	It("counts multiplicity: a tuple seeded twice on the base side and once on the new side is one base-only tuple carrying one excess row", func(ctx SpecContext) {
		baseDB, newDB := seedValidatePair()
		dup := validateSeedRowsFor("daily-sanity")[0]
		execSQL(newDB, `DELETE FROM parsing_warnings WHERE id = (
			SELECT MAX(id) FROM parsing_warnings WHERE issue = ?)`, dup.issue)

		cmp := mustCompareValidate(ctx, baseDB, newDB)

		r := validateRule(cmp, "daily-sanity")
		Expect(r.BaseRows).To(Equal(3))
		Expect(r.NewRows).To(Equal(2))
		Expect(r.Identical).To(Equal(2))
		Expect(r.BaseOnly.Total).To(Equal(1))
		Expect(r.BaseOnlyRows).To(Equal(1))
		Expect(r.BaseOnly.Samples).To(Equal([]parity.ValidateRowDiff{{Warning: validateWarningFor(dup), Base: 2, New: 1}}))
		Expect(r.NewOnly.Total).To(BeZero())
	})

	It("compares a NULL station as NULL: equal to NULL on the other side, unequal to any station", func(ctx SpecContext) {
		baseDB, newDB := seedValidatePair()
		orphan := validateSeedRowsFor("orphan-runs")[0]
		Expect(orphan.station).To(BeEmpty())
		// Identical while NULL on both sides.
		expectValidateRuleClean(validateRule(mustCompareValidate(ctx, baseDB, newDB), "orphan-runs"), 1)

		// Anchoring the new side's row to a station makes it a different tuple.
		execSQL(newDB, `UPDATE parsing_warnings SET station_id = ? WHERE issue = ?`, validateNewA, orphan.issue)
		cmp := mustCompareValidate(ctx, baseDB, newDB)

		r := validateRule(cmp, "orphan-runs")
		Expect(r.Identical).To(BeZero())
		Expect(r.BaseOnly.Samples).To(Equal([]parity.ValidateRowDiff{{Warning: validateWarningFor(orphan), Base: 1, New: 0}}))
		anchored := orphan
		anchored.station = validateExtA
		Expect(r.NewOnly.Samples).To(Equal([]parity.ValidateRowDiff{{Warning: validateWarningFor(anchored), Base: 0, New: 1}}))
	})

	It("surfaces a station_id with no stations row by its surrogate instead of conflating it with NULL", func(ctx SpecContext) {
		baseDB, newDB := seedValidatePair()
		execSQL(baseDB, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
			VALUES (999, 'validate:bbox', NULL, 'warn', 'dangling')`)
		execSQL(newDB, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
			VALUES (NULL, 'validate:bbox', NULL, 'warn', 'dangling')`)

		cmp := mustCompareValidate(ctx, baseDB, newDB)

		r := validateRule(cmp, "bbox")
		Expect(r.Identical).To(Equal(2))
		Expect(r.BaseOnly.Samples).To(Equal([]parity.ValidateRowDiff{{
			Warning: parity.ValidateWarning{Station: "unresolved-station-id:999", SourceFile: "validate:bbox", Severity: "warn", Issue: "dangling"},
			Base:    1,
		}}))
		Expect(r.NewOnly.Samples).To(Equal([]parity.ValidateRowDiff{{
			Warning: parity.ValidateWarning{SourceFile: "validate:bbox", Severity: "warn", Issue: "dangling"},
			New:     1,
		}}))
	})

	It("counts native rule rows per side and never compares them", func(ctx SpecContext) {
		baseDB, newDB := seedValidatePair()
		execSQL(newDB, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
			VALUES (NULL, 'validate:station-refs', NULL, 'error', 'daily_observations: 3 rows reference 2 missing stations; first: 41, 42')`)
		execSQL(newDB, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
			VALUES (?, 'validate:station-refs', NULL, 'error', 'a second finding')`, validateNewA)
		execSQL(baseDB, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
			VALUES (NULL, 'validate:fill-leak', NULL, 'error', 'monthly_supplement.t2m_c: 1 row at the -999 fill')`)

		cmp := mustCompareValidate(ctx, baseDB, newDB)

		Expect(cmp.Clean()).To(BeTrue(), "native rows never fail the comparison")
		for _, r := range cmp.Rules {
			expectValidateRuleClean(r, validateSeedCounts[r.ID])
		}
		Expect(cmp.Native).To(Equal([]parity.ValidateNativeRule{
			{ID: "fill-leak", BaseRows: 1, NewRows: 0},
			{ID: "station-refs", BaseRows: 0, NewRows: 2},
		}))
		Expect(cmp.BaseRows()).To(Equal(9), "totals span the reference rules only")
		Expect(cmp.NewRows()).To(Equal(9))
	})

	It("caps the samples at DBSampleCap in sorted tuple order while the totals stay exact", func(ctx SpecContext) {
		baseDB, newDB := seedValidatePair()
		var issues []string
		for i := range parity.DBSampleCap + 5 {
			issue := fmt.Sprintf("tmin cross-period delta exceeds ±3.0°C in 1 (month, period-pair)(s); top: m=%02d", i)
			issues = append(issues, issue)
			insertValidateRow(newDB, validateSeedRow{validateExtB, "cross-period", "warn", issue})
		}

		cmp := mustCompareValidate(ctx, baseDB, newDB)

		r := validateRule(cmp, "cross-period")
		Expect(r.NewOnly.Total).To(Equal(parity.DBSampleCap + 5))
		Expect(r.NewOnlyRows).To(Equal(parity.DBSampleCap + 5))
		Expect(r.NewOnly.Samples).To(HaveLen(parity.DBSampleCap))
		Expect(r.NewOnly.Truncated()).To(BeTrue())
		for i, s := range r.NewOnly.Samples {
			Expect(s.Warning.Issue).To(Equal(issues[i]), "samples follow sorted tuple order")
			Expect(s.Warning.Station).To(Equal(validateKeyB))
		}
		Expect(r.Identical).To(Equal(1))
	})

	It("returns an error rather than a report when a side lacks the tables", func(ctx SpecContext) {
		baseDB, newDB := seedValidatePair()
		execSQL(newDB, `DROP TABLE parsing_warnings`)

		_, err := parity.CompareValidate(ctx, baseDB, newDB)
		Expect(err).To(MatchError(ContainSubstring("new: query")))
	})
})

var _ = Describe("ValidateWarning", func() {
	It("derives the rule id from the source_file namespace and renders NULL stations as NULL", func() {
		w := parity.ValidateWarning{SourceFile: "validate:orphan-runs", Severity: "warn", Issue: "reconciled"}
		Expect(w.RuleID()).To(Equal("orphan-runs"))
		Expect(w.String()).To(Equal(`NULL warn "reconciled"`))
		w.Station = parity.StationKey("conagua_conventional", "1001")
		Expect(w.String()).To(Equal(`conagua_conventional/1001 warn "reconciled"`))
	})

	It("keys a station by its natural key, source and external_id joined by a slash", func() {
		Expect(parity.StationKey("conagua_conventional", "1001")).To(Equal("conagua_conventional/1001"))
	})
})
