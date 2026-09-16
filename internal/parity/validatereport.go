package parity

import (
	"fmt"
	"strings"
)

// ReferenceValidateRules are the rule ids the reference validate build
// writes under source_file = 'validate:<id>', in its execution order.
// They are the only rules the validate parity gate compares: a rule id
// outside this set has no reference counterpart and is counted, never
// compared.
var ReferenceValidateRules = []string{
	"orphan-runs",
	"bbox",
	"wmo-month-completeness",
	"daily-sanity",
	"cross-period",
}

// validateSourcePrefix is the parsing_warnings.source_file namespace
// both tools write their findings under.
const validateSourcePrefix = "validate:"

// unresolvedStationPrefix marks a station_id that points at no stations
// row. Such a row cannot resolve to a natural key; the surrogate is
// surfaced rather than conflated with a NULL station.
const unresolvedStationPrefix = "unresolved-station-id:"

// ValidateWarning is one parsing_warnings tuple written by validate, as
// the comparator keys it: the owning station's (source, external_id)
// natural key through the stations join, rendered by StationKey — the
// key the DB comparator aligns on, since stations has no UNIQUE on
// external_id alone — empty for a NULL station_id, so NULL compares
// equal to NULL and unequal to any station; plus the source_file,
// severity, and issue text. Surrogate ids and the (always NULL) line
// column never take part; neither build assigns them meaningfully.
type ValidateWarning struct {
	Station    string
	SourceFile string
	Severity   string
	Issue      string
}

// StationKey renders a station's natural key as the comparator carries
// it: source and external_id joined by a slash, the form the bbox rule's
// own issue texts already use.
func StationKey(source, externalID string) string {
	return source + "/" + externalID
}

// RuleID is the rule the tuple belongs to — source_file without the
// 'validate:' namespace.
func (w ValidateWarning) RuleID() string {
	return strings.TrimPrefix(w.SourceFile, validateSourcePrefix)
}

// String renders the tuple for reports and assertion messages.
func (w ValidateWarning) String() string {
	station := w.Station
	if station == "" {
		station = renderedNull
	}
	return fmt.Sprintf("%s %s %q", station, w.Severity, w.Issue)
}

// ValidateRowDiff is one tuple whose multiplicity differs between the
// two sides: Base and New are its row counts on each. A tuple present on
// one side only has a zero on the other.
type ValidateRowDiff struct {
	Warning ValidateWarning
	Base    int
	New     int
}

// ValidateRule tallies one reference rule's multiset comparison. Rows are
// counted with multiplicity: Identical is the number of base rows
// matched one-for-one by a new row of the same tuple; BaseOnly and
// NewOnly hold the distinct tuples with unmatched rows on each side
// (their Total the tuple count, each sample carrying both
// multiplicities), and BaseOnlyRows / NewOnlyRows the unmatched rows
// themselves. A changed issue text or severity therefore shows as one
// base-only and one new-only tuple.
type ValidateRule struct {
	ID string

	BaseRows int
	NewRows  int

	Identical    int
	BaseOnly     Sampled[ValidateRowDiff]
	NewOnly      Sampled[ValidateRowDiff]
	BaseOnlyRows int
	NewOnlyRows  int

	// Per-side severity tallies — the counts the reference build's stdout
	// summary reported for the rule, recovered from its rows.
	BaseWarnings int
	BaseErrors   int
	NewWarnings  int
	NewErrors    int
}

// Clean reports whether the rule's two multisets are identical.
func (r ValidateRule) Clean() bool {
	return r.BaseOnly.Total == 0 && r.NewOnly.Total == 0
}

// ValidateRuleSummary is one rule's Scanned / warn / error summary as a
// tool reported it at run time — for this repo's verb, a
// validate.RuleReport's counts. Scanned is never stored in the DB, so
// the base side has no summary; the base rows' severity tallies stand
// in for it.
type ValidateRuleSummary struct {
	ID       string
	Scanned  int
	Warnings int
	Errors   int
}

// ValidateNativeRule is a validate rule id outside ReferenceValidateRules
// found on either side — a native rule with no reference counterpart — and
// its row count per side. Reported only; never compared.
type ValidateNativeRule struct {
	ID       string
	BaseRows int
	NewRows  int
}

// ValidateComparison is the full validate parity result: one
// ValidateRule per reference rule in ReferenceValidateRules order, the native
// rule counts, and — when the caller supplies them — this repo's own
// per-rule run summaries for the side-by-side table.
type ValidateComparison struct {
	// BaseLabel and NewLabel identify the two databases in the report.
	// CompareValidate receives open handles, not paths, so the caller
	// sets them before rendering.
	BaseLabel string
	NewLabel  string

	Rules  []ValidateRule
	Native []ValidateNativeRule

	// NewSummary carries this repo's verb summaries for the new side,
	// keyed by rule id, when the caller ran it and has the Report; nil
	// when unavailable. Rendered beside the base rows' tallies.
	NewSummary []ValidateRuleSummary
}

// Rule returns the reference rule's tally by id.
func (c *ValidateComparison) Rule(id string) (ValidateRule, bool) {
	for _, r := range c.Rules {
		if r.ID == id {
			return r, true
		}
	}
	return ValidateRule{}, false
}

// Summary returns the caller-supplied new-side summary for a rule.
func (c *ValidateComparison) Summary(id string) (ValidateRuleSummary, bool) {
	for _, s := range c.NewSummary {
		if s.ID == id {
			return s, true
		}
	}
	return ValidateRuleSummary{}, false
}

// Clean reports whether every reference rule compared identical. Native
// rules never count.
func (c *ValidateComparison) Clean() bool {
	for _, r := range c.Rules {
		if !r.Clean() {
			return false
		}
	}
	return true
}

// BaseRows sums the base side's rows over the reference rules.
func (c *ValidateComparison) BaseRows() int {
	n := 0
	for _, r := range c.Rules {
		n += r.BaseRows
	}
	return n
}

// NewRows sums the new side's rows over the reference rules.
func (c *ValidateComparison) NewRows() int {
	n := 0
	for _, r := range c.Rules {
		n += r.NewRows
	}
	return n
}

// Identical sums the one-for-one matched rows over the reference rules.
func (c *ValidateComparison) Identical() int {
	n := 0
	for _, r := range c.Rules {
		n += r.Identical
	}
	return n
}
