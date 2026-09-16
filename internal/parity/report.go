package parity

import "github.com/bioclimamx/conagua-etl/internal/conagua"

// ExampleCap bounds every per-category example list in a Report. Totals
// are always exact; examples beyond the cap are dropped, and the
// markdown renderer notes each truncation explicitly so a capped list
// can never read as a complete one.
const ExampleCap = 100

// Side identifies which of the two snapshots a finding belongs to.
type Side string

// The two snapshots under comparison.
const (
	SideBase Side = "base"
	SideNew  Side = "new"
)

// Capped is an exact counter with a bounded example list. Stations are
// aggregated in sorted order, so the retained examples are deterministic
// across runs regardless of worker scheduling.
type Capped[T any] struct {
	// Total is the exact number of findings in this category.
	Total int

	// Examples holds at most ExampleCap findings, in aggregation order.
	Examples []T
}

// Add records one finding, retaining it as an example if under the cap.
func (c *Capped[T]) Add(v T) {
	if len(c.Examples) < ExampleCap {
		c.Examples = append(c.Examples, v)
	}
	c.Total++
}

// Merge folds another counter into c, retaining examples up to the cap.
func (c *Capped[T]) Merge(o Capped[T]) {
	for _, v := range o.Examples {
		if len(c.Examples) >= ExampleCap {
			break
		}
		c.Examples = append(c.Examples, v)
	}
	c.Total += o.Total
}

// Truncated reports whether findings beyond the retained examples exist.
func (c Capped[T]) Truncated() bool { return c.Total > len(c.Examples) }

// ParseError is a file that one side's parser could not parse. Any parse
// error is a first-class finding: the pull parity claim is that our
// parsers parse every national CONAGUA file, so a failure on either side
// is a defect signal, never routine drift.
type ParseError struct {
	Station string
	Side    Side
	Err     string
}

// RowRef locates one daily row: a station plus the row's ISO date.
type RowRef struct {
	Station string
	Date    string
}

// Revision is one parsed field whose value differs between snapshots.
// Date is set for daily rows, Month (1..12) for normals rows; both are
// zero for station-header fields. Base/New are rendered values, with
// "NULL" standing for an absent (nil) value.
type Revision struct {
	Station string
	Date    string
	Month   int
	Field   string
	Base    string
	New     string
}

// WarningCountDiff records a station+kind whose per-file parser warning
// count differs between snapshots — a coarse signal that the file's
// malformed-content profile changed even if every parsed value matches.
type WarningCountDiff struct {
	Station string
	Base    int
	New     int
}

// KindReport is the comparison result for one file kind. For
// presence-only kinds (no parser exists in either repo) only the
// file/universe counters are populated and ValueCompared is false.
type KindReport struct {
	Kind conagua.Kind

	// ValueCompared is true for the kinds with a parser (daily and the
	// four normals periods); false for monthly and extremes, whose
	// files are counted for presence but never value-compared.
	ValueCompared bool

	// File universe: counts on each side, plus the overlap and the
	// stations present on only one side (universe findings, not parse
	// errors).
	BaseFiles    int
	NewFiles     int
	StationsBoth int
	BaseOnly     Capped[string]
	NewOnly      Capped[string]

	ParseErrors Capped[ParseError]

	// Station-level outcome over stations present and parseable on
	// both sides: identical means the full parsed content (header,
	// rows, warning count) matched; revised means at least one diff.
	StationsIdentical int
	StationsRevised   int

	// Daily row-level accounting (kind == daily only). Appended rows
	// have dates present only in the new snapshot; removed only in the
	// base; revised have the same date but at least one differing field.
	RowsIdentical int
	RowsAppended  Capped[RowRef]
	RowsRemoved   Capped[RowRef]
	RowsRevised   int

	// Field-level diffs: data-field revisions, station-header diffs,
	// and per-file warning-count differences.
	Revisions    Capped[Revision]
	HeaderDiffs  Capped[Revision]
	WarningDiffs Capped[WarningCountDiff]
}

// UniverseIdentical reports whether both snapshots hold exactly the same
// station set for this kind.
func (k KindReport) UniverseIdentical() bool {
	return k.BaseOnly.Total == 0 && k.NewOnly.Total == 0
}

// Report is the full cross-snapshot comparison result, one KindReport
// per CONAGUA file kind in canonical order.
type Report struct {
	BaseDir string
	NewDir  string
	Kinds   []KindReport
}

// TotalParseErrors sums parse errors across every value-compared kind.
func (r Report) TotalParseErrors() int {
	n := 0
	for _, k := range r.Kinds {
		n += k.ParseErrors.Total
	}
	return n
}
