package parity

// DriftCounts tallies per-value classifications for one column or one
// table: identical (equal on both sides,
// including NULL on both for aligned rows), revised (non-NULL on both
// sides with different values), appended (a value present only in the
// new DB), removed (present only in the base DB). Values in one-side-
// only rows count as appended/removed; their NULL columns — no value
// on either side — are not counted.
type DriftCounts struct {
	Identical int
	Revised   int
	Appended  int
	Removed   int
}

// DriftColumn is one supplement value column's drift tally.
type DriftColumn struct {
	Column string
	DriftCounts
}

// DriftTable is one supplement table's drift accounting: row-level
// alignment on the primary key, per-column and summed value
// classifications, and bounded samples. power_run_id is a per-side run
// pointer, not data, and is never compared.
type DriftTable struct {
	Table string

	// RowsAligned counts rows whose primary key exists on both sides;
	// BaseOnly / NewOnly hold whole rows present on one side only.
	RowsAligned int
	BaseOnly    Sampled[DBKey]
	NewOnly     Sampled[DBKey]

	// Values sums the per-column tallies; Columns carries them per
	// value column, in registry (= DDL) order.
	Values  DriftCounts
	Columns []DriftColumn

	// RevisedSamples holds concrete revised values — the drift class
	// worth eyeballing, since it shows upstream re-versioning of data
	// both pulls covered.
	RevisedSamples Sampled[DBDiff]
}

// BaseRows derives the base side's row universe.
func (t DriftTable) BaseRows() int { return t.RowsAligned + t.BaseOnly.Total }

// NewRows derives the new side's row universe.
func (t DriftTable) NewRows() int { return t.RowsAligned + t.NewOnly.Total }

// PowerDrift is the full supplement drift result between two power
// databases. Report-only by design: drift against the base DB is
// upstream POWER movement, never a port gate.
type PowerDrift struct {
	// BaseLabel and NewLabel identify the two databases in the report;
	// ComparePowerDrift receives open handles, so the caller sets them.
	BaseLabel string
	NewLabel  string

	Monthly DriftTable
	Daily   DriftTable
}

// Tables lists the per-table tallies in report order.
func (d *PowerDrift) Tables() []DriftTable {
	return []DriftTable{d.Monthly, d.Daily}
}
