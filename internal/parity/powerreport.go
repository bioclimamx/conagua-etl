package parity

import "database/sql"

// PowerValueDiff is one column whose stored value in the database
// differs from the value this repo's ported math derives for the same
// key. Derived/Stored are rendered values; floats render at full
// round-trip precision because the gate compares them bit-for-bit —
// both sides are float64 arithmetic over the same inputs, so any
// difference is a port defect, never noise.
type PowerValueDiff struct {
	Key     DBKey
	Column  string
	Derived string
	Stored  string
}

// PowerTable tallies one derived-vs-stored table comparison: keys
// aligned on both sides (compared), of which identical vs carrying
// value diffs, plus keys present on one side only.
type PowerTable struct {
	Table string

	RowsCompared  int
	RowsIdentical int

	ValueDiffs Sampled[PowerValueDiff]

	// DerivedOnly holds keys our station math derives that the DB does
	// not store; StoredOnly holds DB rows not derivable from any
	// station in the compared universe.
	DerivedOnly Sampled[DBKey]
	StoredOnly  Sampled[DBKey]
}

// DerivedRows derives the math side's row universe: every derived row
// is either aligned with a stored row or derived-only.
func (t PowerTable) DerivedRows() int { return t.RowsCompared + t.DerivedOnly.Total }

// StoredRows derives the DB side's row universe symmetrically.
func (t PowerTable) StoredRows() int { return t.RowsCompared + t.StoredOnly.Total }

// Clean reports whether the table compared without any finding.
func (t PowerTable) Clean() bool {
	return t.ValueDiffs.Total == 0 && t.DerivedOnly.Total == 0 && t.StoredOnly.Total == 0
}

// PowerRunManifest carries one complete power_runs row's manifest
// columns verbatim, as loaded for the manifest checks and the report's
// side-by-side rendering.
type PowerRunManifest struct {
	ID              int64
	Temporal        string
	EndpointURL     string
	Parameters      string
	Community       string
	PeriodStartYear int64
	PeriodEndYear   int64
	PeriodStartDate sql.NullString
	PeriodEndDate   sql.NullString
	GridResolution  string
	SolarConversion float64
	UnitConversions sql.NullString
}

// ManifestFinding is one manifest field that fails its check: Got is
// the observed value (a power_runs column, or a part of the URL
// rebuilt from the manifest), Want the value this repo's pinned
// constants and registry reproduce.
type ManifestFinding struct {
	RunID int64
	Field string
	Got   string
	Want  string
}

// PowerComparison is the full offline power parity result against one
// ingest database: the cell-math tables
// plus the manifest checks over every complete power_runs row.
type PowerComparison struct {
	// Label identifies the database in the report; ComparePower
	// receives an open handle, not a path, so the caller sets it.
	Label string

	// StationsCompared counts the stations driving the cell math:
	// source conagua_conventional with non-NULL coordinates (5,524
	// nationally).
	StationsCompared int

	Cells PowerTable // nasa_power_grid_cells vs derived cells
	Links PowerTable // station_power_cell vs derived links

	// Runs holds every status='complete' power_runs manifest in id
	// order; ManifestFindings aggregates their check failures.
	Runs             []PowerRunManifest
	ManifestFindings Sampled[ManifestFinding]
}

// Tables lists the cell-math tallies in report order.
func (c *PowerComparison) Tables() []PowerTable {
	return []PowerTable{c.Cells, c.Links}
}

// Clean reports whether cell math and manifests compared without
// findings.
func (c *PowerComparison) Clean() bool {
	return c.Cells.Clean() && c.Links.Clean() && c.ManifestFindings.Total == 0
}
