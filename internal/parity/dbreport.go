package parity

import "database/sql"

// DBSampleCap bounds every per-category sample list in a DBComparison.
// Totals are always exact. Deliberately smaller than ExampleCap: the DB
// gate compares two ingests of the same snapshot, so any finding is a
// hard failure and a handful of samples is diagnosis, not a survey.
const DBSampleCap = 20

// Sampled is an exact counter with a sample list bounded at
// DBSampleCap — Capped's shape at the DB gate's cap. Rows are
// aggregated in natural-key order, so the retained samples are
// deterministic across runs.
type Sampled[T any] struct {
	// Total is the exact number of findings in this category.
	Total int

	// Samples holds at most DBSampleCap findings, in aggregation order.
	Samples []T
}

// Add records one finding, retaining it as a sample if under the cap.
func (s *Sampled[T]) Add(v T) {
	if len(s.Samples) < DBSampleCap {
		s.Samples = append(s.Samples, v)
	}
	s.Total++
}

// Truncated reports whether findings beyond the retained samples exist.
func (s Sampled[T]) Truncated() bool { return s.Total > len(s.Samples) }

// DBKey locates one row by natural key: the owning station's
// (source, external_id) plus a per-table row discriminator — empty for
// stations rows, "<period>/mMM" for normals and extras rows, the ISO
// date for daily rows, and the rendered tuple for parsing_warnings
// (whose station fields are empty when station_id is NULL). Surrogate
// ids never appear: they differ between ingests by construction.
type DBKey struct {
	Source     string
	ExternalID string
	Row        string
}

// String renders the key for reports and assertion messages.
func (k DBKey) String() string {
	station := k.Source + "/" + k.ExternalID
	switch {
	case k.Source == "" && k.ExternalID == "":
		return k.Row
	case k.Row == "":
		return station
	}
	return station + " " + k.Row
}

// DBDiff is one column whose value differs between the two databases
// for the same natural-key row. Base/New are rendered values, with
// "NULL" standing for SQL NULL.
type DBDiff struct {
	Key    DBKey
	Column string
	Base   string
	New    string
}

// TableComparison tallies one table's natural-key comparison: rows
// aligned on both sides (compared), of which identical vs carrying
// value diffs, plus rows present on one side only. For
// parsing_warnings a "row" is a distinct multiset tuple: compared
// means the tuple exists on both sides, and a multiplicity mismatch is
// a single "count" value diff.
type TableComparison struct {
	Table string

	RowsCompared  int
	RowsIdentical int

	ValueDiffs Sampled[DBDiff]
	BaseOnly   Sampled[DBKey]
	NewOnly    Sampled[DBKey]
}

// BaseRows derives the base side's row universe: every base row is
// either aligned with a new-side row or base-only.
func (t TableComparison) BaseRows() int { return t.RowsCompared + t.BaseOnly.Total }

// NewRows derives the new side's row universe symmetrically.
func (t TableComparison) NewRows() int { return t.RowsCompared + t.NewOnly.Total }

// Clean reports whether the table compared without any finding.
func (t TableComparison) Clean() bool {
	return t.ValueDiffs.Total == 0 && t.BaseOnly.Total == 0 && t.NewOnly.Total == 0
}

// RunCounters carries one ingest_runs row verbatim. Runs are metadata
// about how a database was built, not data under comparison — the
// report shows both sides' latest run so the reader can check counter
// agreement by eye.
type RunCounters struct {
	ID                int64
	StartedAt         string
	FinishedAt        sql.NullString
	SnapshotDate      string
	SinkKind          string
	ETLGitSHA         sql.NullString
	Status            string
	StationsAttempted sql.NullInt64
	StationsSucceeded sql.NullInt64
	StationsFailed    sql.NullInt64
	DailyRows         sql.NullInt64
	NormalsRows       sql.NullInt64
	ExtrasRows        sql.NullInt64
	WarningsTotal     sql.NullInt64
}

// DBComparison is the full ingest-DB parity result: one TableComparison
// per compared table plus both sides' latest run metadata.
type DBComparison struct {
	// BaseLabel and NewLabel identify the two databases in the report.
	// CompareDBs receives open handles, not paths, so the caller sets
	// them before rendering.
	BaseLabel string
	NewLabel  string

	Stations TableComparison
	Normals  TableComparison
	Extras   TableComparison
	Daily    TableComparison
	Warnings TableComparison

	// Latest ingest_runs row per side; nil when the table is empty.
	BaseRun *RunCounters
	NewRun  *RunCounters
}

// Tables lists the per-table tallies in report order.
func (c *DBComparison) Tables() []TableComparison {
	return []TableComparison{c.Stations, c.Normals, c.Extras, c.Daily, c.Warnings}
}

// Clean reports whether every table compared without findings.
func (c *DBComparison) Clean() bool {
	for _, t := range c.Tables() {
		if !t.Clean() {
			return false
		}
	}
	return true
}
