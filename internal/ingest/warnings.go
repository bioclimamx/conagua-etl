package ingest

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

// Severity values must match the parsing_warnings.severity CHECK
// constraint in schema.sql. Typed constants so renames surface at
// compile time and so callers don't repeat the literal strings.
const (
	SeverityWarn  = "warn"
	SeverityError = "error"
)

// Warning is the DB-layer warning record. It adapts conagua.Warning
// (parser-level, line + message) with the context the parser doesn't
// have: which file and which station produced it, and whether the
// issue is severe enough to gate publish (error) or merely advisory
// (warn).
//
// Persisted into parsing_warnings. The schema allows NULL
// station_id (some warnings are file-level, e.g. "file had no header")
// and NULL line (some warnings don't point at a specific line).
type Warning struct {
	StationID  *int64
	SourceFile string
	Line       int // 0 means "not line-specific" and stores as NULL
	Severity   string
	Issue      string
}

// warningFromParse adapts a conagua.Warning into an ingest.Warning for
// a specific source file + station. Parser-level warnings are always
// row-level (they report malformed numeric values and the like), so
// severity defaults to "warn". File-level errors (file skipped) are
// constructed directly with severity='error' instead.
func warningFromParse(w conagua.Warning, sourceFile string, stationID *int64) Warning {
	return Warning{
		StationID:  stationID,
		SourceFile: sourceFile,
		Line:       w.Line,
		Severity:   SeverityWarn,
		Issue:      w.Message,
	}
}

// fileErrorWarning builds a file-level 'error' warning with no line
// anchor. Used when a parser returns a fatal err (no header found,
// period mismatch) so the row lands in parsing_warnings and publish
// can gate on it.
func fileErrorWarning(sourceFile string, stationID *int64, issue string) Warning {
	return Warning{
		StationID:  stationID,
		SourceFile: sourceFile,
		Line:       0,
		Severity:   SeverityError,
		Issue:      issue,
	}
}

// clearWarningsForSourceFile removes any existing parsing_warnings
// rows keyed to the given Sink key. Called immediately before
// re-parsing that file so reingest is warning-idempotent — the
// parsing_warnings PK is an AUTOINCREMENT id, so "INSERT OR REPLACE"
// semantics don't apply here; we have to DELETE + INSERT manually.
func clearWarningsForSourceFile(ctx context.Context, tx *sql.Tx, sourceFile string) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM parsing_warnings WHERE source_file = ?`, sourceFile)
	if err != nil {
		return fmt.Errorf("clear warnings for %s: %w", sourceFile, err)
	}
	return nil
}
