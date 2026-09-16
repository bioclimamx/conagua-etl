package validate

import (
	"context"
	"database/sql"
	"fmt"
)

// sourcePrefix namespaces the verb's parsing_warnings rows:
// source_file = sourcePrefix + rule id. The clear before a run and any
// consumer scoping to validate output match on this one prefix.
const sourcePrefix = "validate:"

// SourceFile is the parsing_warnings.source_file value the verb writes
// for this Finding.
func (f Finding) SourceFile() string { return sourcePrefix + f.RuleID }

// writeWarnings persists a batch into parsing_warnings: one
// transaction, one prepared statement, one row per finding —
// station_id NULL for a finding anchored to no station, line always
// NULL (the verb parses no file). A severity outside the two literals
// is refused before it reaches the CHECK constraint, so a rule bug reads
// as the rule's own fault. Skipped on empty input so a clean DB pays no
// transaction.
func writeWarnings(ctx context.Context, db *sql.DB, fs []Finding) error {
	if len(fs) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
VALUES (?, ?, NULL, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare validate warnings insert: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, f := range fs {
		if f.Severity != SeverityWarn && f.Severity != SeverityError {
			return fmt.Errorf("rule %s: invalid severity %q", f.RuleID, f.Severity)
		}
		var stationID any
		if f.StationID != nil {
			stationID = *f.StationID
		}
		if _, err := stmt.ExecContext(ctx, stationID, f.SourceFile(), f.Severity, f.Issue); err != nil {
			return fmt.Errorf("insert validate warning (%s): %w", f.RuleID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
