package validate

import (
	"context"
	"database/sql"
	"fmt"
)

// runTables are the two run ledgers, in the order the ledger rules
// scan them.
var runTables = []string{"ingest_runs", "power_runs"}

// ruleRunsInFlight flags every ingest_runs or power_runs row still
// marked 'running' — with no age threshold. A DB mid-write, or one a
// crashed run left behind, cannot be vouched for, so each such row is
// an error. This is the gate's reading of the orphan-runs rule: its
// 24 h threshold and the reconcile to 'aborted' belong to the
// validate verb, never to this read-only path. Recovery is to finish
// the run or reconcile through validate. Scanned counts the rows found,
// as orphan-runs does.
func ruleRunsInFlight(ctx context.Context, db *sql.DB) (RuleResult, error) {
	var out RuleResult
	for _, table := range runTables {
		findings, err := scanRunning(ctx, db, table)
		if err != nil {
			return RuleResult{}, err
		}
		out.Scanned += len(findings)
		out.Findings = append(out.Findings, findings...)
	}
	return out, nil
}

// scanRunning returns one finding per 'running' row of one run ledger,
// in id order.
func scanRunning(ctx context.Context, db *sql.DB, table string) ([]Finding, error) {
	// The table name is one of the runTables literals, never input.
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
SELECT id, started_at FROM %s
 WHERE status = 'running'
 ORDER BY id`, table))
	if err != nil {
		return nil, fmt.Errorf("scan %s: %w", table, err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	var out []Finding
	for rows.Next() {
		var id int64
		var started string
		if err := rows.Scan(&id, &started); err != nil {
			return nil, fmt.Errorf("scan %s: %w", table, err)
		}
		out = append(out, Finding{
			RuleID:   "runs-in-flight",
			Severity: SeverityError,
			Issue: fmt.Sprintf("%s.id=%d started %s, status='running' — a run in flight or stranded cannot be vouched for "+
				"(finish it, or reconcile through validate)", table, id, started),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", table, err)
	}
	return out, nil
}

// ruleIngestComplete requires at least one complete ingest_runs row:
// without one there is no snapshot to publish. Scanned is the ledger's
// row count.
func ruleIngestComplete(ctx context.Context, db *sql.DB) (RuleResult, error) {
	var total, complete int
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'complete') FROM ingest_runs`).Scan(&total, &complete); err != nil {
		return RuleResult{}, fmt.Errorf("scan ingest_runs: %w", err)
	}
	out := RuleResult{Scanned: total}
	if complete == 0 {
		out.Findings = append(out.Findings, Finding{
			RuleID:   "ingest-complete",
			Severity: SeverityError,
			Issue:    fmt.Sprintf("ingest_runs has no complete row (%d rows in total): nothing to publish", total),
		})
	}
	return out, nil
}
