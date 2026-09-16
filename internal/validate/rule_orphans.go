package validate

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// orphanThreshold is the wall-clock age beyond which a 'running' runs
// row is considered crashed and reconcilable. 24 h is generous — even a
// national ingest finishes in under 30 minutes, so anything older is
// almost certainly a process that died without closing its row.
const orphanThreshold = 24 * time.Hour

// ruleOrphanRuns sweeps any ingest_runs or power_runs row still marked
// 'running' beyond orphanThreshold and transitions it to 'aborted' with
// finished_at stamped, emitting one warn finding per reconciled row so
// the operator has a record of which run was marked dead. Scanned
// counts the orphans found.
//
// This is the one mutating rule in the package, and it runs only on the
// verb's path — never in the gate, whose runs-in-flight reads the same
// ledgers without a threshold and reconciles nothing. validate is the
// natural home for the reconcile: it is the command an operator runs
// after a crash, and the rules that follow read the ledgers and benefit
// from clean status data. The finding set depends on the wall clock — a
// stranded row crosses the threshold with time — and the UPDATE is
// idempotent: a second run sees no orphans left.
func ruleOrphanRuns(ctx context.Context, db *sql.DB) (RuleResult, error) {
	cutoff := time.Now().UTC().Add(-orphanThreshold).Format(time.RFC3339)
	var out RuleResult

	for _, table := range runTables {
		orphans, findings, err := scanOrphans(ctx, db, table, cutoff)
		if err != nil {
			return RuleResult{}, err
		}
		out.Findings = append(out.Findings, findings...)
		out.Scanned += len(orphans)

		// One UPDATE per orphan keeps the SQL trivial and the row count
		// modest; orphans are rare so this is not a hot path.
		for _, id := range orphans {
			if _, err := db.ExecContext(ctx, fmt.Sprintf(
				`UPDATE %s SET status = 'aborted', finished_at = ? WHERE id = ?`, table),
				time.Now().UTC().Format(time.RFC3339), id); err != nil {
				return RuleResult{}, fmt.Errorf("reconcile %s id=%d: %w", table, id, err)
			}
		}
	}

	return out, nil
}

// scanOrphans returns the ids of one ledger's 'running' rows started
// before cutoff, with the finding recording each, in the table's scan
// order.
func scanOrphans(ctx context.Context, db *sql.DB, table, cutoff string) (ids []int64, findings []Finding, err error) {
	// The table name is one of the runTables literals, never input.
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
SELECT id, started_at FROM %s
 WHERE status = 'running' AND started_at < ?`, table), cutoff)
	if err != nil {
		return nil, nil, fmt.Errorf("scan %s: %w", table, err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	for rows.Next() {
		var id int64
		var started string
		if err := rows.Scan(&id, &started); err != nil {
			return nil, nil, fmt.Errorf("scan %s: %w", table, err)
		}
		ids = append(ids, id)
		findings = append(findings, Finding{
			RuleID:   "orphan-runs",
			Severity: SeverityWarn,
			Issue: fmt.Sprintf("%s.id=%d started %s, status='running' beyond %s — reconciled to 'aborted'",
				table, id, started, orphanThreshold),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("scan %s: %w", table, err)
	}
	return ids, findings, nil
}
