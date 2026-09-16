package validate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/power"
)

// The integrity anchors: each is impossible if the pipeline worked, and
// each is checked because foreign keys are OFF at runtime. Every
// anchor is one pass per table on a clean DB; the bounded sample
// queries run only once a pass has found something. Volume anchors
// aggregate to one finding per (table, column) naming the row
// count and up to sampleKeys offending keys, so a corrupt DB cannot
// balloon the report.

// sampleKeys is how many offending keys an aggregate finding names.
const sampleKeys = 3

// powerFill is POWER's missing-data sentinel. The client refuses a
// response whose fill_value diverges across batched sub-requests and
// the rollup drops fill entries, so they reach the tables as NULL; a
// stored -999 is a leak past both — never a value.
const powerFill = -999

// supplementTable is one POWER supplement table with the key columns
// that identify a row in a finding.
type supplementTable struct {
	name string
	key  []string
}

// supplementTables are the two supplement tables, in DDL order.
var supplementTables = []supplementTable{
	{name: "monthly_supplement", key: []string{"cell_id", "period", "month"}},
	{name: "daily_supplement", key: []string{"cell_id", "date"}},
}

func (t supplementTable) keyList() string { return strings.Join(t.key, ", ") }

// refCheck is one referential anchor: rows of child whose key column
// names no parentKey in parent. NULL keys are outside its scope — the
// child columns it is applied to are NOT NULL, bar parsing_warnings,
// where a NULL station_id is a legitimate file-level warning.
type refCheck struct {
	child, key, parent, parentKey string
}

// run counts child rows by key over the key's index, probes the parent
// once per distinct key, and returns the rows examined and — when any
// key dangles — one finding naming the dangling row count, the distinct
// dangling keys, and the first sampleKeys of them in key order.
func (c refCheck) run(ctx context.Context, db *sql.DB, ruleID string) (scanned int, finding *Finding, err error) {
	// Every identifier is a literal from the tables below, never input.
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
SELECT g.k, g.n, p.%[4]s IS NULL
  FROM (SELECT %[2]s AS k, COUNT(*) AS n FROM %[1]s WHERE %[2]s IS NOT NULL GROUP BY %[2]s) g
  LEFT JOIN %[3]s p ON p.%[4]s = g.k
 ORDER BY g.k`, c.child, c.key, c.parent, c.parentKey))
	if err != nil {
		return 0, nil, fmt.Errorf("scan %s: %w", c.child, err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	var dangling, distinct int
	var samples []string
	for rows.Next() {
		var key string
		var n int
		var missing bool
		if err := rows.Scan(&key, &n, &missing); err != nil {
			return 0, nil, fmt.Errorf("scan %s: %w", c.child, err)
		}
		scanned += n
		if !missing {
			continue
		}
		dangling += n
		distinct++
		if len(samples) < sampleKeys {
			samples = append(samples, key)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("scan %s: %w", c.child, err)
	}
	if dangling == 0 {
		return scanned, nil, nil
	}
	return scanned, &Finding{
		RuleID:   ruleID,
		Severity: SeverityError,
		Issue: fmt.Sprintf("%s: %s with a %s naming no %s row (%d distinct: %s)",
			c.child, nRows(dangling), c.key, c.parent, distinct, sampleList(samples, distinct)),
	}, nil
}

// runRefChecks evaluates checks in order, summing Scanned and
// collecting one finding per table that dangles.
func runRefChecks(ctx context.Context, db *sql.DB, ruleID string, checks []refCheck) (RuleResult, error) {
	var out RuleResult
	for _, c := range checks {
		scanned, finding, err := c.run(ctx, db, ruleID)
		if err != nil {
			return RuleResult{}, err
		}
		out.Scanned += scanned
		if finding != nil {
			out.Findings = append(out.Findings, *finding)
		}
	}
	return out, nil
}

// ruleStationRefs requires every station_id in the observed tables,
// station_power_cell, and parsing_warnings (where non-NULL) to name a
// stations row.
func ruleStationRefs(ctx context.Context, db *sql.DB) (RuleResult, error) {
	checks := make([]refCheck, 0, 5)
	for _, child := range []string{
		"daily_observations", "monthly_normals", "monthly_normals_extras", "station_power_cell", "parsing_warnings",
	} {
		checks = append(checks, refCheck{child: child, key: "station_id", parent: "stations", parentKey: "id"})
	}
	return runRefChecks(ctx, db, "station-refs", checks)
}

// ruleCellRefs requires every cell_id in the supplement tables and
// station_power_cell to name a nasa_power_grid_cells row.
func ruleCellRefs(ctx context.Context, db *sql.DB) (RuleResult, error) {
	checks := make([]refCheck, 0, 3)
	for _, child := range []string{"monthly_supplement", "daily_supplement", "station_power_cell"} {
		checks = append(checks, refCheck{child: child, key: "cell_id", parent: "nasa_power_grid_cells", parentKey: "cell_id"})
	}
	return runRefChecks(ctx, db, "cell-refs", checks)
}

// ruleRunRefs requires every supplement row to carry a power_run_id
// that names a power_runs row: the run is the row's reproducibility
// manifest, and a NULL or dangling reference leaves the value without
// provenance. power_run_id is unindexed, so the pass is one scan of
// each table probing the tiny power_runs primary key per row, counting
// the NULL rows, the dangling rows, and the distinct dangling ids at
// once; the bounded samples are fetched only for a table that failed.
func ruleRunRefs(ctx context.Context, db *sql.DB) (RuleResult, error) {
	var out RuleResult
	for _, t := range supplementTables {
		var total, null, dangling, distinct int
		if err := db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT COUNT(*),
       COUNT(*) FILTER (WHERE s.power_run_id IS NULL),
       COUNT(*) FILTER (WHERE s.power_run_id IS NOT NULL AND r.id IS NULL),
       COUNT(DISTINCT CASE WHEN s.power_run_id IS NOT NULL AND r.id IS NULL THEN s.power_run_id END)
  FROM %s s LEFT JOIN power_runs r ON r.id = s.power_run_id`, t.name)).Scan(&total, &null, &dangling, &distinct); err != nil {
			return RuleResult{}, fmt.Errorf("scan %s: %w", t.name, err)
		}
		out.Scanned += total
		if null > 0 {
			samples, err := sampleRows(ctx, db, fmt.Sprintf(`
SELECT %[2]s FROM %[1]s WHERE power_run_id IS NULL ORDER BY %[2]s LIMIT %[3]d`, t.name, t.keyList(), sampleKeys),
				len(t.key))
			if err != nil {
				return RuleResult{}, fmt.Errorf("sample %s: %w", t.name, err)
			}
			out.Findings = append(out.Findings, Finding{
				RuleID:   "run-refs",
				Severity: SeverityError,
				Issue: fmt.Sprintf("%s: %s with a NULL power_run_id (%s)",
					t.name, nRows(null), sampleList(samples, null)),
			})
		}
		if dangling > 0 {
			ids, err := sampleRows(ctx, db, fmt.Sprintf(`
SELECT power_run_id FROM %s
 WHERE power_run_id IS NOT NULL AND power_run_id NOT IN (SELECT id FROM power_runs)
 GROUP BY power_run_id ORDER BY power_run_id LIMIT %d`, t.name, sampleKeys), 1)
			if err != nil {
				return RuleResult{}, fmt.Errorf("sample %s: %w", t.name, err)
			}
			out.Findings = append(out.Findings, Finding{
				RuleID:   "run-refs",
				Severity: SeverityError,
				Issue: fmt.Sprintf("%s: %s with a power_run_id naming no power_runs row (%d distinct: %s)",
					t.name, nRows(dangling), distinct, sampleList(ids, distinct)),
			})
		}
	}
	return out, nil
}

// referencedRunsSQL selects the power runs at least one supplement row
// references — the runs the provenance index publishes by run_label —
// with the run_label's inputs, ordered so runs sharing them are
// adjacent. The referenced-id set is materialized once by the
// non-correlated IN subquery, so the query costs one scan of each
// supplement table.
const referencedRunsSQL = `
SELECT id, temporal_mode, period_start_year, period_end_year
  FROM power_runs
 WHERE id IN (SELECT power_run_id FROM monthly_supplement WHERE power_run_id IS NOT NULL
              UNION
              SELECT power_run_id FROM daily_supplement WHERE power_run_id IS NOT NULL)
 ORDER BY temporal_mode, period_start_year, period_end_year, id`

// ruleRunLabelUnique requires the run_label — a pure function of
// (temporal_mode, period_start_year, period_end_year) — to be unique
// among the referenced power runs: two referenced runs sharing it would
// carry provenance the label cannot represent.
// An unreferenced duplicate is not a collision; nothing traces to it.
// Scanned counts the referenced runs.
func ruleRunLabelUnique(ctx context.Context, db *sql.DB) (RuleResult, error) {
	rows, err := db.QueryContext(ctx, referencedRunsSQL)
	if err != nil {
		return RuleResult{}, fmt.Errorf("scan power_runs: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	type labelKey struct {
		mode       string
		start, end int64
	}
	var out RuleResult
	var groups []labelKey
	ids := map[labelKey][]int64{}
	for rows.Next() {
		var id int64
		var k labelKey
		if err := rows.Scan(&id, &k.mode, &k.start, &k.end); err != nil {
			return RuleResult{}, fmt.Errorf("scan power_runs: %w", err)
		}
		out.Scanned++
		if _, seen := ids[k]; !seen {
			groups = append(groups, k)
		}
		ids[k] = append(ids[k], id)
	}
	if err := rows.Err(); err != nil {
		return RuleResult{}, fmt.Errorf("scan power_runs: %w", err)
	}
	for _, k := range groups {
		if len(ids[k]) < 2 {
			continue
		}
		parts := make([]string, len(ids[k]))
		for i, id := range ids[k] {
			parts[i] = fmt.Sprint(id)
		}
		out.Findings = append(out.Findings, Finding{
			RuleID:   "run-label-unique",
			Severity: SeverityError,
			Issue: fmt.Sprintf("power_runs %s share temporal_mode/period (%s %d-%d): one run_label for %d referenced runs "+
				"(re-run power for that period to convergence)",
				strings.Join(parts, ", "), k.mode, k.start, k.end, len(parts)),
		})
	}
	return out, nil
}

// ruleFillLeak requires no supplement value to equal POWER's fill
// exactly, across every registry column of both tables: one pass per
// table counts every column at once; a column that leaked is then
// sampled by key.
func ruleFillLeak(ctx context.Context, db *sql.DB) (RuleResult, error) {
	columns := make([]string, len(power.Registry))
	for i, p := range power.Registry {
		columns[i] = p.Column
	}
	isFill := func(col string) string { return fmt.Sprintf("%s = %d", col, powerFill) }
	var out RuleResult
	for _, t := range supplementTables {
		total, counts, err := countColumns(ctx, db, t.name, columns, isFill)
		if err != nil {
			return RuleResult{}, err
		}
		out.Scanned += total
		for i, col := range columns {
			if counts[i] == 0 {
				continue
			}
			samples, err := sampleRows(ctx, db, fmt.Sprintf(`
SELECT %[2]s FROM %[1]s WHERE %[3]s = %[4]d ORDER BY %[2]s LIMIT %[5]d`,
				t.name, t.keyList(), col, powerFill, sampleKeys), len(t.key))
			if err != nil {
				return RuleResult{}, fmt.Errorf("sample %s.%s: %w", t.name, col, err)
			}
			out.Findings = append(out.Findings, Finding{
				RuleID:   "fill-leak",
				Severity: SeverityError,
				Issue: fmt.Sprintf("%s.%s: %s holding POWER's %d fill (%s)",
					t.name, col, nRows(counts[i]), powerFill, sampleList(samples, counts[i])),
			})
		}
	}
	return out, nil
}

// ruleWindRange requires every wind direction — the registry's circular
// columns — to lie within the closed [0, 360]: the circular mean can
// yield exactly 360.0 and it is legal. One pass per
// table; an offending column is sampled by key with its value.
func ruleWindRange(ctx context.Context, db *sql.DB) (RuleResult, error) {
	var columns []string
	for _, p := range power.Registry {
		if p.Circular {
			columns = append(columns, p.Column)
		}
	}
	outOfRange := func(col string) string { return col + " < 0 OR " + col + " > 360" }
	var out RuleResult
	for _, t := range supplementTables {
		total, counts, err := countColumns(ctx, db, t.name, columns, outOfRange)
		if err != nil {
			return RuleResult{}, err
		}
		out.Scanned += total
		for i, col := range columns {
			if counts[i] == 0 {
				continue
			}
			samples, err := sampleRows(ctx, db, fmt.Sprintf(`
SELECT %[2]s, %[3]s FROM %[1]s WHERE %[3]s < 0 OR %[3]s > 360 ORDER BY %[2]s LIMIT %[4]d`,
				t.name, t.keyList(), col, sampleKeys), len(t.key))
			if err != nil {
				return RuleResult{}, fmt.Errorf("sample %s.%s: %w", t.name, col, err)
			}
			out.Findings = append(out.Findings, Finding{
				RuleID:   "wind-range",
				Severity: SeverityError,
				Issue: fmt.Sprintf("%s.%s: %s outside [0, 360] (%s)",
					t.name, col, nRows(counts[i]), sampleList(samples, counts[i])),
			})
		}
	}
	return out, nil
}

// countColumns is the one-pass column counter: for table, the row count
// and, per column, the rows matching predicate — the SQL condition
// rendered for that column name — all from a single scan.
func countColumns(ctx context.Context, db *sql.DB, table string, columns []string, predicate func(col string) string,
) (total int, counts []int, err error) {
	selects := make([]string, 0, len(columns)+1)
	selects = append(selects, "COUNT(*)")
	for _, col := range columns {
		selects = append(selects, "COUNT(*) FILTER (WHERE "+predicate(col)+")")
	}
	counts = make([]int, len(columns))
	dst := make([]any, 0, len(columns)+1)
	dst = append(dst, &total)
	for i := range counts {
		dst = append(dst, &counts[i])
	}
	if err := db.QueryRowContext(ctx, "SELECT "+strings.Join(selects, ", ")+" FROM "+table).Scan(dst...); err != nil {
		return 0, nil, fmt.Errorf("scan %s: %w", table, err)
	}
	return total, counts, nil
}

// sampleRows runs a bounded sample query and renders each row: its
// first keyCols columns joined by "/" — a supplement key reads
// 21.5N_89.3750W/1991-2020/3 — and any further column appended as
// " = value".
func sampleRows(ctx context.Context, db *sql.DB, query string, keyCols int) ([]string, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if keyCols > len(cols) {
		return nil, fmt.Errorf("sample query selects %d columns, fewer than the %d key columns", len(cols), keyCols)
	}
	var out []string
	for rows.Next() {
		fields := make([]string, len(cols))
		dst := make([]any, len(cols))
		for i := range fields {
			dst[i] = &fields[i]
		}
		if err := rows.Scan(dst...); err != nil {
			return nil, err
		}
		s := strings.Join(fields[:keyCols], "/")
		for _, v := range fields[keyCols:] {
			s += " = " + v
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// nRows renders a row count with its noun.
func nRows(n int) string {
	if n == 1 {
		return "1 row"
	}
	return fmt.Sprintf("%d rows", n)
}

// sampleList renders the sampled keys of an aggregate finding, marking
// the truncation when the finding covers more than it names.
func sampleList(samples []string, total int) string {
	s := strings.Join(samples, ", ")
	if total > len(samples) {
		s += ", …"
	}
	return s
}
