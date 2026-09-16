package parity

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/power"
)

// ComparePowerDrift compares two databases' supplement tables —
// monthly_supplement on (cell_id, period, month) and daily_supplement
// on (cell_id, date) — and classifies every value per row × column.
// Report-only by design: the base supplement rows came from live pulls of a re-versioning reanalysis
// product, so drift here is upstream movement, never a port gate.
//
// Memory stays bounded: rows stream one cell at a time in primary-key
// order on both sides (daily_supplement is ~10M rows per side
// nationally, but a cell holds only its own dates). Both handles
// should come from schema.OpenReadOnly.
func ComparePowerDrift(ctx context.Context, baseDB, newDB *sql.DB) (*PowerDrift, error) {
	drift := &PowerDrift{
		Monthly: newDriftTable("monthly_supplement"),
		Daily:   newDriftTable("daily_supplement"),
	}
	if err := compareSupplement(ctx, &drift.Monthly, baseDB, newDB, loadMonthlySupplementRows); err != nil {
		return nil, fmt.Errorf("monthly_supplement: %w", err)
	}
	if err := compareSupplement(ctx, &drift.Daily, baseDB, newDB, loadDailySupplementRows); err != nil {
		return nil, fmt.Errorf("daily_supplement: %w", err)
	}
	return drift, nil
}

// supplementValueColumns derives the compared value-column list from
// the power registry — the single source of truth for parameter
// identity and order. Both
// supplement tables carry the same 31 columns; the completeness spec
// pins this list to the applied schema's DDL order. power_run_id is
// deliberately excluded: it points at each side's own run row.
func supplementValueColumns() []string {
	cols := make([]string, len(power.Registry))
	for i, p := range power.Registry {
		cols[i] = p.Column
	}
	return cols
}

func newDriftTable(table string) DriftTable {
	cols := supplementValueColumns()
	t := DriftTable{Table: table, Columns: make([]DriftColumn, len(cols))}
	for i, c := range cols {
		t.Columns[i] = DriftColumn{Column: c}
	}
	return t
}

// supplementRow carries one supplement row: its rendered row key and
// the value columns in registry order.
type supplementRow struct {
	key    string
	values []sql.NullFloat64
}

// supplementLoader fetches one cell's rows ordered ascending by the
// rendered row key, with the value columns in cols order.
type supplementLoader func(ctx context.Context, db *sql.DB, cellID string, cols []string) ([]supplementRow, error)

func compareSupplement(ctx context.Context, t *DriftTable, baseDB, newDB *sql.DB, load supplementLoader) error {
	baseCells, err := distinctCellIDs(ctx, baseDB, t.Table)
	if err != nil {
		return fmt.Errorf("base cells: %w", err)
	}
	newCells, err := distinctCellIDs(ctx, newDB, t.Table)
	if err != nil {
		return fmt.Errorf("new cells: %w", err)
	}

	cols := supplementValueColumns()
	for _, cellID := range sortedStringUnion(baseCells, newCells) {
		if err := ctx.Err(); err != nil {
			return err
		}
		baseRows, err := load(ctx, baseDB, cellID, cols)
		if err != nil {
			return fmt.Errorf("base cell %s: %w", cellID, err)
		}
		newRows, err := load(ctx, newDB, cellID, cols)
		if err != nil {
			return fmt.Errorf("new cell %s: %w", cellID, err)
		}
		mergeSupplementRows(t, cellID, baseRows, newRows)
	}
	return nil
}

// distinctCellIDs lists a table's cell universe in cell_id order. The
// table name is an internal constant, never operator input.
func distinctCellIDs(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT DISTINCT cell_id FROM `+table+` ORDER BY cell_id`)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return out, nil
}

// sortedStringUnion merges two ascending-sorted slices into their
// sorted, deduplicated union.
func sortedStringUnion(a, b []string) []string {
	out := make([]string, 0, max(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) || j < len(b) {
		switch {
		case j == len(b) || (i < len(a) && a[i] < b[j]):
			out = append(out, a[i])
			i++
		case i == len(a) || b[j] < a[i]:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

func loadMonthlySupplementRows(ctx context.Context, db *sql.DB, cellID string, cols []string) ([]supplementRow, error) {
	query := `SELECT period, month, ` + strings.Join(cols, ", ") +
		` FROM monthly_supplement WHERE cell_id = ? ORDER BY period, month`
	rows, err := db.QueryContext(ctx, query, cellID)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []supplementRow
	for rows.Next() {
		var period string
		var month int
		r := supplementRow{values: make([]sql.NullFloat64, len(cols))}
		targets := make([]any, 0, 2+len(cols))
		targets = append(targets, &period, &month)
		for i := range r.values {
			targets = append(targets, &r.values[i])
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		// The zero-padded month keeps string order equal to the
		// SELECT's ORDER BY period, month.
		r.key = fmt.Sprintf("%s/m%02d", period, month)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return out, nil
}

func loadDailySupplementRows(ctx context.Context, db *sql.DB, cellID string, cols []string) ([]supplementRow, error) {
	query := `SELECT date, ` + strings.Join(cols, ", ") +
		` FROM daily_supplement WHERE cell_id = ? ORDER BY date`
	rows, err := db.QueryContext(ctx, query, cellID)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []supplementRow
	for rows.Next() {
		r := supplementRow{values: make([]sql.NullFloat64, len(cols))}
		targets := make([]any, 0, 1+len(cols))
		targets = append(targets, &r.key)
		for i := range r.values {
			targets = append(targets, &r.values[i])
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return out, nil
}

// mergeSupplementRows walks one cell's two row slices — each ordered
// ascending by row key, as their SELECTs guarantee — aligning rows on
// the key and classifying every value.
func mergeSupplementRows(t *DriftTable, cellID string, base, updated []supplementRow) {
	key := func(rowKey string) DBKey {
		return DBKey{Row: cellID + " " + rowKey}
	}
	i, j := 0, 0
	for i < len(base) || j < len(updated) {
		switch {
		case j == len(updated) || (i < len(base) && base[i].key < updated[j].key):
			t.BaseOnly.Add(key(base[i].key))
			classifyOneSidedRow(t, base[i], false)
			i++
		case i == len(base) || updated[j].key < base[i].key:
			t.NewOnly.Add(key(updated[j].key))
			classifyOneSidedRow(t, updated[j], true)
			j++
		default:
			t.RowsAligned++
			classifyAlignedRow(t, key(base[i].key), base[i], updated[j])
			i++
			j++
		}
	}
}

// classifyAlignedRow classifies every value column of a key-aligned
// row pair. Floats compare with == — the classes exist to distinguish
// genuine upstream re-versioning from bit-identical reproduction, so
// tolerance would hide exactly the signal the report exists for.
func classifyAlignedRow(t *DriftTable, k DBKey, base, updated supplementRow) {
	for idx := range t.Columns {
		col := &t.Columns[idx]
		bv, nv := base.values[idx], updated.values[idx]
		switch {
		case !bv.Valid && !nv.Valid:
			col.Identical++
			t.Values.Identical++
		case bv.Valid && nv.Valid && bv.Float64 == nv.Float64:
			col.Identical++
			t.Values.Identical++
		case bv.Valid && nv.Valid:
			col.Revised++
			t.Values.Revised++
			t.RevisedSamples.Add(DBDiff{
				Key: k, Column: col.Column,
				Base: renderFloat(bv.Float64), New: renderFloat(nv.Float64),
			})
		case nv.Valid:
			col.Appended++
			t.Values.Appended++
		default:
			col.Removed++
			t.Values.Removed++
		}
	}
}

// classifyOneSidedRow counts a one-side-only row's non-NULL values as
// appended (new-only row) or removed (base-only row).
func classifyOneSidedRow(t *DriftTable, row supplementRow, isNew bool) {
	for idx := range t.Columns {
		if !row.values[idx].Valid {
			continue
		}
		if isNew {
			t.Columns[idx].Appended++
			t.Values.Appended++
		} else {
			t.Columns[idx].Removed++
			t.Values.Removed++
		}
	}
}
