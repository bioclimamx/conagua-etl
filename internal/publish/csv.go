package publish

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
)

// writeCSV streams rows into w as spec describes: the header row, then
// one record per row with every cell formatted per its Kind. UTF-8
// without BOM, LF line endings, and RFC-4180 quoting are encoding/csv's
// defaults. The row set is never materialized — a station's daily series
// or a state's normals stream through one reused record — so a national
// export runs in constant memory. Cancellation surfaces through
// rows.Err(): database/sql closes a QueryContext row set when its
// context ends.
func writeCSV(w io.Writer, spec FileSpec, rows *sql.Rows) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(spec.Header()); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	targets := make([]any, len(spec.Columns))
	for i, c := range spec.Columns {
		targets[i] = scanTarget(c)
	}
	record := make([]string, len(spec.Columns))
	n := 0
	for rows.Next() {
		if err := rows.Scan(targets...); err != nil {
			return fmt.Errorf("scan row %d: %w", n+1, err)
		}
		for i, c := range spec.Columns {
			cell, err := formatCell(c, targets[i])
			if err != nil {
				return fmt.Errorf("row %d: %w", n+1, err)
			}
			record[i] = cell
		}
		if err := cw.Write(record); err != nil {
			return fmt.Errorf("write row %d: %w", n+1, err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

// writeQuery runs query against db and streams its result into w per
// spec. The query's select list must yield spec's columns in order; the
// builders in this package derive it from the spec itself so the header
// and the scanned values cannot drift apart.
func writeQuery(ctx context.Context, w io.Writer, db *sql.DB, spec FileSpec, query string, args ...any) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query %s: %w", spec.Name, err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	if err := writeCSV(w, spec, rows); err != nil {
		return fmt.Errorf("%s: %w", spec.Name, err)
	}
	return nil
}
