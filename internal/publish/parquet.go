package publish

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// The pinned Parquet writer settings. The writer is kept deterministic —
// stable row order, fixed compression, pinned library version and
// settings, no timestamps in metadata — even though only
// content-reproducibility is advertised.
// Everything not named here is parquet-go v0.32.0's default: PLAIN
// encoding for INT64 and DOUBLE leaves, DELTA_LENGTH_BYTE_ARRAY for
// BYTE_ARRAY, no dictionary pages, data page v2 with page statistics,
// a 256 KiB page buffer, a 32 KiB write buffer, a column-index size
// limit of 16, and no key-value metadata.
const (
	// parquetApplication and parquetLibrary make the footer's created_by
	// the fixed string "conagua-etl version 0.1(build parquet-go-v0.32.0)"
	// — the deposit's own version and the library pin, never a build
	// hash or a timestamp. parquetLibrary is held to go.mod's require
	// line by a lockstep spec, so a library bump cannot leave the string
	// lying.
	parquetApplication = "conagua-etl"
	parquetLibrary     = "parquet-go-v0.32.0"

	// parquetRowGroupRows bounds the writer's memory: a row group is
	// buffered whole before it is encoded, so the buffer is a function
	// of this constant and the row width, never of the table's length.
	// The widest tables (combined_monthly: 41 leaves; combined_daily:
	// 39, 36 of them doubles) cost under 400 bytes a row in column
	// buffers, so 262,144 rows hold ~100 MiB before compression whether
	// the table has 5 rows or 71 M, while the per-row-group overhead
	// stays negligible for the longest table.
	parquetRowGroupRows = 262144

	// parquetBatchRows is how many rows are handed to the writer per
	// WriteRows call: enough to amortize the per-call column dispatch,
	// small enough (~1 MiB of parquet.Value headers for the widest
	// table) to be invisible next to the row-group buffer.
	parquetBatchRows = 1024
)

// parquetWriterOptions is the one place the writer is configured.
// Zstd is the compression: the best ratio of the codecs every mainstream
// reader (Arrow, pandas, R arrow, DuckDB, Spark) opens natively, which
// matters most for the wide daily tables that dominate the archive; the
// level is the library's default (zstd SpeedDefault).
func parquetWriterOptions(schema *parquet.Schema) []parquet.WriterOption {
	return []parquet.WriterOption{
		schema,
		parquet.CreatedBy(parquetApplication, DatasetVersion, parquetLibrary),
		parquet.Compression(&parquet.Zstd),
		parquet.MaxRowsPerRowGroup(parquetRowGroupRows),
	}
}

// parquetSchema builds the Parquet schema of one logical file from its
// column spec, so the header a CSV carries and the leaves a Parquet
// file declares are one list: the export names, in export order. A key
// column is a required leaf — the natural key is never NULL — and every
// other column is optional, its NULLs carried by definition levels
// (native nulls, no sentinel). Kinds map to the plainest physical types
// every reader understands: TEXT, dates, and periods are UTF-8 strings
// (their shapes are fixed as strings; no DATE logical type, so a reader
// sees exactly the CSV's text), INTEGER is INT64 (annotated INT(64,
// signed), the form the library itself reads a plain INT64 back as),
// REAL is DOUBLE.
func parquetSchema(spec FileSpec) *parquet.Schema {
	group := make(parquet.Group, len(spec.Columns))
	order := make([]string, len(spec.Columns))
	for i, c := range spec.Columns {
		var leaf parquet.Node
		switch c.Kind {
		case KindInt:
			leaf = parquet.Int(64)
		case KindReal:
			leaf = parquet.Leaf(parquet.DoubleType)
		default:
			leaf = parquet.String()
		}
		if !c.Key {
			leaf = parquet.Optional(leaf)
		}
		group[c.Name] = leaf
		order[i] = c.Name
	}
	return parquet.NewSchema(path.Base(spec.Name), newOrderedGroup(group, order))
}

// orderedGroup is a parquet.Group whose fields come back in the order
// given, not parquet.Group's alphabetical order: a Parquet file's column
// order is its field order, and the export column order (keys first,
// then DDL order) is part of the format contract, not a cosmetic.
type orderedGroup struct {
	parquet.Group
	fields []parquet.Field
}

// newOrderedGroup reorders group's fields by order, which must name each
// field exactly once.
func newOrderedGroup(group parquet.Group, order []string) orderedGroup {
	byName := make(map[string]parquet.Field, len(group))
	for _, f := range group.Fields() {
		byName[f.Name()] = f
	}
	fields := make([]parquet.Field, len(order))
	for i, name := range order {
		fields[i] = byName[name]
	}
	return orderedGroup{Group: group, fields: fields}
}

func (g orderedGroup) Fields() []parquet.Field { return g.fields }

func (g orderedGroup) String() string {
	var sb strings.Builder
	_ = parquet.PrintSchema(&sb, "", g)
	return sb.String()
}

// parquetReal is the value a REAL column carries in Parquet: the
// nearest double of the decimal rounded to the column's fixed export
// precision, derived from the one fixed-decimal formatter so that a
// double read back from the file and re-formatted at the column's
// decimals is the CSV cell text exactly — CSV, JSON, and Parquet then hold identical values, and
// only the SQLite artifact keeps the native precision. A non-finite
// value is refused by column name, as the CSV cell refuses it. buf is
// scratch the decimal is rendered into and handed back extended, so the
// hottest loop of the deposit (every non-null REAL of the daily twins)
// allocates nothing per cell: the string conversion feeding ParseFloat
// does not escape, since strconv clones the input it keeps in a
// NumError.
func parquetReal(c Column, v float64, buf []byte) (float64, []byte, error) {
	if err := checkFinite(c.Name, v); err != nil {
		return 0, buf, err
	}
	buf = appendReal(buf[:0], v, c.Decimals)
	rounded, err := strconv.ParseFloat(string(buf), 64)
	if err != nil {
		return 0, buf, fmt.Errorf("column %s: %w", c.Name, err)
	}
	return rounded, buf, nil
}

// parquetValue converts one scanned cell to its Parquet value, before
// levels are assigned: NULL is the null value; TEXT is the stored bytes
// verbatim; INTEGER is int64; REAL is parquetReal, over the scratch buf
// it hands back. TEXT goes through the explicit byte-array constructor
// like its siblings, not the reflective ValueOf: one short copy the
// writer consumes on WriteRows.
func parquetValue(c Column, target any, buf []byte) (parquet.Value, []byte, error) {
	switch t := target.(type) {
	case *sql.NullString:
		if !t.Valid {
			return parquet.NullValue(), buf, nil
		}
		return parquet.ByteArrayValue([]byte(t.String)), buf, nil
	case *sql.NullInt64:
		if !t.Valid {
			return parquet.NullValue(), buf, nil
		}
		return parquet.Int64Value(t.Int64), buf, nil
	case *sql.NullFloat64:
		if !t.Valid {
			return parquet.NullValue(), buf, nil
		}
		v, buf, err := parquetReal(c, t.Float64, buf)
		if err != nil {
			return parquet.Value{}, buf, err
		}
		return parquet.DoubleValue(v), buf, nil
	default:
		return parquet.Value{}, buf, fmt.Errorf("column %s: unsupported scan target %T", c.Name, target)
	}
}

// parquetRows turns scanned rows into the writer's batches: targets are
// the scan destinations a rowSource fills (one scanTarget per column, in
// export order), and each append converts them into the next Row of the
// batch. Rows and their Value slices are allocated once and reused
// across batches — the writer copies every value into its column
// buffers on WriteRows, so nothing outlives the call — and scratch is
// the one decimal buffer every REAL cell is rendered through.
type parquetRows struct {
	spec    FileSpec
	targets []any
	batch   []parquet.Row
	n       int
	scratch []byte
}

func newParquetRows(spec FileSpec) *parquetRows {
	r := &parquetRows{
		spec:    spec,
		targets: make([]any, len(spec.Columns)),
		batch:   make([]parquet.Row, parquetBatchRows),
		scratch: make([]byte, 0, 32),
	}
	for i, c := range spec.Columns {
		r.targets[i] = scanTarget(c)
	}
	for i := range r.batch {
		r.batch[i] = make(parquet.Row, len(spec.Columns))
	}
	return r
}

// append converts the scanned targets into the batch's next row. A
// value's column index is its position in export order — the schema's
// leaf order by construction — and its definition level is 1 for a
// present optional value, 0 for a null or a required key. A NULL in a
// key column is refused: the leaf is required, so the file has no way
// to carry it, and a NULL natural key is a corruption signal in any
// case.
func (r *parquetRows) append() error {
	row := r.batch[r.n]
	for i, c := range r.spec.Columns {
		v, buf, err := parquetValue(c, r.targets[i], r.scratch)
		r.scratch = buf
		if err != nil {
			return err
		}
		definition := 0
		switch {
		case v.IsNull() && c.Key:
			return fmt.Errorf("column %s: NULL in a key column", c.Name)
		case !v.IsNull() && !c.Key:
			definition = 1
		}
		row[i] = v.Level(0, definition, i)
	}
	r.n++
	return nil
}

func (r *parquetRows) full() bool { return r.n == len(r.batch) }

// flush hands the buffered rows to w and empties the batch.
func (r *parquetRows) flush(w *parquet.GenericWriter[any]) error {
	if r.n == 0 {
		return nil
	}
	if _, err := w.WriteRows(r.batch[:r.n]); err != nil {
		return err
	}
	r.n = 0
	return nil
}

// rowSource streams one logical table's rows in export order: it scans
// each row into targets — one scanTarget per column of the file's spec,
// in export order — and calls yield once per row, stopping at the first
// error yield returns. A source that is several queries (the per-station
// and per-cell tables) runs them back to back, so the file is the
// concatenation of the same row sets the CSV files carry, in the same
// order.
type rowSource func(ctx context.Context, targets []any, yield func() error) error

// boundQuery is one query of a rowSource with its arguments bound;
// label names the unit the query belongs to in an error (a station or
// a cell) and is empty for a whole-state query.
type boundQuery struct {
	label string
	query string
	args  []any
}

// querySource runs queries in order, streaming every row of each.
func querySource(db *sql.DB, queries ...boundQuery) rowSource {
	return func(ctx context.Context, targets []any, yield func() error) error {
		for _, q := range queries {
			if err := scanQuery(ctx, db, q, targets, yield); err != nil {
				if q.label != "" {
					return fmt.Errorf("%s: %w", q.label, err)
				}
				return err
			}
		}
		return nil
	}
}

// scanQuery streams one bound query's rows through yield. A row is
// numbered within its query — the unit the error's label names — so a
// per-station or per-cell refusal reads like the CSV shard's, where row
// N is the unit's Nth row, never the file's. Cancellation surfaces
// through rows.Err(): database/sql closes a QueryContext row set when
// its context ends.
func scanQuery(ctx context.Context, db *sql.DB, q boundQuery, targets []any, yield func() error) error {
	rows, err := db.QueryContext(ctx, q.query, q.args...)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	n := 0
	for rows.Next() {
		n++
		if err := rows.Scan(targets...); err != nil {
			return fmt.Errorf("scan row %d: %w", n, err)
		}
		if err := yield(); err != nil {
			return fmt.Errorf("row %d: %w", n, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	return nil
}

// writeParquet streams source's rows into w as one Parquet file per
// spec: the schema of parquetSchema, the pinned writer options, rows in
// the order the source yields them, a row group every
// parquetRowGroupRows. The row set is never materialized. A source that
// yields no row still produces a valid file — the schema and zero rows —
// so an empty table is a readable, header-only file like its CSV twin.
func writeParquet(ctx context.Context, w io.Writer, spec FileSpec, source rowSource) error {
	config, err := parquet.NewWriterConfig(parquetWriterOptions(parquetSchema(spec))...)
	if err != nil {
		return fmt.Errorf("writer config: %w", err)
	}
	// A validated config cannot make the constructor panic; the any
	// type parameter is the library's spelling of a schema known only at
	// run time.
	pw := parquet.NewGenericWriter[any](w, config)
	rows := newParquetRows(spec)
	err = source(ctx, rows.targets, func() error {
		if err := rows.append(); err != nil {
			return err
		}
		if rows.full() {
			if err := rows.flush(pw); err != nil {
				return fmt.Errorf("write rows: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := rows.flush(pw); err != nil {
		return fmt.Errorf("write rows: %w", err)
	}
	if err := pw.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
}

// parquetEntry is the archive entry of one logical table's Parquet
// file, lazy: rows are queried and streamed when the entry is written.
// The error is not prefixed with the file — the entry path the zip
// writer adds already names it.
func parquetEntry(ctx context.Context, path string, spec FileSpec, rows rowSource) archive.Entry {
	return archive.Entry{
		Path: path,
		Write: func(w io.Writer) error {
			return writeParquet(ctx, w, spec, rows)
		},
	}
}

// parquetPath is the in-archive path of a logical file's Parquet twin:
// one file per logical table, beside the CSV of the same name (or the
// CSV shard folder, for the per-station and per-cell tables).
func parquetPath(spec FileSpec) string {
	return spec.Name + ".parquet"
}

// ParquetEntries returns the per-table Parquet entries of one state's
// tabular archive, Path-sorted, each streaming its rows from the DB when
// written: the ten logical tables of the conagua/, nasa_power/, and
// combined/ folders (provenance/ stays CSV-only — a 1–3-row table earns
// nothing from Parquet). The station and cell lists are read now; every
// row set is queried at write time, so the single-connection DB is
// touched by one entry at a time. Parquet files are not progress units.
func ParquetEntries(ctx context.Context, db *sql.DB, st State) ([]archive.Entry, error) {
	stations, err := loadStateStations(ctx, db, st)
	if err != nil {
		return nil, err
	}
	cells, err := loadStateCells(ctx, db, st)
	if err != nil {
		return nil, err
	}
	return parquetEntries(ctx, db, st, stations, cells), nil
}

// parquetEntries builds ParquetEntries' list from already-loaded station
// and cell lists — parquetEntriesAt at the state's scope.
func parquetEntries(ctx context.Context, db *sql.DB, st State, stations []stationRef, cells []string) []archive.Entry {
	return parquetEntriesAt(ctx, db, stateScope(st), stations, cells)
}

// parquetEntriesAt builds the ten per-table Parquet entries at sc from
// its already-loaded station and cell lists — the same lists the CSV
// folders are built from, run through the same queries with the same
// bindings, so a table's CSV and Parquet files share one row-set
// definition: the whole-scope tables are one query each, and the
// per-station and per-cell tables are the per-unit queries concatenated
// in unit order, which is the exported primary-key order the CSV shards
// follow. At nationalScope the lists span every state, and the file is
// the national table regenerated — never a concatenation of the
// per-state files, which are not its shards.
func parquetEntriesAt(ctx context.Context, db *sql.DB, sc scope, stations []stationRef, cells []string) []archive.Entry {
	scopeQuery := func(query string) rowSource {
		return querySource(db, boundQuery{query: query, args: sc.args()})
	}
	perStation := func(query string, args func(s stationRef) []any) rowSource {
		queries := make([]boundQuery, len(stations))
		for i, s := range stations {
			queries[i] = boundQuery{label: "station " + s.externalID, query: query, args: args(s)}
		}
		return querySource(db, queries...)
	}
	perCell := func(query string) rowSource {
		queries := make([]boundQuery, len(cells))
		for i, id := range cells {
			queries[i] = boundQuery{label: "cell " + id, query: query, args: []any{id}}
		}
		return querySource(db, queries...)
	}
	table := func(spec FileSpec, rows rowSource) archive.Entry {
		return parquetEntry(ctx, parquetPath(spec), spec, rows)
	}

	entries := []archive.Entry{
		table(Stations, scopeQuery(stationsSQL(sc))),
		table(MonthlyNormals, scopeQuery(normalsSQL(sc))),
		table(MonthlyNormalsExtras, scopeQuery(extrasSQL(sc))),
		table(DailyObservations, perStation(dailySQL, dailyArgs)),
		table(Cells, scopeQuery(cellsSQL(sc))),
		table(StationCellMap, scopeQuery(stationCellMapSQL(sc))),
		table(PowerMonthly, scopeQuery(powerMonthlySQL(sc))),
		table(PowerDaily, perCell(powerDailySQL)),
		table(CombinedMonthly.FileSpec, scopeQuery(combinedMonthlySQL(sc))),
		table(CombinedDaily.FileSpec, perStation(combinedDailySQL, combinedDailyArgs)),
	}
	slices.SortFunc(entries, func(a, b archive.Entry) int { return strings.Compare(a.Path, b.Path) })
	return entries
}
