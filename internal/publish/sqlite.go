package publish

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// tableScope is one table's row set: the WHERE clause that selects
// the rows the state's database carries, over the source attached as
// src with the rows already copied into main visible to its subquery.
// byState marks the one clause bound to (state, source); the rest are
// scoped by what main already holds.
type tableScope struct {
	table   string
	where   string
	byState bool
}

// stateDBPlan is the row set of the filtered {state}.db, table by
// table: the state's CONAGUA conventional stations; every row keyed to
// them — normals, extras, daily observations, the cell map, and their
// parsing warnings; the cells those stations reference and only those
// cells' supplement rows; and every ingest and power run, since
// provenance is global. The order is parent-first, so each scoping
// subquery reads rows the plan already copied, and the surrogate ids
// travel with the rows, so every FK resolves in-file. buildStateDB holds the plan to the DDL's table set
// in both directions: a DDL table without a scope, or a scope naming no
// DDL table, refuses the build rather than shipping a database that
// silently lacks a table.
var stateDBPlan = []tableScope{
	{table: "stations", where: "WHERE state = ? AND source = ?", byState: true},
	{table: "monthly_normals", where: "WHERE station_id IN (SELECT id FROM main.stations)"},
	{table: "monthly_normals_extras", where: "WHERE station_id IN (SELECT id FROM main.stations)"},
	{table: "daily_observations", where: "WHERE station_id IN (SELECT id FROM main.stations)"},
	{table: "parsing_warnings", where: "WHERE station_id IN (SELECT id FROM main.stations)"},
	{table: "ingest_runs"},
	{table: "power_runs"},
	{table: "station_power_cell", where: "WHERE station_id IN (SELECT id FROM main.stations)"},
	{table: "nasa_power_grid_cells", where: "WHERE cell_id IN (SELECT cell_id FROM main.station_power_cell)"},
	{table: "monthly_supplement", where: "WHERE cell_id IN (SELECT cell_id FROM main.nasa_power_grid_cells)"},
	{table: "daily_supplement", where: "WHERE cell_id IN (SELECT cell_id FROM main.nasa_power_grid_cells)"},
}

// stateDBName is the filtered database's file name at the root of a
// state's tabular archive: <slug>.db on the lowercase slug — the one
// spelling the archive writer, the national copy's classifier, and the
// documentation share.
func stateDBName(st State) string {
	return st.Slug + ".db"
}

// StateDBEntry is the filtered {state}.db at the archive root: the full
// schema — every table, index, surrogate id, and FK — holding the row
// set of one state (stateDBPlan), built when the entry is written. Write builds the
// database in a dot-prefixed temp directory under tmpDir (named like the
// archive writer's own temp files, so a crashed run's residue is what
// the out-dir pre-flight refuses), streams the finished file into w,
// and removes the directory — on failure too. tmpDir is created if
// missing; "" means the system temp directory.
//
// The source is the file db is open on, resolved from the connection
// (PRAGMA database_list) rather than passed alongside, so the .db is by
// construction filtered from the same file the archive's flat files
// were read from. It is attached read-only by DSN on a second
// connection and only ever read; nothing is created beside it beyond
// SQLite's own WAL coordination files.
func StateDBEntry(ctx context.Context, db *sql.DB, st State, tmpDir string) archive.Entry {
	return archive.Entry{
		Path: stateDBName(st),
		Write: func(w io.Writer) error {
			return writeStateDB(ctx, w, db, st, tmpDir)
		},
	}
}

// writeStateDB is StateDBEntry's Write: build, compact, finalize, stream,
// clean up. The build lands in build.db; VacuumInto rewrites it into
// <slug>.db compact and with a page layout that is a function of the
// content alone (the build's own layout follows its WAL checkpoints);
// FinalizeShipped then leaves it in rollback-journal mode, stamped.
func writeStateDB(ctx context.Context, w io.Writer, db *sql.DB, st State, tmpDir string) error {
	srcPath, err := sourcePath(ctx, db)
	if err != nil {
		return err
	}
	name := stateDBName(st)
	return buildInTempDir(w, tmpDir, name, func(dir string) (string, error) {
		build := filepath.Join(dir, "build.db")
		final := filepath.Join(dir, name)
		if err := buildStateDB(ctx, build, srcPath, st); err != nil {
			return "", err
		}
		if err := schema.VacuumInto(ctx, build, final); err != nil {
			return "", err
		}
		if err := schema.FinalizeShipped(ctx, final); err != nil {
			return "", err
		}
		return final, nil
	})
}

// buildInTempDir is the on-disk frame every database entry streams
// through: a dot-prefixed temp directory named after the entry under
// tmpDir (like the archive writer's own temp files, so a crashed run's
// residue is what the out-dir pre-flight refuses), build run inside it
// and returning the finished file's path, that file streamed into w,
// and the directory removed — on failure too. The temp directory holds
// every side file SQLite makes along the way, so one RemoveAll clears
// the build whatever step failed; a cleanup failure on an otherwise
// successful write is the write's error, since the residue would fail
// the deposit's post-check anyway. tmpDir is created if missing; ""
// means the system temp directory.
func buildInTempDir(w io.Writer, tmpDir, name string, build func(dir string) (final string, err error)) (err error) {
	if tmpDir != "" {
		if err := os.MkdirAll(tmpDir, 0o755); err != nil {
			return fmt.Errorf("create temp dir: %w", err)
		}
	}
	dir, err := os.MkdirTemp(tmpDir, "."+name+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() {
		if rmErr := os.RemoveAll(dir); rmErr != nil && err == nil {
			err = fmt.Errorf("remove temp dir: %w", rmErr)
		}
	}()

	final, err := build(dir)
	if err != nil {
		return err
	}
	return streamFile(w, final)
}

// sourcePath resolves the file db is open on — the main database's
// path as SQLite reports it, absolute. A connection with no file (an
// in-memory database) is refused: there is nothing to attach.
func sourcePath(ctx context.Context, db *sql.DB) (string, error) {
	var file string
	if err := db.QueryRowContext(ctx, "SELECT file FROM pragma_database_list WHERE name = 'main'").Scan(&file); err != nil {
		return "", fmt.Errorf("resolve source database path: %w", err)
	}
	if file == "" {
		return "", errors.New("resolve source database path: the connection has no database file")
	}
	return file, nil
}

// buildStateDB creates the state's database at path — the DDL, the
// stamp, and the WAL mode the writer gives every new file — attaches
// the source read-only, and copies the plan's row sets in one
// transaction. Everything runs on one dedicated connection: an ATTACH
// is per connection, and the pool must not hand a later statement a
// connection that never saw it. Each table's column list and primary
// key come from pragma_table_info over the freshly applied DDL, never
// from a hand-written list, and the source's shape is held to it
// column for column before a row is copied: a source with a column the
// DDL lacks, or without one it has, is not this schema, and a copy
// that silently dropped or lacked a column would misrepresent what the
// artifact claims to be — the full schema.
func buildStateDB(ctx context.Context, path, srcPath string, st State) (err error) {
	wrap := func(err error) error { return fmt.Errorf("build state database: %w", err) }
	db, err := schema.Open(path)
	if err != nil {
		return wrap(err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil && err == nil {
			err = wrap(fmt.Errorf("close: %w", closeErr))
		}
	}()
	conn, err := db.Conn(ctx)
	if err != nil {
		return wrap(err)
	}
	defer conn.Close() //nolint:errcheck // the pool's Close follows; a connection-return error on a file about to be discarded or already committed has no recovery

	if _, err := conn.ExecContext(ctx, "ATTACH DATABASE ? AS src", schema.ReadOnlyDSN(srcPath)); err != nil {
		return wrap(fmt.Errorf("attach source %s: %w", srcPath, err))
	}
	if err := copyStateRows(ctx, conn, st); err != nil {
		return wrap(err)
	}
	if _, err := conn.ExecContext(ctx, "DETACH DATABASE src"); err != nil {
		return wrap(fmt.Errorf("detach source: %w", err))
	}
	return nil
}

// copyStateRows holds the plan to main's DDL tables, checks each
// source table's shape, and runs the plan's INSERT … SELECT statements
// in one transaction, each selecting the source rows in primary-key
// order so the rowids main assigns — and with them the file's layout —
// follow the content rather than the source's physical order. The copy
// is then held to its own cell references (checkCellRefs) before it
// commits.
func copyStateRows(ctx context.Context, conn *sql.Conn, st State) error {
	tables, err := schemaTables(ctx, conn, "main")
	if err != nil {
		return err
	}
	if err := checkPlan(tables); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // a no-op after Commit; on the error path the file is discarded whole

	for _, scope := range stateDBPlan {
		shape, err := tableShape(ctx, tx, scope.table)
		if err != nil {
			return err
		}
		cols := strings.Join(shape.columns, ", ")
		stmt := "INSERT INTO main." + scope.table + " (" + cols + ") SELECT " + cols + " FROM src." + scope.table
		if scope.where != "" {
			stmt += " " + scope.where
		}
		stmt += " ORDER BY " + strings.Join(shape.key, ", ")
		var args []any
		if scope.byState {
			args = []any{st.Code, string(ingest.SourceConaguaConventional)}
		}
		if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("copy %s: %w", scope.table, err)
		}
	}
	if err := checkCellRefs(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// checkCellRefs refuses a copied station_power_cell row whose cell has
// no nasa_power_grid_cells row — a broken FK SQLite does not enforce
// at runtime. The plan scopes the two supplement tables through the
// copied grid cells, so such a cell's supplement rows would silently
// drop out of the database while the archive's nasa_power/ files,
// scoped through the map, carry them; the JSON archive refuses the
// same reference. A corruption signal, so the build aborts rather than
// ship the two views of one archive disagreeing.
func checkCellRefs(ctx context.Context, tx *sql.Tx) error {
	var cell string
	err := tx.QueryRowContext(ctx, `SELECT cell_id FROM main.station_power_cell
	  WHERE cell_id NOT IN (SELECT cell_id FROM main.nasa_power_grid_cells) ORDER BY cell_id LIMIT 1`).Scan(&cell)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("check cell references: %w", err)
	}
	return fmt.Errorf("cell %q has no nasa_power_grid_cells row", cell)
}

// checkPlan holds stateDBPlan to the DDL's table set in both
// directions, so a table can be neither silently missing from the
// artifact nor named by a stale plan.
func checkPlan(tables []string) error {
	planned := make([]string, 0, len(stateDBPlan))
	for _, s := range stateDBPlan {
		planned = append(planned, s.table)
	}
	slices.Sort(planned)
	for _, t := range tables {
		if !slices.Contains(planned, t) {
			return fmt.Errorf("schema table %s has no row-set rule in the state database plan", t)
		}
	}
	for _, t := range planned {
		if !slices.Contains(tables, t) {
			return fmt.Errorf("state database plan names %s, which the schema has no table for", t)
		}
	}
	if len(slices.Compact(planned)) != len(planned) {
		return errors.New("state database plan names a table twice")
	}
	return nil
}

// schemaTables lists the user tables of the named schema, sorted.
func schemaTables(ctx context.Context, conn *sql.Conn, schemaName string) ([]string, error) {
	rows, err := conn.QueryContext(ctx,
		"SELECT name FROM "+schemaName+".sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("list %s tables: %w", schemaName, err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("list %s tables: scan: %w", schemaName, err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list %s tables: %w", schemaName, err)
	}
	return tables, nil
}

// shape is a table's columns in DDL order and its primary-key columns
// in key order, as pragma_table_info reports them.
type shape struct {
	columns []string
	key     []string
}

// keyColumn is one primary-key column with its 1-based position in the
// key, as pragma_table_info's pk column reports it.
type keyColumn struct {
	pos  int
	name string
}

// tableShape reads table's shape from main and holds src's to it: the
// same column names in the same order. A source that differs is not
// this schema, and the build refuses rather than copy through a list
// that fits one side only.
func tableShape(ctx context.Context, tx *sql.Tx, table string) (shape, error) {
	main, err := readShape(ctx, tx, table, "main")
	if err != nil {
		return shape{}, err
	}
	src, err := readShape(ctx, tx, table, "src")
	if err != nil {
		return shape{}, err
	}
	if !slices.Equal(main.columns, src.columns) {
		return shape{}, fmt.Errorf("source table %s has columns %v, the schema has %v", table, src.columns, main.columns)
	}
	if len(main.key) == 0 {
		return shape{}, fmt.Errorf("schema table %s declares no primary key", table)
	}
	return main, nil
}

// readShape is one pragma_table_info pass over schemaName.table. A
// table the schema lacks yields no rows, which is reported as such
// rather than as an empty copy.
func readShape(ctx context.Context, tx *sql.Tx, table, schemaName string) (shape, error) {
	rows, err := tx.QueryContext(ctx, "SELECT name, pk FROM pragma_table_info(?, ?) ORDER BY cid", table, schemaName)
	if err != nil {
		return shape{}, fmt.Errorf("describe %s.%s: %w", schemaName, table, err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported
	var (
		s   shape
		key []keyColumn
	)
	for rows.Next() {
		var (
			name string
			pk   int
		)
		if err := rows.Scan(&name, &pk); err != nil {
			return shape{}, fmt.Errorf("describe %s.%s: scan: %w", schemaName, table, err)
		}
		s.columns = append(s.columns, name)
		if pk > 0 {
			key = append(key, keyColumn{pos: pk, name: name})
		}
	}
	if err := rows.Err(); err != nil {
		return shape{}, fmt.Errorf("describe %s.%s: %w", schemaName, table, err)
	}
	if len(s.columns) == 0 {
		return shape{}, fmt.Errorf("%s has no table %s", schemaName, table)
	}
	slices.SortFunc(key, func(a, b keyColumn) int { return a.pos - b.pos })
	for _, k := range key {
		s.key = append(s.key, k.name)
	}
	return s, nil
}

// streamFile copies the file at path into w — a finished database or a
// snapshot file — naming the file's base name in the error.
func streamFile(w io.Writer, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("stream %s: %w", filepath.Base(path), err)
	}
	defer f.Close() //nolint:errcheck // read-side close; no recovery possible
	if _, err := io.Copy(w, f); err != nil {
		return fmt.Errorf("stream %s: %w", filepath.Base(path), err)
	}
	return nil
}
