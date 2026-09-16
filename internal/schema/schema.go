// Package schema embeds the canonical SQLite DDL and opens the bioclima
// processing database with the PRAGMAs each workload needs.
//
// This DB is the offline processing substrate. It's never served live —
// downstream consumers read rows from here and (in a future publish
// step) emit static release artifacts.
package schema

import (
	"database/sql"
	_ "embed"
	"fmt"
	"os"
	"regexp"
	"strconv"

	// The CGo-free SQLite driver — matters for cross-compiled citable
	// release binaries.
	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var ddl string

// Version is the integer parsed out of the `-- schema_version: N` header
// at the top of schema.sql. Exported so downstream artifacts can stamp
// the same provenance number the on-disk schema carries — keeping the
// single source of truth in the SQL file.
var Version = mustParseVersion(ddl)

var schemaVersionRe = regexp.MustCompile(`(?m)^--\s*schema_version:\s*(\d+)`)

func mustParseVersion(s string) int {
	m := schemaVersionRe.FindStringSubmatch(s)
	if m == nil {
		panic("schema.sql is missing the `-- schema_version: N` header")
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		panic(fmt.Sprintf("schema.sql carries an unparseable schema_version: %v", err))
	}
	return n
}

// Open returns a *sql.DB configured for bulk ingest *and* ad-hoc queries.
// It is the ETL's only writer entry point:
//
//   - WAL journal mode + NORMAL sync: acceptable durability with
//     dramatically faster bulk inserts than the default (DELETE + FULL).
//   - ~200 MiB cache: keeps indexes hot during the daily pass.
//   - temp_store=MEMORY: avoids spilling GROUP BY / ORDER BY to disk.
//   - foreign_keys=OFF: we rely on the CHECK constraints and the
//     (source, external_id) UNIQUE to guard invariants at load time;
//     FK enforcement during bulk insert is an overhead we don't earn.
//
// The schema is applied via CREATE TABLE IF NOT EXISTS so Open is safe
// against existing databases, and the file header is then stamped with
// Version (PRAGMA user_version) so consumers that never see schema.sql
// can read the schema version from the DB itself. A header carrying a
// different non-zero version is refused before the DDL is applied:
// there is no migration path, and relabelling a file another schema
// wrote would silently misrepresent its shape.
func Open(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	// SQLite drivers serialize writes anyway; a single connection keeps
	// things predictable and avoids WAL checkpoint fights.
	db.SetMaxOpenConns(1)

	// The version check precedes every PRAGMA: switching the journal
	// mode rewrites the file header, and a file this schema refuses must
	// come back byte for byte as it was.
	current, err := checkVersion(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA cache_size = -200000", // negative = KiB, so ~200 MiB
		"PRAGMA foreign_keys = OFF",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("%s: %w", p, err)
		}
	}

	if _, err := db.Exec(ddl); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	if current != Version {
		if err := stampVersion(db); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return db, nil
}

// checkVersion reads the header's PRAGMA user_version and refuses any
// value other than SQLite's default 0 (a fresh file, or one created
// before the stamp existed) and Version itself. Open has no migration
// path — its DDL is CREATE IF NOT EXISTS — so a file stamped by another
// schema version cannot be brought current here, and applying this DDL
// to it, or relabelling it, would misrepresent what the file holds.
func checkVersion(db *sql.DB) (current int, err error) {
	if err := db.QueryRow("PRAGMA user_version").Scan(&current); err != nil {
		return 0, fmt.Errorf("read user_version: %w", err)
	}
	if current != 0 && current != Version {
		return 0, fmt.Errorf("user_version %d does not match schema version %d (migration required)", current, Version)
	}
	return current, nil
}

// stampVersion records Version in the database header as
// PRAGMA user_version: the conagua-site exporter
// and publish's manifest read the schema version from the file rather
// than parsing the DDL header. Writer opens only — a read-only
// connection cannot write the header, and OpenReadOnly never tries.
// Open calls it only when the header does not already carry Version,
// so re-opening a current database writes nothing; a database created
// before the stamp existed (header at SQLite's default 0) is stamped
// on its next writer open.
func stampVersion(db *sql.DB) error {
	// PRAGMA takes no bound parameters; Version is a parsed int, so the
	// formatted statement can carry nothing but digits.
	if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d", Version)); err != nil {
		return fmt.Errorf("stamp user_version = %d: %w", Version, err)
	}
	return nil
}

// OpenReadOnly returns a *sql.DB that is guaranteed never to mutate the
// database at path: the connection is opened with `mode=ro` and, as a
// belt-and-braces second layer, `PRAGMA query_only=ON`. No DDL is
// applied, no user_version is stamped, and the journal mode is left
// untouched; the only PRAGMAs set are read-tuned (temp_store=MEMORY,
// ~200 MiB cache). Any write statement fails.
//
// This is the opener for read-only consumers — the parity comparator,
// publish, and validate's gate path. Open remains the only writer.
//
// The driver defers opening the file, so with mode=ro a missing
// database would surface only at the first query; the explicit
// existence check reports it at open time instead.
func OpenReadOnly(path string) (*sql.DB, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("open read-only %s: %w", path, err)
	}
	db, err := sql.Open("sqlite", ReadOnlyDSN(path))
	if err != nil {
		return nil, fmt.Errorf("open read-only %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)

	pragmas := []string{
		"PRAGMA query_only = ON",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA cache_size = -200000", // negative = KiB, so ~200 MiB
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("%s: %w", p, err)
		}
	}
	return db, nil
}

// ReadOnlyDSN is the mode=ro URI that opens the database at path
// read-only at the file level — the one definition shared by
// OpenReadOnly, VacuumInto, and a publish ATTACH of the source, so no
// second spelling of the guarantee exists. Read-only by DSN is SQLite's
// own refusal to write the file; it says nothing about statements, which
// is why OpenReadOnly adds PRAGMA query_only on top.
//
// Built by plain concatenation, so paths containing '?' or '#' are
// unsupported (they would be parsed as DSN query/fragment).
func ReadOnlyDSN(path string) string {
	return "file:" + path + "?mode=ro"
}
