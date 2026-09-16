package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
)

// VacuumInto writes a compact copy of the database at srcPath to dstPath
// through SQLite's VACUUM INTO, over a connection opened read-only by
// DSN (mode=ro). It is a copy primitive, not a third opener: it returns
// no *sql.DB, applies no DDL, and stamps nothing — the copy carries the
// source's header (user_version included) exactly as it was, and
// FinalizeShipped is the step that brings a copy to the shipped shape.
// dstPath must not exist.
//
// The connection deliberately sets no PRAGMA query_only: VACUUM INTO
// writes only to its destination, but query_only refuses it as a write
// statement. mode=ro is the guarantee the source is never written; a
// read-only open of a WAL-mode source still leaves SQLite's own -shm
// and -wal coordination files beside it, which is the driver's doing
// and touches nothing in the database file. On any failure — including
// cancellation, which interrupts the statement — whatever VACUUM INTO
// left at dstPath is removed, so a partial copy never survives at the
// final path.
func VacuumInto(ctx context.Context, srcPath, dstPath string) error {
	wrap := func(err error) error { return fmt.Errorf("vacuum %s into %s: %w", srcPath, dstPath, err) }
	if _, err := os.Stat(srcPath); err != nil {
		return wrap(err)
	}
	switch _, err := os.Stat(dstPath); {
	case err == nil:
		return wrap(fmt.Errorf("destination: %w", fs.ErrExist))
	case !errors.Is(err, fs.ErrNotExist):
		return wrap(err)
	}
	if err := ctx.Err(); err != nil {
		return wrap(err)
	}

	db, err := sql.Open("sqlite", ReadOnlyDSN(srcPath))
	if err != nil {
		return wrap(err)
	}
	db.SetMaxOpenConns(1)
	// The destination is bound, not spliced: SQLite takes the INTO
	// filename as an expression, and a path with a quote must still be
	// a path.
	_, execErr := db.ExecContext(ctx, "VACUUM INTO ?", dstPath)
	closeErr := db.Close()
	if execErr != nil || closeErr != nil {
		err := execErr
		if err == nil {
			err = fmt.Errorf("close: %w", closeErr)
		}
		// A destination that cannot be removed is reported with the
		// failure that left it, so the caller knows the path is dirty
		// rather than trusting the promise above.
		if rmErr := os.Remove(dstPath); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("remove partial destination: %w", rmErr))
		}
		return wrap(err)
	}
	return nil
}

// FinalizeShipped prepares a copy for shipping: it applies the DDL
// idempotently and stamps user_version through Open (the writer), then
// switches the file to rollback-journal mode (PRAGMA journal_mode =
// DELETE) — which checkpoints the log and removes the -wal/-shm side
// files — so a reader on a read-only medium (an extracted archive, a
// mounted release) can open it without SQLite needing to create
// anything beside it, and closes. The content is unchanged; only
// file-format properties move. It does not compact: a copy VacuumInto
// wrote is compact already, and a file built by INSERT is compacted by
// its builder before this step. A second call yields the same content
// and properties but not the same bytes — each call is a write
// transaction, and SQLite's header change counter moves with it.
func FinalizeShipped(ctx context.Context, path string) error {
	wrap := func(err error) error { return fmt.Errorf("finalize %s: %w", path, err) }
	if err := ctx.Err(); err != nil {
		return wrap(err)
	}
	db, err := Open(path)
	if err != nil {
		return wrap(err)
	}
	// The PRAGMA reports the mode in force after the switch; anything
	// but delete means the switch was refused (another connection holds
	// the file), and shipping a WAL-mode file would be the bug.
	var mode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode = DELETE").Scan(&mode); err != nil {
		_ = db.Close()
		return wrap(fmt.Errorf("PRAGMA journal_mode = DELETE: %w", err))
	}
	if !strings.EqualFold(mode, "delete") {
		_ = db.Close()
		return wrap(fmt.Errorf("journal_mode is %q after PRAGMA journal_mode = DELETE", mode))
	}
	if err := db.Close(); err != nil {
		return wrap(fmt.Errorf("close: %w", err))
	}
	return nil
}
