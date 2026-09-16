package publish

import (
	"context"
	"io"
	"path/filepath"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// nationalDBName is the one entry of national-sqlite.zip: the full
// canonical database under its own name.
const nationalDBName = "bioclima.db"

// NationalDBEntry is bioclima.db at the root of national-sqlite.zip: the
// full canonical database — every table and every row, surrogate ids,
// FKs, indexes, and parsing_warnings included; curation applies to the
// flat files only — copied compact from the file at
// srcPath through schema.VacuumInto and brought to the shipped shape by
// schema.FinalizeShipped (rollback journal, user_version stamped), built
// when the entry is written. Write builds under a dot-prefixed temp
// directory in tmpDir (named like the archive writer's own temp files,
// so a crashed run's residue is what the out-dir pre-flight refuses),
// streams the finished file into w, and removes the directory — on
// failure too. tmpDir is created if missing; "" means the system temp
// directory.
//
// srcPath is the file the run's read-only handle is open on, resolved
// from that handle (sourcePath) rather than from a flag, so the copy is
// by construction of the database the flat files were read from. It is
// opened mode=ro and only ever read; nothing is created beside it
// beyond SQLite's own WAL coordination files. The entry is one unit.
func NationalDBEntry(ctx context.Context, srcPath, tmpDir string) archive.Entry {
	return archive.Entry{
		Path: nationalDBName,
		Write: func(w io.Writer) error {
			return writeNationalDB(ctx, w, srcPath, tmpDir)
		},
	}
}

// writeNationalDB is NationalDBEntry's Write: copy, finalize, stream,
// clean up (buildInTempDir's frame). VacuumInto lands the copy compact
// and with a page layout that is a function of the content alone;
// FinalizeShipped then leaves it in rollback-journal mode, stamped.
func writeNationalDB(ctx context.Context, w io.Writer, srcPath, tmpDir string) error {
	return buildInTempDir(w, tmpDir, nationalDBName, func(dir string) (string, error) {
		final := filepath.Join(dir, nationalDBName)
		if err := schema.VacuumInto(ctx, srcPath, final); err != nil {
			return "", err
		}
		if err := schema.FinalizeShipped(ctx, final); err != nil {
			return "", err
		}
		return final, nil
	})
}
