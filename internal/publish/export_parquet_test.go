package publish

import (
	"context"
	"database/sql"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// Test-only handles on the Parquet writer, so the external spec package
// can hold a file's schema to its spec, re-format a double read back
// through the one fixed-decimal formatter, and drive the writer over a
// crafted query, without the package exporting them.
var (
	FormatReal    = formatReal
	ParquetSchema = parquetSchema
)

// ParquetTableEntry builds one Parquet entry over a single bound query.
func ParquetTableEntry(ctx context.Context, db *sql.DB, path string, spec FileSpec, query string, args ...any,
) archive.Entry {
	return parquetEntry(ctx, path, spec, querySource(db, boundQuery{query: query, args: args}))
}
