package publish

import (
	"context"
	"database/sql"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// NationalParquetEntries returns the ten per-table Parquet files of
// national-parquet.zip, Path-sorted, each streaming its rows from the DB
// when written: the same ten logical tables a state's tabular archive
// carries as twins, through the same queries at national scope — every
// CONAGUA conventional station in external_id order, every cell those
// stations reference in cell_id order, a cell several states share once.
// A national file is regenerated, never assembled from the per-state
// files: a per-state Parquet file is not a shard of the national one
// (its row groups and footer are its own), and the per-unit tables are
// the per-unit queries concatenated in national unit order, which is
// the exported primary-key order. The two lists are read now, every
// cell id checked as a path segment as the state builders check it;
// every row set streams at write time a row group at a time, so the
// national daily tables run in the same bounded memory as a state's
// whatever their length. Parquet files are not progress units.
func NationalParquetEntries(ctx context.Context, db *sql.DB) ([]archive.Entry, error) {
	stations, err := loadStations(ctx, db, nationalScope)
	if err != nil {
		return nil, err
	}
	cells, err := loadCells(ctx, db, nationalScope)
	if err != nil {
		return nil, err
	}
	return parquetEntriesAt(ctx, db, nationalScope, stations, cells), nil
}
