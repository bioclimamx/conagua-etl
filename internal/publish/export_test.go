package publish

import (
	"context"
	"database/sql"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// Test-only handles on the folder queries, so the external spec package
// can hold their EXPLAIN QUERY PLAN to the primary-key walk without the
// package exporting SQL text. The whole-scope queries are exposed at a
// state's scope — the predicate text is the same whichever state —
// bound by the specs as (state, source).
var (
	PowerMonthlySQL    = powerMonthlySQL(stateScope(State{Code: "YUC"}))
	PowerDailySQL      = powerDailySQL
	CombinedMonthlySQL = combinedMonthlySQL(stateScope(State{Code: "YUC"}))
	CombinedDailySQL   = combinedDailySQL
)

// NationalTableEntries is tableEntries at national scope — the seven
// whole-scope CSV tables the national CSV archive regenerates.
func NationalTableEntries(ctx context.Context, db *sql.DB) []archive.Entry {
	return tableEntries(ctx, db, nationalScope)
}

// NationalStationIDs lists the national station list's external_ids in
// list order; NationalCellIDs the national cell list.
func NationalStationIDs(ctx context.Context, db *sql.DB) ([]string, error) {
	stations, err := loadStations(ctx, db, nationalScope)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(stations))
	for i, s := range stations {
		ids[i] = s.externalID
	}
	return ids, nil
}

func NationalCellIDs(ctx context.Context, db *sql.DB) ([]string, error) {
	return loadCells(ctx, db, nationalScope)
}

// StateStationIDs lists a state's station list's external_ids in list
// order.
func StateStationIDs(ctx context.Context, db *sql.DB, st State) ([]string, error) {
	stations, err := loadStateStations(ctx, db, st)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(stations))
	for i, s := range stations {
		ids[i] = s.externalID
	}
	return ids, nil
}
