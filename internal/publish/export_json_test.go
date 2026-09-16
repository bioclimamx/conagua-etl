package publish

import (
	"context"
	"database/sql"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// Test-only handles on the JSON entries and the profile's queries, so
// the external spec package can exercise the daily.json writer per
// station, hold its query to combinedDailySQL's spine, and hold every
// profile query's EXPLAIN QUERY PLAN to a primary-key or indexed walk
// without the package exporting them.
var (
	DailyJSONSQL          = dailyJSONSQL
	ProvenanceJSONEntries = provenanceJSONEntries

	StationRowSQL         = stationRowSQL
	NormalsSeriesSQL      = normalsSeriesSQL
	ExtrasSeriesSQL       = extrasSeriesSQL
	PowerSeriesSQL        = powerSeriesSQL
	GridCellSQL           = gridCellSQL
	DailySeriesSQL        = dailySeriesSQL
	ReanalysisCoverageSQL = reanalysisCoverageSQL
)

// DailyJSONEntries builds the daily.json entry of every station of st,
// in station order, from the one station list the per-station folders
// share.
func DailyJSONEntries(ctx context.Context, db *sql.DB, st State) ([]archive.Entry, error) {
	stations, err := loadStateStations(ctx, db, st)
	if err != nil {
		return nil, err
	}
	entries := make([]archive.Entry, len(stations))
	for i, s := range stations {
		entries[i] = dailyJSONEntry(ctx, db, st, s)
	}
	return entries, nil
}
