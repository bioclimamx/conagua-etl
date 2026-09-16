package publish

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// newProfileMeta fixes the meta block every profile of one build
// carries: the schema version, the build's git SHA, the resolved
// snapshot, the runs loaded before the state loop, and the dataset
// identity. Run builds it once and every station shares it, so the
// profiles of one deposit cannot disagree on their provenance.
func newProfileMeta(gitSHA string, snapshot time.Time, runs Runs, doi string) ProfileMeta {
	return ProfileMeta{
		SchemaVersion: schema.Version,
		ETLGitSHA:     gitSHA,
		SnapshotDate:  snapshot.Format(snapshotDateLayout),
		Runs:          runs,
		Dataset:       DatasetMetadata(snapshot, doi),
	}
}

// JSONEntries assembles one state's JSON archive, {state}-json.zip, as
// the entry list archive.WriteZip takes, Path-sorted: for every station
// of the state's station list — the one list the tabular per-station
// folders are built from, so the two archives' station sets are
// one-to-one by construction — the pair
// combined/<slug>/<station_id>/daily.json + profile.json, plus
// provenance/ingest_runs.json and provenance/power_runs.json rendered
// from runs. The profile is the combined product, so conagua/ and
// nasa_power/ are not materialized here. The station list is read now;
// every entry queries at write time, so the single-connection DB is
// touched by one entry at a time. The unit is
// the station: onUnit (optional) fires once per station, in write
// order, after its profile.json — the second of the pair in path order
// — is written, and units is the station count, the Total the first
// unit event must already carry.
func JSONEntries(ctx context.Context, db *sql.DB, st State, runs Runs, meta ProfileMeta, onUnit UnitFunc,
) (entries []archive.Entry, units int, err error) {
	stations, err := loadStateStations(ctx, db, st)
	if err != nil {
		return nil, 0, err
	}
	entries, units = jsonEntries(ctx, db, st, stations, runs, meta, onUnit)
	return entries, units, nil
}

// jsonEntries builds JSONEntries' list from an already-loaded station
// list and reports how many of the entries are units. The per-cell
// profile reads are memoized across the list — one archive, one cache.
func jsonEntries(ctx context.Context, db *sql.DB, st State, stations []stationRef, runs Runs, meta ProfileMeta,
	onUnit UnitFunc,
) (entries []archive.Entry, units int) {
	u := unitList{onUnit: onUnit}
	cells := newCellCache()
	daily := make([]archive.Entry, 0, len(stations))
	for _, s := range stations {
		daily = append(daily, dailyJSONEntry(ctx, db, st, s))
		profile := profileEntry(ctx, db, st, s, meta, cells)
		u.add(profile.Path, profile.Write)
	}
	entries = slices.Concat(daily, u.entries, provenanceJSONEntries(runs))
	slices.SortFunc(entries, func(a, b archive.Entry) int { return strings.Compare(a.Path, b.Path) })
	return entries, len(u.entries)
}
