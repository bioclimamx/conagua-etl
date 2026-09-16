package publish

import (
	"context"
	"database/sql"
	"io"
	"slices"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// UnitFunc reports one unit of work inside an artifact — a per-station
// or per-cell daily file of a tabular archive, a station's profile +
// daily.json pair of a JSON archive — by the path of the entry whose
// write completes it, right after that entry is written. Ordinals are
// the archive's to assign, not the folder's: a builder knows which of
// its entries are units, only the archive knows how many the other
// folders contribute.
type UnitFunc func(path string)

// unitList collects a folder's unit entries, each built so its write
// fires onUnit with the entry path once the file is written. It is the
// one definition of "unit": a folder's unit count is the number of
// entries it added here, and an archive's Total is the sum over its
// folders — so a unit cannot be counted in one place and not the
// other, and Index can never run past Total.
type unitList struct {
	onUnit  UnitFunc
	entries []archive.Entry
}

func (u *unitList) add(path string, write func(io.Writer) error) {
	u.entries = append(u.entries, archive.Entry{
		Path: path,
		Write: func(w io.Writer) error {
			if err := write(w); err != nil {
				return err
			}
			if u.onUnit != nil {
				u.onUnit(path)
			}
			return nil
		},
	})
}

// selectList renders spec's columns, in export order, through expr —
// the one place a query's select list is derived, so the header row and
// the scanned values are the same list by construction.
func selectList(spec FileSpec, expr func(Column) string) string {
	return selectColumns(spec.Columns, expr)
}

// selectColumns is selectList over a bare column list, for a row shape
// that is not a flat file (daily.json).
func selectColumns(cols []Column, expr func(Column) string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = expr(c)
	}
	return strings.Join(parts, ", ")
}

// childTableSQL selects one station-keyed normals table at a scope: the
// surrogate station_id resolves to stations.external_id through the
// join, and the rows come back in exported-natural-key order (TEXT keys
// bytewise under SQLite's BINARY collation, month numerically).
func childTableSQL(spec FileSpec, sc scope) string {
	return "SELECT " + selectList(spec, func(c Column) string {
		if c.DB == "station_id" {
			return "s.external_id"
		}
		return "t." + c.DB
	}) + " FROM " + spec.Table + " t JOIN stations s ON s.id = t.station_id " +
		sc.predicate() + " ORDER BY s.external_id, t.period, t.month"
}

// The conagua/ folder's whole-scope queries, each the one definition of
// its table's row set: a state archive binds them at the state's scope,
// the national archive at nationalScope.
func stationsSQL(sc scope) string {
	return "SELECT " + selectList(Stations, func(c Column) string { return "s." + c.DB }) +
		" FROM stations s " + sc.predicate() + " ORDER BY s.external_id"
}

func normalsSQL(sc scope) string { return childTableSQL(MonthlyNormals, sc) }
func extrasSQL(sc scope) string  { return childTableSQL(MonthlyNormalsExtras, sc) }

// The per-station daily query walks the (station_id, date) primary key
// — never an expression filter, which would defeat the index and turn a
// national export into a scan. The station_id column is the bound
// external_id: the file already belongs to one station, so no per-row
// join is needed.
var dailySQL = "SELECT " + selectList(DailyObservations, func(c Column) string {
	if c.DB == "station_id" {
		return "?"
	}
	return "d." + c.DB
}) + " FROM daily_observations d WHERE d.station_id = ? ORDER BY d.date"

// dailyArgs binds dailySQL for one station, in the order its
// placeholders occur: the select-list station_id constant, then the
// spine's station. The one binding the CSV shard and the Parquet twin
// share, so the placeholder order is a contract kept in one place.
func dailyArgs(s stationRef) []any {
	return []any{s.externalID, s.id}
}

// dailyPath is the per-station daily file under the state shard
// (the shard is named by the state's lowercase slug).
func dailyPath(st State, externalID string) string {
	return DailyObservations.Name + "/" + st.Slug + "/daily-" + externalID + ".csv"
}

// ConaguaEntries returns the conagua/ CSV entries for one state,
// Path-sorted, each streaming its rows from the DB when written:
// stations.csv, monthly_normals.csv, monthly_normals_extras.csv (one
// file each, that state's rows), and
// daily_observations/<slug>/daily-<station_id>.csv per station — a
// station with no daily rows still gets its header-only file, so the
// archive's station set is one-to-one with the stations file. The
// station list is read now; every row set is queried at write time, so
// the single-connection DB is touched by one entry at a time. onUnit
// (optional) fires once per station, in write order, after its daily
// entry is written. A daily entry's error is not prefixed with the
// station: the entry path the zip writer adds already carries the id.
func ConaguaEntries(ctx context.Context, db *sql.DB, st State, onUnit UnitFunc) ([]archive.Entry, error) {
	stations, err := loadStateStations(ctx, db, st)
	if err != nil {
		return nil, err
	}
	entries, _ := conaguaEntries(ctx, db, st, stations, onUnit)
	return entries, nil
}

// conaguaEntries builds ConaguaEntries' list from an already-loaded
// station list and reports how many of the entries are units.
func conaguaEntries(ctx context.Context, db *sql.DB, st State, stations []stationRef, onUnit UnitFunc,
) (entries []archive.Entry, units int) {
	u := unitList{onUnit: onUnit}
	for _, s := range stations {
		u.add(dailyPath(st, s.externalID), func(w io.Writer) error {
			return writeQuery(ctx, w, db, DailyObservations, dailySQL, dailyArgs(s)...)
		})
	}
	entries = append(conaguaTableEntries(ctx, db, stateScope(st)), u.entries...)
	slices.SortFunc(entries, func(a, b archive.Entry) int { return strings.Compare(a.Path, b.Path) })
	return entries, len(u.entries)
}

// conaguaTableEntries returns the conagua/ folder's three whole-scope
// tables at sc: stations.csv, monthly_normals.csv, and
// monthly_normals_extras.csv.
func conaguaTableEntries(ctx context.Context, db *sql.DB, sc scope) []archive.Entry {
	return []archive.Entry{
		tableEntry(ctx, db, sc, Stations, stationsSQL(sc)),
		tableEntry(ctx, db, sc, MonthlyNormals, normalsSQL(sc)),
		tableEntry(ctx, db, sc, MonthlyNormalsExtras, extrasSQL(sc)),
	}
}
