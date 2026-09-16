package publish

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
)

// scope is the station set a whole-scope file is built over: one
// state's CONAGUA conventional stations (the per-state archives), or —
// state nil — every CONAGUA conventional station in the DB (the national
// archives). Every whole-scope query in the package is written once,
// against the stations alias s, and takes its WHERE clause from here, so
// a national per-table file is the state file's query without the state
// predicate and nothing else: the same select list, joins, and
// primary-key sort. The source predicate is never dropped — the EMA
// stations share the table and belong to no archive.
type scope struct {
	state *State
}

// stateScope scopes to one state's stations.
func stateScope(st State) scope { return scope{state: &st} }

// nationalScope is every CONAGUA conventional station.
var nationalScope = scope{}

// predicate is the WHERE clause over the stations alias s that selects
// the scope's stations. args binds its placeholders, in order; the two
// are one definition read in two places, so a query built from
// predicate and bound from args cannot disagree on the placeholder
// count.
func (sc scope) predicate() string {
	if sc.state == nil {
		return "WHERE s.source = ?"
	}
	return "WHERE s.state = ? AND s.source = ?"
}

// args are the values predicate's placeholders bind to, in order:
// (state, source) for a state, (source) nationally.
func (sc scope) args() []any {
	source := string(ingest.SourceConaguaConventional)
	if sc.state == nil {
		return []any{source}
	}
	return []any{sc.state.Code, source}
}

// label names the scope in an error: the state code, or "all states".
func (sc scope) label() string {
	if sc.state == nil {
		return "all states"
	}
	return sc.state.Code
}

// stationRef is one station of a scope: the surrogate the daily queries
// bind, the external_id the daily files are named after, and the POWER
// cell it maps to — NULL for a station with no station_power_cell row,
// which the combined/ files carry as an empty join context and the
// conagua/ files ignore.
type stationRef struct {
	id         int64
	externalID string
	cellID     sql.NullString
	distanceKm sql.NullFloat64
}

// stationListSQL is the one definition of "the scope's stations": its
// CONAGUA conventional stations with their cell, in external_id order —
// the order the per-station files are written in and the per-station
// Parquet tables concatenate in. Both per-station folders (conagua/,
// combined/), the JSON archive, and the Parquet twins are built from
// this list, so their station sets are one-to-one with each other and
// with stations.csv by construction, not by several WHERE clauses
// agreeing.
func stationListSQL(sc scope) string {
	return "SELECT s.id, s.external_id, spc.cell_id, spc.distance_km" +
		" FROM stations s LEFT JOIN station_power_cell spc ON spc.station_id = s.id " +
		sc.predicate() + " ORDER BY s.external_id"
}

// loadStations lists the scope's stations per stationListSQL.
func loadStations(ctx context.Context, db *sql.DB, sc scope) ([]stationRef, error) {
	rows, err := db.QueryContext(ctx, stationListSQL(sc), sc.args()...)
	if err != nil {
		return nil, fmt.Errorf("list stations of %s: %w", sc.label(), err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	var stations []stationRef
	for rows.Next() {
		var s stationRef
		if err := rows.Scan(&s.id, &s.externalID, &s.cellID, &s.distanceKm); err != nil {
			return nil, fmt.Errorf("list stations of %s: scan: %w", sc.label(), err)
		}
		stations = append(stations, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list stations of %s: %w", sc.label(), err)
	}
	return stations, nil
}

// loadStateStations lists a state's stations — loadStations at the
// state's scope.
func loadStateStations(ctx context.Context, db *sql.DB, st State) ([]stationRef, error) {
	return loadStations(ctx, db, stateScope(st))
}

// cellSetSQL lists the DISTINCT cells the scope's stations reference —
// the one definition of "the scope's cells" that the cells file, the
// monthly file, the per-cell daily files, and the cell list share, so
// none of them can disagree on scope. A cell several states' stations
// reference is one cell of each state's scope and one cell of the
// national scope.
func cellSetSQL(sc scope) string {
	return "SELECT DISTINCT m.cell_id FROM station_power_cell m" +
		" JOIN stations s ON s.id = m.station_id " + sc.predicate()
}

// loadCells lists the cells the scope's stations reference, each once,
// in cell_id order, every id checked as a path segment before any file
// is named after it.
func loadCells(ctx context.Context, db *sql.DB, sc scope) ([]string, error) {
	rows, err := db.QueryContext(ctx, cellSetSQL(sc)+" ORDER BY m.cell_id", sc.args()...)
	if err != nil {
		return nil, fmt.Errorf("list cells of %s: %w", sc.label(), err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	var cells []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("list cells of %s: scan: %w", sc.label(), err)
		}
		if err := checkCellID(id); err != nil {
			return nil, fmt.Errorf("list cells of %s: %w", sc.label(), err)
		}
		cells = append(cells, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list cells of %s: %w", sc.label(), err)
	}
	return cells, nil
}

// loadStateCells lists the cells a state's stations reference —
// loadCells at the state's scope.
func loadStateCells(ctx context.Context, db *sql.DB, st State) ([]string, error) {
	return loadCells(ctx, db, stateScope(st))
}

// tableEntry is the CSV entry of one whole-scope table: spec's file at
// <spec.Name>.csv, streaming query — built from sc's predicate — bound
// to sc's arguments when written.
func tableEntry(ctx context.Context, db *sql.DB, sc scope, spec FileSpec, query string) archive.Entry {
	return archive.Entry{
		Path: spec.Name + ".csv",
		Write: func(w io.Writer) error {
			return writeQuery(ctx, w, db, spec, query, sc.args()...)
		},
	}
}

// tableEntries returns the seven whole-scope CSV tables of the three
// data folders at sc, Path-sorted, each streaming its rows from the DB
// when written: conagua/stations, monthly_normals, and
// monthly_normals_extras; nasa_power/cells, station_cell_map, and
// monthly; combined/combined_monthly. At national scope these are the
// files the national CSV archive regenerates — the same queries every
// state archive's tables run, over every station and every referenced
// cell in primary-key order — while its per-station and per-cell files
// are the state archives' own, copied.
func tableEntries(ctx context.Context, db *sql.DB, sc scope) []archive.Entry {
	entries := slices.Concat(conaguaTableEntries(ctx, db, sc), powerTableEntries(ctx, db, sc), combinedTableEntries(ctx, db, sc))
	slices.SortFunc(entries, func(a, b archive.Entry) int { return strings.Compare(a.Path, b.Path) })
	return entries
}
