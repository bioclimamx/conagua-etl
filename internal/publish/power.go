package publish

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/power"
)

// powerValueDefs declares the POWER-31 value block: the 31 supplement
// columns in power.Registry order, which the power suite holds equal to
// both supplement tables' DDL order. Deriving the list from the registry
// that owns parameter identity and order means a new POWER variable
// reaches the flat files without a transcription here; newFileSpec refuses at init any registry column
// schema.Precision does not pin.
func powerValueDefs() []colDef {
	defs := make([]colDef, len(power.Registry))
	for i, p := range power.Registry {
		defs[i] = colDef{db: p.Column, kind: KindReal}
	}
	return defs
}

// The nasa_power/ folder's file specs: the cells a state's stations
// reference, the station → cell map, and the two reanalysis tables
// keyed by cell. The folder and files are named for what they hold —
// reanalysis — never after CONAGUA's observed tables.
var (
	// Cells is nasa_power/cells: one row per referenced grid cell, sorted
	// by cell_id; grid_resolution is a documented constant and is dropped.
	Cells = newFileSpec("nasa_power/cells", "nasa_power_grid_cells", []colDef{
		{db: "cell_id", kind: KindText, key: true},
		{db: "lat", kind: KindReal},
		{db: "lon", kind: KindReal},
	})

	// StationCellMap is nasa_power/station_cell_map: the join a reader
	// needs to attach nasa_power/ rows to a station. It mirrors
	// station_power_cell — one row per station that has a cell, never a
	// padded row for one that has none — sorted by station_id.
	StationCellMap = newFileSpec("nasa_power/station_cell_map", "station_power_cell", []colDef{
		{db: "station_id", kind: KindText, key: true},
		{db: "cell_id", kind: KindText},
		{db: "distance_km", kind: KindReal},
	})

	// PowerMonthly is nasa_power/monthly: POWER's monthly climatology per
	// cell, sorted by (cell_id, period, month); power_run_id is FK
	// plumbing and is dropped.
	PowerMonthly = newFileSpec("nasa_power/monthly", "monthly_supplement", append([]colDef{
		{db: "cell_id", kind: KindText, key: true},
		{db: "period", kind: KindPeriod, key: true},
		{db: "month", kind: KindInt, key: true},
	}, powerValueDefs()...))

	// PowerDaily is nasa_power/daily: POWER's daily series, one file per
	// referenced cell, sorted by date; power_run_id dropped as above.
	PowerDaily = newFileSpec("nasa_power/daily", "daily_supplement", append([]colDef{
		{db: "cell_id", kind: KindText, key: true},
		{db: "date", kind: KindDate, key: true},
	}, powerValueDefs()...))
)

// The nasa_power/ folder's whole-scope queries, each the one definition
// of its table's row set over the scope's cells (cellSetSQL): a state
// archive binds them at the state's scope, the national archive at
// nationalScope.
func cellsSQL(sc scope) string {
	return "SELECT " + selectList(Cells, func(c Column) string { return "c." + c.DB }) +
		" FROM nasa_power_grid_cells c WHERE c.cell_id IN (" + cellSetSQL(sc) + ") ORDER BY c.cell_id"
}

// stationCellMapSQL resolves the surrogate station_id to external_id
// through the join, as childTableSQL does, and sorts by the exported
// key.
func stationCellMapSQL(sc scope) string {
	return "SELECT " + selectList(StationCellMap, func(c Column) string {
		if c.DB == "station_id" {
			return "s.external_id"
		}
		return "m." + c.DB
	}) + " FROM station_power_cell m JOIN stations s ON s.id = m.station_id " +
		sc.predicate() + " ORDER BY s.external_id"
}

// powerMonthlySQL selects the rows of the scope's cells: the IN set is
// materialized once and each cell walks the (cell_id, period, month)
// primary key.
func powerMonthlySQL(sc scope) string {
	return "SELECT " + selectList(PowerMonthly, func(c Column) string { return "p." + c.DB }) +
		" FROM monthly_supplement p WHERE p.cell_id IN (" + cellSetSQL(sc) + ")" +
		" ORDER BY p.cell_id, p.period, p.month"
}

// The per-cell daily query walks the (cell_id, date) primary key —
// never an expression filter, which would defeat the index and turn a
// national export into a scan.
var powerDailySQL = "SELECT " + selectList(PowerDaily, func(c Column) string { return "d." + c.DB }) +
	" FROM daily_supplement d WHERE d.cell_id = ? ORDER BY d.date"

// checkCellID refuses a cell_id outside the alphabet power.CellID emits
// — digits, '.', '_', and the hemisphere letters; the guard admits every
// ASCII letter rather than transcribe the four. The id becomes the daily
// file's path segment, and any other byte is an integrity signal to
// abort on, not a naming case to accommodate: a separator or a parent
// reference would let a corrupt key escape the archive layout (a
// zip-slip hazard for whoever extracts it), and a character Windows
// reserves in file names would yield an archive that fails to extract
// there.
func checkCellID(id string) error {
	allowed := func(r rune) bool {
		return r >= '0' && r <= '9' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r == '.' || r == '_'
	}
	if id == "" || strings.ContainsFunc(id, func(r rune) bool { return !allowed(r) }) {
		return fmt.Errorf("cell_id %q is not a POWER cell key", id)
	}
	return nil
}

// cellDailyPath is the per-cell daily file: CSV sharding is per cell,
// with no state shard — a cell is not a state's.
func cellDailyPath(cellID string) string {
	return PowerDaily.Name + "/daily-" + cellID + ".csv"
}

// PowerEntries returns the nasa_power/ CSV entries for one state,
// Path-sorted, each streaming its rows from the DB when written:
// cells.csv (the cells the state's stations reference — a cell shared by
// several of them once, a cell only another state's stations reference
// not at all), station_cell_map.csv (the state's stations that have a
// cell), monthly.csv (those cells' rows), and daily/daily-<cell_id>.csv
// per referenced cell — a cell with no daily rows still gets its
// header-only file, so the daily file set is one-to-one with cells.csv.
// A state whose stations reference no cell yields the three header-only
// table files and no daily file. The cell list is read now and its ids
// are checked as path segments; every row set is queried at write time,
// so the single-connection DB is touched by one entry at a time. onUnit
// (optional) fires once per cell, in write order, after its daily entry
// is written. A daily entry's error is not prefixed with the cell: the
// entry path the zip writer adds already carries the id.
func PowerEntries(ctx context.Context, db *sql.DB, st State, onUnit UnitFunc) ([]archive.Entry, error) {
	cells, err := loadStateCells(ctx, db, st)
	if err != nil {
		return nil, err
	}
	entries, _ := powerEntries(ctx, db, st, cells, onUnit)
	return entries, nil
}

// powerEntries builds PowerEntries' list from an already-loaded cell
// list and reports how many of the entries are units.
func powerEntries(ctx context.Context, db *sql.DB, st State, cells []string, onUnit UnitFunc,
) (entries []archive.Entry, units int) {
	u := unitList{onUnit: onUnit}
	for _, id := range cells {
		u.add(cellDailyPath(id), func(w io.Writer) error {
			return writeQuery(ctx, w, db, PowerDaily, powerDailySQL, id)
		})
	}
	entries = append(powerTableEntries(ctx, db, stateScope(st)), u.entries...)
	slices.SortFunc(entries, func(a, b archive.Entry) int { return strings.Compare(a.Path, b.Path) })
	return entries, len(u.entries)
}

// powerTableEntries returns the nasa_power/ folder's three whole-scope
// tables at sc: cells.csv, station_cell_map.csv, and monthly.csv.
func powerTableEntries(ctx context.Context, db *sql.DB, sc scope) []archive.Entry {
	return []archive.Entry{
		tableEntry(ctx, db, sc, Cells, cellsSQL(sc)),
		tableEntry(ctx, db, sc, StationCellMap, stationCellMapSQL(sc)),
		tableEntry(ctx, db, sc, PowerMonthly, powerMonthlySQL(sc)),
	}
}
