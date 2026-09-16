package publish

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// ComposedSpec is a FileSpec whose columns are read from more than one
// DDL table — the combined/ files, a CONAGUA spine LEFT-joined to its
// station's POWER cell. FileSpec.Table is the spine table, whose rows
// are the file's row set; Sources maps each exported column name to the
// DDL table its value is read from — the
// per-column counterpart of FileSpec.Table that the composed select
// lists and the lockstep specs need, since a Column carries no table of
// its own.
type ComposedSpec struct {
	FileSpec
	Sources map[string]string
}

// Source reports the DDL table c's value is read from.
func (s ComposedSpec) Source(c Column) string {
	return s.Sources[c.Name]
}

// sourceBlock is one contiguous run of a composed file's columns, all
// read from the same DDL table; a composed file is its blocks in export
// order (keys → join context → spine values → POWER-31).
type sourceBlock struct {
	table string
	defs  []colDef
}

// sourceBlockOf re-declares the key (key=true) or value (key=false)
// columns of a single-table spec as a block, so a composed file's spine
// columns are the base file's columns by construction and cannot drift
// from them in name, kind, or decimals.
func sourceBlockOf(spec FileSpec, key bool) sourceBlock {
	b := sourceBlock{table: spec.Table}
	for _, c := range spec.Columns {
		if c.Key == key {
			b.defs = append(b.defs, colDef{db: c.DB, kind: c.Kind, key: c.Key})
		}
	}
	return b
}

// joinContextBlock is the (cell_id, distance_km) pair from
// station_power_cell that ties a spine row to its POWER cell. Both are
// NULL for a station with no cell — the join is LEFT at both hops.
var joinContextBlock = sourceBlock{table: "station_power_cell", defs: []colDef{
	{db: "cell_id", kind: KindText},
	{db: "distance_km", kind: KindReal},
}}

// powerSourceBlock is POWER-31 against one supplement table — the same
// registry-derived block the nasa_power/ specs carry, never listed by
// hand.
func powerSourceBlock(table string) sourceBlock {
	return sourceBlock{table: table, defs: powerValueDefs()}
}

// newComposedSpec assembles a composed file from its blocks in export
// order. Each block goes through newFileSpec, so every column carries
// the same init-time declaration checks and reads its export name and
// decimals from the same two sources as a single-table file. Two blocks
// exporting the same name is a declaration error — the header would
// carry a duplicate — and is caught at init like the other checks.
func newComposedSpec(name, spine string, blocks []sourceBlock) ComposedSpec {
	spec := ComposedSpec{FileSpec: FileSpec{Name: name, Table: spine}, Sources: map[string]string{}}
	for _, b := range blocks {
		for _, c := range newFileSpec(name, b.table, b.defs).Columns {
			if _, dup := spec.Sources[c.Name]; dup {
				panic(fmt.Sprintf("publish: %s exports %s twice", name, c.Name))
			}
			spec.Sources[c.Name] = b.table
			spec.Columns = append(spec.Columns, c)
		}
	}
	return spec
}

// The combined/ folder's file specs. "Combined" is
// a CONAGUA-spine record augmented with its station's POWER-cell
// reanalysis on matching keys — a LEFT join from the CONAGUA side, never
// a temporal union: every spine row is kept, and the join context and
// POWER-31 are NULL where the station has no cell or the cell has no
// POWER row for the key. The blocks run keys → join context → spine
// values → POWER-31.
var (
	// CombinedMonthly is combined/combined_monthly: every monthly_normals
	// row of the state's stations, all four periods, joined to the
	// cell's monthly_supplement row on (period, month); POWER-31 is NULL
	// for the two periods POWER monthly was never pulled for. Sorted by
	// (station_id, period, month).
	CombinedMonthly = newComposedSpec("combined/combined_monthly", MonthlyNormals.Table, []sourceBlock{
		sourceBlockOf(MonthlyNormals, true),
		joinContextBlock,
		sourceBlockOf(MonthlyNormals, false),
		powerSourceBlock("monthly_supplement"),
	})

	// CombinedDaily is combined/combined_daily: one file per station
	// under the state shard, every daily_observations row of the station
	// joined to the cell's daily_supplement row on date; POWER-31 is NULL
	// before 1981-01-01, where POWER daily begins. Sorted by date.
	CombinedDaily = newComposedSpec("combined/combined_daily", DailyObservations.Table, []sourceBlock{
		sourceBlockOf(DailyObservations, true),
		joinContextBlock,
		sourceBlockOf(DailyObservations, false),
		powerSourceBlock("daily_supplement"),
	})
)

// combinedMonthlySQL is the combined/ folder's whole-scope query, the
// one definition of combined_monthly's row set: a state archive binds
// it at the state's scope, the national archive at nationalScope. It
// follows childTableSQL's shape — the surrogate station_id resolves to
// external_id through stations, rows come back in exported-natural-key
// order — with two LEFT hops added: the station's cell, then
// that cell's supplement row on the spine's own (period, month).
func combinedMonthlySQL(sc scope) string {
	return "SELECT " + selectList(CombinedMonthly.FileSpec, func(c Column) string {
		switch {
		case c.DB == "station_id":
			return "s.external_id"
		case CombinedMonthly.Source(c) == "station_power_cell":
			return "spc." + c.DB
		case CombinedMonthly.Source(c) == "monthly_supplement":
			return "ms." + c.DB
		default:
			return "n." + c.DB
		}
	}) + " FROM monthly_normals n JOIN stations s ON s.id = n.station_id" +
		" LEFT JOIN station_power_cell spc ON spc.station_id = n.station_id" +
		" LEFT JOIN monthly_supplement ms ON ms.cell_id = spc.cell_id AND ms.period = n.period AND ms.month = n.month " +
		sc.predicate() + " ORDER BY s.external_id, n.period, n.month"
}

// The per-station daily query is combinedDailyFromSQL's spine under
// the CSV's select list. The station's external_id, cell_id, and
// distance_km are bound as constants: the file already belongs to one
// station, so no per-row join is needed, and for a station with no cell
// the bound NULL makes the supplement join match nothing — the row
// keeps its observed values with the join context and POWER-31 empty.
var combinedDailySQL = "SELECT " + selectList(CombinedDaily.FileSpec, func(c Column) string {
	switch {
	case c.DB == "station_id", CombinedDaily.Source(c) == "station_power_cell":
		return "?"
	case CombinedDaily.Source(c) == "daily_supplement":
		return "ds." + c.DB
	default:
		return "d." + c.DB
	}
}) + combinedDailyFromSQL

// combinedDailyArgs binds combinedDailySQL for one station, in the
// order its placeholders occur: the three select-list constants
// (station_id, cell_id, distance_km), the supplement join's cell, then
// the spine's station. The one binding the CSV shard and the Parquet
// twin share, so the placeholder order is a contract kept in one place.
func combinedDailyArgs(s stationRef) []any {
	return []any{s.externalID, s.cellID, s.distanceKm, s.cellID, s.id}
}

// combinedDailyFromSQL is the per-station combined daily row set from
// its FROM clause on — the daily_observations spine (d), the LEFT join
// to the cell's daily_supplement row on date (ds), the station filter,
// and the date sort — shared by combined_daily.csv and daily.json, so
// the two products are one row set by construction (one product, two
// formats). It walks the (station_id, date) primary key of the spine
// and probes the (cell_id, date) primary key of the supplement — never
// an expression filter, which would defeat both indexes and turn a
// national export into a scan. Its placeholders, in order: the cell,
// then the station.
const combinedDailyFromSQL = " FROM daily_observations d" +
	" LEFT JOIN daily_supplement ds ON ds.cell_id = ? AND ds.date = d.date" +
	" WHERE d.station_id = ? ORDER BY d.date"

// combinedDailyPath is the per-station combined daily file under the
// state shard, the combined/ twin of dailyPath.
func combinedDailyPath(st State, externalID string) string {
	return CombinedDaily.Name + "/" + st.Slug + "/daily-" + externalID + ".csv"
}

// CombinedEntries returns the combined/ CSV entries for one state,
// Path-sorted, each streaming its rows from the DB when written:
// combined_monthly.csv (one file, that state's normals rows) and
// combined_daily/<slug>/daily-<station_id>.csv per station — a station
// with no daily rows still gets its header-only file, so the archive's
// combined daily file set is one-to-one with its stations file, as the
// conagua/ one is: both are built from the same station list. That list,
// with each station's cell and distance, is read now; every row set is
// queried at write time, so the single-connection DB is touched by one
// entry at a time. onUnit (optional) fires once per station, in write
// order, after its daily entry is written. A daily entry's error is not
// prefixed with the station: the entry path the zip writer adds already
// carries the id.
func CombinedEntries(ctx context.Context, db *sql.DB, st State, onUnit UnitFunc) ([]archive.Entry, error) {
	stations, err := loadStateStations(ctx, db, st)
	if err != nil {
		return nil, err
	}
	entries, _ := combinedEntries(ctx, db, st, stations, onUnit)
	return entries, nil
}

// combinedEntries builds CombinedEntries' list from an already-loaded
// station list and reports how many of the entries are units.
func combinedEntries(ctx context.Context, db *sql.DB, st State, stations []stationRef, onUnit UnitFunc,
) (entries []archive.Entry, units int) {
	u := unitList{onUnit: onUnit}
	for _, s := range stations {
		u.add(combinedDailyPath(st, s.externalID), func(w io.Writer) error {
			return writeQuery(ctx, w, db, CombinedDaily.FileSpec, combinedDailySQL, combinedDailyArgs(s)...)
		})
	}
	entries = append(combinedTableEntries(ctx, db, stateScope(st)), u.entries...)
	slices.SortFunc(entries, func(a, b archive.Entry) int { return strings.Compare(a.Path, b.Path) })
	return entries, len(u.entries)
}

// combinedTableEntries returns the combined/ folder's one whole-scope
// table at sc: combined_monthly.csv.
func combinedTableEntries(ctx context.Context, db *sql.DB, sc scope) []archive.Entry {
	return []archive.Entry{tableEntry(ctx, db, sc, CombinedMonthly.FileSpec, combinedMonthlySQL(sc))}
}
