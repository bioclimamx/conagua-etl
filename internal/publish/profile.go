package publish

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

// The profile's source tags. A tag sits on a block or a variable
// series, never on an array element; slot 12 of every month series is
// bioclima_derived by one global convention, stated once in the data
// dictionary.
const (
	sourceConaguaPublished = "conagua_published"
	sourceConaguaObserved  = "conagua_observed"
	sourceNasaPower        = "nasa_power"
	sourceBioclimaDerived  = "bioclima_derived"
)

// Profile is one station's citable profile, combined/<state>/<station_id>/
// profile.json. Its blocks, in this order, are the JSON
// object's keys; every field is written, an absent value as an explicit
// null. Numbers go through the fixed-decimal formatter, key order
// and indentation are fixed by the writer, and nothing in it depends on
// the wall clock, so two builds from the same DB and binary are
// byte-identical.
type Profile struct {
	StationID       string             `json:"station_id"`
	Identity        Identity           `json:"identity"`
	WMOCompleteness WMOCompleteness    `json:"wmo_completeness"`
	Normals         NormalsBlock       `json:"normals"`
	Extras          ExtrasBlock        `json:"extras"`
	PowerCell       *PowerCellBlock    `json:"power_cell"`
	PowerMonthly    *PowerMonthlyBlock `json:"power_monthly"`
	DailySummary    DailySummary       `json:"daily_summary"`
	Meta            MetaBlock          `json:"meta"`
}

// Identity is the station's CONAGUA catalog record: state is the code
// as stored (uppercase), state_name the official name (the code → name
// lookup), coordinates at six decimals, altitude at one.
type Identity struct {
	Name         jsonText `json:"name"`
	State        jsonText `json:"state"`
	StateName    string   `json:"state_name"`
	Municipality jsonText `json:"municipality"`
	Lat          jsonReal `json:"lat"`
	Lon          jsonReal `json:"lon"`
	AltitudeM    jsonReal `json:"altitude_m"`
	Status       jsonText `json:"status"`
	FirstYear    jsonInt  `json:"first_year"`
	LastYear     jsonInt  `json:"last_year"`
}

// WMOCompleteness is the per-period WMO-No. 1203 completeness pair,
// bioclima_derived, keyed by period over the whole normals period set: a
// period is null when both scores are NULL (no extras rows for it).
type WMOCompleteness struct {
	Source  string               `json:"source"`
	Periods map[string]*WMOScore `json:"periods"`
}

// WMOScore is one period's scores: bin, the share of the 36 (variable,
// month) cells passing the 80 % rule; cont, the mean coverage density.
type WMOScore struct {
	Bin  jsonReal `json:"bin"`
	Cont jsonReal `json:"cont"`
}

// NormalsBlock is CONAGUA's published monthly normals per period, each
// variable a 13-slot [months 1–12, annual] series; a period with no
// monthly_normals rows is null. annual_slot names slot 12's tag.
type NormalsBlock struct {
	Source     string                 `json:"source"`
	AnnualSlot string                 `json:"annual_slot"`
	Periods    map[string]*monthBlock `json:"periods"`
}

// ExtrasBlock is the monthly_normals_extras columns per period as
// 13-slot series; slot 12 is null for every column: extremes, dates,
// and counts have no annual aggregation.
type ExtrasBlock struct {
	Source  string                 `json:"source"`
	Periods map[string]*monthBlock `json:"periods"`
}

// PowerCellBlock is the station's POWER cell snap — the cell key, its
// centroid at three decimals, and the great-circle distance from the
// station at three — bioclima_derived. The whole block is null for a
// station with no station_power_cell row.
type PowerCellBlock struct {
	Source     string   `json:"source"`
	CellID     string   `json:"cell_id"`
	Lat        jsonReal `json:"lat"`
	Lon        jsonReal `json:"lon"`
	DistanceKm jsonReal `json:"distance_km"`
}

// PowerMonthlyBlock is the cell's POWER-31 monthly climatology per
// period as 13-slot series, in registry order. The whole block is null
// for a station with no cell; a period with no monthly_supplement rows
// for the cell (the two pre-1981 periods today) is null.
type PowerMonthlyBlock struct {
	Source     string                 `json:"source"`
	AnnualSlot string                 `json:"annual_slot"`
	Periods    map[string]*monthBlock `json:"periods"`
}

// DailySummary is the daily-summary block, computed at export from
// daily_observations and daily_supplement.
type DailySummary struct {
	Source   string    `json:"source"`
	Coverage Coverage  `json:"coverage"`
	Extremes Extremes  `json:"extremes"`
	DrySpell *DrySpell `json:"dry_spell"`
}

// Coverage is the extent of the two daily series: observed is the
// station's, null for a station with no daily rows; reanalysis is the
// cell's whole daily_supplement extent — not clipped to the station's
// dates — null for a station with no cell or a cell with no rows.
type Coverage struct {
	Observed   *ObservedCoverage   `json:"observed"`
	Reanalysis *ReanalysisCoverage `json:"reanalysis"`
}

// ObservedCoverage is the station's daily row extent, the count of days
// with at least one of the four observed variables non-null, and the
// non-null day count per variable.
type ObservedCoverage struct {
	FirstDate      string         `json:"first_date"`
	LastDate       string         `json:"last_date"`
	DaysWithObs    int64          `json:"days_with_obs"`
	DaysByVariable DaysByVariable `json:"days_by_variable"`
}

// DaysByVariable counts non-null days per observed variable, keyed by
// the export name.
type DaysByVariable struct {
	TmaxC    int64 `json:"tmax_c"`
	TminC    int64 `json:"tmin_c"`
	PrecipMm int64 `json:"precip_mm"`
	EvapMm   int64 `json:"evap_mm"`
}

// ReanalysisCoverage is the cell's daily_supplement extent and row
// count — the cell's series, which is what the combined/ join draws on.
type ReanalysisCoverage struct {
	FirstDate string `json:"first_date"`
	LastDate  string `json:"last_date"`
	Days      int64  `json:"days"`
}

// Extremes are the station's all-time daily records from observed
// daily only (POWER extremes are omitted on purpose — reanalysis is not
// a station record), each with its date, earliest on ties; a record is
// null when the variable has no non-null day.
type Extremes struct {
	Source             string  `json:"source"`
	RecordTmaxC        *Record `json:"record_tmax_c"`
	RecordTminC        *Record `json:"record_tmin_c"`
	RecordPrecipMm1Day *Record `json:"record_precip_mm_1day"`
}

// Record is one all-time extreme: the value at the variable's decimals
// and the date it was observed.
type Record struct {
	Value jsonReal `json:"value"`
	Date  string   `json:"date"`
}

// DrySpell is the longest run of consecutive calendar days with observed
// precip_mm == 0. A NULL precip day or a missing date row terminates a
// run (unknown is not dry); the earliest run wins a tie on length.
type DrySpell struct {
	LongestDryRunDays int64  `json:"longest_dry_run_days"`
	StartDate         string `json:"start_date"`
	EndDate           string `json:"end_date"`
}

// MetaBlock is the profile's provenance: the schema and code that
// produced it, the snapshot it was built from, the runs the shipped rows
// trace to by natural label, the license id, and the suggested
// citation. No generation time — that is stamped in manifest.json only
// so profiles stay byte-reproducible.
type MetaBlock struct {
	SchemaVersion     int       `json:"schema_version"`
	ETLGitSHA         string    `json:"etl_git_sha"`
	SnapshotDate      string    `json:"snapshot_date"`
	Runs              RunLabels `json:"runs"`
	License           string    `json:"license"`
	SuggestedCitation string    `json:"suggested_citation"`
}

// RunLabels lists the provenance runs by natural label: ingest runs by
// snapshot_date, power runs by run_label. Empty lists are [], never
// null — a run set is a list, not a value that can be absent.
type RunLabels struct {
	Ingest []string `json:"ingest"`
	Power  []string `json:"power"`
}

// ProfileMeta is what every profile's meta block carries, fixed once per
// build and shared by every station: the integrator fills it from
// schema.Version, the build's git SHA, the resolved snapshot, the runs
// loaded before the state loop, and DatasetMetadata.
type ProfileMeta struct {
	SchemaVersion int
	ETLGitSHA     string
	SnapshotDate  string
	Runs          Runs
	Dataset       Dataset
}

// normalsPeriods is the profile's period axis — conagua's normals period
// set, the one owner of that vocabulary — oldest first. Every period
// map is built from it, never from the rows, so a period with no rows is
// present as an explicit null.
var normalsPeriods = func() []string {
	periods := make([]string, 0, len(conagua.NormalsKinds))
	for _, k := range conagua.NormalsKinds {
		p, ok := conagua.PeriodForKind(k)
		if !ok {
			panic(fmt.Sprintf("publish: %s is not a normals kind", k))
		}
		periods = append(periods, p)
	}
	return periods
}()

// decimalsOf resolves a fixed-shape profile field's decimals from the
// flat-file spec that exports the same column, so the profile cannot
// disagree with the CSV on a count. Called at init only: a name the spec
// does not export as a numeric is a declaration error, impossible at
// runtime and caught before any profile is written.
func decimalsOf(spec FileSpec, name string) int {
	for _, c := range spec.Columns {
		if c.Name == name && (c.Kind == KindReal || c.Kind == KindInt) {
			return c.Decimals
		}
	}
	panic(fmt.Sprintf("publish: %s does not export numeric column %s", spec.Name, name))
}

// The decimals of the profile's fixed-shape numeric fields, each read
// from the flat file that exports the same DDL column.
var (
	stationLatDecimals = decimalsOf(Stations, "lat")
	stationLonDecimals = decimalsOf(Stations, "lon")
	altitudeDecimals   = decimalsOf(Stations, "altitude_m")
	cellLatDecimals    = decimalsOf(Cells, "lat")
	cellLonDecimals    = decimalsOf(Cells, "lon")
	distanceDecimals   = decimalsOf(StationCellMap, "distance_km")
	recordTmaxName     = ExportName("daily_observations", "tmax")
	recordTminName     = ExportName("daily_observations", "tmin")
	recordPrecName     = ExportName("daily_observations", "precip")
	recordTmaxDecimals = decimalsOf(DailyObservations, recordTmaxName)
	recordTminDecimals = decimalsOf(DailyObservations, recordTminName)
	recordPrecDecimals = decimalsOf(DailyObservations, recordPrecName)
)

// wmoColumns is one period's pair of stations columns, resolved at init
// from the period set against the Stations spec, so the profile reads
// the eight scores the flat file exports and at the same decimals.
type wmoColumns struct {
	period, bin, cont string
	decimals          int
}

var wmoByPeriod = func() []wmoColumns {
	out := make([]wmoColumns, 0, len(normalsPeriods))
	for _, p := range normalsPeriods {
		suffix := strings.ReplaceAll(p, "-", "_")
		w := wmoColumns{period: p, bin: "wmo_completeness_bin_" + suffix, cont: "wmo_completeness_cont_" + suffix}
		w.decimals = decimalsOf(Stations, w.bin)
		if d := decimalsOf(Stations, w.cont); d != w.decimals {
			panic(fmt.Sprintf("publish: %s and %s differ in decimals", w.bin, w.cont))
		}
		out = append(out, w)
	}
	return out
}()

// stationRowSQL reads the identity fields and the WMO scores of one
// station by surrogate id — the stations primary key.
var stationRowSQL = func() string {
	cols := []string{"name", "state", "municipality", "lat", "lon", "altitude_m", "status", "first_year", "last_year"}
	for _, w := range wmoByPeriod {
		cols = append(cols, w.bin, w.cont)
	}
	for i, c := range cols {
		cols[i] = "s." + c
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM stations s WHERE s.id = ?"
}()

// valueColumns is spec's non-key columns in export order — the
// variables a month block carries.
func valueColumns(spec FileSpec) []Column {
	var cols []Column
	for _, c := range spec.Columns {
		if !c.Key {
			cols = append(cols, c)
		}
	}
	return cols
}

// monthSeriesSQL selects the (period, month) key and spec's value columns
// for one owner — a station's normals or extras by surrogate id, a cell's
// monthly supplement by cell_id — walking the (owner, period, month)
// primary key in period, month order.
func monthSeriesSQL(spec FileSpec, owner string) string {
	cols := valueColumns(spec)
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = "t." + c.DB
	}
	return "SELECT t.period, t.month, " + strings.Join(parts, ", ") +
		" FROM " + spec.Table + " t WHERE t." + owner + " = ? ORDER BY t.period, t.month"
}

var (
	normalsSeriesSQL = monthSeriesSQL(MonthlyNormals, "station_id")
	extrasSeriesSQL  = monthSeriesSQL(MonthlyNormalsExtras, "station_id")
	powerSeriesSQL   = monthSeriesSQL(PowerMonthly, "cell_id")

	// The cell's centroid, by the nasa_power_grid_cells primary key.
	gridCellSQL = "SELECT c.lat, c.lon FROM nasa_power_grid_cells c WHERE c.cell_id = ?"

	// The station's daily series, walking the (station_id, date) primary
	// key so one pass in date order yields the coverage counts, the three
	// records (earliest on ties, since rows arrive date-ascending and
	// only a strict improvement replaces), and the dry-spell runs.
	dailySeriesSQL = "SELECT d.date, d.tmax, d.tmin, d.precip, d.evap" +
		" FROM daily_observations d WHERE d.station_id = ? ORDER BY d.date"

	// The cell's daily_supplement extent, one aggregate over the
	// (cell_id, date) primary key; MIN and MAX of the ISO dates are
	// bytewise and so chronological.
	reanalysisCoverageSQL = "SELECT MIN(d.date), MAX(d.date), COUNT(*) FROM daily_supplement d WHERE d.cell_id = ?"
)

// monthSeries is one variable's 13-slot positional series: slots 0–11 the
// calendar months 1–12, slot 12 the derived annual (null where the
// variable has no annual aggregation or any month is null).
// Every slot holds a rendered cell — an explicit null where the source
// has no value — never a nil.
type monthSeries [13]json.Marshaler

// monthBlock is one period's series for a spec's value columns, keyed
// by export name in the spec's column order — the profile's counterpart
// of a flat file's value block, so the JSON keys and the CSV header are
// the same list by construction and cannot drift.
type monthBlock struct {
	cols   []Column
	series []monthSeries
}

// newMonthBlock is a block with every slot an explicit null.
func newMonthBlock(cols []Column) *monthBlock {
	b := &monthBlock{cols: cols, series: make([]monthSeries, len(cols))}
	for i, c := range cols {
		for m := range b.series[i] {
			b.series[i][m] = nullCell(c)
		}
	}
	return b
}

// deriveAnnual fills slot 12 of every series the annual dispatch names
// from the stored full-precision values of slots 0–11; every other series
// keeps its null. Called once a period's rows are all in.
func (b *monthBlock) deriveAnnual() {
	for i, c := range b.cols {
		kind, ok := annualBy[c.Name]
		if !ok {
			continue
		}
		var months [12]*float64
		for m := range months {
			if r, ok := b.series[i][m].(jsonReal); ok && r.v.Valid {
				v := r.v.Float64
				months[m] = &v
			}
		}
		b.series[i][12] = realPtr(kind.fold(months), c.Decimals)
	}
}

// MarshalJSON renders {"<name>": [13 slots], …} in column order.
func (b *monthBlock) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, c := range b.cols {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, err := marshalNoEscape(c.Name)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", c.Name, err)
		}
		series, err := marshalNoEscape(b.series[i])
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", c.Name, err)
		}
		buf.Write(key)
		buf.WriteByte(':')
		buf.Write(series)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// newPeriodMap is the period axis with every period null.
func newPeriodMap() map[string]*monthBlock {
	periods := make(map[string]*monthBlock, len(normalsPeriods))
	for _, p := range normalsPeriods {
		periods[p] = nil
	}
	return periods
}

// loadMonthBlocks runs query (a monthSeriesSQL of spec) for owner and
// folds the rows into one block per period over the whole period axis:
// a period with no rows stays null, a period with rows carries every
// variable's 13-slot series with the annual derived. A period outside
// the axis or a month outside 1–12 is refused — the DDL CHECKs forbid
// both, so either is a corruption signal.
func loadMonthBlocks(ctx context.Context, db *sql.DB, spec FileSpec, query string, owner any,
) (map[string]*monthBlock, error) {
	rows, err := db.QueryContext(ctx, query, owner)
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", spec.Table, err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	cols := valueColumns(spec)
	periods := newPeriodMap()
	var (
		period string
		month  int
	)
	targets := make([]any, 0, 2+len(cols))
	targets = append(targets, &period, &month)
	for _, c := range cols {
		targets = append(targets, scanTarget(c))
	}
	for rows.Next() {
		if err := rows.Scan(targets...); err != nil {
			return nil, fmt.Errorf("%s: scan: %w", spec.Table, err)
		}
		block, known := periods[period]
		if !known {
			return nil, fmt.Errorf("%s: period %q is not a normals period", spec.Table, period)
		}
		if month < 1 || month > 12 {
			return nil, fmt.Errorf("%s: period %s: month %d is not in 1..12", spec.Table, period, month)
		}
		if block == nil {
			block = newMonthBlock(cols)
			periods[period] = block
		}
		for i, c := range cols {
			cell, err := cellOf(c, targets[2+i])
			if err != nil {
				return nil, fmt.Errorf("%s: period %s month %d: %w", spec.Table, period, month, err)
			}
			block.series[i][month-1] = cell
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: read rows: %w", spec.Table, err)
	}
	for _, block := range periods {
		if block != nil {
			block.deriveAnnual()
		}
	}
	return periods, nil
}

// loadIdentity reads the stations row: the identity block and the WMO
// scores over the period axis (a period null when both scores are NULL).
func loadIdentity(ctx context.Context, db *sql.DB, st State, id int64) (Identity, WMOCompleteness, error) {
	var (
		name, state, municipality, status sql.NullString
		lat, lon, altitude                sql.NullFloat64
		firstYear, lastYear               sql.NullInt64
	)
	scores := make([]sql.NullFloat64, 2*len(wmoByPeriod))
	targets := []any{&name, &state, &municipality, &lat, &lon, &altitude, &status, &firstYear, &lastYear}
	for i := range scores {
		targets = append(targets, &scores[i])
	}
	err := db.QueryRowContext(ctx, stationRowSQL, id).Scan(targets...)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Identity{}, WMOCompleteness{}, fmt.Errorf("stations row %d not found", id)
	case err != nil:
		return Identity{}, WMOCompleteness{}, fmt.Errorf("query stations: %w", err)
	}
	for _, r := range []struct {
		name string
		v    sql.NullFloat64
	}{{"lat", lat}, {"lon", lon}, {"altitude_m", altitude}} {
		if err := checkFiniteNull(r.name, r.v); err != nil {
			return Identity{}, WMOCompleteness{}, err
		}
	}
	for i, w := range wmoByPeriod {
		if err := checkFiniteNull(w.bin, scores[2*i]); err != nil {
			return Identity{}, WMOCompleteness{}, err
		}
		if err := checkFiniteNull(w.cont, scores[2*i+1]); err != nil {
			return Identity{}, WMOCompleteness{}, err
		}
	}
	identity := Identity{
		Name:         jsonText{v: name},
		State:        jsonText{v: state},
		StateName:    st.Name,
		Municipality: jsonText{v: municipality},
		Lat:          realOf(lat, stationLatDecimals),
		Lon:          realOf(lon, stationLonDecimals),
		AltitudeM:    realOf(altitude, altitudeDecimals),
		Status:       jsonText{v: status},
		FirstYear:    jsonInt{v: firstYear},
		LastYear:     jsonInt{v: lastYear},
	}
	wmo := WMOCompleteness{Source: sourceBioclimaDerived, Periods: make(map[string]*WMOScore, len(wmoByPeriod))}
	for i, w := range wmoByPeriod {
		bin, cont := scores[2*i], scores[2*i+1]
		if !bin.Valid && !cont.Valid {
			wmo.Periods[w.period] = nil
			continue
		}
		wmo.Periods[w.period] = &WMOScore{Bin: realOf(bin, w.decimals), Cont: realOf(cont, w.decimals)}
	}
	return identity, wmo, nil
}

// loadPowerCell resolves the station's cell block from its
// station_power_cell reference and the cell's centroid. A reference to
// a cell with no nasa_power_grid_cells row is a broken FK — SQLite does
// not enforce them by default — and is refused as the integrity signal
// it is.
func loadPowerCell(ctx context.Context, db *sql.DB, s stationRef) (*PowerCellBlock, error) {
	if !s.cellID.Valid {
		return nil, nil
	}
	var lat, lon sql.NullFloat64
	err := db.QueryRowContext(ctx, gridCellSQL, s.cellID.String).Scan(&lat, &lon)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("cell %q has no nasa_power_grid_cells row", s.cellID.String)
	case err != nil:
		return nil, fmt.Errorf("query nasa_power_grid_cells: %w", err)
	}
	for _, r := range []struct {
		name string
		v    sql.NullFloat64
	}{{"lat", lat}, {"lon", lon}, {"distance_km", s.distanceKm}} {
		if err := checkFiniteNull(r.name, r.v); err != nil {
			return nil, err
		}
	}
	return &PowerCellBlock{
		Source:     sourceBioclimaDerived,
		CellID:     s.cellID.String,
		Lat:        realOf(lat, cellLatDecimals),
		Lon:        realOf(lon, cellLonDecimals),
		DistanceKm: realOf(s.distanceKm, distanceDecimals),
	}, nil
}

// record tracks one all-time extreme through the date-ordered daily
// scan: a strictly better value replaces, so the earliest date wins a tie.
// name is the exported column the record is rendered under, for the
// finite check at block time.
type record struct {
	name     string
	value    float64
	date     string
	found    bool
	better   func(candidate, best float64) bool
	decimals int
}

func (r *record) offer(v sql.NullFloat64, date string) {
	if !v.Valid {
		return
	}
	if !r.found || r.better(v.Float64, r.value) {
		r.value, r.date, r.found = v.Float64, date, true
	}
}

// block renders the record, nil when no non-null day was offered. A
// non-finite record is refused by name: ±Inf always wins its comparison
// and a leading NaN is never replaced, so the check on the winner covers
// every stored value that could reach the profile.
func (r *record) block() (*Record, error) {
	if !r.found {
		return nil, nil
	}
	if err := checkFinite(r.name, r.value); err != nil {
		return nil, err
	}
	return &Record{Value: realPtr(&r.value, r.decimals), Date: r.date}, nil
}

func higher(candidate, best float64) bool { return candidate > best }
func lower(candidate, best float64) bool  { return candidate < best }

// countValid counts a non-null observation into its variable's day
// counter.
func countValid(v sql.NullFloat64, days *int64) {
	if v.Valid {
		*days++
	}
}

// checkFiniteNull is checkFinite over a scanned nullable REAL; NULL is
// an honest gap and passes.
func checkFiniteNull(name string, v sql.NullFloat64) error {
	if !v.Valid {
		return nil
	}
	return checkFinite(name, v.Float64)
}

// loadObservedSummary makes one pass over the station's daily rows in
// date order and returns the observed coverage (nil for a station with
// no daily rows), the three records, and the longest dry spell (nil when
// no dry day exists). A date that is not YYYY-MM-DD is refused: ingest
// pins the shape, and the consecutive-day test depends on it.
func loadObservedSummary(ctx context.Context, db *sql.DB, id int64,
) (coverage *ObservedCoverage, extremes Extremes, dry *DrySpell, err error) {
	extremes = Extremes{Source: sourceConaguaObserved}
	rows, err := db.QueryContext(ctx, dailySeriesSQL, id)
	if err != nil {
		return nil, extremes, nil, fmt.Errorf("query daily_observations: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	tmaxRecord := record{name: recordTmaxName, better: higher, decimals: recordTmaxDecimals}
	tminRecord := record{name: recordTminName, better: lower, decimals: recordTminDecimals}
	precRecord := record{name: recordPrecName, better: higher, decimals: recordPrecDecimals}
	var (
		cov                      ObservedCoverage
		n                        int64
		date                     string
		tmax, tmin, precip, evap sql.NullFloat64
		best, cur                DrySpell
		last                     time.Time
		haveLast                 bool
	)
	for rows.Next() {
		if err := rows.Scan(&date, &tmax, &tmin, &precip, &evap); err != nil {
			return nil, extremes, nil, fmt.Errorf("daily_observations: scan row %d: %w", n+1, err)
		}
		day, err := time.Parse(snapshotDateLayout, date)
		if err != nil {
			return nil, extremes, nil, fmt.Errorf("daily_observations: date %q is not YYYY-MM-DD: %w", date, err)
		}
		if n == 0 {
			cov.FirstDate = date
		}
		cov.LastDate = date
		n++
		if tmax.Valid || tmin.Valid || precip.Valid || evap.Valid {
			cov.DaysWithObs++
		}
		countValid(tmax, &cov.DaysByVariable.TmaxC)
		countValid(tmin, &cov.DaysByVariable.TminC)
		countValid(precip, &cov.DaysByVariable.PrecipMm)
		countValid(evap, &cov.DaysByVariable.EvapMm)
		tmaxRecord.offer(tmax, date)
		tminRecord.offer(tmin, date)
		precRecord.offer(precip, date)

		// Dry = an observed zero. A NULL day is not dry (unknown is not
		// dry), and a calendar gap breaks a run as a wet day does; the
		// earliest run keeps a tie because only a strictly longer run
		// replaces the best.
		isDry := precip.Valid && precip.Float64 == 0
		consecutive := haveLast && day.Equal(last.AddDate(0, 0, 1))
		switch {
		case isDry && cur.LongestDryRunDays > 0 && consecutive:
			cur.LongestDryRunDays++
		case isDry:
			cur = DrySpell{LongestDryRunDays: 1, StartDate: date}
		default:
			cur.LongestDryRunDays = 0
		}
		if cur.LongestDryRunDays > best.LongestDryRunDays {
			cur.EndDate = date
			best = cur
		}
		last, haveLast = day, true
	}
	if err := rows.Err(); err != nil {
		return nil, extremes, nil, fmt.Errorf("daily_observations: read rows: %w", err)
	}
	for _, r := range []struct {
		rec *record
		dst **Record
	}{{&tmaxRecord, &extremes.RecordTmaxC}, {&tminRecord, &extremes.RecordTminC}, {&precRecord, &extremes.RecordPrecipMm1Day}} {
		if *r.dst, err = r.rec.block(); err != nil {
			return nil, extremes, nil, fmt.Errorf("daily_observations: %w", err)
		}
	}
	if best.LongestDryRunDays > 0 {
		dry = &best
	}
	if n == 0 {
		return nil, extremes, dry, nil
	}
	return &cov, extremes, dry, nil
}

// loadReanalysisCoverage returns the cell's daily_supplement extent —
// the cell's whole series, which is what the combined/ join draws on —
// nil for a cell with no daily rows.
func loadReanalysisCoverage(ctx context.Context, db *sql.DB, cellID string) (*ReanalysisCoverage, error) {
	var (
		first, last sql.NullString
		days        int64
	)
	if err := db.QueryRowContext(ctx, reanalysisCoverageSQL, cellID).Scan(&first, &last, &days); err != nil {
		return nil, fmt.Errorf("query daily_supplement: %w", err)
	}
	if days == 0 || !first.Valid || !last.Valid {
		return nil, nil
	}
	return &ReanalysisCoverage{FirstDate: first.String, LastDate: last.String, Days: days}, nil
}

// cellProfile is what a profile carries per cell rather than per
// station: the cell's monthly climatology as period blocks and its
// daily extent. Both are read-only once built, so stations on the same
// cell share one instance.
type cellProfile struct {
	monthly    map[string]*monthBlock
	reanalysis *ReanalysisCoverage
}

// cellCache memoizes cellProfile per cell within one archive's build.
// A state's stations sit on far fewer cells than there are stations,
// and the cell's queries — a monthly climatology read and a MIN/MAX/
// COUNT over its whole daily series — are the profile path's largest
// cost, so they run once per cell per archive instead of once per
// station. The cache is per archive, so byte reproducibility holds as
// before (the DB is read-only for the run's duration and the value is
// the same whichever station reads it first), and entries are written
// sequentially, so a plain map is safe. A failed load is not cached:
// the archive fails anyway, and a retry must see the DB again.
type cellCache struct {
	cells map[string]*cellProfile
}

func newCellCache() *cellCache {
	return &cellCache{cells: map[string]*cellProfile{}}
}

// get returns the cell's memoized profile, loading it on first use.
func (c *cellCache) get(ctx context.Context, db *sql.DB, cellID string) (*cellProfile, error) {
	if p, ok := c.cells[cellID]; ok {
		return p, nil
	}
	monthly, err := loadMonthBlocks(ctx, db, PowerMonthly, powerSeriesSQL, cellID)
	if err != nil {
		return nil, fmt.Errorf("power_monthly: %w", err)
	}
	reanalysis, err := loadReanalysisCoverage(ctx, db, cellID)
	if err != nil {
		return nil, fmt.Errorf("daily_summary: %w", err)
	}
	p := &cellProfile{monthly: monthly, reanalysis: reanalysis}
	c.cells[cellID] = p
	return p, nil
}

// metaBlock renders the shared build metadata as the profile carries
// it: runs by natural label, in the order LoadRuns sorted them.
func metaBlock(meta ProfileMeta) MetaBlock {
	labels := RunLabels{
		Ingest: make([]string, 0, len(meta.Runs.Ingest)),
		Power:  make([]string, 0, len(meta.Runs.Power)),
	}
	for _, r := range meta.Runs.Ingest {
		labels.Ingest = append(labels.Ingest, r.SnapshotDate)
	}
	for _, r := range meta.Runs.Power {
		labels.Power = append(labels.Power, r.RunLabel)
	}
	return MetaBlock{
		SchemaVersion:     meta.SchemaVersion,
		ETLGitSHA:         meta.ETLGitSHA,
		SnapshotDate:      meta.SnapshotDate,
		Runs:              labels,
		License:           meta.Dataset.License,
		SuggestedCitation: meta.Dataset.SuggestedCitation,
	}
}

// buildProfile assembles one station's profile from the DB — the
// stations row, its normals and extras by surrogate id, its cell, one
// pass over its daily rows, and through cells the cell's monthly
// climatology and daily extent — every query walking a primary key or
// an indexed path. Errors are not prefixed with the station: the caller
// (the entry path, in the archive) names it once.
func buildProfile(ctx context.Context, db *sql.DB, st State, s stationRef, meta ProfileMeta, cells *cellCache,
) (*Profile, error) {
	identity, wmo, err := loadIdentity(ctx, db, st, s.id)
	if err != nil {
		return nil, fmt.Errorf("identity: %w", err)
	}
	normals, err := loadMonthBlocks(ctx, db, MonthlyNormals, normalsSeriesSQL, s.id)
	if err != nil {
		return nil, fmt.Errorf("normals: %w", err)
	}
	extras, err := loadMonthBlocks(ctx, db, MonthlyNormalsExtras, extrasSeriesSQL, s.id)
	if err != nil {
		return nil, fmt.Errorf("extras: %w", err)
	}
	cell, err := loadPowerCell(ctx, db, s)
	if err != nil {
		return nil, fmt.Errorf("power_cell: %w", err)
	}
	var (
		powerMonthly *PowerMonthlyBlock
		reanalysis   *ReanalysisCoverage
	)
	if cell != nil {
		cp, err := cells.get(ctx, db, cell.CellID)
		if err != nil {
			return nil, err
		}
		powerMonthly = &PowerMonthlyBlock{Source: sourceNasaPower, AnnualSlot: sourceBioclimaDerived, Periods: cp.monthly}
		reanalysis = cp.reanalysis
	}
	observed, extremes, dry, err := loadObservedSummary(ctx, db, s.id)
	if err != nil {
		return nil, fmt.Errorf("daily_summary: %w", err)
	}
	return &Profile{
		StationID:       s.externalID,
		Identity:        identity,
		WMOCompleteness: wmo,
		Normals:         NormalsBlock{Source: sourceConaguaPublished, AnnualSlot: sourceBioclimaDerived, Periods: normals},
		Extras:          ExtrasBlock{Source: sourceConaguaPublished, Periods: extras},
		PowerCell:       cell,
		PowerMonthly:    powerMonthly,
		DailySummary: DailySummary{
			Source:   sourceBioclimaDerived,
			Coverage: Coverage{Observed: observed, Reanalysis: reanalysis},
			Extremes: extremes,
			DrySpell: dry,
		},
		Meta: metaBlock(meta),
	}, nil
}

// encodeProfile writes p as two-space-indented JSON with a trailing
// newline and HTML escaping off — the fixed layout byte-reproducibility
// rests on. Struct fields render in declaration order and period maps in
// sorted key order, so the bytes depend on the data alone.
func encodeProfile(w io.Writer, p *Profile) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(p); err != nil {
		return fmt.Errorf("encode profile: %w", err)
	}
	return nil
}

// profilePath is the station's profile inside the JSON archive:
// combined/<slug>/<station_id>/profile.json (the profile is the
// combined product), sibling of daily.json.
func profilePath(st State, externalID string) string {
	return "combined/" + st.Slug + "/" + externalID + "/profile.json"
}

// profileEntry is one station's profile.json entry. The queries run at
// write time, so the single-connection DB is touched by one entry at a
// time; the entry is not a unit of its own — the per-station unit is the
// profile + daily.json pair, which the archive assembler counts. cells
// is the archive's per-cell memo, shared by every station of the list.
func profileEntry(ctx context.Context, db *sql.DB, st State, s stationRef, meta ProfileMeta, cells *cellCache,
) archive.Entry {
	return archive.Entry{
		Path: profilePath(st, s.externalID),
		Write: func(w io.Writer) error {
			p, err := buildProfile(ctx, db, st, s, meta, cells)
			if err != nil {
				return err
			}
			return encodeProfile(w, p)
		},
	}
}

// ProfileEntries returns the profile.json entries of one state,
// Path-sorted, one per station of the state's station list (the same
// list the tabular folders are built from), each building its profile
// from the DB when written; the per-cell reads are memoized across the
// list.
func ProfileEntries(ctx context.Context, db *sql.DB, st State, meta ProfileMeta) ([]archive.Entry, error) {
	stations, err := loadStateStations(ctx, db, st)
	if err != nil {
		return nil, err
	}
	cells := newCellCache()
	entries := make([]archive.Entry, 0, len(stations))
	for _, s := range stations {
		entries = append(entries, profileEntry(ctx, db, st, s, meta, cells))
	}
	slices.SortFunc(entries, func(a, b archive.Entry) int { return strings.Compare(a.Path, b.Path) })
	return entries, nil
}
