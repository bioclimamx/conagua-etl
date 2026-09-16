package publish

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// The data dictionary's two files: the same model rendered for a
// reader and for a tool.
const (
	DictionaryMarkdownName = "DATA-DICTIONARY.md"
	DictionaryJSONName     = "DATA-DICTIONARY.json"
)

// Dictionary is the data dictionary's model — DATA-DICTIONARY.md and
// DATA-DICTIONARY.json render it. It is generated, never written by
// hand: every exported file comes from the FileSpecs the archives are
// built from, every column's decimals from schema.Precision, every
// description from schema.sql's trailing column comment where the
// column carries one (a bare format note excepted, and the four
// monthly_supplement columns whose meaning changes with the grain —
// describeColumn) and from fallbackDescriptions otherwise, the POWER
// rows from
// power.Registry, the profile shape from the Profile struct's JSON tags,
// and the two path placeholders from the path functions the archives
// use — so the dictionary cannot drift from the code that writes the
// files. Nothing in it depends on the wall clock; two builds of the same
// database and binary render the same bytes.
type Dictionary struct {
	Dataset       Dataset               `json:"dataset"`
	SchemaVersion int                   `json:"schema_version"`
	SnapshotDate  string                `json:"snapshot_date"`
	ETLGitSHA     string                `json:"etl_git_sha"`
	Tags          []DictionaryTag       `json:"source_tags"`
	Units         []DictionaryUnit      `json:"units"`
	Files         []DictionaryFile      `json:"files"`
	Power         []PowerParameter      `json:"power_parameters"`
	Constants     []DictionaryConstant  `json:"documented_constants"`
	Dropped       []DictionaryDropped   `json:"dropped_columns"`
	Profile       DictionaryProfile     `json:"profile_json"`
	DailyJSON     DictionaryDailyJSON   `json:"daily_json"`
	Annual        DictionaryAnnual      `json:"annual_slot"`
	Format        []DictionaryRule      `json:"format_contract"`
	Precision     []DictionaryPrecision `json:"precision"`
	SQLite        DictionarySQLite      `json:"sqlite"`
}

// DictionaryTag is one provenance tag and what it means.
type DictionaryTag struct {
	Tag     string `json:"tag"`
	Meaning string `json:"meaning"`
}

// DictionaryUnit is one row of the unit map: a column-name suffix (or a
// whole column name) and the unit it pins.
type DictionaryUnit struct {
	Pattern string `json:"pattern"`
	Unit    string `json:"unit"`
}

// DictionaryFile is one logical exported file: its spine table, row set,
// sort key, grain, the in-archive path per format with the archives that
// carry it, notes, and its columns in export order.
type DictionaryFile struct {
	Name    string             `json:"name"`
	Table   string             `json:"table"`
	RowSet  string             `json:"row_set"`
	SortKey []string           `json:"sort_key"`
	Grain   string             `json:"grain"`
	Paths   []DictionaryPath   `json:"paths"`
	Notes   []string           `json:"notes"`
	Columns []DictionaryColumn `json:"columns"`
}

// DictionaryPath is one format's in-archive path for a file, with the
// top-level archives that carry it. Placeholders: <state> is the
// lowercase CONAGUA state code, <station_id> the CONAGUA station id,
// <cell_id> the POWER cell key.
type DictionaryPath struct {
	Format   string   `json:"format"`
	Path     string   `json:"path"`
	Archives []string `json:"archives"`
}

// DictionaryColumn is one exported column. Source is the DDL
// table.column the value is read from — a child table's station_id
// noted as resolving to stations.external_id — or "synthesized" for the
// one column no DDL column backs (run_label), or "join context" for the
// cell_id / distance_km pair of a combined/ file. Decimals and Unit are
// null for a text column; Tag is null for a run ledger, which is
// provenance rather than data; Power is set for a POWER variable.
type DictionaryColumn struct {
	Name        string          `json:"name"`
	Key         bool            `json:"key"`
	Source      string          `json:"source"`
	Kind        string          `json:"kind"`
	Decimals    *int            `json:"decimals"`
	Unit        *string         `json:"unit"`
	Tag         *string         `json:"tag"`
	Description string          `json:"description"`
	Power       *PowerParameter `json:"power"`
}

// DictionaryConstant is a single-valued DDL column the flat files omit
// and this dictionary states once.
type DictionaryConstant struct {
	Table  string `json:"table"`
	Column string `json:"column"`
	Value  string `json:"value"`
	Reason string `json:"reason"`
}

// DictionaryDropped is a DDL column the flat files omit, with the
// category and its reason.
type DictionaryDropped struct {
	Table  string `json:"table"`
	Column string `json:"column"`
	Reason string `json:"reason"`
	Note   string `json:"note"`
}

// DictionaryProfile is profile.json's shape: the keys of the Profile
// struct in declaration order, nested.
type DictionaryProfile struct {
	Path  string           `json:"path"`
	Notes []string         `json:"notes"`
	Keys  []DictionaryNode `json:"keys"`
}

// DictionaryNode is one JSON key of the profile: its type as the file
// carries it ("number | null", "object keyed by period", "array[13] of
// number | null", …), decimals and unit for a number, and its children
// for an object. A "<period>" child stands for every reference period.
type DictionaryNode struct {
	Key         string           `json:"key"`
	Type        string           `json:"type"`
	Decimals    *int             `json:"decimals"`
	Unit        *string          `json:"unit"`
	Description string           `json:"description"`
	Children    []DictionaryNode `json:"children"`
}

// DictionaryDailyJSON is daily.json's row shape: the date key, the
// observed block, the reanalysis block, and the rules that fix nulls
// and layout.
type DictionaryDailyJSON struct {
	Path       string             `json:"path"`
	Date       string             `json:"date_key"`
	Observed   []DictionaryColumn `json:"observed"`
	Reanalysis []DictionaryColumn `json:"reanalysis"`
	Rules      []string           `json:"rules"`
}

// DictionaryAnnual is the annual-slot convention: the slot count, the
// tag slot 12 carries, the aggregation per variable, and the rules.
type DictionaryAnnual struct {
	Slots     int                     `json:"slots"`
	Tag       string                  `json:"tag"`
	Variables []DictionaryAggregation `json:"variables"`
	Rules     []string                `json:"rules"`
}

// DictionaryAggregation is one variable's slot-12 aggregation.
type DictionaryAggregation struct {
	Name        string `json:"name"`
	Aggregation string `json:"aggregation"`
}

// DictionaryRule is one topic of the format contract.
type DictionaryRule struct {
	Topic string `json:"topic"`
	Rule  string `json:"rule"`
}

// DictionaryPrecision is one decimal count and the DDL columns pinned to
// it, table.column, with the export name after an arrow where the
// export renames the column.
type DictionaryPrecision struct {
	Decimals int      `json:"decimals"`
	Columns  []string `json:"columns"`
}

// DictionarySQLite describes the two SQLite artifacts.
type DictionarySQLite struct {
	StatePath    string   `json:"state_path"`
	NationalPath string   `json:"national_path"`
	Tables       []string `json:"tables"`
	Notes        []string `json:"notes"`
}

// The path placeholders the dictionary writes where an archive path
// carries a per-unit segment.
const (
	placeholderState   = "<state>"
	placeholderStation = "<station_id>"
	placeholderCell    = "<cell_id>"
	placeholderPeriod  = "<period>"
)

// placeholderShard is the state whose slug is the <state> placeholder,
// handed to the archives' own path functions so the dictionary's paths
// are those functions' output.
var placeholderShard = State{Slug: placeholderState}

// The names of the three flat formats and the grains a file is sharded at.
const (
	formatCSV     = "csv"
	formatParquet = "parquet"
	formatJSON    = "json"

	grainTable   = "one file per table"
	grainStation = "one file per station under the state shard"
	grainCell    = "one file per referenced cell"
)

// The source strings of the two column kinds no DDL column backs.
const (
	sourceSynthesized = "synthesized"
	sourceJoinContext = "join context"
)

// formatArchives lists, per flat format, the artifact groups whose
// archives carry the format — the per-state group and the national one.
var formatArchives = map[string][]Group{
	formatCSV:     {GroupTabular, GroupNationalCSV},
	formatParquet: {GroupTabular, GroupNationalParquet},
	formatJSON:    {GroupJSON, GroupNationalJSON},
}

// unitSuffixes is the column-name suffix → unit map (the unit is pinned
// in the suffix), the one place a unit is read from a name. No suffix
// here is a suffix of another, so first match is the only match.
var unitSuffixes = []DictionaryUnit{
	{Pattern: "_c", Unit: "°C"},
	{Pattern: "_mm", Unit: "mm"},
	{Pattern: "_mmpd", Unit: "mm/day"},
	{Pattern: "_pct", Unit: "%"},
	{Pattern: "_gkg", Unit: "g/kg"},
	{Pattern: "_ms", Unit: "m/s"},
	{Pattern: "_deg", Unit: "degrees"},
	{Pattern: "_wm2", Unit: "W/m²"},
	{Pattern: "_kpa", Unit: "kPa"},
	{Pattern: "_km", Unit: "km"},
	{Pattern: "_m", Unit: "m"},
	{Pattern: "_days", Unit: "days"},
}

// unitNames are the units of the numeric columns whose names carry no
// unit suffix: bare coordinates and dimensionless quantities.
var unitNames = []DictionaryUnit{
	{Pattern: "lat", Unit: "decimal degrees"},
	{Pattern: "lon", Unit: "decimal degrees"},
	{Pattern: "clearness_index", Unit: "dimensionless"},
	{Pattern: "gwet_top", Unit: "dimensionless"},
	{Pattern: "gwet_root", Unit: "dimensionless"},
	{Pattern: "gwet_prof", Unit: "dimensionless"},
	{Pattern: wmoPrefix + "*", Unit: "dimensionless"},
}

// wmoPrefix names the WMO completeness columns of stations.
const wmoPrefix = "wmo_completeness_"

// wmoColumnCount is the number of WMO completeness columns the stations
// file exports — the count the README and the stations note state.
func wmoColumnCount() int {
	n := 0
	for _, c := range Stations.Columns {
		if strings.HasPrefix(c.DB, wmoPrefix) {
			n++
		}
	}
	return n
}

// unitOf resolves a numeric column's unit from its name: a whole-name
// entry, the WMO prefix, then the suffix map; nil when the column has no
// unit (a month, a year, a count).
func unitOf(name string) *string {
	for _, u := range unitNames {
		if u.Pattern == name || (strings.HasSuffix(u.Pattern, "*") && strings.HasPrefix(name, strings.TrimSuffix(u.Pattern, "*"))) {
			return &u.Unit
		}
	}
	for _, u := range unitSuffixes {
		if strings.HasSuffix(name, u.Pattern) {
			return &u.Unit
		}
	}
	return nil
}

// dictionaryTags is the tag legend, in the order the tags are met.
var dictionaryTags = []DictionaryTag{
	{Tag: sourceConaguaPublished, Meaning: "a value CONAGUA publishes as such: the station catalog and the normals files (monthly_normals, monthly_normals_extras)"},
	{Tag: sourceConaguaObserved, Meaning: "a value CONAGUA's daily observation file records for a date"},
	{Tag: sourceNasaPower, Meaning: "NASA POWER reanalysis, keyed by grid cell — never a station observation"},
	{Tag: sourceBioclimaDerived, Meaning: "computed by this ETL from the sources: the WMO completeness scores, first_year / last_year, the cell snap and its distance, slot 12 of every profile month series, and the profile's daily_summary block"},
}

// The reasons a DDL column is left out of the flat files, per category.
var dropNotes = map[DropReason]string{
	DropSurrogate: "the database surrogate key is never exported; the flat files carry natural keys only (station_id = stations.external_id, cell_id, snapshot_date, run_label)",
	DropFK:        "the per-row reference to power_runs is dropped; the file (temporal mode) and the row's own period or date identify the run — see provenance/power_runs",
	DropConstant:  "single-valued across every row; stated once under documented constants",
}

// constantValues resolves each documented constant's value from the code
// that owns it, never from a literal here: the station source ingest
// writes, the grid resolution the POWER cell math pins, the solar factor
// the registry applies.
var constantValues = map[string]map[string]DictionaryConstant{
	"stations": {"source": {
		Value:  string(ingest.SourceConaguaConventional),
		Reason: "every exported station is a CONAGUA conventional station; stations of other sources are never exported, so the column would hold one value",
	}},
	"nasa_power_grid_cells": {"grid_resolution": {
		Value:  power.Resolution,
		Reason: "every cell is on POWER's one grid (latitude step × longitude step, degrees); per run the value stays in provenance/power_runs as a request field",
	}},
	"power_runs": {"solar_conversion": {
		Value:  strconv.FormatFloat(power.SolarMJpm2dToWm2, 'g', -1, 64),
		Reason: "the MJ/m²/day → W/m² factor (1e6 / 86400) applied to every radiation stream, identical in every run and represented per variable in unit_conversions",
	}},
}

// kindName is a Column kind as the dictionary names it.
func kindName(k Kind) string {
	switch k {
	case KindDate:
		return "date"
	case KindPeriod:
		return "period"
	case KindInt:
		return "integer"
	case KindReal:
		return "real"
	default:
		return "text"
	}
}

// jsonTypeName is a Column kind as a JSON value type.
func jsonTypeName(k Kind) string {
	switch k {
	case KindInt:
		return "integer"
	case KindReal:
		return "number"
	default:
		return "string"
	}
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// ddlIndex is schema.Tables() keyed by table then column.
type ddlIndex map[string]map[string]schema.Column

func indexDDL(tables []schema.Table) ddlIndex {
	idx := make(ddlIndex, len(tables))
	for _, t := range tables {
		cols := make(map[string]schema.Column, len(t.Columns))
		for _, c := range t.Columns {
			cols[c.Name] = c
		}
		idx[t.Name] = cols
	}
	return idx
}

// dictionaryFileDef declares one exported file's dictionary metadata
// beside its FileSpec: sources maps export names to DDL tables for a
// composed file (nil for a single-table file), tag gives each column's
// provenance tag ("" for none), and the paths are the archives' own path
// functions applied to the placeholders.
type dictionaryFileDef struct {
	spec    FileSpec
	sources map[string]string
	tag     func(Column) string
	rowSet  string
	grain   string
	csv     string
	parquet bool
	json    string
	notes   []string
}

// tagged returns a tag function: base for every column but the derived
// exceptions named.
func tagged(base string, derived ...string) func(Column) string {
	return func(c Column) string {
		if slices.Contains(derived, c.DB) {
			return sourceBioclimaDerived
		}
		return base
	}
}

// stationsTag tags the stations file: the catalog fields are CONAGUA's,
// the eight WMO scores and first_year / last_year are computed at ingest.
func stationsTag(c Column) string {
	if strings.HasPrefix(c.DB, wmoPrefix) || c.DB == "first_year" || c.DB == "last_year" {
		return sourceBioclimaDerived
	}
	return sourceConaguaPublished
}

// composedTag tags a combined/ file by block: the join context is the
// ETL's, POWER-31 is POWER's, the spine keeps the spine's tag.
func composedTag(spec ComposedSpec, spineTag string) func(Column) string {
	return func(c Column) string {
		switch spec.Source(c) {
		case joinContextBlock.table:
			return sourceBioclimaDerived
		case PowerMonthly.Table, PowerDaily.Table:
			return sourceNasaPower
		default:
			return spineTag
		}
	}
}

func untagged(Column) string { return "" }

// dailyGrainNote qualifies the POWER-31 descriptions on the daily
// files. The DDL states the block once, on monthly_supplement, and
// writes each comment at the daily grain, so the daily files read it
// verbatim; two consequences a reader has to be handed are where the
// monthly table departs from that text (powerMonthlyDescriptions) and
// that daily wind direction is not averaged at this layer.
const dailyGrainNote = "The POWER-31 descriptions are the schema's own, written at this daily grain and stated once on monthly_supplement; where the monthly climatology means something else — the four extreme columns — nasa_power/monthly carries its own description. Wind direction (wd2m_deg, wd10m_deg) is stored as POWER delivers it — a 24-h vector mean upstream — with no circular averaging at this layer."

// monthlyClimatologyNote states the grain of every nasa_power/monthly
// value. It is the premise the four extreme columns' descriptions rest
// on: the roll-up averages POWER's own monthly figure, so what the
// figure means at the monthly grain is what the column holds.
const monthlyClimatologyNote = "Every value is a climatological mean: POWER's monthly figure for that calendar month, averaged unweighted over the years of the period POWER returned a value for — a year POWER reports as missing is dropped from the average, never filled. For the four extreme columns (t2m_max_c, t2m_min_c, ts_max_c, ts_min_c) the figure being averaged is POWER's extreme over the whole month, not the mean of that month's daily extremes; see their descriptions."

// The "combined" glossary definition, carried where the combined/ files
// are described.
const combinedGlossary = `"Combined" / "Augmented" = a CONAGUA-spine record augmented with its station's POWER-cell reanalysis on matching keys — a LEFT join from the CONAGUA side. Never a temporal union. Every CONAGUA row is kept; POWER columns are NULL where POWER has no matching row, and cell_id / distance_km are NULL as well for a station with no station_power_cell row.`

// dictionaryFileDefs lists the exported files in the dictionary's
// per-file order.
func dictionaryFileDefs() []dictionaryFileDef {
	return []dictionaryFileDef{
		{
			spec: Stations, tag: stationsTag, grain: grainTable, parquet: true,
			rowSet: "one row per CONAGUA conventional station in scope (the state's stations in a state archive, every state's in a national archive); stations of other sources are never exported",
			notes: []string{
				fmt.Sprintf("The %d %s* columns and first_year / last_year are the conagua/ folder's %s columns; every other column mirrors CONAGUA's catalog.", wmoColumnCount(), wmoPrefix, sourceBioclimaDerived),
				fmt.Sprintf("The completeness fractions are written with %d decimals (provisional: the fewest that keep every attainable k/36 and k/1080 value distinct; subject to confirmation in a later version).", wmoByPeriod[0].decimals),
				"first_year / last_year bound the station's daily record, not its data: CONAGUA publishes dates whose every value is missing, and such a row sets them like any other, so the span can begin or end in a year the station reported nothing at all in.",
			},
		},
		{
			spec: MonthlyNormals, tag: tagged(sourceConaguaPublished), grain: grainTable, parquet: true,
			rowSet: "CONAGUA's published monthly normals: months 1–12 of every reference period the station has a normals file for — a pure source mirror, no derived rows (the annual lives in profile.json only)",
		},
		{
			spec: MonthlyNormalsExtras, tag: tagged(sourceConaguaPublished), grain: grainTable, parquet: true,
			rowSet: "one row per (station, period, month) CONAGUA's normals file publishes extremes or counts for; not joined to monthly_normals — either table may hold a row the other lacks",
		},
		{
			spec: DailyObservations, tag: tagged(sourceConaguaObserved), grain: grainStation, parquet: true,
			csv:    dailyPath(placeholderShard, placeholderStation),
			rowSet: "the station's observed dates, date ascending; a station with no daily rows still has a header-only file, so the file set is one-to-one with stations",
		},
		{
			spec: Cells, tag: tagged(sourceBioclimaDerived), grain: grainTable, parquet: true,
			rowSet: "the POWER grid cells the stations in scope reference, each once",
		},
		{
			spec: StationCellMap, tag: tagged(sourceBioclimaDerived), grain: grainTable, parquet: true,
			rowSet: "one row per station in scope that has a cell — never a padded row for one that has none; the join a reader needs to attach nasa_power/ rows to a station",
		},
		{
			spec: PowerMonthly, tag: tagged(sourceNasaPower), grain: grainTable, parquet: true,
			rowSet: "the referenced cells' monthly climatology for every reference period POWER monthly was pulled for (provenance/power_runs lists them)",
			notes:  []string{monthlyClimatologyNote},
		},
		{
			spec: PowerDaily, tag: tagged(sourceNasaPower), grain: grainCell, parquet: true,
			csv:    cellDailyPath(placeholderCell),
			rowSet: "the cell's daily reanalysis series, date ascending; a referenced cell with no daily rows still has a header-only file, one-to-one with cells",
			notes:  []string{dailyGrainNote},
		},
		{
			spec: CombinedMonthly.FileSpec, sources: CombinedMonthly.Sources, grain: grainTable, parquet: true,
			tag:    composedTag(CombinedMonthly, sourceConaguaPublished),
			rowSet: "every monthly_normals row in scope — all reference periods — LEFT-joined to the station's cell and to that cell's monthly_supplement row on (period, month); POWER-31 is NULL where the cell has no row for the period",
			notes:  []string{combinedGlossary, monthlyClimatologyNote},
		},
		{
			spec: CombinedDaily.FileSpec, sources: CombinedDaily.Sources, grain: grainStation, parquet: true,
			tag:    composedTag(CombinedDaily, sourceConaguaObserved),
			csv:    combinedDailyPath(placeholderShard, placeholderStation),
			json:   dailyJSONPath(placeholderShard, placeholderStation),
			rowSet: "every daily_observations row of the station (the observed dates) LEFT-joined to the cell's daily_supplement row on date; POWER-31 is NULL where the cell has no row for the date — before the cell's series begins, or where the variable is missing",
			notes:  []string{combinedGlossary, dailyGrainNote, "The JSON rendering is the per-station daily.json (its row shape is described below): one product, two formats, one row set."},
		},
		{
			spec: ProvenanceIngestRuns, tag: untagged, grain: grainTable,
			json:   ProvenanceIngestRuns.Name + "." + formatJSON,
			rowSet: "the latest complete ingest run per snapshot_date; global — identical in every archive",
			notes:  []string{"A run ledger is provenance, not data: its columns carry no source tag. manifest.json is the global provenance index."},
		},
		{
			spec: ProvenancePowerRuns, tag: untagged, grain: grainTable,
			json:   ProvenancePowerRuns.Name + "." + formatJSON,
			rowSet: "the power runs at least one monthly_supplement or daily_supplement row references, by run_label; global — identical in every archive; two referenced runs sharing a label fail the build",
			notes: []string{
				"A run ledger is provenance, not data: its columns carry no source tag. The trace path is flat value → (temporal mode, period) → run_label → this file or manifest.json → the reproducible POWER request.",
				"parameters is the verbatim comma-joined POWER id list (order is load-bearing: the request URL a reproducer pastes); unit_conversions is the verbatim JSON text, quoted in CSV and embedded raw in JSON.",
			},
		},
	}
}

// parquetTableCount is the number of per-table Parquet files a tabular
// archive carries: the dictionary's files with a Parquet path.
func parquetTableCount() int {
	n := 0
	for _, d := range dictionaryFileDefs() {
		if d.parquet {
			n++
		}
	}
	return n
}

// fallbackDescriptions describes the exported columns schema.sql carries
// no trailing comment for, keyed by DDL table then column (the
// synthesized run_label under power_runs). Explicit, one entry per
// column; the two families with a regular shape — the extras and the
// WMO scores — are generated into it at init (familyDescriptions). A
// column with neither a DDL comment nor an entry here fails
// BuildDictionary rather than ship undescribed.
var fallbackDescriptions = map[string]map[string]string{
	"stations": {
		"external_id":  "CONAGUA station identifier (stations.external_id) — an opaque string, never a number; the natural key of every per-station file",
		"name":         "station name as CONAGUA's catalog lists it",
		"state":        "CONAGUA state code as stored (uppercase); the code → official name lookup ships in manifest.json and the README",
		"municipality": "municipality as CONAGUA's catalog lists it",
		"lat":          "station latitude, decimal degrees (DMS-derived)",
		"lon":          "station longitude, decimal degrees, west-negative",
		"altitude_m":   "station altitude above sea level",
		"status":       fmt.Sprintf("CONAGUA's station status, English as stored (%s / %s)", conagua.StatusOperating, conagua.StatusSuspended),
		"first_year":   "year of the station's first record in the daily series: the earliest date daily_observations holds a row for, whether or not that row carries a measurement; the earliest normals period's start year when the station has no daily rows at all (computed at ingest)",
		"last_year":    "year of the station's last record in the daily series: the latest date daily_observations holds a row for, whether or not that row carries a measurement; the latest normals period's end year when the station has no daily rows at all (computed at ingest)",
	},
	"monthly_normals": {
		"station_id": "CONAGUA station identifier: the surrogate key resolved through stations to external_id",
		"period":     "30-year reference period of the normal, YYYY-YYYY",
		"month":      "calendar month, 1–12",
		"tmax":       "monthly normal of the daily maximum temperature",
		"tmin":       "monthly normal of the daily minimum temperature",
		"tmean":      "monthly normal of the daily mean temperature",
		"precip":     "monthly normal precipitation total",
		"evap":       "monthly normal evaporation total",
	},
	"monthly_normals_extras": {
		"station_id":                "CONAGUA station identifier: the surrogate key resolved through stations to external_id",
		"period":                    "30-year reference period, YYYY-YYYY",
		"month":                     "calendar month, 1–12",
		"rain_days":                 "mean number of days with rain in the month (CONAGUA's NÚMERO DE DÍAS CON LLUVIA)",
		"rain_days_years_with_data": "years in the period with valid rain-day data for the month (CONAGUA's AÑOS CON DATOS)",
	},
	"daily_observations": {
		"station_id": "CONAGUA station identifier: the surrogate key resolved through stations to external_id",
		"date":       "calendar date of the observation, YYYY-MM-DD",
		"tmax":       "daily maximum temperature",
		"tmin":       "daily minimum temperature",
		"precip":     "daily precipitation total",
		"evap":       "daily evaporation total",
	},
	"nasa_power_grid_cells": {
		"cell_id": "POWER grid cell key, derived from the cell centroid; the key nasa_power/ rows and station_cell_map share",
		"lat":     "cell centroid latitude, decimal degrees",
		"lon":     "cell centroid longitude, decimal degrees, west-negative",
	},
	"station_power_cell": {
		"station_id":  "CONAGUA station identifier: the surrogate key resolved through stations to external_id",
		"cell_id":     "the POWER grid cell enclosing the station (nasa_power/cells.cell_id)",
		"distance_km": "great-circle distance from the station to the cell centroid; a large value flags a coastal or edge cell whose reanalysis may not represent the station",
	},
	"monthly_supplement": {
		"cell_id": "POWER grid cell key (nasa_power/cells.cell_id)",
		"period":  "30-year reference period of the climatology, YYYY-YYYY",
		"month":   "calendar month, 1–12",
	},
	"daily_supplement": {
		"cell_id": "POWER grid cell key (nasa_power/cells.cell_id)",
		"date":    "calendar date of the reanalysis value",
	},
	"ingest_runs": {
		"snapshot_date":      "the pull snapshot the run ingested (conagua-raw/<date>); the natural key — the latest complete run per date is the one exported",
		"started_at":         "run start, RFC 3339 UTC as ingest wrote it",
		"finished_at":        "run end, RFC 3339 UTC; NULL while running",
		"sink_kind":          "the snapshot storage the run read from, as ingest recorded it",
		"etl_git_sha":        "git commit of the ETL binary that ran",
		"status":             "run status as stored; only complete runs are exported",
		"stations_attempted": "stations the run tried to ingest",
		"stations_succeeded": "stations ingested",
		"stations_failed":    "stations that failed and were rolled back",
		"daily_rows":         "daily_observations rows the run wrote",
		"normals_rows":       "monthly_normals rows the run wrote",
		"extras_rows":        "monthly_normals_extras rows the run wrote",
		"warnings_total":     "parsing_warnings rows the run wrote",
	},
	"power_runs": {
		RunLabelColumn:      "natural label power-<temporal_mode>-<period_start_year>-<period_end_year>, synthesized at export; the surrogate id is never exported",
		"started_at":        "run start, RFC 3339 UTC as power wrote it",
		"finished_at":       "run end, RFC 3339 UTC; NULL while running",
		"status":            "run status as stored",
		"endpoint_url":      "the POWER API endpoint requested",
		"parameters":        "the comma-joined POWER parameter ids requested, in request order — the URL fragment a reproducer pastes",
		"community":         fmt.Sprintf("the POWER user community requested (%s by default)", power.DefaultCommunity),
		"period_start_year": "first year of the requested span",
		"period_end_year":   "last year of the requested span",
		"grid_resolution":   "the POWER grid step requested, latitude × longitude in degrees",
		"unit_conversions":  "per-parameter {power_unit, stored_unit, factor} manifest as JSON text, verbatim as stored",
		"temporal_mode":     "monthly (a climatology request → nasa_power/monthly) or daily (a calendar range → nasa_power/daily)",
		"period_start_date": "first date of a daily-mode request, YYYY-MM-DD; NULL for a monthly run",
		"period_end_date":   "last date of a daily-mode request, YYYY-MM-DD; NULL for a monthly run",
		"cells_attempted":   "cells the run tried to fetch",
		"cells_succeeded":   "cells fetched",
		"cells_failed":      "cells that failed and were rolled back",
		"supplement_rows":   "supplement rows the run wrote",
		"etl_git_sha":       "git commit of the ETL binary that ran",
	},
}

// powerMonthlyDescriptions replace the DDL comment on the
// monthly_supplement columns whose meaning does not survive the change
// of grain. schema.sql states POWER-31 once, on monthly_supplement, and
// daily_supplement reads those comments (ddlComment) — so each comment
// is written at one grain, and it is written at the daily one. That is
// right for daily_supplement and wrong for these four: a monthly value
// is POWER's extreme over the whole calendar month, averaged across the
// period's years, not the mean of that month's daily extremes. The two
// are different numbers, so a reader handed the daily wording would
// recompute the column from nasa_power/daily and get a value that does
// not match — the failure this text exists to prevent.
var powerMonthlyDescriptions = map[string]string{
	"t2m_max_c": "monthly maximum air temperature at 2 m: POWER's maximum for the calendar month, averaged over the period's years — not the mean of the month's daily maxima, so averaging nasa_power/daily's t2m_max_c gives a different number",
	"t2m_min_c": "monthly minimum air temperature at 2 m: POWER's minimum for the calendar month, averaged over the period's years — not the mean of the month's daily minima, so averaging nasa_power/daily's t2m_min_c gives a different number",
	"ts_max_c":  "monthly maximum earth-skin temperature: POWER's maximum for the calendar month, averaged over the period's years — not the mean of the month's daily maxima, so averaging nasa_power/daily's ts_max_c gives a different number",
	"ts_min_c":  "monthly minimum earth-skin temperature: POWER's minimum for the calendar month, averaged over the period's years — not the mean of the month's daily minima, so averaging nasa_power/daily's ts_min_c gives a different number",
}

// joinContextDescriptions describe the combined/ files' join context,
// where NULL means "no cell" rather than a missing map row.
var joinContextDescriptions = map[string]string{
	"cell_id":     "the station's POWER grid cell (nasa_power/station_cell_map); NULL for a station with no cell",
	"distance_km": "great-circle distance from the station to its cell's centroid (nasa_power/station_cell_map); NULL for a station with no cell",
}

// extrasVariables names the quantities behind the extras columns, per
// variable prefix, and the sense of each extreme.
var extrasVariables = map[string]struct{ noun, monthly, daily string }{
	"tmax": {
		noun:    "daily maximum temperature",
		monthly: "highest monthly mean of the daily maximum temperature in the period",
		daily:   "highest daily maximum temperature in the period",
	},
	"tmin": {
		noun:    "daily minimum temperature",
		monthly: "lowest monthly mean of the daily minimum temperature in the period",
		daily:   "lowest daily minimum temperature in the period",
	},
	"precip": {
		noun:    "precipitation",
		monthly: "highest monthly precipitation total in the period",
		daily:   "highest one-day precipitation total in the period",
	},
	"tmean": {noun: "daily mean temperature"},
	"evap":  {noun: "evaporation"},
}

// familyDescriptions generates the two regular families into
// fallbackDescriptions: the extras (per variable: the monthly and daily
// extremes, their year and date, the years-with-data count) and the
// eight WMO scores (per system and period). A pattern that names an
// unknown variable is a declaration error caught at init.
func familyDescriptions() {
	extras := fallbackDescriptions[MonthlyNormalsExtras.Table]
	for _, c := range MonthlyNormalsExtras.Columns {
		if _, done := extras[c.DB]; done {
			continue
		}
		extras[c.DB] = extrasDescription(c.DB)
	}
	stations := fallbackDescriptions[Stations.Table]
	for _, w := range wmoByPeriod {
		stations[w.bin] = fmt.Sprintf("WMO-No. 1203 §4.4.2 completeness of the %s normals, binary system (_bin_): the share of the (variable, month) cells that pass the WMO threshold, 0–1 — see the stations note", w.period)
		stations[w.cont] = fmt.Sprintf("WMO-No. 1203 §4.4.2 completeness of the %s normals, continuous system (_cont_): the mean coverage density over the same cells, 0–1 — see the stations note", w.period)
	}
}

// extrasDescription derives one extras column's description from its
// name: <variable>_<monthly|daily>_extreme[_year|_date] or
// <variable>_years_with_data.
func extrasDescription(db string) string {
	for _, suffix := range []string{"_monthly_extreme_year", "_daily_extreme_date", "_monthly_extreme", "_daily_extreme", "_years_with_data"} {
		if !strings.HasSuffix(db, suffix) {
			continue
		}
		v, ok := extrasVariables[strings.TrimSuffix(db, suffix)]
		if !ok {
			panic(fmt.Sprintf("publish: extras column %s names no known variable", db))
		}
		switch suffix {
		case "_monthly_extreme":
			return v.monthly
		case "_daily_extreme":
			return v.daily
		case "_monthly_extreme_year":
			return "year of " + ExportName(MonthlyNormalsExtras.Table, strings.TrimSuffix(db, "_year"))
		case "_daily_extreme_date":
			return "date of " + ExportName(MonthlyNormalsExtras.Table, strings.TrimSuffix(db, "_date"))
		default:
			return fmt.Sprintf("years in the period with valid %s data for the month (CONAGUA's AÑOS CON DATOS)", v.noun)
		}
	}
	panic(fmt.Sprintf("publish: extras column %s matches no family", db))
}

func init() { familyDescriptions() }

// formatNotes are the DDL's trailing comments that name a value's
// layout and nothing else. Such a comment describes the column's kind,
// not the column, so the description comes from the fallback and the
// note is appended to it as a format hint; a spec holds every entry to
// a comment the DDL carries.
var formatNotes = map[string]bool{
	"ISO 'YYYY-MM-DD'": true,
}

// describeColumn resolves a column's description: the DDL's trailing
// comment on the column when it has one that is not a bare format
// note — for a daily_supplement variable the POWER block's comment on
// monthly_supplement, where the DDL describes POWER-31 once — else the
// explicit fallback, with a format note appended in parentheses. Two
// sets of columns are answered before the DDL is consulted at all,
// because no comment there could be right for them: the join context of
// a combined/ file, and the monthly_supplement columns the shared
// POWER-31 comment describes at the wrong grain.
func describeColumn(table string, c Column, joinContext bool, ddl ddlIndex) (string, error) {
	if joinContext {
		if d, ok := joinContextDescriptions[c.DB]; ok {
			return d, nil
		}
	}
	if table == PowerMonthly.Table {
		if d, ok := powerMonthlyDescriptions[c.DB]; ok {
			return d, nil
		}
	}
	comment := ddlComment(table, c.DB, ddl)
	if comment != "" && !formatNotes[comment] {
		return comment, nil
	}
	key := c.DB
	if key == "" {
		key = c.Name
	}
	d, ok := fallbackDescriptions[table][key]
	if !ok {
		return "", fmt.Errorf("column %s.%s has no description", table, key)
	}
	if comment != "" {
		d += " (" + comment + ")"
	}
	return d, nil
}

// ddlComment is the trailing DDL comment on table.column: the column's
// own, or for a daily_supplement variable monthly_supplement's comment
// on the same column; "" when neither carries one.
func ddlComment(table, column string, ddl ddlIndex) string {
	if col, ok := ddl[table][column]; ok && col.Comment != "" {
		return col.Comment
	}
	if table == PowerDaily.Table {
		if col, ok := ddl[PowerMonthly.Table][column]; ok {
			return col.Comment
		}
	}
	return ""
}

// columnSource renders the source string: the DDL table.column, a child
// table's station_id noted as resolving through stations, or the two
// non-DDL sources.
func columnSource(table string, c Column, joinContext bool) string {
	switch {
	case c.DB == "":
		return sourceSynthesized
	case joinContext:
		return sourceJoinContext
	case c.DB == "station_id" && table != Stations.Table:
		return table + "." + c.DB + " → " + Stations.Table + ".external_id"
	default:
		return table + "." + c.DB
	}
}

// columns builds the dictionary columns of one file.
func (d dictionaryFileDef) columns(ddl ddlIndex, byColumn map[string]PowerParameter) ([]DictionaryColumn, error) {
	cols := make([]DictionaryColumn, 0, len(d.spec.Columns))
	for _, c := range d.spec.Columns {
		table := d.spec.Table
		if d.sources != nil {
			table = d.sources[c.Name]
		}
		joinContext := d.sources != nil && table == joinContextBlock.table
		desc, err := describeColumn(table, c, joinContext, ddl)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", d.spec.Name, err)
		}
		col := DictionaryColumn{
			Name:        c.Name,
			Key:         c.Key,
			Source:      columnSource(table, c, joinContext),
			Kind:        kindName(c.Kind),
			Tag:         optional(d.tag(c)),
			Description: desc,
		}
		if c.Kind == KindInt || c.Kind == KindReal {
			decimals := c.Decimals
			col.Decimals = &decimals
			col.Unit = unitOf(c.Name)
		}
		if table == PowerMonthly.Table || table == PowerDaily.Table {
			if p, ok := byColumn[c.DB]; ok {
				col.Power = &p
			}
		}
		cols = append(cols, col)
	}
	return cols, nil
}

// archivesFor lists the top-level archives that carry a format, the
// per-state one on the placeholder slug.
func archivesFor(format, snapshotDate string) []string {
	groups := formatArchives[format]
	out := make([]string, 0, len(groups))
	for _, g := range groups {
		if g.PerState() {
			out = append(out, archiveName(placeholderShard, g))
		} else {
			out = append(out, nationalArchiveName(g, snapshotDate))
		}
	}
	return out
}

// paths lists a file's in-archive path per format.
func (d dictionaryFileDef) paths(snapshotDate string) []DictionaryPath {
	csv := d.csv
	if csv == "" {
		csv = d.spec.Name + "." + formatCSV
	}
	out := []DictionaryPath{{Format: formatCSV, Path: csv, Archives: archivesFor(formatCSV, snapshotDate)}}
	if d.parquet {
		out = append(out, DictionaryPath{Format: formatParquet, Path: parquetPath(d.spec), Archives: archivesFor(formatParquet, snapshotDate)})
	}
	if d.json != "" {
		out = append(out, DictionaryPath{Format: formatJSON, Path: d.json, Archives: archivesFor(formatJSON, snapshotDate)})
	}
	return out
}

// file builds one DictionaryFile.
func (d dictionaryFileDef) file(snapshotDate string, ddl ddlIndex, byColumn map[string]PowerParameter) (DictionaryFile, error) {
	cols, err := d.columns(ddl, byColumn)
	if err != nil {
		return DictionaryFile{}, err
	}
	keys := []string{}
	for _, c := range d.spec.Columns {
		if c.Key {
			keys = append(keys, c.Name)
		}
	}
	notes := slices.Clone(d.notes)
	if notes == nil {
		notes = []string{}
	}
	return DictionaryFile{
		Name:    d.spec.Name,
		Table:   d.spec.Table,
		RowSet:  d.rowSet,
		SortKey: keys,
		Grain:   d.grain,
		Paths:   d.paths(snapshotDate),
		Notes:   notes,
		Columns: cols,
	}, nil
}

// The profile's month-series period maps, keyed by the block type that
// holds them, to the flat file whose value columns are the series: the
// reflect walk reads the shape, the spec names the variables — the same
// spec the profile builder reads them from.
var periodSpecs = map[reflect.Type]FileSpec{
	reflect.TypeOf(NormalsBlock{}):      MonthlyNormals,
	reflect.TypeOf(ExtrasBlock{}):       MonthlyNormalsExtras,
	reflect.TypeOf(PowerMonthlyBlock{}): PowerMonthly,
}

// The profile's scalar types, matched by identity in the walk.
var (
	jsonTextType  = reflect.TypeOf(jsonText{})
	jsonIntType   = reflect.TypeOf(jsonInt{})
	jsonRealType  = reflect.TypeOf(jsonReal{})
	monthBlockPtr = reflect.TypeOf((*monthBlock)(nil))
)

// monthSlots is the length of a profile month series: months 1–12 and
// the annual.
const monthSlots = len(monthSeries{})

// profileDecimals pins the decimals of the profile's fixed-shape numbers
// by JSON path, from the same variables the profile builder formats
// with; a jsonReal at a path absent here fails BuildDictionary.
var profileDecimals = map[string]int{
	"identity.lat":                                       stationLatDecimals,
	"identity.lon":                                       stationLonDecimals,
	"identity.altitude_m":                                altitudeDecimals,
	"wmo_completeness.periods.<period>.bin":              wmoByPeriod[0].decimals,
	"wmo_completeness.periods.<period>.cont":             wmoByPeriod[0].decimals,
	"power_cell.lat":                                     cellLatDecimals,
	"power_cell.lon":                                     cellLonDecimals,
	"power_cell.distance_km":                             distanceDecimals,
	"daily_summary.extremes.record_tmax_c.value":         recordTmaxDecimals,
	"daily_summary.extremes.record_tmin_c.value":         recordTminDecimals,
	"daily_summary.extremes.record_precip_mm_1day.value": recordPrecDecimals,
}

// profileUnits pins the units of the profile's fixed-shape numbers by
// JSON path where the key carries no unit suffix.
var profileUnits = map[string]string{
	"identity.lat":                           "decimal degrees",
	"identity.lon":                           "decimal degrees",
	"power_cell.lat":                         "decimal degrees",
	"power_cell.lon":                         "decimal degrees",
	"wmo_completeness.periods.<period>.bin":  "dimensionless",
	"wmo_completeness.periods.<period>.cont": "dimensionless",
}

// periodKeyDescription describes a period map's keys.
const periodKeyDescription = "one key per 30-year reference period, sorted; null when the block has no rows for the period"

// profileDescriptions describes every profile key by JSON path; the
// month series under a period reuse their flat file's column
// descriptions. A key with no entry fails BuildDictionary, so a new
// Profile field cannot ship undescribed.
var profileDescriptions = map[string]string{
	"station_id":                                       "CONAGUA station identifier (stations.external_id) — the file's identity",
	"identity":                                         "the station's CONAGUA catalog record",
	"identity.name":                                    fallbackDescriptions["stations"]["name"],
	"identity.state":                                   fallbackDescriptions["stations"]["state"],
	"identity.state_name":                              "the official state name of the code, so a profile stands alone",
	"identity.municipality":                            fallbackDescriptions["stations"]["municipality"],
	"identity.lat":                                     fallbackDescriptions["stations"]["lat"],
	"identity.lon":                                     fallbackDescriptions["stations"]["lon"],
	"identity.altitude_m":                              fallbackDescriptions["stations"]["altitude_m"],
	"identity.status":                                  fallbackDescriptions["stations"]["status"],
	"identity.first_year":                              fallbackDescriptions["stations"]["first_year"],
	"identity.last_year":                               fallbackDescriptions["stations"]["last_year"],
	"wmo_completeness":                                 "WMO-No. 1203 §4.4.2 completeness scores per reference period",
	"wmo_completeness.source":                          "the block's source tag (" + sourceBioclimaDerived + ")",
	"wmo_completeness.periods":                         periodKeyDescription,
	"wmo_completeness.periods.<period>":                "the period's two scores; null when both are NULL (no extras rows for the period)",
	"wmo_completeness.periods.<period>.bin":            "binary system: the share of the (variable, month) cells that pass the WMO threshold, 0–1 (the stations file's " + wmoPrefix + "bin_* columns)",
	"wmo_completeness.periods.<period>.cont":           "continuous system: the mean coverage density over the same cells, 0–1 (the stations file's " + wmoPrefix + "cont_* columns)",
	"normals":                                          "CONAGUA's published monthly normals per reference period, each variable a positional month series",
	"normals.source":                                   "the block's source tag for slots 0–11 (" + sourceConaguaPublished + ")",
	"normals.annual_slot":                              "the tag of slot 12, the annual recomputed at export (" + sourceBioclimaDerived + ")",
	"normals.periods":                                  periodKeyDescription,
	"normals.periods.<period>":                         "the period's series, one per variable of conagua/monthly_normals; null when the station has no monthly_normals rows for the period",
	"extras":                                           "the monthly_normals_extras columns per reference period, each a positional month series",
	"extras.source":                                    "the block's source tag (" + sourceConaguaPublished + ")",
	"extras.periods":                                   periodKeyDescription,
	"extras.periods.<period>":                          "the period's series, one per value column of conagua/monthly_normals_extras; slot 12 is always null (provisional: no annual aggregation is defined for the extras columns yet); null when the station has no rows for the period",
	"power_cell":                                       "the station's POWER cell snap; the whole block is null for a station with no station_power_cell row",
	"power_cell.source":                                "the block's source tag (" + sourceBioclimaDerived + ")",
	"power_cell.cell_id":                               fallbackDescriptions["station_power_cell"]["cell_id"],
	"power_cell.lat":                                   fallbackDescriptions["nasa_power_grid_cells"]["lat"],
	"power_cell.lon":                                   fallbackDescriptions["nasa_power_grid_cells"]["lon"],
	"power_cell.distance_km":                           fallbackDescriptions["station_power_cell"]["distance_km"],
	"power_monthly":                                    "the cell's POWER-31 monthly climatology per reference period, registry order; the whole block is null for a station with no cell",
	"power_monthly.source":                             "the block's source tag for slots 0–11 (" + sourceNasaPower + ")",
	"power_monthly.annual_slot":                        "the tag of slot 12, the annual recomputed at export (" + sourceBioclimaDerived + ")",
	"power_monthly.periods":                            periodKeyDescription,
	"power_monthly.periods.<period>":                   "the period's series, one per variable of nasa_power/monthly; null when the cell has no monthly_supplement rows for the period",
	"daily_summary":                                    "computed at export from the station's daily rows and the cell's daily series",
	"daily_summary.source":                             "the block's source tag (" + sourceBioclimaDerived + ")",
	"daily_summary.coverage":                           "the extents of the two daily series",
	"daily_summary.coverage.observed":                  "the station's observed daily extent; null for a station with no daily rows",
	"daily_summary.coverage.observed.first_date":       "first observed date",
	"daily_summary.coverage.observed.last_date":        "last observed date",
	"daily_summary.coverage.observed.days_with_obs":    "days with at least one of the four observed variables non-null",
	"daily_summary.coverage.observed.days_by_variable": "non-null days per observed variable",
	"daily_summary.coverage.observed.days_by_variable.tmax_c":    "non-null days of tmax_c",
	"daily_summary.coverage.observed.days_by_variable.tmin_c":    "non-null days of tmin_c",
	"daily_summary.coverage.observed.days_by_variable.precip_mm": "non-null days of precip_mm",
	"daily_summary.coverage.observed.days_by_variable.evap_mm":   "non-null days of evap_mm",
	"daily_summary.coverage.reanalysis":                          "the cell's whole daily_supplement extent — not clipped to the station's dates; null for a station with no cell or a cell with no rows",
	"daily_summary.coverage.reanalysis.first_date":               "first date of the cell's series",
	"daily_summary.coverage.reanalysis.last_date":                "last date of the cell's series",
	"daily_summary.coverage.reanalysis.days":                     "rows of the cell's series",
	"daily_summary.extremes":                                     "the station's all-time records from observed daily rows only, each with its date (earliest on ties); a record is null when the variable has no non-null day",
	"daily_summary.extremes.source":                              "the block's source tag (" + sourceConaguaObserved + ")",
	"daily_summary.extremes.record_tmax_c":                       "highest observed daily maximum temperature",
	"daily_summary.extremes.record_tmax_c.value":                 "the record value",
	"daily_summary.extremes.record_tmax_c.date":                  "the date it was observed",
	"daily_summary.extremes.record_tmin_c":                       "lowest observed daily minimum temperature",
	"daily_summary.extremes.record_tmin_c.value":                 "the record value",
	"daily_summary.extremes.record_tmin_c.date":                  "the date it was observed",
	"daily_summary.extremes.record_precip_mm_1day":               "highest observed one-day precipitation total",
	"daily_summary.extremes.record_precip_mm_1day.value":         "the record value",
	"daily_summary.extremes.record_precip_mm_1day.date":          "the date it was observed",
	"daily_summary.dry_spell":                                    "the longest run of consecutive calendar days with observed precip_mm equal to zero; a NULL day or a missing date ends a run, the earliest run wins a tie; null when no dry day exists",
	"daily_summary.dry_spell.longest_dry_run_days":               "length of the run in days",
	"daily_summary.dry_spell.start_date":                         "first day of the run",
	"daily_summary.dry_spell.end_date":                           "last day of the run",
	"meta":                                                       "the build's provenance, identical in every profile of a deposit",
	"meta.schema_version":                                        "schema.sql's version, also PRAGMA user_version of the shipped SQLite files",
	"meta.etl_git_sha":                                           "git commit of the ETL binary that built the deposit",
	"meta.snapshot_date":                                         "the CONAGUA pull snapshot the deposit was built from",
	"meta.runs":                                                  "the runs the shipped rows trace to, by natural label",
	"meta.runs.ingest":                                           "ingest runs by snapshot_date",
	"meta.runs.power":                                            "power runs by run_label",
	"meta.license":                                               "SPDX identifier of the compilation's license",
	"meta.suggested_citation":                                    "the suggested citation of the dataset",
}

// jsonKeyOf reads a struct field's JSON key; ok is false for a field the
// encoder skips.
func jsonKeyOf(f reflect.StructField) (key string, ok bool) {
	tag := f.Tag.Get("json")
	if tag == "" || tag == "-" {
		return "", false
	}
	key, _, _ = strings.Cut(tag, ",")
	return key, key != ""
}

// profileNodes walks a struct type's fields in declaration order — the
// order the encoder writes them.
func profileNodes(t reflect.Type, path string, fileColumns map[string]map[string]DictionaryColumn) ([]DictionaryNode, error) {
	nodes := make([]DictionaryNode, 0, t.NumField())
	for i := range t.NumField() {
		f := t.Field(i)
		key, ok := jsonKeyOf(f)
		if !ok {
			return nil, fmt.Errorf("%s.%s has no json key", t.Name(), f.Name)
		}
		node, err := profileNode(f.Type, t, joinPath(path, key), key, fileColumns)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func joinPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

// profileNode describes one field: its JSON type from the Go type, its
// children from a nested struct, a period map, or a month-series block.
func profileNode(ft, parent reflect.Type, path, key string, fileColumns map[string]map[string]DictionaryColumn,
) (DictionaryNode, error) {
	desc, ok := profileDescriptions[path]
	if !ok {
		return DictionaryNode{}, fmt.Errorf("profile key %s has no description", path)
	}
	n := DictionaryNode{Key: key, Description: desc, Children: []DictionaryNode{}}
	nullable := ft.Kind() == reflect.Pointer
	if nullable {
		ft = ft.Elem()
	}
	var err error
	switch {
	case ft == jsonTextType:
		n.Type, nullable = "string", true
	case ft == jsonIntType:
		n.Type, nullable = "integer", true
	case ft == jsonRealType:
		n.Type, nullable = "number", true
		decimals, ok := profileDecimals[path]
		if !ok {
			return DictionaryNode{}, fmt.Errorf("profile key %s has no decimals", path)
		}
		n.Decimals = &decimals
		n.Unit = optional(profileUnits[path])
	case ft.Kind() == reflect.String:
		n.Type = "string"
	case ft.Kind() == reflect.Int || ft.Kind() == reflect.Int64:
		n.Type = "integer"
	case ft.Kind() == reflect.Slice && ft.Elem().Kind() == reflect.String:
		n.Type = "array of string"
	case ft.Kind() == reflect.Map && ft.Key().Kind() == reflect.String:
		n.Type = "object keyed by period"
		child := DictionaryNode{Key: placeholderPeriod, Type: "object | null", Children: []DictionaryNode{}}
		if child.Description, ok = profileDescriptions[joinPath(path, placeholderPeriod)]; !ok {
			return DictionaryNode{}, fmt.Errorf("profile key %s has no description", joinPath(path, placeholderPeriod))
		}
		switch elem := ft.Elem(); {
		case elem == monthBlockPtr:
			child.Children, err = seriesNodes(parent, fileColumns)
		case elem.Kind() == reflect.Pointer && elem.Elem().Kind() == reflect.Struct:
			child.Children, err = profileNodes(elem.Elem(), joinPath(path, placeholderPeriod), fileColumns)
		default:
			err = fmt.Errorf("profile key %s: unsupported map element %s", path, elem)
		}
		n.Children = []DictionaryNode{child}
	case ft.Kind() == reflect.Struct:
		n.Type = "object"
		n.Children, err = profileNodes(ft, path, fileColumns)
	default:
		return DictionaryNode{}, fmt.Errorf("profile key %s: unsupported type %s", path, ft)
	}
	if err != nil {
		return DictionaryNode{}, err
	}
	if nullable {
		n.Type += " | null"
	}
	return n, nil
}

// seriesNodes lists a period block's variables — the flat file's value
// columns, each a positional month series — with the file's decimals,
// unit, and description.
func seriesNodes(block reflect.Type, fileColumns map[string]map[string]DictionaryColumn) ([]DictionaryNode, error) {
	spec, ok := periodSpecs[block]
	if !ok {
		return nil, fmt.Errorf("profile block %s has no month-series spec", block.Name())
	}
	cols := fileColumns[spec.Name]
	nodes := make([]DictionaryNode, 0, len(spec.Columns))
	for _, c := range valueColumns(spec) {
		fc, ok := cols[c.Name]
		if !ok {
			return nil, fmt.Errorf("profile block %s: %s has no dictionary column %s", block.Name(), spec.Name, c.Name)
		}
		nodes = append(nodes, DictionaryNode{
			Key:         c.Name,
			Type:        fmt.Sprintf("array[%d] of %s | null", monthSlots, jsonTypeName(c.Kind)),
			Decimals:    fc.Decimals,
			Unit:        fc.Unit,
			Description: fc.Description,
			Children:    []DictionaryNode{},
		})
	}
	return nodes, nil
}

// annualNames renders the annual aggregation names.
var annualNames = map[annualKind]string{
	annualMean:     "mean",
	annualSum:      "sum",
	annualCircular: "circular mean",
}

// buildAnnual lists the variables that carry a derived annual — the
// normals variables, then POWER-31 in registry order — with each one's
// aggregation from the profile's own dispatch table.
func buildAnnual() (DictionaryAnnual, error) {
	a := DictionaryAnnual{
		Slots:     monthSlots,
		Tag:       sourceBioclimaDerived,
		Variables: []DictionaryAggregation{},
		Rules: []string{
			fmt.Sprintf("Every month series in profile.json has %d slots: slots 0–11 are the calendar months 1–12 as published, slot 12 is the annual, recomputed at export from slots 0–11 (unweighted) and tagged %s by the block's annual_slot key; slots 0–11 keep the block's source tag.", monthSlots, sourceBioclimaDerived),
			"The derived annual lives in profile.json only: conagua/monthly_normals stays months 1–12, a pure source mirror.",
			"Slot 12 is null unless all twelve months are non-null (provisional, subject to confirmation in a later version): a partial sum understates a total and a partial mean is seasonally biased.",
			"The extras series carry no annual: their slot 12 is always null (provisional: no annual aggregation is defined for the extras columns yet; subject to confirmation in a later version).",
			"Never a uniform arithmetic mean over precip_mm / evap_mm: they are monthly totals and their annual is a sum. The unit suffix tells the aggregation — precip_mm sums, precip_mmpd (a rate) averages.",
			"The recompute reproduces CONAGUA's own published annual for every sum and for every mean that does not sit on an exact decimal tie; when the twelve one-decimal months sum to an exact half at the next decimal, CONAGUA's rounding follows no decimal rule and the derived mean may differ from the published value by one unit in the last decimal (provisional: the correctly rounded mean is kept; subject to confirmation in a later version).",
		},
	}
	for _, c := range append(valueColumns(MonthlyNormals), valueColumns(PowerMonthly)...) {
		kind, ok := annualBy[c.Name]
		if !ok {
			return DictionaryAnnual{}, fmt.Errorf("annual: %s has no aggregation", c.Name)
		}
		a.Variables = append(a.Variables, DictionaryAggregation{Name: c.Name, Aggregation: annualNames[kind]})
	}
	return a, nil
}

// formatRules is the format contract, one rule per topic.
var formatRules = []DictionaryRule{
	{Topic: "nulls", Rule: "CSV: an empty field. JSON: an explicit null, never an omitted key — including an empty slot of a month series, where position is load-bearing. Parquet: a native null. Never a sentinel value: a magic number is an invented gap. No nullable TEXT column ever holds an empty string, so an empty CSV field is unambiguously NULL."},
	{Topic: "dates and periods", Rule: "Dates are YYYY-MM-DD; reference periods are YYYY-YYYY; month is the integer 1–12 in the flat files and positional in profile.json (slot 0 is January)."},
	{Topic: "text", Rule: "TEXT columns pass through verbatim as stored: run timestamps are the RFC 3339 UTC strings ingest and power wrote, status is English as stored, state is CONAGUA's uppercase code, parameters and unit_conversions are the stored strings."},
	{Topic: "numbers", Rule: "Every numeric column is written with its pinned decimal count (the precision section) in CSV, JSON, and Parquet alike; Parquet stores the nearest double of the rounded value, so the three flat formats carry identical values and only the SQLite files carry the native full-precision value. A value that rounds to zero is written as positive zero. JSON numbers go through the same formatter as CSV."},
	{Topic: "encoding", Rule: "UTF-8 without BOM, LF line endings, a header row, RFC 4180 quoting, '.' as the decimal separator, no thousands separator."},
	{Topic: "sort", Rule: "Rows sort by the file's exported natural key ascending, TEXT keys bytewise — station_id is an opaque string, never a number. This is what makes the flat files deterministic."},
	{Topic: "reproducibility", Rule: "CSV and JSON are byte-reproducible from the same database and binary. Parquet and the SQLite files are content-reproducible: the same values on every re-export, bytes not guaranteed."},
}

// buildPrecision renders schema.Precision grouped by decimal count,
// tables and columns in DDL order, the export name after an arrow where
// the export renames the column.
func buildPrecision(tables []schema.Table) []DictionaryPrecision {
	byDecimals := map[int][]string{}
	for _, t := range tables {
		for _, c := range t.Columns {
			decimals, ok := schema.Decimals(t.Name, c.Name)
			if !ok {
				continue
			}
			name := t.Name + "." + c.Name
			if export := ExportName(t.Name, c.Name); export != c.Name {
				name += " → " + export
			}
			byDecimals[decimals] = append(byDecimals[decimals], name)
		}
	}
	counts := make([]int, 0, len(byDecimals))
	for d := range byDecimals {
		counts = append(counts, d)
	}
	slices.Sort(counts)
	out := make([]DictionaryPrecision, 0, len(counts))
	for _, d := range counts {
		out = append(out, DictionaryPrecision{Decimals: d, Columns: byDecimals[d]})
	}
	return out
}

// buildDropped lists Dropped in DDL order.
func buildDropped(tables []schema.Table) []DictionaryDropped {
	out := []DictionaryDropped{}
	for _, t := range tables {
		for _, c := range t.Columns {
			reason, ok := Dropped[t.Name][c.Name]
			if !ok {
				continue
			}
			out = append(out, DictionaryDropped{Table: t.Name, Column: c.Name, Reason: string(reason), Note: dropNotes[reason]})
		}
	}
	return out
}

// buildConstants lists the documented constants — every Dropped column
// of the constant category — with the value the owning code pins; a
// constant with no value is a declaration error.
func buildConstants(dropped []DictionaryDropped) ([]DictionaryConstant, error) {
	out := []DictionaryConstant{}
	for _, d := range dropped {
		if d.Reason != string(DropConstant) {
			continue
		}
		c, ok := constantValues[d.Table][d.Column]
		if !ok {
			return nil, fmt.Errorf("documented constant %s.%s has no value", d.Table, d.Column)
		}
		c.Table, c.Column = d.Table, d.Column
		out = append(out, c)
	}
	return out, nil
}

// buildSQLite describes the two SQLite artifacts.
func buildSQLite(tables []schema.Table, snapshotDate string) DictionarySQLite {
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t.Name
	}
	statePath, nationalPath := stateDBName(placeholderShard), nationalDBName
	return DictionarySQLite{
		StatePath:    statePath,
		NationalPath: nationalPath,
		Tables:       names,
		Notes: []string{
			fmt.Sprintf("%s sits at the root of %s; %s, the full national database, is the single entry of %s.",
				mdCode(statePath), mdCode(archiveName(placeholderShard, GroupTabular)), mdCode(nationalPath),
				mdCode(nationalArchiveName(GroupNationalSQLite, snapshotDate))),
			fmt.Sprintf("Both carry the full schema.sql: all %d tables, every index, the surrogate ids and FKs, and parsing_warnings, which ships nowhere else. Curation — the renames and drops above — applies to the flat files only, so the column names here are the DDL names (tmax, not tmax_c) and the native full-precision values are stored.", len(names)),
			"Row set of " + mdCode(statePath) + " (provisional, subject to confirmation in a later version): the state's CONAGUA conventional stations and every row keyed to them (monthly_normals, monthly_normals_extras, daily_observations, station_power_cell, parsing_warnings); the cells those stations reference and only those cells' monthly_supplement and daily_supplement rows; every ingest_runs and power_runs row — so every FK resolves in-file.",
			"Surrogate ids in " + mdCode(statePath) + " are the national " + mdCode(nationalPath) + " ids: join-compatible across state files and with the national database. sqlite_sequence continues from the state's MAX(id), so ids minted in a writable copy of a state file are not globally unique — treat it as a read-only extract.",
			fmt.Sprintf("Shipped in rollback-journal mode with PRAGMA user_version = %d, so a file opens from read-only media with no side files.", schema.Version),
		},
	}
}

// BuildDictionary assembles the dictionary for one build from the
// schema, the file specs, the POWER registry, the Profile shape, and
// meta — the build's identity — with no database access: the dictionary
// describes the shape of the files, which the data does not change. An
// exported column or profile key with no description, or a constant with
// no value, is refused rather than shipped blank.
func BuildDictionary(meta ProfileMeta) (*Dictionary, error) {
	tables := schema.Tables()
	ddl := indexDDL(tables)
	params := PowerParameters()
	byColumn := make(map[string]PowerParameter, len(params))
	for _, p := range params {
		byColumn[p.Column] = p
	}

	defs := dictionaryFileDefs()
	files := make([]DictionaryFile, 0, len(defs))
	fileColumns := make(map[string]map[string]DictionaryColumn, len(defs))
	for _, d := range defs {
		f, err := d.file(meta.SnapshotDate, ddl, byColumn)
		if err != nil {
			return nil, fmt.Errorf("dictionary: %w", err)
		}
		files = append(files, f)
		byName := make(map[string]DictionaryColumn, len(f.Columns))
		for _, c := range f.Columns {
			byName[c.Name] = c
		}
		fileColumns[f.Name] = byName
	}

	keys, err := profileNodes(reflect.TypeOf(Profile{}), "", fileColumns)
	if err != nil {
		return nil, fmt.Errorf("dictionary: %w", err)
	}
	dropped := buildDropped(tables)
	constants, err := buildConstants(dropped)
	if err != nil {
		return nil, fmt.Errorf("dictionary: %w", err)
	}
	annual, err := buildAnnual()
	if err != nil {
		return nil, fmt.Errorf("dictionary: %w", err)
	}
	combined := fileColumns[CombinedDaily.Name]
	daily := DictionaryDailyJSON{
		Path:       dailyJSONPath(placeholderShard, placeholderStation),
		Date:       dailyJSON.columns[0].Name,
		Observed:   pick(combined, dailyJSON.observed),
		Reanalysis: pick(combined, dailyJSON.reanalysis),
		Rules: []string{
			"A top-level array of row objects, date ascending, the same row set as combined/combined_daily (one product, two formats).",
			"Each row is {date, observed, reanalysis}: observed is always present (it is the spine) with per-field nulls for variables the station did not report that day; reanalysis is the whole object null when the cell has no daily_supplement row for the date, and the full object — per-field nulls included — when it has one.",
			"Layout: '[' on its own line, one compact row object per line joined by ',' and a line feed, ']' on its own line, a trailing line feed; an empty series is '[' and ']' on two lines. Numbers are the fixed-decimal literals the CSV carries.",
			"The join context (cell_id, distance_km) is per station, not per day, and lives in profile.json's power_cell block.",
		},
	}

	return &Dictionary{
		Dataset:       meta.Dataset,
		SchemaVersion: meta.SchemaVersion,
		SnapshotDate:  meta.SnapshotDate,
		ETLGitSHA:     meta.ETLGitSHA,
		Tags:          slices.Clone(dictionaryTags),
		Units:         slices.Concat(unitSuffixes, unitNames),
		Files:         files,
		Power:         params,
		Constants:     constants,
		Dropped:       dropped,
		Profile: DictionaryProfile{
			Path: profilePath(placeholderShard, placeholderStation),
			Notes: []string{
				"Two-space-indented JSON with a trailing line feed; keys in the order listed; every key present, every absent value an explicit null. Numbers are the fixed-decimal literals the CSV carries.",
				"A source tag sits on a block, never on an array element; slot 12 of every month series is " + sourceBioclimaDerived + " by the convention in the annual-slot section.",
				"No generation time: profiles are byte-reproducible; the build time is stamped in manifest.json only (provisional: subject to confirmation in a later version).",
			},
			Keys: keys,
		},
		DailyJSON: daily,
		Annual:    annual,
		Format:    slices.Clone(formatRules),
		Precision: buildPrecision(tables),
		SQLite:    buildSQLite(tables, meta.SnapshotDate),
	}, nil
}

// pick returns the dictionary columns of cols, in order.
func pick(byName map[string]DictionaryColumn, cols []Column) []DictionaryColumn {
	out := make([]DictionaryColumn, 0, len(cols))
	for _, c := range cols {
		out = append(out, byName[c.Name])
	}
	return out
}

// RenderDictionaryJSON writes d as DATA-DICTIONARY.json: two-space
// indented, HTML escaping off (an arrow or a degree sign reads as
// written), a trailing newline, every absent value an explicit null.
func RenderDictionaryJSON(w io.Writer, d *Dictionary) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(d); err != nil {
		return fmt.Errorf("encode data dictionary: %w", err)
	}
	return nil
}

// RenderDictionaryMarkdown writes d as DATA-DICTIONARY.md: deterministic
// Markdown in the QA report's style — LF line ends, a trailing newline,
// fixed section order, tables with pinned columns, no wall clock.
func RenderDictionaryMarkdown(w io.Writer, d *Dictionary) error {
	var b bytes.Buffer
	m := mdWriter{b: &b}
	m.dictionaryHeader(d)
	m.tagsSection(d)
	m.unitsSection(d)
	m.filesSection(d)
	m.powerSection(d)
	m.constantsSection(d)
	m.droppedSection(d)
	m.profileSection(d)
	m.dailyJSONSection(d)
	m.annualSection(d)
	m.formatSection(d)
	m.precisionSection(d)
	m.sqliteSection(d)
	out := append(bytes.TrimRight(b.Bytes(), "\n"), '\n')
	if _, err := w.Write(out); err != nil {
		return fmt.Errorf("write data dictionary: %w", err)
	}
	return nil
}

// The Markdown rendering of an absent value.
const mdNone = "—"

func mdOptional(s *string) string {
	if s == nil {
		return mdNone
	}
	return *s
}

func mdDecimals(n *int) string {
	if n == nil {
		return mdNone
	}
	return strconv.Itoa(*n)
}

func mdCodes(names []string) string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = mdCode(n)
	}
	return strings.Join(out, ", ")
}

func (m mdWriter) dictionaryHeader(d *Dictionary) {
	m.line("# Data dictionary — %s", d.Dataset.Title)
	m.blank()
	m.line("Version %s. Snapshot %s, schema version %d, ETL git SHA %s. License %s.",
		d.Dataset.Version, mdCode(d.SnapshotDate), d.SchemaVersion, mdCode(d.ETLGitSHA), d.Dataset.License)
	m.blank()
	m.line("Generated by `%s` from the file specs the archives are built", toolCommand)
	m.line("from, `schema.sql` (its trailing column comments where a column carries one,")
	m.line("the pinned precision annotation), the NASA POWER parameter registry, and the")
	m.line("profile writer's own shape, so every exported column of every flat file is")
	m.line("listed and the dictionary cannot drift from the files. %s is", DictionaryJSONName)
	m.line("the same content, machine-readable. No timestamp: two builds of the same")
	m.line("database and binary render the same bytes.")
	m.blank()
	m.line("Column order in every file: the natural key(s) first, in primary-key order,")
	m.line("then the value columns in `schema.sql` order, renamed where the export layer")
	m.line("pins a unit in the name (`tmax` → `tmax_c`, `external_id` → `station_id`).")
	m.line("Rows sort by the key. Kinds: text, date (YYYY-MM-DD), period (YYYY-YYYY),")
	m.line("integer, real (fixed decimals).")
	m.blank()
}

func (m mdWriter) tagsSection(d *Dictionary) {
	m.line("## 1. Source tags")
	m.blank()
	m.line("Provenance is carried by file and folder in the flat files and by an explicit")
	m.line("`source` on every block of profile.json. The tags:")
	m.blank()
	rows := make([][]string, 0, len(d.Tags))
	for _, t := range d.Tags {
		rows = append(rows, []string{mdCode(t.Tag), t.Meaning})
	}
	m.table([]string{"tag", "meaning"}, rows)
}

func (m mdWriter) unitsSection(d *Dictionary) {
	m.line("## 2. Units")
	m.blank()
	m.line("SI / metric throughout; the unit is pinned in the column-name suffix, or")
	m.line("by the column's name where it has none. Columns not listed — months, years,")
	m.line("counts, text — carry no unit.")
	m.blank()
	rows := make([][]string, 0, len(d.Units))
	for _, u := range d.Units {
		rows = append(rows, []string{mdCode(u.Pattern), u.Unit})
	}
	m.table([]string{"suffix or name", "unit"}, rows)
}

func (m mdWriter) filesSection(d *Dictionary) {
	m.line("## 3. Files")
	m.blank()
	m.line("One entry per logical file, in the order of the four scope folders:")
	m.line("`conagua/` (CONAGUA's own tables), `nasa_power/` (reanalysis by cell),")
	m.line("`combined/` (the CONAGUA spine joined to its cell), `provenance/` (the runs).")
	m.line("Paths are relative to an archive's root; `<state>` is the lowercase CONAGUA")
	m.line("state code, `<station_id>` the CONAGUA station id, `<cell_id>` the POWER cell key.")
	m.blank()
	for _, f := range d.Files {
		m.line("### %s", f.Name)
		m.blank()
		m.bullet("Table: " + mdCode(f.Table))
		m.bullet("Sort key: " + mdCodes(f.SortKey))
		m.bullet("Grain: " + f.Grain)
		m.bullet("Row set: " + f.RowSet)
		for _, n := range f.Notes {
			m.bullet(n)
		}
		m.blank()
		rows := make([][]string, 0, len(f.Paths))
		for _, p := range f.Paths {
			rows = append(rows, []string{p.Format, mdCode(p.Path), mdCodes(p.Archives)})
		}
		m.table([]string{"format", "path", "archives"}, rows)
		rows = rows[:0]
		for _, c := range f.Columns {
			source := c.Source
			if source != sourceSynthesized && source != sourceJoinContext {
				source = mdCode(source)
			}
			rows = append(rows, []string{mdCode(c.Name), c.Kind, mdDecimals(c.Decimals), mdOptional(c.Unit),
				source, mdOptional(c.Tag), c.Description})
		}
		m.table([]string{"column", "kind", "decimals", "unit", "source", "tag", "description"}, rows)
	}
}

func (m mdWriter) powerSection(d *Dictionary) {
	m.line("## 4. NASA POWER parameters")
	m.blank()
	m.line("The POWER parameter registry, in the order the supplement tables declare the")
	m.line("columns: the POWER id a run's `parameters` string carries, the column it lands")
	m.line("in, POWER's native unit, the unit stored, and the factor applied once at ingest.")
	m.line("Every run's `unit_conversions` carries the same factors per parameter.")
	m.blank()
	rows := make([][]string, 0, len(d.Power))
	for _, p := range d.Power {
		rows = append(rows, []string{mdCode(p.ID), mdCode(p.Column), p.PowerUnit, p.StoredUnit,
			strconv.FormatFloat(p.Factor, 'g', -1, 64)})
	}
	m.table([]string{"POWER id", "column", "native unit", "stored unit", "factor"}, rows)
}

func (m mdWriter) constantsSection(d *Dictionary) {
	m.line("## 5. Documented constants")
	m.blank()
	m.line("Columns of the database that hold one value in every row; the flat files omit")
	m.line("them and this section states the value once. The SQLite files keep them.")
	m.blank()
	rows := make([][]string, 0, len(d.Constants))
	for _, c := range d.Constants {
		rows = append(rows, []string{mdCode(c.Table + "." + c.Column), mdCode(c.Value), c.Reason})
	}
	m.table([]string{"column", "value", "reason"}, rows)
}

func (m mdWriter) droppedSection(d *Dictionary) {
	m.line("## 6. Dropped columns")
	m.blank()
	m.line("Every column of an exported table the flat files omit, with its category. The")
	m.line("SQLite files carry every column.")
	m.blank()
	rows := make([][]string, 0, len(d.Dropped))
	for _, c := range d.Dropped {
		rows = append(rows, []string{mdCode(c.Table + "." + c.Column), c.Reason, c.Note})
	}
	m.table([]string{"column", "category", "note"}, rows)
}

func (m mdWriter) profileSection(d *Dictionary) {
	m.line("## 7. profile.json")
	m.blank()
	m.line("The per-station citable profile, %s, sibling of daily.json.", mdCode(d.Profile.Path))
	m.blank()
	for _, n := range d.Profile.Notes {
		m.bullet(n)
	}
	m.blank()
	m.nodes(d.Profile.Keys, 0)
	m.blank()
}

// nodes writes a key tree as a nested list.
func (m mdWriter) nodes(nodes []DictionaryNode, depth int) {
	for _, n := range nodes {
		var extra []string
		if n.Decimals != nil {
			extra = append(extra, fmt.Sprintf("%d decimal%s", *n.Decimals, plural(*n.Decimals)))
		}
		if n.Unit != nil {
			extra = append(extra, *n.Unit)
		}
		typ := n.Type
		if len(extra) > 0 {
			typ += " (" + strings.Join(extra, ", ") + ")"
		}
		m.line("%s- %s — %s — %s", strings.Repeat("  ", depth), mdCode(n.Key), typ,
			strings.ReplaceAll(n.Description, "\n", " "))
		m.nodes(n.Children, depth+1)
	}
}

func (m mdWriter) dailyJSONSection(d *Dictionary) {
	m.line("## 8. daily.json")
	m.blank()
	m.line("The per-station daily series, %s.", mdCode(d.DailyJSON.Path))
	m.blank()
	for _, r := range d.DailyJSON.Rules {
		m.bullet(r)
	}
	m.blank()
	m.line("Row keys: %s (string, YYYY-MM-DD), `observed`, `reanalysis`.", mdCode(d.DailyJSON.Date))
	m.blank()
	m.line("### observed")
	m.blank()
	m.columnsTable(d.DailyJSON.Observed)
	m.line("### reanalysis")
	m.blank()
	m.columnsTable(d.DailyJSON.Reanalysis)
}

// columnsTable writes a block's fields as a column table without the
// source column, which the flat file's own table already carries.
func (m mdWriter) columnsTable(cols []DictionaryColumn) {
	rows := make([][]string, 0, len(cols))
	for _, c := range cols {
		rows = append(rows, []string{mdCode(c.Name), jsonTypeName(kindOfName(c.Kind)), mdDecimals(c.Decimals),
			mdOptional(c.Unit), mdOptional(c.Tag), c.Description})
	}
	m.table([]string{"key", "type", "decimals", "unit", "tag", "description"}, rows)
}

// kindOfName is kindName's inverse, for the JSON type of a rendered kind.
func kindOfName(name string) Kind {
	switch name {
	case "integer":
		return KindInt
	case "real":
		return KindReal
	case "date":
		return KindDate
	case "period":
		return KindPeriod
	default:
		return KindText
	}
}

func (m mdWriter) annualSection(d *Dictionary) {
	m.line("## 9. The annual slot")
	m.blank()
	for _, r := range d.Annual.Rules {
		m.bullet(r)
	}
	m.blank()
	m.line("Aggregation per variable (slot %d, tag %s):", d.Annual.Slots-1, mdCode(d.Annual.Tag))
	m.blank()
	rows := make([][]string, 0, len(d.Annual.Variables))
	for _, v := range d.Annual.Variables {
		rows = append(rows, []string{mdCode(v.Name), v.Aggregation})
	}
	m.table([]string{"variable", "annual"}, rows)
}

func (m mdWriter) formatSection(d *Dictionary) {
	m.line("## 10. Format contract")
	m.blank()
	rows := make([][]string, 0, len(d.Format))
	for _, r := range d.Format {
		rows = append(rows, []string{r.Topic, r.Rule})
	}
	m.table([]string{"topic", "rule"}, rows)
}

func (m mdWriter) precisionSection(d *Dictionary) {
	m.line("## 11. Precision")
	m.blank()
	m.line("The fixed decimal count of every exported numeric column, by database column")
	m.line("(the export name after the arrow where it differs). Source-terminating values")
	m.line("keep the source's real precision; computed values are rounded, which removes")
	m.line("fake precision rather than adding it.")
	m.blank()
	rows := make([][]string, 0, len(d.Precision))
	for _, p := range d.Precision {
		rows = append(rows, []string{strconv.Itoa(p.Decimals), mdCodes(p.Columns)})
	}
	m.table([]string{"decimals", "columns"}, rows)
}

func (m mdWriter) sqliteSection(d *Dictionary) {
	m.line("## 12. The SQLite files")
	m.blank()
	for _, n := range d.SQLite.Notes {
		m.bullet(n)
	}
	m.blank()
	m.line("Tables: %s.", mdCodes(d.SQLite.Tables))
	m.blank()
}
