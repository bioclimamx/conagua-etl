package publish

import (
	"fmt"

	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// Kind classifies how a column is formatted.
type Kind int

// The formatting classes. Text, dates, and periods pass through
// verbatim as stored — ingest pins their shapes (YYYY-MM-DD, YYYY-YYYY)
// and the export never reformats them; integers and reals go through
// the fixed-decimal formatter.
const (
	KindText Kind = iota
	KindDate
	KindPeriod
	KindInt
	KindReal
)

// Column is one exported column: its export name, the DB column it
// reads, its Kind, its fixed decimals (from schema.Precision), and
// whether it is part of the sort key.
//
// DB names the DDL column of the owning FileSpec.Table the value is read
// from. A child table's station_id is the one indirection: the query
// resolves the surrogate through stations to external_id, so the
// surrogate value never reaches a file while the DDL column still counts
// as exported (not dropped) for the lockstep spec. Two exceptions: a
// synthesized column carries an empty DB (provenance/power_runs'
// run_label), and a ComposedSpec column's table is
// ComposedSpec.Source(c), not FileSpec.Table.
type Column struct {
	Name     string
	DB       string
	Kind     Kind
	Decimals int
	Key      bool
}

// FileSpec is one logical exported file: its archive path without the format suffix
// ("conagua/stations"), the DDL table it mirrors, and its columns in
// export order — natural key(s) first in primary-key order, then the
// value columns in DDL order, renamed by the export rename map.
type FileSpec struct {
	Name    string
	Table   string
	Columns []Column
}

// Header returns the export column names in order — the CSV header row.
func (f FileSpec) Header() []string {
	h := make([]string, len(f.Columns))
	for i, c := range f.Columns {
		h[i] = c.Name
	}
	return h
}

// DropReason is the category under which a DDL column is left out of
// the flat files: they carry natural keys only, no FK plumbing, and
// documented constants are stated once in the data dictionary.
type DropReason string

// The three reasons a DDL column is left out of the flat files.
const (
	DropSurrogate DropReason = "surrogate id"
	DropFK        DropReason = "FK plumbing"
	DropConstant  DropReason = "documented constant"
)

// Dropped records every DDL column of an exported table that the flat
// files omit, with its reason — the drop list of the per-file
// column spec, covering every exported table. A table absent here drops
// nothing. The SQLite artifacts carry every column regardless; curation
// applies to CSV/Parquet/JSON only.
var Dropped = map[string]map[string]DropReason{
	"stations":              {"id": DropSurrogate, "source": DropConstant},
	"nasa_power_grid_cells": {"grid_resolution": DropConstant},
	"monthly_supplement":    {"power_run_id": DropFK},
	"daily_supplement":      {"power_run_id": DropFK},
	"ingest_runs":           {"id": DropSurrogate},
	"power_runs":            {"id": DropSurrogate, "solar_conversion": DropConstant},
}

// observedRenames is the unit suffixing of CONAGUA's bare observed
// variables, shared by monthly_normals and daily_observations (the same
// five quantities in the same units).
var observedRenames = map[string]string{
	"tmax":   "tmax_c",
	"tmin":   "tmin_c",
	"tmean":  "tmean_c",
	"precip": "precip_mm",
	"evap":   "evap_mm",
}

// renames is the export-layer rename map, keyed by DDL table then
// column: a bare observed quantity takes its unit suffix. Only the value
// column of an extras extreme takes the suffix; its companion _year /
// _date columns keep their names. Columns absent here export under their
// DDL name.
var renames = map[string]map[string]string{
	"stations":           {"external_id": "station_id"},
	"monthly_normals":    observedRenames,
	"daily_observations": observedRenames,
	"monthly_normals_extras": {
		"tmax_monthly_extreme":   "tmax_monthly_extreme_c",
		"tmax_daily_extreme":     "tmax_daily_extreme_c",
		"tmin_monthly_extreme":   "tmin_monthly_extreme_c",
		"tmin_daily_extreme":     "tmin_daily_extreme_c",
		"precip_monthly_extreme": "precip_monthly_extreme_mm",
		"precip_daily_extreme":   "precip_daily_extreme_mm",
	},
}

// ExportName applies the rename map to a DB column (identity when
// unmapped).
func ExportName(table, column string) string {
	if name, ok := renames[table][column]; ok {
		return name
	}
	return column
}

// colDef declares one column of a file in export order. The export name
// and the decimal count are derived at init from the rename map and
// schema.Precision — never written here — so the two single sources
// stay the only places those values live.
type colDef struct {
	db   string
	kind Kind
	key  bool
}

// newFileSpec builds a FileSpec from its declaration. A numeric column
// with no Precision entry, or a text column that has one, is a
// programming error in the declaration table — impossible at runtime
// and caught at init, before any file is written.
func newFileSpec(name, table string, defs []colDef) FileSpec {
	cols := make([]Column, len(defs))
	for i, d := range defs {
		decimals, numeric := schema.Decimals(table, d.db)
		switch {
		case (d.kind == KindInt || d.kind == KindReal) && !numeric:
			panic(fmt.Sprintf("publish: %s.%s is declared numeric but schema.Precision does not pin it", table, d.db))
		case d.kind != KindInt && d.kind != KindReal && numeric:
			panic(fmt.Sprintf("publish: %s.%s is declared text but schema.Precision pins %d decimals", table, d.db, decimals))
		}
		cols[i] = Column{
			Name:     ExportName(table, d.db),
			DB:       d.db,
			Kind:     d.kind,
			Decimals: decimals,
			Key:      d.key,
		}
	}
	return FileSpec{Name: name, Table: table, Columns: cols}
}

// The conagua/ folder's file specs, built from the rename map +
// schema.Precision at init. Column declarations follow the export-order
// rule: natural key(s) first in primary-key order, then the value
// columns in DDL order; the lockstep specs hold both orders to the
// embedded schema.
var (
	// Stations is conagua/stations: one row per station, sorted by
	// station_id (external_id — the surrogate id and the single-valued
	// source column are dropped).
	Stations = newFileSpec("conagua/stations", "stations", []colDef{
		{db: "external_id", kind: KindText, key: true},
		{db: "name", kind: KindText},
		{db: "state", kind: KindText},
		{db: "municipality", kind: KindText},
		{db: "lat", kind: KindReal},
		{db: "lon", kind: KindReal},
		{db: "altitude_m", kind: KindReal},
		{db: "status", kind: KindText},
		{db: "first_year", kind: KindInt},
		{db: "last_year", kind: KindInt},
		{db: "wmo_completeness_bin_1961_1990", kind: KindReal},
		{db: "wmo_completeness_bin_1971_2000", kind: KindReal},
		{db: "wmo_completeness_bin_1981_2010", kind: KindReal},
		{db: "wmo_completeness_bin_1991_2020", kind: KindReal},
		{db: "wmo_completeness_cont_1961_1990", kind: KindReal},
		{db: "wmo_completeness_cont_1971_2000", kind: KindReal},
		{db: "wmo_completeness_cont_1981_2010", kind: KindReal},
		{db: "wmo_completeness_cont_1991_2020", kind: KindReal},
	})

	// MonthlyNormals is conagua/monthly_normals: CONAGUA's published
	// normals, months 1–12 only (the derived annual lives in the JSON
	// profile), sorted by (station_id, period, month).
	MonthlyNormals = newFileSpec("conagua/monthly_normals", "monthly_normals", []colDef{
		{db: "station_id", kind: KindText, key: true},
		{db: "period", kind: KindPeriod, key: true},
		{db: "month", kind: KindInt, key: true},
		{db: "tmax", kind: KindReal},
		{db: "tmin", kind: KindReal},
		{db: "tmean", kind: KindReal},
		{db: "precip", kind: KindReal},
		{db: "evap", kind: KindReal},
	})

	// MonthlyNormalsExtras is conagua/monthly_normals_extras: the
	// extremes, their years / dates, the AÑOS CON DATOS counts, and rain
	// days, sorted by (station_id, period, month).
	MonthlyNormalsExtras = newFileSpec("conagua/monthly_normals_extras", "monthly_normals_extras", []colDef{
		{db: "station_id", kind: KindText, key: true},
		{db: "period", kind: KindPeriod, key: true},
		{db: "month", kind: KindInt, key: true},
		{db: "tmax_monthly_extreme", kind: KindReal},
		{db: "tmax_monthly_extreme_year", kind: KindInt},
		{db: "tmax_daily_extreme", kind: KindReal},
		{db: "tmax_daily_extreme_date", kind: KindDate},
		{db: "tmin_monthly_extreme", kind: KindReal},
		{db: "tmin_monthly_extreme_year", kind: KindInt},
		{db: "tmin_daily_extreme", kind: KindReal},
		{db: "tmin_daily_extreme_date", kind: KindDate},
		{db: "precip_monthly_extreme", kind: KindReal},
		{db: "precip_monthly_extreme_year", kind: KindInt},
		{db: "precip_daily_extreme", kind: KindReal},
		{db: "precip_daily_extreme_date", kind: KindDate},
		{db: "tmax_years_with_data", kind: KindInt},
		{db: "tmin_years_with_data", kind: KindInt},
		{db: "tmean_years_with_data", kind: KindInt},
		{db: "precip_years_with_data", kind: KindInt},
		{db: "evap_years_with_data", kind: KindInt},
		{db: "rain_days", kind: KindReal},
		{db: "rain_days_years_with_data", kind: KindInt},
	})

	// DailyObservations is conagua/daily_observations: one file per
	// station under the state shard, sorted by date.
	DailyObservations = newFileSpec("conagua/daily_observations", "daily_observations", []colDef{
		{db: "station_id", kind: KindText, key: true},
		{db: "date", kind: KindDate, key: true},
		{db: "tmax", kind: KindReal},
		{db: "tmin", kind: KindReal},
		{db: "precip", kind: KindReal},
		{db: "evap", kind: KindReal},
	})
)
