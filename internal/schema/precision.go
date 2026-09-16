package schema

import "maps"

// Precision pins the fixed decimal count of every numeric column the
// export layer emits, keyed by DDL table name then DDL column name. It
// is kept next to schema.sql so the export formatter and the generated
// data dictionary read one source and cannot drift — the same
// single-source discipline the DDL itself has. The counts are the
// source's measured precision, column by column.
//
// Every count is fixed rather than left to a float's shortest
// representation, for two different reasons:
//
//   - Class A (source-terminating) matches the source's real precision,
//     which is not uniform across CONAGUA's own products. The daily
//     observations carry two decimals — precipitation publishes trace
//     ("inappreciable") rain as 0.01 mm, and evap/tmax/tmin carry
//     hundredths too — so one decimal would publish a trace-rain day as
//     a dry 0.0 and contradict the value the shipped SQLite holds for
//     the same observation. The three daily extremes in
//     monthly_normals_extras are single daily readings carried into the
//     normals sheet, so they carry two for the same reason; the normals
//     proper, the monthly extremes and rain_days do terminate at one.
//     POWER meteorology carries two, wind direction one; grid centroids
//     are exact at three (the 0.5° × 0.625° step), and station
//     coordinates are DMS-derived with truncation noise past six.
//   - Class B (computed, non-terminating) is rounded: the solar *_wm2
//     conversion tail (×1e6/86400), the great-circle distance_km, and
//     the WMO completeness fractions carry digits the source never
//     had, so rounding removes fake precision rather than adding it.
//
// Keys are DDL names; the export renames (tmax → tmax_c, external_id
// → station_id, …) are the export layer's job. Absent on purpose, as
// they are not exported numerics: the surrogate ids, the surrogate-FK
// plumbing (child-table station_id, exported as the TEXT external_id;
// power_run_id, dropped from the export), the documented constant
// power_runs.solar_conversion, and parsing_warnings, which ships only
// inside the SQLite artifacts. The specs hold every REAL/INTEGER column
// of the DDL to be either here or in their explicit allowlist of those
// exclusions, so a new DDL numeric cannot land unannotated.
var Precision = map[string]map[string]int{
	"stations": {
		"lat":        6,
		"lon":        6,
		"altitude_m": 1,
		"first_year": 0,
		"last_year":  0,
		// The eight k/36 and k/1080 completeness fractions have no
		// source precision; 4 is the minimum that keeps every
		// attainable value distinct after rounding.
		"wmo_completeness_bin_1961_1990":  4,
		"wmo_completeness_bin_1971_2000":  4,
		"wmo_completeness_bin_1981_2010":  4,
		"wmo_completeness_bin_1991_2020":  4,
		"wmo_completeness_cont_1961_1990": 4,
		"wmo_completeness_cont_1971_2000": 4,
		"wmo_completeness_cont_1981_2010": 4,
		"wmo_completeness_cont_1991_2020": 4,
	},
	"monthly_normals": {
		"month":  0,
		"tmax":   1,
		"tmin":   1,
		"tmean":  1,
		"precip": 1,
		"evap":   1,
	},
	"monthly_normals_extras": {
		"month":                       0,
		"tmax_monthly_extreme":        1,
		"tmax_monthly_extreme_year":   0,
		"tmax_daily_extreme":          2,
		"tmin_monthly_extreme":        1,
		"tmin_monthly_extreme_year":   0,
		"tmin_daily_extreme":          2,
		"precip_monthly_extreme":      1,
		"precip_monthly_extreme_year": 0,
		"precip_daily_extreme":        2,
		"tmax_years_with_data":        0,
		"tmin_years_with_data":        0,
		"tmean_years_with_data":       0,
		"precip_years_with_data":      0,
		"evap_years_with_data":        0,
		"rain_days":                   1,
		"rain_days_years_with_data":   0,
	},
	"daily_observations": {
		"tmax":   2,
		"tmin":   2,
		"precip": 2,
		"evap":   2,
	},
	"nasa_power_grid_cells": {
		"lat": 3,
		"lon": 3,
	},
	"station_power_cell": {
		"distance_km": 3,
	},
	"monthly_supplement": supplementPrecision(true),
	"daily_supplement":   supplementPrecision(false),
	"ingest_runs": {
		"stations_attempted": 0,
		"stations_succeeded": 0,
		"stations_failed":    0,
		"daily_rows":         0,
		"normals_rows":       0,
		"extras_rows":        0,
		"warnings_total":     0,
	},
	"power_runs": {
		"period_start_year": 0,
		"period_end_year":   0,
		"cells_attempted":   0,
		"cells_succeeded":   0,
		"cells_failed":      0,
		"supplement_rows":   0,
	},
}

// powerValuePrecision is the POWER-31 block, shared verbatim by
// monthly_supplement and daily_supplement (the two tables mirror the
// same 31 value columns in registry order): two decimals everywhere
// except the two wind directions at one.
var powerValuePrecision = map[string]int{
	// Temperature (°C)
	"t2m_c":     2,
	"t2m_max_c": 2,
	"t2m_min_c": 2,
	"t2m_wet_c": 2,
	"t2m_dew_c": 2,
	"ts_c":      2,
	"ts_max_c":  2,
	"ts_min_c":  2,
	// Humidity
	"rh2m_pct": 2,
	"qv2m_gkg": 2,
	// Wind
	"ws2m_ms":   2,
	"ws10m_ms":  2,
	"ws50m_ms":  2,
	"wd2m_deg":  1,
	"wd10m_deg": 1,
	// Solar — shortwave
	"solar_ghi_wm2":    2,
	"solar_dhi_wm2":    2,
	"solar_dni_wm2":    2,
	"solar_clrsky_wm2": 2,
	"clearness_index":  2,
	"par_wm2":          2,
	"uva_wm2":          2,
	"uvb_wm2":          2,
	// Solar — longwave
	"lw_dwn_wm2": 2,
	// Sky / cloud / atmosphere
	"cloud_amt_pct": 2,
	"ps_kpa":        2,
	// Moisture and evapotranspiration
	"precip_mmpd": 2,
	"evland_mmpd": 2,
	// Soil moisture
	"gwet_top":  2,
	"gwet_root": 2,
	"gwet_prof": 2,
}

// supplementPrecision returns a fresh copy of the POWER-31 block, with
// the integer month key added for the monthly table. Each table gets
// its own map so neither can be mutated through the other.
func supplementPrecision(monthly bool) map[string]int {
	m := make(map[string]int, len(powerValuePrecision)+1)
	maps.Copy(m, powerValuePrecision)
	if monthly {
		m["month"] = 0
	}
	return m
}

// Decimals reports the pinned decimal count for table.column. ok is
// false when the pair is not an exported numeric — a TEXT column, a
// surrogate id or FK, a documented constant, or an unknown name — so a
// caller can never format such a column as a number by accident.
func Decimals(table, column string) (decimals int, ok bool) {
	decimals, ok = Precision[table][column]
	return decimals, ok
}

// DDL returns the embedded schema.sql text verbatim. It is the source
// the data-dictionary generator reads for per-column unit comments,
// paired with Precision for the decimal counts.
func DDL() string {
	return ddl
}
