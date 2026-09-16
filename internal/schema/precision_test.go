package schema

import (
	"database/sql"
	"maps"
	"slices"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// ddlColumn is one pragma_table_info row of the applied DDL — the name
// and declared type are what the lockstep specs need.
type ddlColumn struct {
	Name string
	Type string
}

// ddlTables applies the embedded DDL to a fresh database and returns
// every table's columns in declaration order, so the specs hold
// Precision to the DDL SQLite actually parses rather than to a regexp
// reading of the file.
func ddlTables() map[string][]ddlColumn {
	GinkgoHelper()
	db := mustOpen(tempDBPath())
	defer mustClose(db)

	rows, err := db.Query(
		`SELECT name FROM sqlite_master
		 WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		Expect(rows.Scan(&n)).To(Succeed())
		names = append(names, n)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())

	tables := make(map[string][]ddlColumn, len(names))
	for _, table := range names {
		tables[table] = tableColumns(db, table)
	}
	return tables
}

func tableColumns(db *sql.DB, table string) []ddlColumn {
	GinkgoHelper()
	rows, err := db.Query(
		`SELECT name, type FROM pragma_table_info(?) ORDER BY cid`, table)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	var cols []ddlColumn
	for rows.Next() {
		var c ddlColumn
		Expect(rows.Scan(&c.Name, &c.Type)).To(Succeed())
		cols = append(cols, c)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return cols
}

// isNumeric reports whether a declared column type is one of the two
// Precision must cover.
func isNumeric(declared string) bool {
	return declared == "REAL" || declared == "INTEGER"
}

// notExportedNumerics is the explicit allowlist of REAL/INTEGER columns
// that carry no precision because the export layer never emits them as
// numbers: the surrogate ids; the surrogate-FK plumbing (child-table
// station_id is exported as the TEXT external_id, power_run_id is
// dropped from the export); the documented constant solar_conversion; and
// parsing_warnings, which ships only inside the SQLite artifacts. A new
// DDL numeric must be added here or to Precision — never left silently
// unannotated.
var notExportedNumerics = map[string][]string{
	"stations":               {"id"},
	"monthly_normals":        {"station_id"},
	"monthly_normals_extras": {"station_id"},
	"daily_observations":     {"station_id"},
	"parsing_warnings":       {"id", "station_id", "line"},
	"ingest_runs":            {"id"},
	"power_runs":             {"id", "solar_conversion"},
	"station_power_cell":     {"station_id"},
	"monthly_supplement":     {"power_run_id"},
	"daily_supplement":       {"power_run_id"},
}

// power31 is the POWER value block in schema DDL (= registry) order,
// transcribed by hand from the published POWER-31 column list.
var power31 = []string{
	"t2m_c", "t2m_max_c", "t2m_min_c", "t2m_wet_c", "t2m_dew_c",
	"ts_c", "ts_max_c", "ts_min_c",
	"rh2m_pct", "qv2m_gkg",
	"ws2m_ms", "ws10m_ms", "ws50m_ms", "wd2m_deg", "wd10m_deg",
	"solar_ghi_wm2", "solar_dhi_wm2", "solar_dni_wm2", "solar_clrsky_wm2",
	"clearness_index", "par_wm2", "uva_wm2", "uvb_wm2",
	"lw_dwn_wm2",
	"cloud_amt_pct", "ps_kpa",
	"precip_mmpd", "evland_mmpd",
	"gwet_top", "gwet_root", "gwet_prof",
}

// wantPower31 is the precision rule for the block — two decimals except the
// two wind directions at one — applied to the transcribed name list.
func wantPower31() map[string]int {
	m := make(map[string]int, len(power31))
	for _, c := range power31 {
		m[c] = 2
	}
	m["wd2m_deg"] = 1
	m["wd10m_deg"] = 1
	return m
}

var _ = Describe("Precision", func() {
	It("names only existing REAL/INTEGER columns of existing tables", func() {
		tables := ddlTables()
		for table, cols := range Precision {
			Expect(tables).To(HaveKey(table), "Precision names a table the DDL lacks: %s", table)
			byName := make(map[string]string, len(tables[table]))
			for _, c := range tables[table] {
				byName[c.Name] = c.Type
			}
			for column := range cols {
				Expect(byName).To(HaveKey(column),
					"Precision names a column the DDL lacks: %s.%s", table, column)
				Expect(isNumeric(byName[column])).To(BeTrue(),
					"%s.%s is declared %s, not REAL/INTEGER", table, column, byName[column])
			}
		}
	})

	It("annotates every REAL/INTEGER column of the DDL, or allowlists it explicitly", func() {
		for table, cols := range ddlTables() {
			for _, c := range cols {
				if !isNumeric(c.Type) {
					continue
				}
				_, annotated := Precision[table][c.Name]
				allowlisted := slices.Contains(notExportedNumerics[table], c.Name)
				Expect(annotated != allowlisted).To(BeTrue(),
					"%s.%s (%s) must be in exactly one of Precision or notExportedNumerics "+
						"(annotated=%t, allowlisted=%t)", table, c.Name, c.Type, annotated, allowlisted)
			}
		}
	})

	It("allowlists only columns the DDL actually declares", func() {
		tables := ddlTables()
		for table, cols := range notExportedNumerics {
			Expect(tables).To(HaveKey(table), "allowlist names a table the DDL lacks: %s", table)
			names := make([]string, 0, len(tables[table]))
			for _, c := range tables[table] {
				names = append(names, c.Name)
			}
			for _, column := range cols {
				Expect(names).To(ContainElement(column),
					"allowlist names a column the DDL lacks: %s.%s", table, column)
			}
		}
	})

	It("carries one identical POWER-31 block in both supplement tables", func() {
		monthly := maps.Clone(Precision["monthly_supplement"])
		daily := Precision["daily_supplement"]

		Expect(monthly).To(HaveKeyWithValue("month", 0))
		delete(monthly, "month")
		Expect(daily).NotTo(HaveKey("month"), "daily_supplement is keyed by date, not month")

		Expect(monthly).To(HaveLen(31))
		Expect(daily).To(Equal(monthly))
	})

	It("keeps the observed values and the daily extremes apart from the derived normals", func() {
		for _, c := range []string{"tmax", "tmin", "precip", "evap"} {
			Expect(Precision["daily_observations"]).To(HaveKeyWithValue(c, 2),
				"daily_observations.%s is a raw observation and carries two decimals", c)
		}
		for _, c := range []string{"tmax", "tmin", "tmean", "precip", "evap"} {
			Expect(Precision["monthly_normals"]).To(HaveKeyWithValue(c, 1),
				"monthly_normals.%s is a published normal and terminates at one decimal", c)
		}
		for _, c := range []string{"tmax_daily_extreme", "tmin_daily_extreme", "precip_daily_extreme"} {
			Expect(Precision["monthly_normals_extras"]).To(HaveKeyWithValue(c, 2),
				"%s is a single daily reading carried into the normals sheet", c)
		}
		for _, c := range []string{
			"tmax_monthly_extreme", "tmin_monthly_extreme", "precip_monthly_extreme", "rain_days",
		} {
			Expect(Precision["monthly_normals_extras"]).To(HaveKeyWithValue(c, 1),
				"%s is a monthly aggregate and terminates at one decimal", c)
		}
	})

	// CONAGUA publishes trace ("inappreciable") rain as 0.01 mm. At one
	// decimal it renders "0.0" — a dry day — which both loses the
	// observation and contradicts the value the shipped SQLite holds for
	// the same row, so the pinned count has to preserve it.
	It("keeps trace precipitation distinguishable from a dry day", func() {
		const traceRainMM = 0.01
		for _, col := range []struct{ table, column string }{
			{"daily_observations", "precip"},
			{"monthly_normals_extras", "precip_daily_extreme"},
		} {
			decimals, ok := Decimals(col.table, col.column)
			Expect(ok).To(BeTrue(), "%s.%s must be an exported numeric", col.table, col.column)

			rendered := strconv.FormatFloat(traceRainMM, 'f', decimals, 64)
			Expect(rendered).To(Equal("0.01"), "%s.%s renders trace rain as %s", col.table, col.column, rendered)
			Expect(strconv.ParseFloat(rendered, 64)).To(Equal(traceRainMM))
		}
	})

	It("pins the export precision column for column", func() {
		monthlySupplement := wantPower31()
		monthlySupplement["month"] = 0

		want := map[string]map[string]int{
			"stations": {
				"lat": 6, "lon": 6, "altitude_m": 1, "first_year": 0, "last_year": 0,
				// 4 decimals for the eight completeness fractions.
				"wmo_completeness_bin_1961_1990": 4, "wmo_completeness_bin_1971_2000": 4,
				"wmo_completeness_bin_1981_2010": 4, "wmo_completeness_bin_1991_2020": 4,
				"wmo_completeness_cont_1961_1990": 4, "wmo_completeness_cont_1971_2000": 4,
				"wmo_completeness_cont_1981_2010": 4, "wmo_completeness_cont_1991_2020": 4,
			},
			"monthly_normals": {
				"month": 0, "tmax": 1, "tmin": 1, "tmean": 1, "precip": 1, "evap": 1,
			},
			"monthly_normals_extras": {
				"month":                0,
				"tmax_monthly_extreme": 1, "tmax_monthly_extreme_year": 0, "tmax_daily_extreme": 2,
				"tmin_monthly_extreme": 1, "tmin_monthly_extreme_year": 0, "tmin_daily_extreme": 2,
				"precip_monthly_extreme": 1, "precip_monthly_extreme_year": 0, "precip_daily_extreme": 2,
				"tmax_years_with_data": 0, "tmin_years_with_data": 0, "tmean_years_with_data": 0,
				"precip_years_with_data": 0, "evap_years_with_data": 0,
				"rain_days": 1, "rain_days_years_with_data": 0,
			},
			"daily_observations": {
				"tmax": 2, "tmin": 2, "precip": 2, "evap": 2,
			},
			"nasa_power_grid_cells": {"lat": 3, "lon": 3},
			"station_power_cell":    {"distance_km": 3},
			"monthly_supplement":    monthlySupplement,
			"daily_supplement":      wantPower31(),
			"ingest_runs": {
				"stations_attempted": 0, "stations_succeeded": 0, "stations_failed": 0,
				"daily_rows": 0, "normals_rows": 0, "extras_rows": 0, "warnings_total": 0,
			},
			"power_runs": {
				"period_start_year": 0, "period_end_year": 0,
				"cells_attempted": 0, "cells_succeeded": 0, "cells_failed": 0, "supplement_rows": 0,
			},
		}
		Expect(Precision).To(Equal(want))
	})
})

var _ = Describe("Decimals", func() {
	DescribeTable("round-trips known entries and rejects the rest",
		func(table, column string, wantDecimals int, wantOK bool) {
			decimals, ok := Decimals(table, column)
			Expect(ok).To(Equal(wantOK))
			Expect(decimals).To(Equal(wantDecimals))
		},
		Entry("station lat", "stations", "lat", 6, true),
		Entry("altitude", "stations", "altitude_m", 1, true),
		Entry("a completeness fraction", "stations", "wmo_completeness_cont_1991_2020", 4, true),
		Entry("a normals month", "monthly_normals", "month", 0, true),
		Entry("an extras daily extreme", "monthly_normals_extras", "precip_daily_extreme", 2, true),
		Entry("an extras monthly extreme", "monthly_normals_extras", "precip_monthly_extreme", 1, true),
		Entry("an extras year", "monthly_normals_extras", "tmax_monthly_extreme_year", 0, true),
		Entry("a daily observation", "daily_observations", "evap", 2, true),
		Entry("a grid-cell coordinate", "nasa_power_grid_cells", "lon", 3, true),
		Entry("distance_km", "station_power_cell", "distance_km", 3, true),
		Entry("a monthly POWER temperature", "monthly_supplement", "t2m_c", 2, true),
		Entry("a daily POWER wind direction", "daily_supplement", "wd10m_deg", 1, true),
		Entry("an ingest counter", "ingest_runs", "warnings_total", 0, true),
		Entry("a power period bound", "power_runs", "period_end_year", 0, true),
		Entry("a TEXT column", "stations", "name", 0, false),
		Entry("a surrogate id", "stations", "id", 0, false),
		Entry("a child-table surrogate FK", "daily_observations", "station_id", 0, false),
		Entry("FK plumbing", "daily_supplement", "power_run_id", 0, false),
		Entry("the documented constant solar_conversion", "power_runs", "solar_conversion", 0, false),
		Entry("an unexported table", "parsing_warnings", "line", 0, false),
		Entry("an unknown column", "stations", "elevation", 0, false),
		Entry("an unknown table", "no_such_table", "lat", 0, false),
	)
})

var _ = Describe("DDL", func() {
	It("returns the embedded schema.sql verbatim, carrying the version header", func() {
		Expect(DDL()).To(Equal(ddl))
		Expect(DDL()).To(HavePrefix("-- schema_version: "))
		Expect(mustParseVersion(DDL())).To(Equal(Version))
	})
})
