package schema

// parseTables against the comment and constraint shapes schema.sql
// uses, each isolated in a synthetic DDL so one shape is held on its
// own: a column carrying a trailing comment under a comment block that
// is not read, a CHECK that spans lines, the last column before a
// table-level PRIMARY KEY, an inline key, a table with no comments, the
// comment lines skipped wherever they sit. Then the embedded DDL characterized column by
// column — every table's columns, types, NOT NULL flags, and key
// positions written out by hand from the file — so a change to
// schema.sql's shape fails here by name, not only by count.

import (
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// synth joins DDL lines the way schema.sql lays them out.
func synth(lines ...string) string { return strings.Join(lines, "\n") }

// shapeOf renders a parsed column as "name TYPE [NOT NULL] [PKn]" — the
// notation the characterization below is written in.
func shapeOf(c Column) string {
	s := c.Name + " " + c.Type
	if c.NotNull {
		s += " NOT NULL"
	}
	if c.PK > 0 {
		s += fmt.Sprintf(" PK%d", c.PK)
	}
	return s
}

func shapesOf(cols []Column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = shapeOf(c)
	}
	return out
}

// withType suffixes every name with a type, for the POWER-31 block.
func withType(names []string, typ string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = n + " " + typ
	}
	return out
}

var _ = Describe("parseTables on the shapes schema.sql uses", func() {
	It("gives a column its trailing comment and reads no block above it", func() {
		got, err := parseTables(synth(
			"CREATE TABLE IF NOT EXISTS t (",
			"    k TEXT NOT NULL,",
			"",
			"    -- Temperature (°C)",
			"    a REAL,  -- mean air temperature at 2 m",
			"    b REAL,  -- daily max at 2 m",
			"    c REAL",
			");",
		))
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]Table{{Name: "t", Columns: []Column{
			{Name: "k", Type: "TEXT", NotNull: true},
			{Name: "a", Type: "REAL", Comment: "mean air temperature at 2 m"},
			{Name: "b", Type: "REAL", Comment: "daily max at 2 m"},
			{Name: "c", Type: "REAL"},
		}}}))
	})

	It("reads a CHECK that spans lines as its column's continuation: the literal lines add no column, the flags stay the column's, a '--' inside a literal is code", func() {
		got, err := parseTables(synth(
			"CREATE TABLE IF NOT EXISTS t (",
			"    source TEXT NOT NULL",
			"      CHECK (source IN (",
			"        'conagua_conventional',",
			"        'era5',",
			"        'meta--legacy')),",
			"    period TEXT NOT NULL",
			"                CHECK (period IN ('1961-1990', '1971-2000', '1981-2010', '1991-2020')),",
			"    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),",
			"    temporal_mode TEXT NOT NULL DEFAULT 'monthly'",
			"                       CHECK (temporal_mode IN ('monthly', 'daily')),",
			"    note TEXT",
			");",
		))
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(1))
		Expect(got[0].Columns).To(Equal([]Column{
			{Name: "source", Type: "TEXT", NotNull: true},
			{Name: "period", Type: "TEXT", NotNull: true},
			{Name: "month", Type: "INTEGER", NotNull: true},
			{Name: "temporal_mode", Type: "TEXT", NotNull: true},
			{Name: "note", Type: "TEXT"},
		}))
	})

	It("closes on a table-level PRIMARY KEY after the last column: the key numbers its columns in key order, a comment above the key adds nothing, a REFERENCES folds no flag", func() {
		got, err := parseTables(synth(
			"CREATE TABLE IF NOT EXISTS t (",
			"    cell_id TEXT NOT NULL REFERENCES cells(cell_id),",
			"    period  TEXT NOT NULL,",
			"    month   INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),",
			"",
			"    -- Soil moisture (dimensionless 0..1 wetness)",
			"    gwet_top  REAL,  -- top-layer soil wetness",
			"",
			"    -- Reproducibility back-pointer: every supplement value can be",
			"    -- traced to the run that wrote it.",
			"    power_run_id INTEGER REFERENCES power_runs(id),",
			"",
			"    PRIMARY KEY (cell_id, period, month)",
			");",
			"",
			"CREATE TABLE IF NOT EXISTS u (",
			"    a TEXT NOT NULL,",
			"    b TEXT NOT NULL,",
			"    -- the natural key",
			"    PRIMARY KEY (b, a)  -- key order, not declaration order",
			");",
		))
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(2))
		Expect(got[0].Columns).To(Equal([]Column{
			{Name: "cell_id", Type: "TEXT", NotNull: true, PK: 1},
			{Name: "period", Type: "TEXT", NotNull: true, PK: 2},
			{Name: "month", Type: "INTEGER", NotNull: true, PK: 3},
			{Name: "gwet_top", Type: "REAL", Comment: "top-layer soil wetness"},
			{Name: "power_run_id", Type: "INTEGER"},
		}))
		Expect(got[1].Columns).To(Equal([]Column{
			{Name: "a", Type: "TEXT", NotNull: true, PK: 2},
			{Name: "b", Type: "TEXT", NotNull: true, PK: 1},
		}))
	})

	It("numbers an inline PRIMARY KEY as position 1 without implying NOT NULL, on a TEXT key and on a REFERENCES key alike", func() {
		got, err := parseTables(synth(
			"CREATE TABLE IF NOT EXISTS cells (",
			"    cell_id         TEXT PRIMARY KEY,",
			"    lat             REAL NOT NULL",
			");",
			"CREATE TABLE IF NOT EXISTS link (",
			"    station_id  INTEGER PRIMARY KEY REFERENCES stations(id),",
			"    cell_id     TEXT NOT NULL REFERENCES cells(cell_id)",
			");",
		))
		Expect(err).NotTo(HaveOccurred())
		Expect(shapesOf(got[0].Columns)).To(Equal([]string{"cell_id TEXT PK1", "lat REAL NOT NULL"}))
		Expect(shapesOf(got[1].Columns)).To(Equal([]string{"station_id INTEGER PK1", "cell_id TEXT NOT NULL"}))
	})

	It("parses a table with no comments to empty comments, and skips the index after it", func() {
		got, err := parseTables(synth(
			"CREATE TABLE IF NOT EXISTS daily (",
			"    station_id INTEGER NOT NULL REFERENCES stations(id),",
			"    date       TEXT NOT NULL,",
			"    tmax       REAL,",
			"    PRIMARY KEY (station_id, date)",
			");",
			"",
			"CREATE INDEX IF NOT EXISTS idx_daily ON daily(station_id, date);",
		))
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]Table{{Name: "daily", Columns: []Column{
			{Name: "station_id", Type: "INTEGER", NotNull: true, PK: 1},
			{Name: "date", Type: "TEXT", NotNull: true, PK: 2},
			{Name: "tmax", Type: "REAL"},
		}}}))
	})

	It("skips comment lines wherever they sit — the file header, a block separated by a blank line, one attached to the column below — and upper-cases a type token", func() {
		got, err := parseTables(synth(
			"-- schema_version: 1",
			"-- the file header",
			"",
			"CREATE TABLE IF NOT EXISTS t (",
			"    -- orphaned by the blank line below",
			"",
			"    a real,",
			"    -- attached: no blank line",
			"    b integer",
			");",
		))
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]Table{{Name: "t", Columns: []Column{
			{Name: "a", Type: "REAL"},
			{Name: "b", Type: "INTEGER"},
		}}}))
	})
})

// wantDDLShape is the embedded schema.sql, table by table in DDL order,
// each column as "name TYPE [NOT NULL] [PKn]" — written by hand from
// the file, never from the parser, so the two are held to each other.
var wantDDLShape = []struct {
	table   string
	columns []string
}{
	{"stations", []string{
		"id INTEGER PK1", "source TEXT NOT NULL", "external_id TEXT NOT NULL", "name TEXT NOT NULL",
		"state TEXT", "municipality TEXT", "lat REAL", "lon REAL", "altitude_m REAL", "status TEXT",
		"first_year INTEGER", "last_year INTEGER",
		"wmo_completeness_bin_1961_1990 REAL", "wmo_completeness_bin_1971_2000 REAL",
		"wmo_completeness_bin_1981_2010 REAL", "wmo_completeness_bin_1991_2020 REAL",
		"wmo_completeness_cont_1961_1990 REAL", "wmo_completeness_cont_1971_2000 REAL",
		"wmo_completeness_cont_1981_2010 REAL", "wmo_completeness_cont_1991_2020 REAL",
	}},
	{"monthly_normals", []string{
		"station_id INTEGER NOT NULL PK1", "period TEXT NOT NULL PK2", "month INTEGER NOT NULL PK3",
		"tmax REAL", "tmin REAL", "tmean REAL", "precip REAL", "evap REAL",
	}},
	{"monthly_normals_extras", []string{
		"station_id INTEGER NOT NULL PK1", "period TEXT NOT NULL PK2", "month INTEGER NOT NULL PK3",
		"tmax_monthly_extreme REAL", "tmax_monthly_extreme_year INTEGER", "tmax_daily_extreme REAL", "tmax_daily_extreme_date TEXT",
		"tmin_monthly_extreme REAL", "tmin_monthly_extreme_year INTEGER", "tmin_daily_extreme REAL", "tmin_daily_extreme_date TEXT",
		"precip_monthly_extreme REAL", "precip_monthly_extreme_year INTEGER", "precip_daily_extreme REAL", "precip_daily_extreme_date TEXT",
		"tmax_years_with_data INTEGER", "tmin_years_with_data INTEGER", "tmean_years_with_data INTEGER",
		"precip_years_with_data INTEGER", "evap_years_with_data INTEGER",
		"rain_days REAL", "rain_days_years_with_data INTEGER",
	}},
	{"daily_observations", []string{
		"station_id INTEGER NOT NULL PK1", "date TEXT NOT NULL PK2",
		"tmax REAL", "tmin REAL", "precip REAL", "evap REAL",
	}},
	{"parsing_warnings", []string{
		"id INTEGER PK1", "station_id INTEGER", "source_file TEXT", "line INTEGER",
		"severity TEXT NOT NULL", "issue TEXT NOT NULL",
	}},
	{"ingest_runs", []string{
		"id INTEGER PK1", "started_at TEXT NOT NULL", "finished_at TEXT", "snapshot_date TEXT NOT NULL",
		"sink_kind TEXT NOT NULL", "etl_git_sha TEXT", "status TEXT NOT NULL",
		"stations_attempted INTEGER", "stations_succeeded INTEGER", "stations_failed INTEGER",
		"daily_rows INTEGER", "normals_rows INTEGER", "extras_rows INTEGER", "warnings_total INTEGER",
	}},
	{"power_runs", []string{
		"id INTEGER PK1", "started_at TEXT NOT NULL", "finished_at TEXT", "status TEXT NOT NULL",
		"endpoint_url TEXT NOT NULL", "parameters TEXT NOT NULL", "community TEXT NOT NULL",
		"period_start_year INTEGER NOT NULL", "period_end_year INTEGER NOT NULL",
		"grid_resolution TEXT NOT NULL", "solar_conversion REAL NOT NULL",
		"unit_conversions TEXT", "temporal_mode TEXT NOT NULL",
		"period_start_date TEXT", "period_end_date TEXT",
		"cells_attempted INTEGER", "cells_succeeded INTEGER", "cells_failed INTEGER", "supplement_rows INTEGER",
		"etl_git_sha TEXT",
	}},
	{"nasa_power_grid_cells", []string{
		"cell_id TEXT PK1", "lat REAL NOT NULL", "lon REAL NOT NULL", "grid_resolution TEXT NOT NULL",
	}},
	{"station_power_cell", []string{
		"station_id INTEGER PK1", "cell_id TEXT NOT NULL", "distance_km REAL NOT NULL",
	}},
	{"monthly_supplement", append(append([]string{
		"cell_id TEXT NOT NULL PK1", "period TEXT NOT NULL PK2", "month INTEGER NOT NULL PK3",
	}, withType(power31, "REAL")...), "power_run_id INTEGER")},
	{"daily_supplement", append(append([]string{
		"cell_id TEXT NOT NULL PK1", "date TEXT NOT NULL PK2",
	}, withType(power31, "REAL")...), "power_run_id INTEGER")},
}

var _ = Describe("the embedded schema.sql, characterized column by column", func() {
	It("declares exactly the eleven tables with these columns, types, NOT NULL flags, and key positions, in this order", func() {
		got := Tables()
		Expect(got).To(HaveLen(len(wantDDLShape)))
		total := 0
		for i, want := range wantDDLShape {
			Expect(got[i].Name).To(Equal(want.table), "table %d", i)
			Expect(shapesOf(got[i].Columns)).To(Equal(want.columns), want.table)
			total += len(want.columns)
		}
		Expect(total).To(Equal(172))
	})

	It("carries the POWER-31 block between the key and power_run_id in both supplement tables, in registry order", func() {
		Expect(power31).To(HaveLen(31))
		for _, t := range Tables() {
			switch t.Name {
			case "monthly_supplement":
				Expect(shapesOf(t.Columns[3:34])).To(Equal(withType(power31, "REAL")))
			case "daily_supplement":
				Expect(shapesOf(t.Columns[2:33])).To(Equal(withType(power31, "REAL")))
			}
		}
	})
})
