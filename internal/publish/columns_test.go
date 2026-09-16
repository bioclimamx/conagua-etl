package publish_test

import (
	"database/sql"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// ddlColumn is one pragma_table_info row of the applied DDL: name,
// declared type, declaration position, and 1-based primary-key position
// (0 when not part of the PK).
type ddlColumn struct {
	Name string
	Type string
	CID  int
	PK   int
}

// ddlColumns returns table's columns in declaration order from a DB
// that applied schema.DDL(), so the lockstep specs hold the file specs
// to the schema SQLite actually parses.
func ddlColumns(db *sql.DB, table string) []ddlColumn {
	GinkgoHelper()
	rows, err := db.Query(`SELECT name, type, cid, pk FROM pragma_table_info(?) ORDER BY cid`, table)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	var cols []ddlColumn
	for rows.Next() {
		var c ddlColumn
		Expect(rows.Scan(&c.Name, &c.Type, &c.CID, &c.PK)).To(Succeed())
		cols = append(cols, c)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return cols
}

// sliceSpecs lists every FileSpec the package exports, so the lockstep
// covers every flat file.
var sliceSpecs = []publish.FileSpec{
	publish.Stations, publish.MonthlyNormals, publish.MonthlyNormalsExtras, publish.DailyObservations,
}

var _ = Describe("FileSpec DDL lockstep", func() {
	var db *sql.DB

	BeforeEach(func() {
		db = openTempDB()
		Expect(schema.DDL()).To(HavePrefix("-- schema_version: "))
	})

	It("exports or drops every DDL column of each exported table, exactly once", func() {
		for _, spec := range sliceSpecs {
			cols := ddlColumns(db, spec.Table)
			Expect(cols).NotTo(BeEmpty(), "spec %s names a table the DDL lacks: %s", spec.Name, spec.Table)

			exported := map[string]bool{}
			for _, c := range spec.Columns {
				Expect(exported).NotTo(HaveKey(c.DB), "%s exports %s twice", spec.Name, c.DB)
				exported[c.DB] = true
			}
			for _, c := range cols {
				_, dropped := publish.Dropped[spec.Table][c.Name]
				Expect(exported[c.Name] != dropped).To(BeTrue(),
					"%s.%s must be in exactly one of %s's columns or Dropped (exported=%t, dropped=%t)",
					spec.Table, c.Name, spec.Name, exported[c.Name], dropped)
			}
			for db := range exported {
				Expect(slices.ContainsFunc(cols, func(c ddlColumn) bool { return c.Name == db })).To(BeTrue(),
					"%s reads a column the DDL lacks: %s.%s", spec.Name, spec.Table, db)
			}
		}
	})

	It("types every column by its DDL declaration and pins decimals from schema.Precision", func() {
		for _, spec := range sliceSpecs {
			byName := map[string]ddlColumn{}
			for _, c := range ddlColumns(db, spec.Table) {
				byName[c.Name] = c
			}
			for _, c := range spec.Columns {
				ddl := byName[c.DB]
				decimals, numeric := schema.Decimals(spec.Table, c.DB)
				switch {
				case c.DB == "station_id" && spec.Table != "stations":
					// The surrogate FK resolves to the TEXT external_id.
					Expect(ddl.Type).To(Equal("INTEGER"))
					Expect(c.Kind).To(Equal(publish.KindText), "%s.%s", spec.Name, c.Name)
					Expect(c.Key).To(BeTrue())
				case ddl.Type == "INTEGER":
					Expect(c.Kind).To(Equal(publish.KindInt), "%s.%s", spec.Name, c.Name)
				case ddl.Type == "REAL":
					Expect(c.Kind).To(Equal(publish.KindReal), "%s.%s", spec.Name, c.Name)
				case ddl.Type == "TEXT":
					Expect(c.Kind).To(BeElementOf(publish.KindText, publish.KindDate, publish.KindPeriod),
						"%s.%s", spec.Name, c.Name)
				default:
					Fail("unexpected DDL type " + ddl.Type + " for " + spec.Table + "." + c.DB)
				}
				if c.Kind == publish.KindInt || c.Kind == publish.KindReal {
					Expect(numeric).To(BeTrue(), "%s.%s has no Precision entry", spec.Table, c.DB)
					Expect(c.Decimals).To(Equal(decimals), "%s.%s", spec.Name, c.Name)
				} else {
					Expect(numeric).To(BeFalse(), "%s.%s is text but Precision pins it", spec.Table, c.DB)
					Expect(c.Decimals).To(BeZero())
				}
				Expect(c.Name).To(Equal(publish.ExportName(spec.Table, c.DB)))
			}
		}
	})

	It("orders columns natural key first in PK order, then values in DDL order", func() {
		for _, spec := range sliceSpecs {
			cols := ddlColumns(db, spec.Table)
			var pkOrder []string
			if spec.Table == "stations" {
				// The exported natural key is external_id; the DDL PK is
				// the surrogate id, which is never exported.
				pkOrder = []string{"external_id"}
			} else {
				pk := slices.DeleteFunc(slices.Clone(cols), func(c ddlColumn) bool { return c.PK == 0 })
				slices.SortFunc(pk, func(a, b ddlColumn) int { return a.PK - b.PK })
				for _, c := range pk {
					pkOrder = append(pkOrder, c.Name)
				}
			}

			var keys, values []string
			for _, c := range spec.Columns {
				if c.Key {
					Expect(values).To(BeEmpty(), "%s: key %s after a value column", spec.Name, c.DB)
					keys = append(keys, c.DB)
				} else {
					values = append(values, c.DB)
				}
			}
			Expect(keys).To(Equal(pkOrder), spec.Name)

			cid := func(name string) int {
				i := slices.IndexFunc(cols, func(c ddlColumn) bool { return c.Name == name })
				Expect(i).To(BeNumerically(">=", 0), name)
				return cols[i].CID
			}
			for i := 1; i < len(values); i++ {
				Expect(cid(values[i-1])).To(BeNumerically("<", cid(values[i])),
					"%s: %s must precede %s (DDL order)", spec.Name, values[i-1], values[i])
			}
		}
	})

	It("pins the published header of every conagua/ file", func() {
		Expect(publish.Stations.Header()).To(Equal([]string{
			"station_id", "name", "state", "municipality", "lat", "lon", "altitude_m", "status",
			"first_year", "last_year",
			"wmo_completeness_bin_1961_1990", "wmo_completeness_bin_1971_2000",
			"wmo_completeness_bin_1981_2010", "wmo_completeness_bin_1991_2020",
			"wmo_completeness_cont_1961_1990", "wmo_completeness_cont_1971_2000",
			"wmo_completeness_cont_1981_2010", "wmo_completeness_cont_1991_2020",
		}))
		Expect(publish.MonthlyNormals.Header()).To(Equal([]string{
			"station_id", "period", "month", "tmax_c", "tmin_c", "tmean_c", "precip_mm", "evap_mm",
		}))
		Expect(publish.MonthlyNormalsExtras.Header()).To(Equal([]string{
			"station_id", "period", "month",
			"tmax_monthly_extreme_c", "tmax_monthly_extreme_year", "tmax_daily_extreme_c", "tmax_daily_extreme_date",
			"tmin_monthly_extreme_c", "tmin_monthly_extreme_year", "tmin_daily_extreme_c", "tmin_daily_extreme_date",
			"precip_monthly_extreme_mm", "precip_monthly_extreme_year", "precip_daily_extreme_mm", "precip_daily_extreme_date",
			"tmax_years_with_data", "tmin_years_with_data", "tmean_years_with_data", "precip_years_with_data",
			"evap_years_with_data", "rain_days", "rain_days_years_with_data",
		}))
		Expect(publish.DailyObservations.Header()).To(Equal([]string{
			"station_id", "date", "tmax_c", "tmin_c", "precip_mm", "evap_mm",
		}))

		Expect(publish.Stations.Name).To(Equal("conagua/stations"))
		Expect(publish.MonthlyNormals.Name).To(Equal("conagua/monthly_normals"))
		Expect(publish.MonthlyNormalsExtras.Name).To(Equal("conagua/monthly_normals_extras"))
		Expect(publish.DailyObservations.Name).To(Equal("conagua/daily_observations"))
	})

	It("keys the sort by the exported natural key of each file", func() {
		keysOf := func(spec publish.FileSpec) []string {
			var keys []string
			for _, c := range spec.Columns {
				if c.Key {
					keys = append(keys, c.Name)
				}
			}
			return keys
		}
		Expect(keysOf(publish.Stations)).To(Equal([]string{"station_id"}))
		Expect(keysOf(publish.MonthlyNormals)).To(Equal([]string{"station_id", "period", "month"}))
		Expect(keysOf(publish.MonthlyNormalsExtras)).To(Equal([]string{"station_id", "period", "month"}))
		Expect(keysOf(publish.DailyObservations)).To(Equal([]string{"station_id", "date"}))
	})
})

var _ = Describe("Dropped", func() {
	It("is the published drop list, table by table", func() {
		Expect(publish.Dropped).To(Equal(map[string]map[string]publish.DropReason{
			"stations":              {"id": publish.DropSurrogate, "source": publish.DropConstant},
			"nasa_power_grid_cells": {"grid_resolution": publish.DropConstant},
			"monthly_supplement":    {"power_run_id": publish.DropFK},
			"daily_supplement":      {"power_run_id": publish.DropFK},
			"ingest_runs":           {"id": publish.DropSurrogate},
			"power_runs":            {"id": publish.DropSurrogate, "solar_conversion": publish.DropConstant},
		}))
	})

	It("names only DDL columns that exist and that no FileSpec exports", func() {
		db := openTempDB()
		for table, cols := range publish.Dropped {
			ddl := ddlColumns(db, table)
			Expect(ddl).NotTo(BeEmpty(), "Dropped names a table the DDL lacks: %s", table)
			for column := range cols {
				Expect(slices.ContainsFunc(ddl, func(c ddlColumn) bool { return c.Name == column })).To(BeTrue(),
					"Dropped names a column the DDL lacks: %s.%s", table, column)
				for _, spec := range sliceSpecs {
					if spec.Table != table {
						continue
					}
					Expect(slices.ContainsFunc(spec.Columns, func(c publish.Column) bool { return c.DB == column })).
						To(BeFalse(), "%s both exports and drops %s", spec.Name, column)
				}
			}
		}
	})
})

var _ = Describe("ExportName", func() {
	DescribeTable("applies the export rename map and is the identity elsewhere",
		func(table, column, want string) {
			Expect(publish.ExportName(table, column)).To(Equal(want))
		},
		Entry("normals tmax", "monthly_normals", "tmax", "tmax_c"),
		Entry("normals tmin", "monthly_normals", "tmin", "tmin_c"),
		Entry("normals tmean", "monthly_normals", "tmean", "tmean_c"),
		Entry("normals precip", "monthly_normals", "precip", "precip_mm"),
		Entry("normals evap", "monthly_normals", "evap", "evap_mm"),
		Entry("daily tmax", "daily_observations", "tmax", "tmax_c"),
		Entry("daily precip", "daily_observations", "precip", "precip_mm"),
		Entry("extras temperature extreme value", "monthly_normals_extras", "tmax_monthly_extreme", "tmax_monthly_extreme_c"),
		Entry("extras daily temperature extreme value", "monthly_normals_extras", "tmin_daily_extreme", "tmin_daily_extreme_c"),
		Entry("extras precipitation extreme value", "monthly_normals_extras", "precip_daily_extreme", "precip_daily_extreme_mm"),
		Entry("extras extreme year keeps its name", "monthly_normals_extras", "tmax_monthly_extreme_year", "tmax_monthly_extreme_year"),
		Entry("extras extreme date keeps its name", "monthly_normals_extras", "precip_daily_extreme_date", "precip_daily_extreme_date"),
		Entry("extras years_with_data keeps its name", "monthly_normals_extras", "precip_years_with_data", "precip_years_with_data"),
		Entry("extras rain_days keeps its name", "monthly_normals_extras", "rain_days", "rain_days"),
		Entry("external_id becomes station_id", "stations", "external_id", "station_id"),
		Entry("station lat stays bare", "stations", "lat", "lat"),
		Entry("altitude_m unchanged", "stations", "altitude_m", "altitude_m"),
		Entry("a child-table station_id is already the natural key name", "monthly_normals", "station_id", "station_id"),
		Entry("distance_km unchanged", "station_power_cell", "distance_km", "distance_km"),
		Entry("a POWER column unchanged", "daily_supplement", "precip_mmpd", "precip_mmpd"),
		Entry("a table outside the map", "no_such_table", "tmax", "tmax"),
	)
})
