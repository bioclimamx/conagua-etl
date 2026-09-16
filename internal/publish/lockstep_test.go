package publish_test

// The DDL lockstep for the nasa_power/ and provenance/ file specs — the
// same holds columns_test.go applies to the conagua/ specs, plus the two
// cases those files do not have: a natural key the DDL does not declare
// as the primary key (ingest_runs is keyed by snapshot_date; power_runs
// by the synthesized run_label, the one column no DDL column backs) and
// a value block derived from power.Registry rather than declared.

import (
	"database/sql"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// powerSpecs lists the nasa_power/ and provenance/ file specs.
var powerSpecs = []publish.FileSpec{
	publish.Cells, publish.StationCellMap, publish.PowerMonthly, publish.PowerDaily,
	publish.ProvenanceIngestRuns, publish.ProvenancePowerRuns,
}

// naturalKeys overrides the DDL primary key for the two provenance
// files, whose DDL PK is the never-exported surrogate id.
var naturalKeys = map[string][]string{
	"ingest_runs": {"snapshot_date"},
	"power_runs":  {publish.RunLabelColumn},
}

// ddlBacked returns spec's columns that read a DDL column — every
// column but the synthesized run_label.
func ddlBacked(spec publish.FileSpec) []publish.Column {
	return slices.DeleteFunc(slices.Clone(spec.Columns), func(c publish.Column) bool { return c.DB == "" })
}

var _ = Describe("FileSpec DDL lockstep — nasa_power/ and provenance/ files", func() {
	var db *sql.DB

	BeforeEach(func() {
		db = openTempDB()
	})

	It("synthesizes exactly one column, provenance/power_runs' run_label key", func() {
		for _, spec := range powerSpecs {
			for _, c := range spec.Columns {
				if c.DB != "" {
					continue
				}
				Expect(spec.Name).To(Equal(publish.ProvenancePowerRuns.Name), "%s synthesizes %s", spec.Name, c.Name)
				Expect(c).To(Equal(publish.Column{Name: publish.RunLabelColumn, Kind: publish.KindText, Key: true}))
			}
		}
		synthesized := slices.DeleteFunc(slices.Clone(publish.ProvenancePowerRuns.Columns),
			func(c publish.Column) bool { return c.DB != "" })
		Expect(synthesized).To(HaveLen(1))
	})

	It("exports or drops every DDL column of each exported table, exactly once", func() {
		for _, spec := range powerSpecs {
			cols := ddlColumns(db, spec.Table)
			Expect(cols).NotTo(BeEmpty(), "spec %s names a table the DDL lacks: %s", spec.Name, spec.Table)

			exported := map[string]bool{}
			for _, c := range ddlBacked(spec) {
				Expect(exported).NotTo(HaveKey(c.DB), "%s exports %s twice", spec.Name, c.DB)
				exported[c.DB] = true
			}
			for _, c := range cols {
				_, dropped := publish.Dropped[spec.Table][c.Name]
				Expect(exported[c.Name] != dropped).To(BeTrue(),
					"%s.%s must be in exactly one of %s's columns or Dropped (exported=%t, dropped=%t)",
					spec.Table, c.Name, spec.Name, exported[c.Name], dropped)
			}
			for name := range exported {
				Expect(slices.ContainsFunc(cols, func(c ddlColumn) bool { return c.Name == name })).To(BeTrue(),
					"%s reads a column the DDL lacks: %s.%s", spec.Name, spec.Table, name)
			}
		}
	})

	It("types every column by its DDL declaration and pins decimals from schema.Precision", func() {
		for _, spec := range powerSpecs {
			byName := map[string]ddlColumn{}
			for _, c := range ddlColumns(db, spec.Table) {
				byName[c.Name] = c
			}
			for _, c := range ddlBacked(spec) {
				ddl := byName[c.DB]
				decimals, numeric := schema.Decimals(spec.Table, c.DB)
				switch {
				case c.DB == "station_id":
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
		for _, spec := range powerSpecs {
			cols := ddlColumns(db, spec.Table)
			pkOrder, overridden := naturalKeys[spec.Table]
			if !overridden {
				pk := slices.DeleteFunc(slices.Clone(cols), func(c ddlColumn) bool { return c.PK == 0 })
				slices.SortFunc(pk, func(a, b ddlColumn) int { return a.PK - b.PK })
				for _, c := range pk {
					pkOrder = append(pkOrder, c.Name)
				}
			}

			var keys, values []string
			for _, c := range spec.Columns {
				if c.Key {
					Expect(values).To(BeEmpty(), "%s: key %s after a value column", spec.Name, c.Name)
					keys = append(keys, c.Name)
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

	It("derives the POWER-31 block from power.Registry, which is the published column list", func() {
		registry := make([]string, len(power.Registry))
		for i, p := range power.Registry {
			registry[i] = p.Column
		}
		Expect(registry).To(Equal(power31))

		block := func(spec publish.FileSpec) []string {
			var out []string
			for _, c := range spec.Columns {
				if !c.Key {
					out = append(out, c.DB)
				}
			}
			return out
		}
		Expect(block(publish.PowerMonthly)).To(Equal(power31))
		Expect(block(publish.PowerDaily)).To(Equal(power31))
		for _, spec := range []publish.FileSpec{publish.PowerMonthly, publish.PowerDaily} {
			for _, c := range spec.Columns {
				if c.Key {
					continue
				}
				want := 2
				if c.DB == "wd2m_deg" || c.DB == "wd10m_deg" {
					want = 1
				}
				Expect(c.Decimals).To(Equal(want), "%s.%s", spec.Name, c.Name)
				Expect(c.Kind).To(Equal(publish.KindReal), "%s.%s", spec.Name, c.Name)
			}
		}
	})

	It("pins the published header of every nasa_power/ and provenance/ file", func() {
		Expect(publish.Cells.Header()).To(Equal([]string{"cell_id", "lat", "lon"}))
		Expect(publish.StationCellMap.Header()).To(Equal([]string{"station_id", "cell_id", "distance_km"}))
		Expect(publish.PowerMonthly.Header()).To(Equal(append([]string{"cell_id", "period", "month"}, power31...)))
		Expect(publish.PowerDaily.Header()).To(Equal(append([]string{"cell_id", "date"}, power31...)))
		Expect(publish.ProvenanceIngestRuns.Header()).To(Equal([]string{
			"snapshot_date", "started_at", "finished_at", "sink_kind", "etl_git_sha", "status",
			"stations_attempted", "stations_succeeded", "stations_failed",
			"daily_rows", "normals_rows", "extras_rows", "warnings_total",
		}))
		Expect(publish.ProvenancePowerRuns.Header()).To(Equal([]string{
			"run_label", "started_at", "finished_at", "status", "endpoint_url", "parameters", "community",
			"period_start_year", "period_end_year", "grid_resolution", "unit_conversions", "temporal_mode",
			"period_start_date", "period_end_date",
			"cells_attempted", "cells_succeeded", "cells_failed", "supplement_rows", "etl_git_sha",
		}))

		Expect(publish.Cells.Name).To(Equal("nasa_power/cells"))
		Expect(publish.StationCellMap.Name).To(Equal("nasa_power/station_cell_map"))
		Expect(publish.PowerMonthly.Name).To(Equal("nasa_power/monthly"))
		Expect(publish.PowerDaily.Name).To(Equal("nasa_power/daily"))
		Expect(publish.ProvenanceIngestRuns.Name).To(Equal("provenance/ingest_runs"))
		Expect(publish.ProvenancePowerRuns.Name).To(Equal("provenance/power_runs"))

		Expect(publish.Cells.Table).To(Equal("nasa_power_grid_cells"))
		Expect(publish.StationCellMap.Table).To(Equal("station_power_cell"))
		Expect(publish.PowerMonthly.Table).To(Equal("monthly_supplement"))
		Expect(publish.PowerDaily.Table).To(Equal("daily_supplement"))
		Expect(publish.ProvenanceIngestRuns.Table).To(Equal("ingest_runs"))
		Expect(publish.ProvenancePowerRuns.Table).To(Equal("power_runs"))
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
		Expect(keysOf(publish.Cells)).To(Equal([]string{"cell_id"}))
		Expect(keysOf(publish.StationCellMap)).To(Equal([]string{"station_id"}))
		Expect(keysOf(publish.PowerMonthly)).To(Equal([]string{"cell_id", "period", "month"}))
		Expect(keysOf(publish.PowerDaily)).To(Equal([]string{"cell_id", "date"}))
		Expect(keysOf(publish.ProvenanceIngestRuns)).To(Equal([]string{"snapshot_date"}))
		Expect(keysOf(publish.ProvenancePowerRuns)).To(Equal([]string{"run_label"}))
	})

	It("never exports a column Dropped names", func() {
		for _, spec := range powerSpecs {
			for column := range publish.Dropped[spec.Table] {
				Expect(slices.ContainsFunc(spec.Columns, func(c publish.Column) bool { return c.DB == column })).
					To(BeFalse(), "%s both exports and drops %s", spec.Name, column)
			}
		}
	})
})
