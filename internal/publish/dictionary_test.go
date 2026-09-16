package publish_test

// The generated data dictionary: every exported column of every file
// listed once with its decimals and unit, every DDL column of every
// exported table accounted for, the POWER rows verbatim from the
// registry, the profile shape from the Profile struct, and the two
// renderings deterministic and in agreement.

import (
	"bytes"
	"encoding/json"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// dictionarySpecs lists the twelve exported files in deposit order, each
// with the DDL table a column named c is read from.
var dictionarySpecs = []struct {
	spec   publish.FileSpec
	source func(publish.Column) string
}{
	{publish.Stations, nil}, {publish.MonthlyNormals, nil}, {publish.MonthlyNormalsExtras, nil},
	{publish.DailyObservations, nil},
	{publish.Cells, nil}, {publish.StationCellMap, nil}, {publish.PowerMonthly, nil}, {publish.PowerDaily, nil},
	{publish.CombinedMonthly.FileSpec, publish.CombinedMonthly.Source},
	{publish.CombinedDaily.FileSpec, publish.CombinedDaily.Source},
	{publish.ProvenanceIngestRuns, nil}, {publish.ProvenancePowerRuns, nil},
}

// orcidShapedRE matches an ORCID-shaped identifier — four groups of
// four, the last character possibly X — anywhere in a rendering.
var orcidShapedRE = regexp.MustCompile(`\b\d{4}-\d{4}-\d{4}-\d{3}[\dX]\b`)

// ledgerNumberRE matches a bare E<n> reference, an internal tracking
// number, which means nothing to a reader of the deposit.
var ledgerNumberRE = regexp.MustCompile(`\bE\d{1,2}\b`)

func dictionaryMeta() publish.ProfileMeta {
	return publish.ProfileMeta{
		SchemaVersion: schema.Version,
		ETLGitSHA:     "0123abcd",
		SnapshotDate:  "2026-07-18",
		Dataset:       publish.DatasetMetadata(time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC), ""),
	}
}

func buildDictionary() *publish.Dictionary {
	GinkgoHelper()
	d, err := publish.BuildDictionary(dictionaryMeta())
	Expect(err).NotTo(HaveOccurred())
	return d
}

func fileByName(d *publish.Dictionary, name string) publish.DictionaryFile {
	GinkgoHelper()
	for _, f := range d.Files {
		if f.Name == name {
			return f
		}
	}
	Fail("dictionary lists no file " + name)
	return publish.DictionaryFile{}
}

func columnByName(f publish.DictionaryFile, name string) publish.DictionaryColumn {
	GinkgoHelper()
	for _, c := range f.Columns {
		if c.Name == name {
			return c
		}
	}
	Fail(f.Name + " lists no column " + name)
	return publish.DictionaryColumn{}
}

// ddlComment reads schema.sql's trailing comment on a column — the
// dictionary's own source for every column it does not answer from its
// own tables, so a spec can hold the two to each other by mechanism
// instead of pinning the DDL's wording twice.
func ddlComment(table, column string) string {
	GinkgoHelper()
	for _, t := range schema.Tables() {
		if t.Name != table {
			continue
		}
		for _, c := range t.Columns {
			if c.Name == column {
				return c.Comment
			}
		}
	}
	Fail("schema.sql has no column " + table + "." + column)
	return ""
}

func names(cols []publish.DictionaryColumn) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}

func deref[T any](p *T) T {
	GinkgoHelper()
	Expect(p).NotTo(BeNil())
	return *p
}

// renderMarkdown and renderJSON render the dictionary to bytes.
func renderMarkdown(d *publish.Dictionary) []byte {
	GinkgoHelper()
	var b bytes.Buffer
	Expect(publish.RenderDictionaryMarkdown(&b, d)).To(Succeed())
	return b.Bytes()
}

func renderJSON(d *publish.Dictionary) []byte {
	GinkgoHelper()
	var b bytes.Buffer
	Expect(publish.RenderDictionaryJSON(&b, d)).To(Succeed())
	return b.Bytes()
}

// markdownColumns parses the per-file column tables out of the Markdown:
// under each "### <file>" heading, the rows of the table whose header
// starts with "| column |", first cell unquoted.
func markdownColumns(md []byte) map[string][]string {
	out := map[string][]string{}
	var file string
	inColumns := false
	for _, line := range strings.Split(string(md), "\n") {
		switch {
		case strings.HasPrefix(line, "## "):
			file, inColumns = "", false
		case strings.HasPrefix(line, "### "):
			file, inColumns = strings.TrimPrefix(line, "### "), false
		case file != "" && strings.HasPrefix(line, "| column |"):
			inColumns = true
			out[file] = []string{}
		case inColumns && strings.HasPrefix(line, "|---"):
		case inColumns && strings.HasPrefix(line, "| "):
			cell := strings.TrimSpace(strings.SplitN(line, "|", 3)[1])
			out[file] = append(out[file], strings.Trim(cell, "`"))
		default:
			inColumns = false
		}
	}
	return out
}

var _ = Describe("BuildDictionary", func() {
	var d *publish.Dictionary

	BeforeEach(func() {
		d = buildDictionary()
	})

	It("needs no database and carries the build's identity from meta", func() {
		meta := dictionaryMeta()
		Expect(d.Dataset).To(Equal(meta.Dataset))
		Expect(d.SchemaVersion).To(Equal(schema.Version))
		Expect(d.SnapshotDate).To(Equal("2026-07-18"))
		Expect(d.ETLGitSHA).To(Equal("0123abcd"))
		Expect(d.Dataset.Creator).To(Equal(publish.DatasetCreator))
	})

	It("lists every FileSpec once, in the published file order, with exactly its columns in export order", func() {
		Expect(d.Files).To(HaveLen(len(dictionarySpecs)))
		for i, s := range dictionarySpecs {
			f := d.Files[i]
			Expect(f.Name).To(Equal(s.spec.Name))
			Expect(f.Table).To(Equal(s.spec.Table))
			Expect(names(f.Columns)).To(Equal(s.spec.Header()), f.Name)
			var keys []string
			for _, c := range s.spec.Columns {
				if c.Key {
					keys = append(keys, c.Name)
				}
			}
			Expect(f.SortKey).To(Equal(keys), f.Name)
			for j, c := range s.spec.Columns {
				Expect(f.Columns[j].Key).To(Equal(c.Key), "%s.%s", f.Name, c.Name)
			}
		}
	})

	It("pins every numeric column's decimals to schema.Decimals and its unit to the name; text columns carry neither", func() {
		for _, s := range dictionarySpecs {
			f := fileByName(d, s.spec.Name)
			for i, c := range s.spec.Columns {
				col := f.Columns[i]
				table := s.spec.Table
				if s.source != nil {
					table = s.source(c)
				}
				switch c.Kind {
				case publish.KindInt, publish.KindReal:
					want, ok := schema.Decimals(table, c.DB)
					Expect(ok).To(BeTrue(), "%s.%s", table, c.DB)
					Expect(deref(col.Decimals)).To(Equal(want), "%s.%s", f.Name, c.Name)
					Expect(deref(col.Decimals)).To(Equal(c.Decimals))
				default:
					Expect(col.Decimals).To(BeNil(), "%s.%s", f.Name, c.Name)
					Expect(col.Unit).To(BeNil(), "%s.%s", f.Name, c.Name)
				}
				switch c.Kind {
				case publish.KindReal:
					Expect(col.Unit).NotTo(BeNil(), "every REAL column has a unit: %s.%s", f.Name, c.Name)
				case publish.KindInt:
					Expect(col.Unit).To(BeNil(), "a month, year, or count has no unit: %s.%s", f.Name, c.Name)
				}
				Expect(col.Description).NotTo(BeEmpty(), "%s.%s", f.Name, c.Name)
			}
		}
	})

	DescribeTable("derives the unit from the column-name suffix, or from the name where it has none",
		func(file, column, unit string) {
			Expect(deref(columnByName(fileByName(d, file), column).Unit)).To(Equal(unit))
		},
		Entry("_c", "conagua/monthly_normals", "tmax_c", "°C"),
		Entry("_mm", "conagua/daily_observations", "precip_mm", "mm"),
		Entry("_mmpd", "nasa_power/monthly", "precip_mmpd", "mm/day"),
		Entry("_pct", "nasa_power/monthly", "rh2m_pct", "%"),
		Entry("_gkg", "nasa_power/monthly", "qv2m_gkg", "g/kg"),
		Entry("_ms", "nasa_power/daily", "ws2m_ms", "m/s"),
		Entry("_deg", "nasa_power/daily", "wd2m_deg", "degrees"),
		Entry("_wm2", "combined/combined_daily", "solar_ghi_wm2", "W/m²"),
		Entry("_kpa", "combined/combined_monthly", "ps_kpa", "kPa"),
		Entry("_km", "nasa_power/station_cell_map", "distance_km", "km"),
		Entry("_m", "conagua/stations", "altitude_m", "m"),
		Entry("_days", "conagua/monthly_normals_extras", "rain_days", "days"),
		Entry("an extras extreme", "conagua/monthly_normals_extras", "precip_daily_extreme_mm", "mm"),
		Entry("station lat", "conagua/stations", "lat", "decimal degrees"),
		Entry("cell lon", "nasa_power/cells", "lon", "decimal degrees"),
		Entry("clearness", "nasa_power/monthly", "clearness_index", "dimensionless"),
		Entry("soil wetness", "nasa_power/daily", "gwet_prof", "dimensionless"),
		Entry("a WMO score", "conagua/stations", "wmo_completeness_cont_1991_2020", "dimensionless"),
	)

	It("accounts for every DDL column of every exported table: exported under some file, or dropped, never both or neither", func() {
		exported := map[string]map[string]bool{}
		for _, f := range d.Files {
			for _, c := range f.Columns {
				if c.Source == "synthesized" || c.Source == "join context" {
					continue
				}
				table, column, ok := strings.Cut(strings.SplitN(c.Source, " ", 2)[0], ".")
				Expect(ok).To(BeTrue(), "%s.%s source %q", f.Name, c.Name, c.Source)
				if exported[table] == nil {
					exported[table] = map[string]bool{}
				}
				exported[table][column] = true
			}
		}
		dropped := map[string]map[string]string{}
		for _, x := range d.Dropped {
			if dropped[x.Table] == nil {
				dropped[x.Table] = map[string]string{}
			}
			dropped[x.Table][x.Column] = x.Reason
		}
		exportedTables := map[string]bool{}
		for _, s := range dictionarySpecs {
			exportedTables[s.spec.Table] = true
			for _, c := range s.spec.Columns {
				if s.source != nil {
					exportedTables[s.source(c)] = true
				}
			}
		}
		Expect(exportedTables).To(HaveLen(10), "every DDL table but parsing_warnings is exported")
		for _, t := range schema.Tables() {
			if !exportedTables[t.Name] {
				Expect(t.Name).To(Equal("parsing_warnings"))
				Expect(exported).NotTo(HaveKey(t.Name))
				Expect(dropped).NotTo(HaveKey(t.Name))
				continue
			}
			for _, c := range t.Columns {
				_, isDropped := dropped[t.Name][c.Name]
				Expect(exported[t.Name][c.Name] != isDropped).To(BeTrue(),
					"%s.%s must be exported or dropped, not both or neither (exported=%t, dropped=%t)",
					t.Name, c.Name, exported[t.Name][c.Name], isDropped)
				if isDropped {
					Expect(dropped[t.Name][c.Name]).To(Equal(string(publish.Dropped[t.Name][c.Name])))
				}
			}
		}
		for table, cols := range publish.Dropped {
			for column := range cols {
				Expect(dropped[table]).To(HaveKey(column), "%s.%s", table, column)
			}
		}
		total := 0
		for _, cols := range publish.Dropped {
			total += len(cols)
		}
		Expect(d.Dropped).To(HaveLen(total))
	})

	It("states each documented constant once, with the value the owning code pins", func() {
		byColumn := map[string]publish.DictionaryConstant{}
		for _, c := range d.Constants {
			byColumn[c.Table+"."+c.Column] = c
			Expect(c.Reason).NotTo(BeEmpty())
		}
		Expect(byColumn).To(HaveLen(3))
		Expect(byColumn["stations.source"].Value).To(Equal(string(ingest.SourceConaguaConventional)))
		Expect(byColumn["nasa_power_grid_cells.grid_resolution"].Value).To(Equal(power.Resolution))
		Expect(byColumn["power_runs.solar_conversion"].Value).To(Equal(strconv.FormatFloat(power.SolarMJpm2dToWm2, 'g', -1, 64)))
		Expect(byColumn["power_runs.solar_conversion"].Value).To(Equal("11.574074074074074"))
		for _, x := range d.Dropped {
			_, constant := byColumn[x.Table+"."+x.Column]
			Expect(constant).To(Equal(x.Reason == string(publish.DropConstant)), "%s.%s", x.Table, x.Column)
		}
	})

	It("carries the POWER registry verbatim, in registry order, and on every POWER column", func() {
		Expect(d.Power).To(Equal(publish.PowerParameters()))
		Expect(d.Power).To(HaveLen(31))
		byColumn := map[string]publish.PowerParameter{}
		for i, p := range d.Power {
			Expect(p.ID).To(Equal(power.Registry[i].Name))
			Expect(p.Column).To(Equal(power.Registry[i].Column))
			Expect(p.PowerUnit).To(Equal(power.Registry[i].PowerUnit))
			Expect(p.StoredUnit).To(Equal(power.Registry[i].StoredUnit))
			Expect(p.Factor).To(Equal(power.Registry[i].Factor))
			byColumn[p.Column] = p
		}
		for _, name := range []string{"nasa_power/monthly", "nasa_power/daily", "combined/combined_monthly", "combined/combined_daily"} {
			f := fileByName(d, name)
			for _, c := range f.Columns {
				if p, ok := byColumn[c.Name]; ok {
					Expect(c.Power).NotTo(BeNil(), "%s.%s", name, c.Name)
					Expect(*c.Power).To(Equal(p), "%s.%s", name, c.Name)
					Expect(deref(c.Tag)).To(Equal("nasa_power"))
				} else {
					Expect(c.Power).To(BeNil(), "%s.%s", name, c.Name)
				}
			}
		}
	})

	It("lists POWER-31 in registry order on every POWER-bearing file", func() {
		want := make([]string, len(power.Registry))
		for i, p := range power.Registry {
			want[i] = p.Column
		}
		for _, name := range []string{"nasa_power/monthly", "nasa_power/daily", "combined/combined_monthly", "combined/combined_daily"} {
			var got []string
			for _, c := range fileByName(d, name).Columns {
				if c.Power != nil {
					got = append(got, c.Name)
				}
			}
			Expect(got).To(Equal(want), name)
		}
	})

	DescribeTable("names the source of a column",
		func(file, column, source string) {
			Expect(columnByName(fileByName(d, file), column).Source).To(Equal(source))
		},
		Entry("the stations key", "conagua/stations", "station_id", "stations.external_id"),
		Entry("a child table's key resolves through stations", "conagua/daily_observations", "station_id",
			"daily_observations.station_id → stations.external_id"),
		Entry("the map's key", "nasa_power/station_cell_map", "station_id", "station_power_cell.station_id → stations.external_id"),
		Entry("the map's cell", "nasa_power/station_cell_map", "cell_id", "station_power_cell.cell_id"),
		Entry("a renamed value column", "conagua/monthly_normals", "tmax_c", "monthly_normals.tmax"),
		Entry("a POWER column", "nasa_power/daily", "t2m_c", "daily_supplement.t2m_c"),
		Entry("the synthesized run_label", "provenance/power_runs", "run_label", "synthesized"),
		Entry("combined's cell_id", "combined/combined_monthly", "cell_id", "join context"),
		Entry("combined's distance_km", "combined/combined_daily", "distance_km", "join context"),
		Entry("combined's spine value", "combined/combined_daily", "tmax_c", "daily_observations.tmax"),
		Entry("combined's POWER value", "combined/combined_monthly", "ps_kpa", "monthly_supplement.ps_kpa"),
	)

	DescribeTable("tags a column by its provenance",
		func(file, column string, tag *string) {
			got := columnByName(fileByName(d, file), column).Tag
			if tag == nil {
				Expect(got).To(BeNil())
			} else {
				Expect(deref(got)).To(Equal(*tag))
			}
		},
		Entry("a catalog field", "conagua/stations", "name", ptr("conagua_published")),
		Entry("a WMO score", "conagua/stations", "wmo_completeness_bin_1961_1990", ptr("bioclima_derived")),
		Entry("first_year is computed at ingest", "conagua/stations", "first_year", ptr("bioclima_derived")),
		Entry("last_year is computed at ingest", "conagua/stations", "last_year", ptr("bioclima_derived")),
		Entry("a normal", "conagua/monthly_normals", "precip_mm", ptr("conagua_published")),
		Entry("an extra", "conagua/monthly_normals_extras", "rain_days", ptr("conagua_published")),
		Entry("an observation", "conagua/daily_observations", "tmax_c", ptr("conagua_observed")),
		Entry("a cell", "nasa_power/cells", "lat", ptr("bioclima_derived")),
		Entry("the distance", "nasa_power/station_cell_map", "distance_km", ptr("bioclima_derived")),
		Entry("a POWER value", "nasa_power/monthly", "t2m_c", ptr("nasa_power")),
		Entry("combined's key", "combined/combined_daily", "date", ptr("conagua_observed")),
		Entry("combined's join context", "combined/combined_daily", "distance_km", ptr("bioclima_derived")),
		Entry("combined's spine", "combined/combined_monthly", "tmean_c", ptr("conagua_published")),
		Entry("combined's POWER", "combined/combined_daily", "gwet_top", ptr("nasa_power")),
		Entry("a run ledger column", "provenance/ingest_runs", "snapshot_date", nil),
		Entry("the run label", "provenance/power_runs", "run_label", nil),
	)

	DescribeTable("describes a column from the DDL comment where there is one, else from the explicit fallback",
		func(file, column, description string) {
			Expect(columnByName(fileByName(d, file), column).Description).To(Equal(description))
		},
		Entry("a DDL comment", "nasa_power/monthly", "t2m_c", "mean air temperature at 2 m"),
		Entry("the same comment on the daily table (the DDL describes the block once)", "nasa_power/daily", "t2m_c", "mean air temperature at 2 m"),
		Entry("the same comment on combined", "combined/combined_daily", "solar_ghi_wm2", "ALLSKY_SFC_SW_DWN  — global horizontal"),
		Entry("a DDL comment that is a bare format note is appended to the fallback", "conagua/monthly_normals_extras", "tmax_daily_extreme_date",
			"date of tmax_daily_extreme_c (ISO 'YYYY-MM-DD')"),
		Entry("the same on daily_supplement's date", "nasa_power/daily", "date", "calendar date of the reanalysis value (ISO 'YYYY-MM-DD')"),
		Entry("a fallback", "conagua/daily_observations", "tmax_c", "daily maximum temperature"),
		Entry("the extras family: a monthly extreme", "conagua/monthly_normals_extras", "tmin_monthly_extreme_c",
			"lowest monthly mean of the daily minimum temperature in the period"),
		Entry("the extras family: a year", "conagua/monthly_normals_extras", "tmax_monthly_extreme_year", "year of tmax_monthly_extreme_c"),
		Entry("the extras family: a date without a DDL comment", "conagua/monthly_normals_extras", "precip_daily_extreme_date",
			"date of precip_daily_extreme_mm"),
		Entry("the extras family: a count", "conagua/monthly_normals_extras", "evap_years_with_data",
			"years in the period with valid evaporation data for the month (CONAGUA's AÑOS CON DATOS)"),
		Entry("the WMO family", "conagua/stations", "wmo_completeness_cont_1981_2010",
			"WMO-No. 1203 §4.4.2 completeness of the 1981-2010 normals, continuous system (_cont_): the mean coverage density over the same cells, 0–1 — see the stations note"),
		Entry("the join context", "combined/combined_monthly", "cell_id",
			"the station's POWER grid cell (nasa_power/station_cell_map); NULL for a station with no cell"),
		Entry("the status values come from the catalog vocabulary", "conagua/stations", "status",
			"CONAGUA's station status, English as stored (operating / suspended)"),
	)

	It("says first_year / last_year bound the daily record, not the data — a row that carries no measurement sets them like any other", func() {
		stations := fileByName(d, publish.Stations.Name)
		for _, name := range []string{"first_year", "last_year"} {
			desc := columnByName(stations, name).Description
			Expect(desc).To(ContainSubstring("record in the daily series"), name)
			Expect(desc).To(ContainSubstring("whether or not that row carries a measurement"), name)
			// Not "year with data": a station's first or last record can
			// carry no measurement at all.
			Expect(desc).NotTo(ContainSubstring("year with data"), name)
			Expect(desc).NotTo(ContainSubstring("daily observation"), name)
		}
		Expect(columnByName(stations, "first_year").Description).To(HavePrefix("year of the station's first record in the daily series:"))
		Expect(columnByName(stations, "last_year").Description).To(HavePrefix("year of the station's last record in the daily series:"))
		Expect(strings.Join(stations.Notes, "\n")).To(ContainSubstring("bound the station's daily record, not its data"))
		// profile.json repeats the flat file's text, so one fix travels.
		Expect(nodeAt(d.Profile.Keys, "identity.first_year").Description).
			To(Equal(columnByName(stations, "first_year").Description))
		Expect(nodeAt(d.Profile.Keys, "identity.last_year").Description).
			To(Equal(columnByName(stations, "last_year").Description))
	})

	It("gives monthly_supplement's four extreme columns their own description, distinct from the daily table's, wherever they are exported", func() {
		monthly := fileByName(d, publish.PowerMonthly.Name)
		daily := fileByName(d, publish.PowerDaily.Name)
		combined := fileByName(d, publish.CombinedMonthly.Name)
		for _, name := range []string{"t2m_max_c", "t2m_min_c", "ts_max_c", "ts_min_c"} {
			monthlyDesc := columnByName(monthly, name).Description
			dailyDesc := columnByName(daily, name).Description
			// The defect: one string served both grains. The daily table
			// keeps the DDL's comment, which is written at its grain.
			Expect(dailyDesc).To(Equal(ddlComment(publish.PowerMonthly.Table, name)), name)
			Expect(monthlyDesc).NotTo(Equal(dailyDesc), name)
			Expect(monthlyDesc).To(HavePrefix("monthly "), name)
			Expect(monthlyDesc).To(ContainSubstring("for the calendar month, averaged over the period's years"), name)
			Expect(monthlyDesc).To(ContainSubstring("not the mean of the month's daily "), name)
			Expect(monthlyDesc).To(ContainSubstring("nasa_power/daily's "+name), name)
			// combined/ and profile.json read the same monthly column.
			Expect(columnByName(combined, name).Description).To(Equal(monthlyDesc), name)
			Expect(nodeAt(d.Profile.Keys, "power_monthly.periods.<period>."+name).Description).To(Equal(monthlyDesc), name)
		}
		// Every other POWER column still means the same thing at both
		// grains and still comes from the one DDL comment.
		for _, c := range monthly.Columns {
			if c.Power == nil || strings.HasSuffix(c.Name, "_max_c") || strings.HasSuffix(c.Name, "_min_c") {
				continue
			}
			Expect(c.Description).To(Equal(ddlComment(publish.PowerMonthly.Table, c.Name)), c.Name)
			Expect(columnByName(daily, c.Name).Description).To(Equal(c.Description), c.Name)
		}
	})

	It("states the monthly grain on the files that carry it and the daily grain on the files that carry that", func() {
		monthlyNote := "Every value is a climatological mean"
		for _, name := range []string{publish.PowerMonthly.Name, publish.CombinedMonthly.Name} {
			notes := strings.Join(fileByName(d, name).Notes, "\n")
			Expect(notes).To(ContainSubstring(monthlyNote), name)
			Expect(notes).To(ContainSubstring("(t2m_max_c, t2m_min_c, ts_max_c, ts_min_c)"), name)
			Expect(notes).To(ContainSubstring("not the mean of that month's daily extremes"), name)
		}
		for _, name := range []string{publish.PowerDaily.Name, publish.CombinedDaily.Name} {
			notes := strings.Join(fileByName(d, name).Notes, "\n")
			Expect(notes).NotTo(ContainSubstring(monthlyNote), name)
			Expect(notes).To(ContainSubstring("written at this daily grain"), name)
			Expect(notes).To(ContainSubstring("nasa_power/monthly carries its own description"), name)
			Expect(notes).To(ContainSubstring("no circular averaging at this layer"), name)
		}
	})

	It("gives every file its in-archive path per format and the archives that carry it", func() {
		paths := map[string][]publish.DictionaryPath{}
		for _, f := range d.Files {
			paths[f.Name] = f.Paths
		}
		tabular := []string{"<state>-tabular.zip", "national-csv.zip"}
		parquet := []string{"<state>-tabular.zip", "national-parquet.zip"}
		jsonArchives := []string{"<state>-json.zip", "national-json.zip"}
		Expect(paths["conagua/stations"]).To(Equal([]publish.DictionaryPath{
			{Format: "csv", Path: "conagua/stations.csv", Archives: tabular},
			{Format: "parquet", Path: "conagua/stations.parquet", Archives: parquet},
		}))
		Expect(paths["conagua/daily_observations"]).To(Equal([]publish.DictionaryPath{
			{Format: "csv", Path: "conagua/daily_observations/<state>/daily-<station_id>.csv", Archives: tabular},
			{Format: "parquet", Path: "conagua/daily_observations.parquet", Archives: parquet},
		}))
		Expect(paths["nasa_power/daily"]).To(Equal([]publish.DictionaryPath{
			{Format: "csv", Path: "nasa_power/daily/daily-<cell_id>.csv", Archives: tabular},
			{Format: "parquet", Path: "nasa_power/daily.parquet", Archives: parquet},
		}))
		Expect(paths["combined/combined_daily"]).To(Equal([]publish.DictionaryPath{
			{Format: "csv", Path: "combined/combined_daily/<state>/daily-<station_id>.csv", Archives: tabular},
			{Format: "parquet", Path: "combined/combined_daily.parquet", Archives: parquet},
			{Format: "json", Path: "combined/<state>/<station_id>/daily.json", Archives: jsonArchives},
		}))
		Expect(paths["provenance/power_runs"]).To(Equal([]publish.DictionaryPath{
			{Format: "csv", Path: "provenance/power_runs.csv", Archives: tabular},
			{Format: "json", Path: "provenance/power_runs.json", Archives: jsonArchives},
		}))
		for name, p := range paths {
			formats := make([]string, len(p))
			for i, x := range p {
				formats[i] = x.Format
			}
			if strings.HasPrefix(name, "provenance/") {
				Expect(formats).To(Equal([]string{"csv", "json"}), name)
			} else {
				Expect(formats[:2]).To(Equal([]string{"csv", "parquet"}), name)
			}
		}
		Expect(d.Profile.Path).To(Equal("combined/<state>/<station_id>/profile.json"))
		Expect(d.DailyJSON.Path).To(Equal("combined/<state>/<station_id>/daily.json"))
		Expect(d.SQLite.StatePath).To(Equal("<state>.db"))
		Expect(d.SQLite.NationalPath).To(Equal("bioclima.db"))
	})

	It("describes the profile shape from the Profile struct: every JSON key, in declaration order, described", func() {
		want := profilePaths(reflect.TypeOf(publish.Profile{}), "")
		got := map[string]publish.DictionaryNode{}
		var walk func(nodes []publish.DictionaryNode, path string)
		walk = func(nodes []publish.DictionaryNode, path string) {
			for _, n := range nodes {
				p := n.Key
				if path != "" {
					p = path + "." + n.Key
				}
				Expect(got).NotTo(HaveKey(p), "listed twice")
				got[p] = n
				Expect(n.Description).NotTo(BeEmpty(), p)
				Expect(n.Type).NotTo(BeEmpty(), p)
				Expect(n.Children).NotTo(BeNil(), p)
				walk(n.Children, p)
			}
		}
		walk(d.Profile.Keys, "")
		for _, p := range want {
			Expect(got).To(HaveKey(p), "profile key %s is not in the dictionary", p)
		}
		// The period blocks' series are the flat files' value columns.
		series := map[string]publish.FileSpec{
			"normals.periods.<period>":       publish.MonthlyNormals,
			"extras.periods.<period>":        publish.MonthlyNormalsExtras,
			"power_monthly.periods.<period>": publish.PowerMonthly,
		}
		for path, spec := range series {
			var wantKeys []string
			for _, c := range spec.Columns {
				if !c.Key {
					wantKeys = append(wantKeys, c.Name)
				}
			}
			var gotKeys []string
			for _, n := range got[path].Children {
				gotKeys = append(gotKeys, n.Key)
				Expect(n.Type).To(HavePrefix("array[13] of "), path+"."+n.Key)
				Expect(n.Type).To(HaveSuffix(" | null"))
			}
			Expect(gotKeys).To(Equal(wantKeys), path)
			Expect(got[path].Type).To(Equal("object | null"))
		}
		Expect(got["wmo_completeness.periods.<period>"].Type).To(Equal("object | null"))
		Expect(got["normals.periods"].Type).To(Equal("object keyed by period"))
		Expect(len(got)).To(Equal(len(want)+55), "every reflected key plus the 5 + 19 + 31 series keys")

		top := make([]string, len(d.Profile.Keys))
		for i, n := range d.Profile.Keys {
			top[i] = n.Key
		}
		Expect(top).To(Equal([]string{"station_id", "identity", "wmo_completeness", "normals", "extras",
			"power_cell", "power_monthly", "daily_summary", "meta"}))
	})

	DescribeTable("types and pins the profile's fixed-shape values",
		func(path, typ string, decimals int, unit string) {
			n := nodeAt(d.Profile.Keys, path)
			Expect(n.Type).To(Equal(typ))
			if decimals < 0 {
				Expect(n.Decimals).To(BeNil())
			} else {
				Expect(deref(n.Decimals)).To(Equal(decimals))
			}
			if unit == "" {
				Expect(n.Unit).To(BeNil())
			} else {
				Expect(deref(n.Unit)).To(Equal(unit))
			}
		},
		Entry("station_id", "station_id", "string", -1, ""),
		Entry("a nullable text", "identity.name", "string | null", -1, ""),
		Entry("station lat", "identity.lat", "number | null", 6, "decimal degrees"),
		Entry("altitude", "identity.altitude_m", "number | null", 1, ""),
		Entry("a nullable integer", "identity.first_year", "integer | null", -1, ""),
		Entry("a WMO score", "wmo_completeness.periods.<period>.bin", "number | null", 4, "dimensionless"),
		Entry("a nullable block", "power_cell", "object | null", -1, ""),
		Entry("the cell lat", "power_cell.lat", "number | null", 3, "decimal degrees"),
		Entry("the distance", "power_cell.distance_km", "number | null", 3, ""),
		Entry("a normals series", "normals.periods.<period>.precip_mm", "array[13] of number | null", 1, "mm"),
		Entry("an extras year series", "extras.periods.<period>.tmax_monthly_extreme_year", "array[13] of integer | null", 0, ""),
		Entry("an extras date series", "extras.periods.<period>.tmax_daily_extreme_date", "array[13] of string | null", -1, ""),
		Entry("a POWER series", "power_monthly.periods.<period>.wd2m_deg", "array[13] of number | null", 1, "degrees"),
		// The observed daily record carries daily_observations.tmax's own
		// precision — two decimals, not the normals' one.
		Entry("a record value", "daily_summary.extremes.record_tmax_c.value", "number | null", 2, ""),
		Entry("a record", "daily_summary.extremes.record_precip_mm_1day", "object | null", -1, ""),
		Entry("a count", "daily_summary.coverage.observed.days_with_obs", "integer", -1, ""),
		Entry("the dry spell", "daily_summary.dry_spell", "object | null", -1, ""),
		Entry("the run lists", "meta.runs.power", "array of string", -1, ""),
		Entry("a plain string", "meta.license", "string", -1, ""),
	)

	It("describes daily.json's row shape from the combined daily columns", func() {
		Expect(d.DailyJSON.Date).To(Equal("date"))
		Expect(names(d.DailyJSON.Observed)).To(Equal([]string{"tmax_c", "tmin_c", "precip_mm", "evap_mm"}))
		want := make([]string, len(power.Registry))
		for i, p := range power.Registry {
			want[i] = p.Column
		}
		Expect(names(d.DailyJSON.Reanalysis)).To(Equal(want))
		combined := fileByName(d, "combined/combined_daily")
		for _, c := range append(d.DailyJSON.Observed, d.DailyJSON.Reanalysis...) {
			Expect(c).To(Equal(columnByName(combined, c.Name)))
		}
		Expect(d.DailyJSON.Rules).To(HaveLen(4))
		Expect(strings.Join(d.DailyJSON.Rules, " ")).To(ContainSubstring("reanalysis is the whole object null"))
	})

	It("states the annual-slot convention per variable from the profile's dispatch", func() {
		Expect(d.Annual.Slots).To(Equal(13))
		Expect(d.Annual.Tag).To(Equal("bioclima_derived"))
		by := map[string]string{}
		var order []string
		for _, v := range d.Annual.Variables {
			by[v.Name] = v.Aggregation
			order = append(order, v.Name)
		}
		want := []string{"tmax_c", "tmin_c", "tmean_c", "precip_mm", "evap_mm"}
		for _, p := range power.Registry {
			want = append(want, p.Column)
		}
		Expect(order).To(Equal(want))
		Expect(by["tmax_c"]).To(Equal("mean"))
		Expect(by["tmean_c"]).To(Equal("mean"))
		Expect(by["precip_mm"]).To(Equal("sum"))
		Expect(by["evap_mm"]).To(Equal("sum"))
		Expect(by["wd2m_deg"]).To(Equal("circular mean"))
		Expect(by["wd10m_deg"]).To(Equal("circular mean"))
		Expect(by["precip_mmpd"]).To(Equal("mean"))
		Expect(by["solar_ghi_wm2"]).To(Equal("mean"))
		rules := strings.Join(d.Annual.Rules, " ")
		Expect(rules).To(ContainSubstring("null unless all twelve months are non-null"))
		Expect(rules).To(ContainSubstring("non-null (provisional, subject to confirmation in a later version): a partial sum understates a total"))
		Expect(rules).To(ContainSubstring("extras series carry no annual"))
		Expect(rules).To(ContainSubstring("always null (provisional: no annual aggregation is defined for the extras columns yet; subject to confirmation in a later version)"))
		Expect(rules).To(ContainSubstring("exact decimal tie"))
		Expect(rules).To(ContainSubstring("last decimal (provisional: the correctly rounded mean is kept; subject to confirmation in a later version)"))
		Expect(rules).To(ContainSubstring("Never a uniform arithmetic mean over precip_mm / evap_mm"))
	})

	It("renders the precision table from schema.Precision, every entry once, grouped by decimals ascending", func() {
		total := 0
		seen := map[string]int{}
		last := -1
		for _, p := range d.Precision {
			Expect(p.Decimals).To(BeNumerically(">", last))
			last = p.Decimals
			for _, name := range p.Columns {
				column := strings.SplitN(name, " ", 2)[0]
				Expect(seen).NotTo(HaveKey(column))
				seen[column] = p.Decimals
				total++
			}
		}
		for table, cols := range schema.Precision {
			for column, decimals := range cols {
				Expect(seen).To(HaveKeyWithValue(table+"."+column, decimals))
			}
			total -= len(cols)
		}
		Expect(total).To(BeZero(), "no entry beyond schema.Precision")
		Expect(d.Precision[1].Columns).To(ContainElement("monthly_normals.tmax → tmax_c"))
		Expect(d.Precision[1].Columns).To(ContainElement("stations.altitude_m"))
	})

	It("states the format contract and the SQLite notes", func() {
		topics := make([]string, len(d.Format))
		for i, r := range d.Format {
			topics[i] = r.Topic
			Expect(r.Rule).NotTo(BeEmpty())
		}
		Expect(topics).To(Equal([]string{"nulls", "dates and periods", "text", "numbers", "encoding", "sort", "reproducibility"}))
		var tables []string
		for _, t := range schema.Tables() {
			tables = append(tables, t.Name)
		}
		Expect(d.SQLite.Tables).To(Equal(tables))
		notes := strings.Join(d.SQLite.Notes, " ")
		Expect(notes).To(ContainSubstring("PRAGMA user_version = " + strconv.Itoa(schema.Version)))
		Expect(notes).To(ContainSubstring("all " + strconv.Itoa(len(tables)) + " tables"))
		Expect(notes).To(ContainSubstring("Row set of `<state>.db` (provisional, subject to confirmation in a later version): the state's CONAGUA conventional stations"))
		Expect(notes).To(ContainSubstring("national `bioclima.db` ids"))
		Expect(notes).To(ContainSubstring("not globally unique"))
		Expect(notes).To(ContainSubstring("parsing_warnings, which ships nowhere else"))
	})

	It("lists the unit map and the tag legend", func() {
		patterns := make([]string, len(d.Units))
		for i, u := range d.Units {
			patterns[i] = u.Pattern
		}
		Expect(patterns).To(Equal([]string{"_c", "_mm", "_mmpd", "_pct", "_gkg", "_ms", "_deg", "_wm2", "_kpa", "_km", "_m", "_days",
			"lat", "lon", "clearness_index", "gwet_top", "gwet_root", "gwet_prof", "wmo_completeness_*"}))
		tags := make([]string, len(d.Tags))
		for i, t := range d.Tags {
			tags[i] = t.Tag
		}
		Expect(tags).To(Equal([]string{"conagua_published", "conagua_observed", "nasa_power", "bioclima_derived"}))
	})
})

var _ = Describe("RenderDictionaryMarkdown and RenderDictionaryJSON", func() {
	var (
		d  *publish.Dictionary
		md []byte
		js []byte
	)

	BeforeEach(func() {
		d = buildDictionary()
		md = renderMarkdown(d)
		js = renderJSON(d)
	})

	It("renders byte-identical output across two builds and two renders", func() {
		again := buildDictionary()
		Expect(renderMarkdown(again)).To(Equal(md))
		Expect(renderJSON(again)).To(Equal(js))
		Expect(renderMarkdown(d)).To(Equal(md))
	})

	It("writes LF-only Markdown ending in exactly one newline, in the fixed section order", func() {
		Expect(bytes.Contains(md, []byte("\r"))).To(BeFalse())
		Expect(bytes.HasSuffix(md, []byte("\n"))).To(BeTrue())
		Expect(bytes.HasSuffix(md, []byte("\n\n"))).To(BeFalse())
		var sections []string
		for _, line := range strings.Split(string(md), "\n") {
			if strings.HasPrefix(line, "## ") {
				sections = append(sections, line)
			}
		}
		Expect(sections).To(Equal([]string{
			"## 1. Source tags", "## 2. Units", "## 3. Files", "## 4. NASA POWER parameters",
			"## 5. Documented constants", "## 6. Dropped columns", "## 7. profile.json", "## 8. daily.json",
			"## 9. The annual slot", "## 10. Format contract", "## 11. Precision", "## 12. The SQLite files",
		}))
		Expect(string(md)).To(HavePrefix("# Data dictionary — " + publish.DatasetTitle + "\n"))
		Expect(string(md)).To(ContainSubstring("Snapshot `2026-07-18`, schema version " + strconv.Itoa(schema.Version) + ", ETL git SHA `0123abcd`"))
	})

	It("parses as JSON with explicit nulls, two-space indent, and no HTML escaping, ending in one newline", func() {
		Expect(json.Valid(js)).To(BeTrue())
		Expect(bytes.HasSuffix(js, []byte("}\n"))).To(BeTrue())
		Expect(bytes.HasPrefix(js, []byte("{\n  \"dataset\": {\n"))).To(BeTrue())
		Expect(bytes.Contains(js, []byte(`\u003c`))).To(BeFalse(), "a placeholder's angle brackets read as written")
		Expect(bytes.Contains(js, []byte(`"<state>.db"`))).To(BeTrue())
		Expect(bytes.Contains(js, []byte("→"))).To(BeTrue())
		Expect(bytes.Contains(js, []byte(`"decimals": null`))).To(BeTrue(), "a text column's decimals are an explicit null")
		Expect(bytes.Contains(js, []byte(`"tag": null`))).To(BeTrue(), "a run ledger's tag is an explicit null")
		Expect(bytes.Contains(js, []byte(`"power": null`))).To(BeTrue())

		var back publish.Dictionary
		Expect(json.Unmarshal(js, &back)).To(Succeed())
		Expect(back.Files).To(HaveLen(len(d.Files)))
		for i, f := range back.Files {
			Expect(f).To(Equal(d.Files[i]))
		}
		Expect(back.Power).To(Equal(d.Power))
		Expect(back.Constants).To(Equal(d.Constants))
		Expect(back.Dropped).To(Equal(d.Dropped))
		Expect(back.Profile).To(Equal(d.Profile))
		Expect(back.DailyJSON).To(Equal(d.DailyJSON))
		Expect(back.Annual).To(Equal(d.Annual))
		Expect(back.Format).To(Equal(d.Format))
		Expect(back.Precision).To(Equal(d.Precision))
		Expect(back.SQLite).To(Equal(d.SQLite))
		Expect(back.Dataset).To(Equal(d.Dataset))
		Expect(back.Tags).To(Equal(d.Tags))
		Expect(back.Units).To(Equal(d.Units))
	})

	It("lists the same columns per file in the Markdown as in the JSON", func() {
		var back publish.Dictionary
		Expect(json.Unmarshal(js, &back)).To(Succeed())
		fromMarkdown := markdownColumns(md)
		Expect(fromMarkdown).To(HaveLen(len(back.Files)))
		for _, f := range back.Files {
			Expect(fromMarkdown).To(HaveKey(f.Name))
			Expect(fromMarkdown[f.Name]).To(Equal(names(f.Columns)), f.Name)
		}
	})

	It("renders every column's decimals, unit, source, tag, and description into its row", func() {
		Expect(string(md)).To(ContainSubstring(
			"| `t2m_c` | real | 2 | °C | `monthly_supplement.t2m_c` | nasa_power | mean air temperature at 2 m |"))
		Expect(string(md)).To(ContainSubstring(
			"| `run_label` | text | — | — | synthesized | — | natural label power-<temporal_mode>-<period_start_year>-<period_end_year>, synthesized at export; the surrogate id is never exported |"))
		Expect(string(md)).To(ContainSubstring(
			"| `distance_km` | real | 3 | km | join context | bioclima_derived |"))
		Expect(string(md)).To(ContainSubstring(
			"| `station_id` | text | — | — | `monthly_normals.station_id → stations.external_id` | conagua_published |"))
		Expect(string(md)).To(ContainSubstring("| `ALLSKY_SFC_SW_DWN` | `solar_ghi_wm2` | MJ/m^2/day | W/m^2 | 11.574074074074074 |"))
		Expect(string(md)).To(ContainSubstring("| `power_runs.solar_conversion` | `11.574074074074074` |"))
		Expect(string(md)).To(ContainSubstring("  - `lat` — number | null (6 decimals, decimal degrees) — "))
		Expect(string(md)).To(ContainSubstring("  - `altitude_m` — number | null (1 decimal) — "))
		Expect(string(md)).To(ContainSubstring("      - `tmax_c` — array[13] of number | null (1 decimal, °C) — "))
	})

	It("documents at two decimals the seven columns whose second decimal is load-bearing, and leaves the genuinely one-decimal ones alone", func() {
		// CONAGUA publishes trace ("inappreciable") rain as 0.01 mm, and
		// the other observed dailies carry hundredths too. Documenting
		// one decimal would tell a reader that a trace day is a dry 0.0 —
		// and disagree with the value the shipped SQLite holds for that
		// same observation. The count is pinned here as a literal, not
		// read from schema.Precision, so a revert of the contract fails
		// this spec instead of travelling silently into the deposit.
		for _, x := range []struct {
			spec    publish.FileSpec
			columns []string
		}{
			{publish.DailyObservations, []string{"tmax", "tmin", "precip", "evap"}},
			{publish.MonthlyNormalsExtras, []string{"tmax_daily_extreme", "tmin_daily_extreme", "precip_daily_extreme"}},
		} {
			for _, dbName := range x.columns {
				name := publish.ExportName(x.spec.Table, dbName)
				col := columnByName(fileByName(d, x.spec.Name), name)
				Expect(deref(col.Decimals)).To(Equal(2), "%s.%s", x.spec.Name, name)
				Expect(string(md)).To(ContainSubstring(
					"| `" + name + "` | real | 2 | " + deref(col.Unit) + " | `" + x.spec.Table + "." + dbName + "` |"))
			}
		}
		// The normals proper, the monthly extremes and rain_days do
		// terminate at one decimal upstream, and stay there.
		for _, x := range []struct{ file, column string }{
			{publish.MonthlyNormals.Name, "precip_mm"},
			{publish.MonthlyNormals.Name, "tmax_c"},
			{publish.MonthlyNormalsExtras.Name, "precip_monthly_extreme_mm"},
			{publish.MonthlyNormalsExtras.Name, "rain_days"},
		} {
			Expect(deref(columnByName(fileByName(d, x.file), x.column).Decimals)).To(Equal(1), "%s.%s", x.file, x.column)
		}
	})

	It("carries the corrected descriptions into both renderings, and the wording the audit refused into neither", func() {
		var back publish.Dictionary
		Expect(json.Unmarshal(js, &back)).To(Succeed())

		monthly := columnByName(fileByName(&back, publish.PowerMonthly.Name), "t2m_max_c").Description
		daily := columnByName(fileByName(&back, publish.PowerDaily.Name), "t2m_max_c").Description
		Expect(daily).To(Equal("daily max at 2 m"))
		Expect(monthly).To(HavePrefix("monthly maximum air temperature at 2 m: POWER's maximum for the calendar month"))
		Expect(string(md)).To(ContainSubstring("| `t2m_max_c` | real | 2 | °C | `monthly_supplement.t2m_max_c` | nasa_power | " + monthly + " |"))
		Expect(string(md)).To(ContainSubstring("| `t2m_max_c` | real | 2 | °C | `daily_supplement.t2m_max_c` | nasa_power | " + daily + " |"))

		first := columnByName(fileByName(&back, publish.Stations.Name), "first_year").Description
		Expect(first).To(Equal(nodeAt(back.Profile.Keys, "identity.first_year").Description))
		Expect(string(md)).To(ContainSubstring("| `first_year` | integer | 0 | — | `stations.first_year` | bioclima_derived | " + first + " |"))
		Expect(string(md)).To(ContainSubstring("  - `first_year` — integer | null — " + first))

		for _, out := range [][]byte{md, js} {
			Expect(string(out)).NotTo(ContainSubstring("first year with data"))
			Expect(string(out)).NotTo(ContainSubstring("last year with data"))
		}
	})

	It("carries exactly the settled creator identity in the JSON rendering, and no personal fields in either", func() {
		// The JSON's dataset block names the creator and carries the ORCID once.
		Expect(strings.ToLower(string(js))).To(ContainSubstring("pablo trinidad"))
		ids := orcidShapedRE.FindAllString(string(js), -1)
		Expect(ids).To(HaveLen(1))
		Expect(ids[0]).To(Equal("0009-0007-4050-494X"))
		for _, out := range [][]byte{md, js} {
			for _, forbidden := range []string{"affiliation", "@"} {
				Expect(string(out)).NotTo(ContainSubstring(forbidden), forbidden)
			}
		}
		var back publish.Dictionary
		Expect(json.Unmarshal(js, &back)).To(Succeed())
		Expect(back.Dataset.Creator).To(Equal(publish.DatasetCreator))
		Expect(back.Dataset.SuggestedCitation).To(HavePrefix(publish.DatasetCreator + " (2026). "))
	})

	It("states every provisional choice in reader terms: no internal tracking numbers or internal paths in either rendering", func() {
		for _, out := range [][]byte{md, js} {
			text := string(out)
			Expect(strings.ToLower(text)).NotTo(ContainSubstring("escalation"))
			Expect(strings.ToLower(text)).NotTo(ContainSubstring("interim"))
			Expect(text).NotTo(ContainSubstring("handoff"))
			Expect(ledgerNumberRE.FindString(text)).To(BeEmpty())
			Expect(strings.Count(text, "provisional")).To(BeNumerically(">=", 6))
		}
	})
})

func ptr(s string) *string { return &s }

// nodeAt resolves a dotted path in a node tree.
func nodeAt(nodes []publish.DictionaryNode, path string) publish.DictionaryNode {
	GinkgoHelper()
	key, rest, more := strings.Cut(path, ".")
	for _, n := range nodes {
		if n.Key != key {
			continue
		}
		if !more {
			return n
		}
		return nodeAt(n.Children, rest)
	}
	Fail("no profile key " + path)
	return publish.DictionaryNode{}
}

// profilePaths lists every JSON key path of a struct type, recursing
// into nested structs, pointers to structs, and string-keyed maps (a
// "<period>" child standing for the keys) — written independently of
// the dictionary's own walk.
func profilePaths(t reflect.Type, path string) []string {
	var out []string
	for i := range t.NumField() {
		f := t.Field(i)
		key, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		p := key
		if path != "" {
			p = path + "." + key
		}
		out = append(out, p)
		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		switch {
		case ft.Kind() == reflect.Map:
			p += ".<period>"
			out = append(out, p)
			elem := ft.Elem()
			if elem.Kind() == reflect.Pointer {
				elem = elem.Elem()
			}
			// A month-series block's keys are the flat file's columns, not
			// struct fields; every other period map holds a struct.
			if elem.Kind() == reflect.Struct && elem.Name() != "monthBlock" {
				out = append(out, profilePaths(elem, p)...)
			}
		case ft.Kind() == reflect.Struct && ft.PkgPath() == t.PkgPath() && !strings.HasPrefix(ft.Name(), "json"):
			out = append(out, profilePaths(ft, p)...)
		}
	}
	return out
}
