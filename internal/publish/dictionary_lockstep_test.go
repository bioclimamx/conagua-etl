package publish_test

// The generated dictionary held to the embedded DDL in both directions
// through schema.Tables(), the parse the dictionary itself reads: every
// column with a DDL source names a column the DDL declares, under the
// kind its type implies and the export name; per exported table the
// exported and dropped columns together are exactly the DDL's column
// set, with no overlap; every description the DDL comments is the DDL's,
// a bare format note appended to the fallback instead; the export
// renames are export-only; and the FileSpecs the files are built from
// name only DDL columns, so a spec column absent from the DDL cannot
// reach the dictionary either.

import (
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// parsedDDL indexes schema.Tables() by table then column.
func parsedDDL() map[string]map[string]schema.Column {
	idx := map[string]map[string]schema.Column{}
	for _, t := range schema.Tables() {
		cols := map[string]schema.Column{}
		for _, c := range t.Columns {
			cols[c.Name] = c
		}
		idx[t.Name] = cols
	}
	return idx
}

// sourceOf splits a dictionary column's DDL source — "table.column",
// with an optional " → stations.external_id" resolution note — into its
// table and column; ok is false for the two non-DDL sources.
func sourceOf(source string) (table, column string, ok bool) {
	if source == "synthesized" || source == "join context" {
		return "", "", false
	}
	table, column, found := strings.Cut(strings.SplitN(source, " ", 2)[0], ".")
	return table, column, found
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

var _ = Describe("Dictionary ↔ DDL lockstep, both directions", func() {
	var (
		d   *publish.Dictionary
		ddl map[string]map[string]schema.Column
	)

	BeforeEach(func() {
		d = buildDictionary()
		ddl = parsedDDL()
	})

	It("names, for every column with a DDL source, a column the DDL declares — under the kind its type implies and its renamed export name", func() {
		checked := 0
		for _, f := range d.Files {
			for _, c := range f.Columns {
				table, column, ok := sourceOf(c.Source)
				if !ok {
					continue
				}
				col, declared := ddl[table][column]
				Expect(declared).To(BeTrue(), "%s.%s: source %q names no DDL column", f.Name, c.Name, c.Source)
				Expect(c.Name).To(Equal(publish.ExportName(table, column)), "%s: %s", f.Name, c.Source)
				switch {
				case col.Type == "REAL":
					Expect(c.Kind).To(Equal("real"), "%s.%s", f.Name, c.Name)
				case col.Type == "INTEGER" && column == "station_id":
					// The surrogate resolves to stations.external_id, a TEXT.
					Expect(c.Kind).To(Equal("text"), "%s.%s", f.Name, c.Name)
					Expect(c.Source).To(HaveSuffix(" → stations.external_id"))
				case col.Type == "INTEGER":
					Expect(c.Kind).To(Equal("integer"), "%s.%s", f.Name, c.Name)
				default:
					Expect(col.Type).To(Equal("TEXT"), "%s.%s", f.Name, c.Name)
					Expect(c.Kind).To(BeElementOf("text", "date", "period"), "%s.%s", f.Name, c.Name)
				}
				checked++
			}
		}
		// Every DDL-backed spec column was seen: the specs' columns less
		// the synthesized run_label and the combined files' join context.
		want := 0
		for _, s := range dictionarySpecs {
			for _, c := range s.spec.Columns {
				if c.DB == "" || s.source != nil && s.source(c) == "station_power_cell" {
					continue
				}
				want++
			}
		}
		Expect(checked).To(Equal(want))
		Expect(checked).To(Equal(234))
	})

	It("reads the combined files' join context from station_power_cell, whose DDL declares both columns under the kinds their types imply", func() {
		for _, name := range []string{"combined/combined_monthly", "combined/combined_daily"} {
			var context []string
			for _, c := range fileByName(d, name).Columns {
				if c.Source != "join context" {
					continue
				}
				context = append(context, c.Name)
				col, ok := ddl["station_power_cell"][c.Name]
				Expect(ok).To(BeTrue(), "%s: %s", name, c.Name)
				Expect(c.Kind).To(Equal(map[string]string{"TEXT": "text", "REAL": "real"}[col.Type]), "%s: %s", name, c.Name)
			}
			Expect(context).To(Equal([]string{"cell_id", "distance_km"}), name)
		}
	})

	It("covers exactly the DDL's column set of every exported table: exported ∪ dropped = the DDL's columns, exported ∩ dropped = ∅, parsing_warnings untouched", func() {
		exported := map[string]map[string]bool{}
		note := func(table, column string) {
			if exported[table] == nil {
				exported[table] = map[string]bool{}
			}
			exported[table][column] = true
		}
		for _, f := range d.Files {
			for _, c := range f.Columns {
				if c.Source == "join context" {
					note("station_power_cell", c.Name)
					continue
				}
				if table, column, ok := sourceOf(c.Source); ok {
					note(table, column)
				}
			}
		}
		dropped := map[string]map[string]bool{}
		for _, x := range d.Dropped {
			if dropped[x.Table] == nil {
				dropped[x.Table] = map[string]bool{}
			}
			dropped[x.Table][x.Column] = true
		}
		Expect(sortedKeys(exported)).To(Equal([]string{
			"daily_observations", "daily_supplement", "ingest_runs", "monthly_normals", "monthly_normals_extras",
			"monthly_supplement", "nasa_power_grid_cells", "power_runs", "station_power_cell", "stations",
		}))
		for _, t := range schema.Tables() {
			if t.Name == "parsing_warnings" {
				Expect(exported).NotTo(HaveKey(t.Name))
				Expect(dropped).NotTo(HaveKey(t.Name))
				continue
			}
			declared := make([]string, 0, len(t.Columns))
			for _, c := range t.Columns {
				declared = append(declared, c.Name)
			}
			slices.Sort(declared)
			union := map[string]bool{}
			for c := range exported[t.Name] {
				union[c] = true
			}
			for c := range dropped[t.Name] {
				Expect(exported[t.Name]).NotTo(HaveKey(c), "%s.%s is both exported and dropped", t.Name, c)
				union[c] = true
			}
			Expect(sortedKeys(union)).To(Equal(declared), t.Name)
		}
	})

	It("takes every description from the DDL's trailing comment where the column has one — daily_supplement's POWER-31 from monthly_supplement's — and appends a bare format note to the fallback", func() {
		// The four monthly_supplement columns whose meaning does not
		// survive the change of grain. schema.sql states POWER-31 once,
		// written at the daily grain, so the monthly files answer these
		// from their own text instead; dictionary_test pins that wording,
		// and the lockstep holds the departure to exactly this set.
		regrainedColumns := []string{"t2m_max_c", "t2m_min_c", "ts_max_c", "ts_min_c"}
		fromDDL, noted, regrained := 0, 0, 0
		for _, f := range d.Files {
			for _, c := range f.Columns {
				table, column, ok := sourceOf(c.Source)
				if !ok {
					continue
				}
				comment := ddl[table][column].Comment
				if comment == "" && table == "daily_supplement" {
					comment = ddl["monthly_supplement"][column].Comment
				}
				switch {
				case comment == "":
				case table == publish.PowerMonthly.Table && slices.Contains(regrainedColumns, column):
					Expect(c.Description).NotTo(Equal(comment), "%s.%s", f.Name, c.Name)
					regrained++
				case comment == "ISO 'YYYY-MM-DD'":
					// The one format-only comment the DDL carries: the
					// description is the column's own, the note in parentheses.
					Expect(c.Description).To(HaveSuffix(" ("+comment+")"), "%s.%s", f.Name, c.Name)
					Expect(strings.TrimSuffix(c.Description, " ("+comment+")")).NotTo(BeEmpty())
					noted++
				default:
					Expect(c.Description).To(Equal(comment), "%s.%s", f.Name, c.Name)
					fromDDL++
				}
			}
		}
		// The four re-grained columns on each of the two files that read
		// monthly_supplement: nasa_power/monthly and combined_monthly.
		Expect(regrained).To(Equal(2 * 4))
		// POWER-31 on the four POWER-bearing files less those.
		Expect(fromDDL).To(Equal(4*len(power.Registry) - regrained))
		// The two dated columns the DDL annotates with the format alone:
		// daily_supplement.date and
		// monthly_normals_extras.tmax_daily_extreme_date.
		Expect(noted).To(Equal(2))
	})

	It("agrees with the FileSpecs: every spec column with a DB name is a column of its source table in the DDL, so a spec column the DDL lacks cannot reach the dictionary", func() {
		for _, s := range dictionarySpecs {
			for _, c := range s.spec.Columns {
				if c.DB == "" {
					Expect(c.Name).To(Equal(publish.RunLabelColumn), s.spec.Name)
					continue
				}
				table := s.spec.Table
				if s.source != nil {
					table = s.source(c)
				}
				_, ok := ddl[table][c.DB]
				Expect(ok).To(BeTrue(), "%s: %s.%s is not in the DDL", s.spec.Name, table, c.DB)
				Expect(c.Name).To(Equal(publish.ExportName(table, c.DB)), "%s: %s.%s", s.spec.Name, table, c.DB)
			}
		}
	})

	It("keeps the export renames export-only: a renamed export name is never a DDL column of its table", func() {
		renamed := 0
		for _, f := range d.Files {
			for _, c := range f.Columns {
				table, column, ok := sourceOf(c.Source)
				if !ok || c.Name == column {
					continue
				}
				Expect(ddl[table]).NotTo(HaveKey(c.Name), "%s: %s exists in the DDL beside %s", f.Name, c.Name, column)
				renamed++
			}
		}
		// stations.external_id; the five normals variables on
		// monthly_normals and combined_monthly, the four daily ones on
		// daily_observations and combined_daily; the six extras extremes.
		Expect(renamed).To(Equal(1 + (5+4)*2 + 6))
	})
})
