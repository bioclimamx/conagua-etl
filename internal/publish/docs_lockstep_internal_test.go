package publish

// In-package lockstep specs for the README's and the dictionary's
// unexported tables: every docs-group file and every archive group has
// a README description and no description names a file or group that
// does not exist; every README unit gloss names a pattern of the unit
// map; every DDL format note is a trailing comment the embedded DDL
// carries, and a column so annotated is described from its fallback
// with the note appended; the README's decimals table reads the
// dictionary's own file list; and the state database's name has one
// spelling.

import (
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/schema"
)

var _ = ginkgo.Describe("README ↔ artifact set lockstep", func() {
	ginkgo.It("describes every docs-group file DocsFiles lists, and nothing else", func() {
		for _, name := range DocsFiles() {
			gomega.Expect(docsFileDescriptions).To(gomega.HaveKey(name))
			gomega.Expect(docsFileDescriptions[name]).NotTo(gomega.BeEmpty(), name)
		}
		gomega.Expect(docsFileDescriptions).To(gomega.HaveLen(len(DocsFiles())))
	})

	ginkgo.It("describes every archive group Groups lists, and nothing else — the docs group is the metadata table, not an archive", func() {
		archives := 0
		for _, g := range Groups() {
			if g == GroupDocs {
				gomega.Expect(groupDescriptions).NotTo(gomega.HaveKey(g))
				continue
			}
			gomega.Expect(groupDescriptions).To(gomega.HaveKey(g))
			gomega.Expect(groupDescriptions[g]).NotTo(gomega.BeEmpty(), string(g))
			archives++
		}
		gomega.Expect(groupDescriptions).To(gomega.HaveLen(archives))
		gomega.Expect(groupDescriptions[GroupTabular]).To(gomega.HaveSuffix(" " + stateDBName(placeholderShard)))
		gomega.Expect(groupDescriptions[GroupJSON]).To(gomega.ContainSubstring("under combined/<state>/<station_id>/,"))
		gomega.Expect(groupDescriptions[GroupNationalParquet]).To(gomega.Equal("the 10 per-table Parquet files at national scope"))
		gomega.Expect(groupDescriptions[GroupNationalSQLite]).To(gomega.HavePrefix(nationalDBName + ", "))
	})

	ginkgo.It("glosses only patterns of the unit map", func() {
		patterns := map[string]bool{}
		for _, u := range append(append([]DictionaryUnit{}, unitSuffixes...), unitNames...) {
			patterns[u.Pattern] = true
		}
		for key := range unitGlosses {
			gomega.Expect(patterns).To(gomega.HaveKey(key))
		}
		gomega.Expect(unitGlosses).To(gomega.HaveLen(4))
	})

	ginkgo.It("summarizes the dictionary's own files in the decimals table", func() {
		defs := dictionaryFileDefs()
		specs := readmeSpecs()
		gomega.Expect(specs).To(gomega.HaveLen(len(defs)))
		for i, d := range defs {
			gomega.Expect(specs[i].Name).To(gomega.Equal(d.spec.Name))
		}
	})

	ginkgo.It("names the state database once: <slug>.db", func() {
		gomega.Expect(stateDBName(State{Slug: "yuc"})).To(gomega.Equal("yuc.db"))
		gomega.Expect(stateDBName(placeholderShard)).To(gomega.Equal("<state>.db"))
	})
})

var _ = ginkgo.Describe("formatNotes ↔ schema.sql lockstep", func() {
	ginkgo.It("lists only trailing comments the embedded DDL carries, each on a TEXT column", func() {
		carried := map[string][]string{}
		for _, t := range schema.Tables() {
			for _, c := range t.Columns {
				if formatNotes[c.Comment] {
					gomega.Expect(c.Type).To(gomega.Equal("TEXT"), "%s.%s", t.Name, c.Name)
					carried[c.Comment] = append(carried[c.Comment], t.Name+"."+c.Name)
				}
			}
		}
		for note := range formatNotes {
			gomega.Expect(carried).To(gomega.HaveKey(note))
		}
		gomega.Expect(carried["ISO 'YYYY-MM-DD'"]).To(gomega.Equal([]string{
			"monthly_normals_extras.tmax_daily_extreme_date", "daily_supplement.date",
		}))
	})

	ginkgo.It("describes a column so annotated from its fallback with the note appended, and its siblings from the fallback alone", func() {
		ddl := indexDDL(schema.Tables())
		describe := func(spec FileSpec, name string) string {
			for _, c := range spec.Columns {
				if c.Name == name {
					d, err := describeColumn(spec.Table, c, false, ddl)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					return d
				}
			}
			ginkgo.Fail(spec.Name + " has no column " + name)
			return ""
		}
		gomega.Expect(describe(MonthlyNormalsExtras, "tmax_daily_extreme_date")).
			To(gomega.Equal("date of tmax_daily_extreme_c (ISO 'YYYY-MM-DD')"))
		gomega.Expect(describe(MonthlyNormalsExtras, "tmin_daily_extreme_date")).To(gomega.Equal("date of tmin_daily_extreme_c"))
		gomega.Expect(describe(MonthlyNormalsExtras, "precip_daily_extreme_date")).To(gomega.Equal("date of precip_daily_extreme_mm"))
		gomega.Expect(describe(PowerDaily, "date")).To(gomega.Equal("calendar date of the reanalysis value (ISO 'YYYY-MM-DD')"))
		// A real comment is the description as is, the daily table
		// reading the monthly table's.
		gomega.Expect(describe(PowerMonthly, "t2m_c")).To(gomega.Equal("mean air temperature at 2 m"))
		gomega.Expect(describe(PowerDaily, "t2m_c")).To(gomega.Equal("mean air temperature at 2 m"))
		gomega.Expect(strings.HasSuffix(describe(PowerDaily, "t2m_c"), ")")).To(gomega.BeFalse())
	})
})
