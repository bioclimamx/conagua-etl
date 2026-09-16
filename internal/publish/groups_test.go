package publish_test

// Specs for the artifact-group selector: the full set in build order,
// case-insensitive matching, duplicates dropped, build order kept
// regardless of how the names were typed, the per-state scope of a
// --state run — an empty request selecting the per-state groups alone,
// a national or raw name refused, the docs group allowed when named —
// a copy-built national group refused without the group it copies
// from, and an unknown name refused with the valid list.

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
)

const validGroups = "tabular, json, national-csv, national-parquet, national-json, national-sqlite, raw, docs"

var _ = Describe("ResolveOnly", func() {
	It("selects every group, in build order, for an empty request in a full run", func() {
		got, err := publish.ResolveOnly(nil, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]publish.Group{
			publish.GroupTabular, publish.GroupJSON,
			publish.GroupNationalCSV, publish.GroupNationalParquet, publish.GroupNationalJSON, publish.GroupNationalSQLite,
			publish.GroupRaw,
			publish.GroupDocs,
		}))
		Expect(got).To(Equal(publish.Groups()))
	})

	It("selects the per-state groups alone for an empty request in a --state run", func() {
		got, err := publish.ResolveOnly(nil, true)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]publish.Group{publish.GroupTabular, publish.GroupJSON}))
		for _, g := range got {
			Expect(g.PerState()).To(BeTrue(), string(g))
			Expect(g.FullRunOnly()).To(BeFalse(), string(g))
		}
		for _, g := range publish.Groups()[2:] {
			Expect(g.PerState()).To(BeFalse(), string(g))
		}
	})

	It("allows the docs group in a --state run when named, alone or beside a per-state group, last in build order", func() {
		got, err := publish.ResolveOnly([]string{"docs"}, true)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]publish.Group{publish.GroupDocs}))
		got, err = publish.ResolveOnly([]string{"DOCS", "json"}, true)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]publish.Group{publish.GroupJSON, publish.GroupDocs}))
		Expect(publish.GroupDocs.PerState()).To(BeFalse())
		Expect(publish.GroupDocs.FullRunOnly()).To(BeFalse())
		for _, g := range publish.Groups()[2:7] {
			Expect(g.FullRunOnly()).To(BeTrue(), string(g))
		}
	})

	It("matches names case-insensitively, drops duplicates, and keeps build order", func() {
		got, err := publish.ResolveOnly([]string{"JSON", "tabular", "Json"}, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]publish.Group{publish.GroupTabular, publish.GroupJSON}))

		got, err = publish.ResolveOnly([]string{"json"}, true)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]publish.Group{publish.GroupJSON}))

		got, err = publish.ResolveOnly([]string{"raw", "National-SQLite", "national-csv", "tabular", "RAW"}, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]publish.Group{
			publish.GroupTabular, publish.GroupNationalCSV, publish.GroupNationalSQLite, publish.GroupRaw,
		}))
	})

	It("refuses a national or raw group in a --state run, as typed", func() {
		for _, name := range []string{"national-csv", "national-parquet", "national-json", "National-SQLite", "raw"} {
			_, err := publish.ResolveOnly([]string{"tabular", name}, true)
			Expect(err).To(MatchError(`artifact group "` + name + `" is built only in a full run: drop --state`))
		}
		got, err := publish.ResolveOnly([]string{"raw"}, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]publish.Group{publish.GroupRaw}))
	})

	It("refuses a copy-built national group without the per-state group it copies from", func() {
		_, err := publish.ResolveOnly([]string{"national-csv"}, false)
		Expect(err).To(MatchError(`artifact group "national-csv" copies from the tabular archives: add tabular to --only`))
		_, err = publish.ResolveOnly([]string{"tabular", "national-json"}, false)
		Expect(err).To(MatchError(`artifact group "national-json" copies from the json archives: add json to --only`))

		got, err := publish.ResolveOnly([]string{"national-json", "json"}, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]publish.Group{publish.GroupJSON, publish.GroupNationalJSON}))
		// The regenerated archives copy from nothing.
		got, err = publish.ResolveOnly([]string{"national-parquet", "national-sqlite", "raw"}, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]publish.Group{publish.GroupNationalParquet, publish.GroupNationalSQLite, publish.GroupRaw}))
	})

	It("refuses an unknown name, listing the valid ones", func() {
		_, err := publish.ResolveOnly([]string{"tabular", "xyz"}, false)
		Expect(err).To(MatchError(`unknown artifact group "xyz" (valid: ` + validGroups + `)`))
		_, err = publish.ResolveOnly([]string{""}, true)
		Expect(err).To(MatchError(`unknown artifact group "" (valid: ` + validGroups + `)`))
	})

	It("hands out a copy of the group list, so a caller cannot reorder the build", func() {
		groups := publish.Groups()
		groups[0] = publish.GroupJSON
		Expect(publish.Groups()[0]).To(Equal(publish.GroupTabular))
	})
})
