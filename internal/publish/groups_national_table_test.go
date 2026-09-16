package publish_test

// The --only selector over the deposit-wide groups, as a table: every
// national group and the raw snapshot refused under --state as typed,
// alone or beside a per-state group, whatever the case; a full-run
// request resolving into build order whatever its spelling, order, or
// repetition — every permutation of a mixed request the same list; a
// copy-built group refused without its source wherever it is typed —
// two of them naming the first in build order; near-miss names refused
// as unknown; and the refusals judged in the order the names were
// typed.

import (
	"fmt"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
)

var allGroupsInOrder = []publish.Group{
	publish.GroupTabular, publish.GroupJSON,
	publish.GroupNationalCSV, publish.GroupNationalParquet, publish.GroupNationalJSON, publish.GroupNationalSQLite,
	publish.GroupRaw,
	publish.GroupDocs,
}

// permutations lists every ordering of names.
func permutations(names []string) [][]string {
	if len(names) <= 1 {
		return [][]string{slices.Clone(names)}
	}
	var out [][]string
	for i, n := range names {
		rest := slices.Concat(names[:i], names[i+1:])
		for _, p := range permutations(rest) {
			out = append(out, append([]string{n}, p...))
		}
	}
	return out
}

var _ = Describe("ResolveOnly over the deposit-wide groups", func() {
	DescribeTable("refuses every deposit-wide group under --state, as typed, alone or beside a per-state group",
		func(name string) {
			for _, requested := range [][]string{
				{name}, {"tabular", name}, {name, "json"}, {"TABULAR", "json", name}, {name, name},
			} {
				got, err := publish.ResolveOnly(requested, true)
				Expect(err).To(MatchError(fmt.Sprintf("artifact group %q is built only in a full run: drop --state", name)), "%v", requested)
				Expect(got).To(BeNil())
			}
			// The same request is fine in a full run once its source rides
			// along.
			_, err := publish.ResolveOnly([]string{"tabular", "json", name}, false)
			Expect(err).NotTo(HaveOccurred())
		},
		Entry("national-csv", "national-csv"),
		Entry("NATIONAL-CSV", "NATIONAL-CSV"),
		Entry("national-parquet", "national-parquet"),
		Entry("National-Parquet", "National-Parquet"),
		Entry("national-json", "national-json"),
		Entry("national-JSON", "national-JSON"),
		Entry("national-sqlite", "national-sqlite"),
		Entry("NATIONAL-SQLITE", "NATIONAL-SQLITE"),
		Entry("raw", "raw"),
		Entry("Raw", "Raw"),
		Entry("RAW", "RAW"),
	)

	DescribeTable("resolves a full-run request into build order whatever its spelling, order, or repetition",
		func(requested []string, want []publish.Group) {
			got, err := publish.ResolveOnly(requested, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(want))
		},
		Entry("all eight in build order",
			[]string{"tabular", "json", "national-csv", "national-parquet", "national-json", "national-sqlite", "raw", "docs"},
			allGroupsInOrder),
		Entry("all eight reversed",
			[]string{"docs", "raw", "national-sqlite", "national-json", "national-parquet", "national-csv", "json", "tabular"},
			allGroupsInOrder),
		Entry("all eight rotated and upper-cased",
			[]string{"NATIONAL-JSON", "NATIONAL-SQLITE", "RAW", "DOCS", "TABULAR", "JSON", "NATIONAL-CSV", "NATIONAL-PARQUET"},
			allGroupsInOrder),
		Entry("every name twice",
			[]string{"raw", "raw", "json", "json", "tabular", "tabular", "national-csv", "national-csv", "docs", "docs",
				"national-json", "national-json", "national-parquet", "national-parquet", "national-sqlite", "national-sqlite"},
			allGroupsInOrder),
		Entry("docs alone", []string{"docs"}, []publish.Group{publish.GroupDocs}),
		Entry("docs ahead of the raw snapshot",
			[]string{"docs", "raw"}, []publish.Group{publish.GroupRaw, publish.GroupDocs}),
		Entry("raw alone, repeated across cases",
			[]string{"raw", "RAW", "Raw"}, []publish.Group{publish.GroupRaw}),
		Entry("the regenerated pair out of order",
			[]string{"national-sqlite", "national-parquet"},
			[]publish.Group{publish.GroupNationalParquet, publish.GroupNationalSQLite}),
		Entry("the regenerated groups and raw, raw first",
			[]string{"raw", "national-parquet", "national-sqlite"},
			[]publish.Group{publish.GroupNationalParquet, publish.GroupNationalSQLite, publish.GroupRaw}),
		Entry("a copy-built group before its source",
			[]string{"national-json", "json"},
			[]publish.Group{publish.GroupJSON, publish.GroupNationalJSON}),
		Entry("both copy-built groups and both sources, shuffled and repeated",
			[]string{"national-csv", "json", "national-json", "tabular", "JSON", "national-csv"},
			[]publish.Group{publish.GroupTabular, publish.GroupJSON, publish.GroupNationalCSV, publish.GroupNationalJSON}),
	)

	It("resolves every permutation of a mixed four-group request to the same build order", func() {
		names := []string{"raw", "national-sqlite", "json", "TABULAR"}
		perms := permutations(names)
		Expect(perms).To(HaveLen(24))
		for _, requested := range perms {
			got, err := publish.ResolveOnly(requested, false)
			Expect(err).NotTo(HaveOccurred(), "%v", requested)
			Expect(got).To(Equal([]publish.Group{
				publish.GroupTabular, publish.GroupJSON, publish.GroupNationalSQLite, publish.GroupRaw,
			}), "%v", requested)
		}
	})

	DescribeTable("refuses a copy-built group without its source wherever it is typed",
		func(requested []string, group, source string) {
			got, err := publish.ResolveOnly(requested, false)
			Expect(err).To(MatchError(fmt.Sprintf("artifact group %q copies from the %s archives: add %s to --only", group, source, source)))
			Expect(got).To(BeNil())
		},
		Entry("national-csv first, beside json", []string{"national-csv", "json"}, "national-csv", "tabular"),
		Entry("national-csv last, beside json", []string{"json", "national-csv"}, "national-csv", "tabular"),
		Entry("national-csv upper-cased, named in the message lower-cased", []string{"NATIONAL-CSV", "json"}, "national-csv", "tabular"),
		Entry("national-json beside tabular", []string{"tabular", "national-json"}, "national-json", "json"),
		Entry("national-json among the regenerated groups", []string{"national-parquet", "national-json", "raw"}, "national-json", "json"),
		Entry("a repeated copy-built name is one refusal", []string{"national-csv", "national-csv"}, "national-csv", "tabular"),
		Entry("the copy-built group alone", []string{"national-json"}, "national-json", "json"),
	)

	It("refuses two copy-built groups lacking both sources naming the first in build order, however they were typed", func() {
		for _, requested := range [][]string{
			{"national-csv", "national-json"}, {"national-json", "national-csv"}, {"NATIONAL-JSON", "raw", "national-csv"},
		} {
			got, err := publish.ResolveOnly(requested, false)
			Expect(got).To(BeNil(), "%v", requested)
			Expect(err).To(MatchError(`artifact group "national-csv" copies from the tabular archives: add tabular to --only`), "%v", requested)
		}
	})

	DescribeTable("refuses a near-miss of a deposit-wide name as unknown, in a full run and under --state alike",
		func(name string) {
			for _, subset := range []bool{false, true} {
				got, err := publish.ResolveOnly([]string{name}, subset)
				Expect(err).To(MatchError(fmt.Sprintf("unknown artifact group %q (valid: %s)", name, validGroups)), "subset=%t", subset)
				Expect(got).To(BeNil())
			}
		},
		Entry("the family without a format", "national"),
		Entry("a bare hyphen", "national-"),
		Entry("an underscore", "national_csv"),
		Entry("the archive name", "national-csv.zip"),
		Entry("the dated raw name", "conagua-raw"),
		Entry("padded", " raw"),
		Entry("two names in one token", "national-csv,raw"),
		Entry("the format alone", "sqlite"),
	)

	It("allows the docs group under --state wherever it is typed, and refuses a national name beside it", func() {
		for _, requested := range [][]string{{"docs"}, {"tabular", "docs"}, {"docs", "json"}, {"DOCS", "Docs"}} {
			got, err := publish.ResolveOnly(requested, true)
			Expect(err).NotTo(HaveOccurred(), "%v", requested)
			Expect(got[len(got)-1]).To(Equal(publish.GroupDocs), "%v", requested)
		}
		_, err := publish.ResolveOnly([]string{"docs", "raw"}, true)
		Expect(err).To(MatchError(`artifact group "raw" is built only in a full run: drop --state`))
	})

	It("judges the names in the order typed: an unknown name before a national one ahead of it under --state, and a national one before an unknown name after it", func() {
		_, err := publish.ResolveOnly([]string{"xyz", "raw"}, true)
		Expect(err).To(MatchError(`unknown artifact group "xyz" (valid: ` + validGroups + `)`))
		_, err = publish.ResolveOnly([]string{"raw", "xyz"}, true)
		Expect(err).To(MatchError(`artifact group "raw" is built only in a full run: drop --state`))
		// The scope refusal precedes the source check: a copy-built group
		// under --state is refused for the scope, not for its source.
		_, err = publish.ResolveOnly([]string{"national-csv"}, true)
		Expect(err).To(MatchError(`artifact group "national-csv" is built only in a full run: drop --state`))
	})
})
