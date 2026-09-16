package publish_test

// Specs for the --only selector as a table — every valid spelling
// resolving into build order with duplicates dropped, every invalid one
// refused as typed — and for the out-dir discipline across groups
// through Run: a second build whose group set does not produce the
// archive the first left is refused before anything is written, naming
// the archive; one whose set does produce it converges in place, the
// earlier archive's bytes unchanged; and a group-subset build meeting a
// wider build's residue is refused with one sorted list naming the other
// state's archives and the other group's alike.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
)

var (
	bothGroups  = []publish.Group{publish.GroupTabular, publish.GroupJSON}
	tabularOnly = []publish.Group{publish.GroupTabular}
	jsonOnly    = []publish.Group{publish.GroupJSON}
)

var _ = Describe("the --only selector", func() {
	DescribeTable("resolves every valid request of a --state run into build order, duplicates dropped",
		func(requested []string, want []publish.Group) {
			got, err := publish.ResolveOnly(requested, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(want))
		},
		Entry("nothing requested", nil, bothGroups),
		Entry("an empty request", []string{}, bothGroups),
		Entry("tabular", []string{"tabular"}, tabularOnly),
		Entry("json", []string{"json"}, jsonOnly),
		Entry("both, in build order", []string{"tabular", "json"}, bothGroups),
		Entry("both, reversed", []string{"json", "tabular"}, bothGroups),
		Entry("upper case", []string{"JSON", "TABULAR"}, bothGroups),
		Entry("mixed case", []string{"jSoN"}, jsonOnly),
		Entry("the same name repeated", []string{"json", "json", "JSON"}, jsonOnly),
		Entry("both repeated across cases", []string{"Json", "tabular", "JSON", "Tabular"}, bothGroups),
	)

	DescribeTable("refuses an invalid name as typed, naming the valid groups",
		func(requested []string, offender string) {
			got, err := publish.ResolveOnly(requested, false)
			Expect(err).To(MatchError(fmt.Sprintf("unknown artifact group %q (valid: %s)", offender, validGroups)))
			Expect(got).To(BeNil())
		},
		Entry("a misspelling", []string{"jsn"}, "jsn"),
		Entry("a format rather than a group", []string{"parquet"}, "parquet"),
		Entry("the empty string", []string{""}, ""),
		Entry("surrounding whitespace", []string{" json"}, " json"),
		Entry("two names in one token", []string{"tabular,json"}, "tabular,json"),
		Entry("a valid name beside an invalid one", []string{"json", "csv"}, "csv"),
		Entry("the first invalid name of several", []string{"xyz", "abc"}, "xyz"),
		Entry("the archive suffix rather than the group", []string{"json.zip"}, "json.zip"),
	)
})

var _ = Describe("the out-dir discipline across groups", func() {
	var (
		db  *sql.DB
		out string
	)

	BeforeEach(func() {
		db = openTempDB()
		seedTwoStates(db)
		seedRuns(db)
		out = filepath.Join(GinkgoT().TempDir(), "publish")
	})

	run := func(states, only []string) (*publish.Report, error) {
		return publish.Run(context.Background(), db,
			publish.Options{OutDir: out, States: states, Only: only, Now: fixedClock}, nil)
	}

	// snapshotDir reads every file of the out dir, so a refused run can be
	// held to have changed nothing.
	snapshotDir := func() map[string][]byte {
		GinkgoHelper()
		files := map[string][]byte{}
		for _, name := range dirNames(out) {
			data, err := os.ReadFile(filepath.Join(out, name))
			Expect(err).NotTo(HaveOccurred())
			files[name] = data
		}
		return files
	}

	DescribeTable("refuses a second build whose group set does not produce the archive the first left, and converges when it does",
		func(first, second []string, foreign string, wantFiles []string) {
			_, err := run([]string{"yuc"}, first)
			Expect(err).NotTo(HaveOccurred())
			before := snapshotDir()

			report, err := run([]string{"yuc"}, second)
			if foreign != "" {
				Expect(err).To(MatchError("out dir " + out + " holds entries this run does not produce: " +
					foreign + " (remove them or use a fresh --out)"))
				Expect(report.Artifacts).To(BeEmpty())
				Expect(report.TopLevelFiles).To(BeZero())
				Expect(snapshotDir()).To(Equal(before))
				return
			}
			Expect(err).NotTo(HaveOccurred())
			Expect(report.Failed).To(BeZero())
			Expect(dirNames(out)).To(Equal(wantFiles))
			Expect(report.TopLevelFiles).To(Equal(len(wantFiles)))
			listed := []string{"CHECKSUMS"}
			for _, s := range readChecksums(out) {
				listed = append(listed, s.Name)
			}
			Expect(listed).To(Equal(wantFiles))
			// The archives the first build wrote are the same bytes.
			after := snapshotDir()
			for name, data := range before {
				if strings.HasSuffix(name, ".zip") {
					Expect(after[name]).To(Equal(data), name)
				}
			}
		},
		Entry("tabular, then json", []string{"tabular"}, []string{"json"}, "yuc-tabular.zip", nil),
		Entry("json, then tabular", []string{"json"}, []string{"tabular"}, "yuc-json.zip", nil),
		Entry("both, then json", nil, []string{"json"}, "yuc-tabular.zip", nil),
		Entry("both, then tabular", nil, []string{"tabular"}, "yuc-json.zip", nil),
		Entry("tabular, then both", []string{"tabular"}, nil, "",
			[]string{"CHECKSUMS", "manifest.json", "yuc-json.zip", "yuc-tabular.zip"}),
		Entry("json, then both", []string{"json"}, nil, "",
			[]string{"CHECKSUMS", "manifest.json", "yuc-json.zip", "yuc-tabular.zip"}),
		Entry("json, then json", []string{"json"}, []string{"json"}, "",
			[]string{"CHECKSUMS", "manifest.json", "yuc-json.zip"}),
		Entry("tabular, then tabular", []string{"tabular"}, []string{"tabular"}, "",
			[]string{"CHECKSUMS", "manifest.json", "yuc-tabular.zip"}),
	)

	It("names a foreign state's archives and the foreign group's archive together, sorted, when a group-subset build meets a wider build's residue", func() {
		_, err := run(nil, stateGroups)
		Expect(err).NotTo(HaveOccurred())
		Expect(dirNames(out)).To(Equal([]string{
			"CHECKSUMS", "ags-json.zip", "ags-tabular.zip", "manifest.json", "yuc-json.zip", "yuc-tabular.zip",
		}))
		before := snapshotDir()

		report, err := run([]string{"yuc"}, []string{"json"})
		Expect(err).To(MatchError("out dir " + out + " holds entries this run does not produce: " +
			"ags-json.zip, ags-tabular.zip, yuc-tabular.zip (remove them or use a fresh --out)"))
		Expect(report.Artifacts).To(BeEmpty())
		Expect(snapshotDir()).To(Equal(before))
	})
})
