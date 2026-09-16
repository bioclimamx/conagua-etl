package publish

// In-package specs holding README.md's artifact tables to plannedNames
// — the list Run checks the out dir against — for a full run over one,
// three, and all 32 states, and holding a --state docs-only run's plan
// to a subset of what the README names: the README describes the
// deposit whole, never the run's scope.

import (
	"bytes"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

const plannedSnapshot = "2026-07-18"

// statesOf builds the State list of the given stored codes, as
// LoadStates would.
func statesOf(codes ...string) []State {
	ginkgo.GinkgoHelper()
	out := make([]State, 0, len(codes))
	for _, code := range codes {
		st, err := stateFromCode(code)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		out = append(out, st)
	}
	return out
}

// allStateCodes is every CONAGUA state code as stations.state stores it.
func allStateCodes() []string {
	out := make([]string, len(conagua.AllStates))
	for i, c := range conagua.AllStates {
		out[i] = strings.ToUpper(string(c))
	}
	return out
}

// readmeOver renders README.md for a deposit of the given states with
// no input beyond the build's identity.
func readmeOver(states []State) string {
	ginkgo.GinkgoHelper()
	in := DocsInput{
		Meta: ProfileMeta{
			SchemaVersion: schema.Version, ETLGitSHA: "abc123", SnapshotDate: plannedSnapshot,
			Dataset: DatasetMetadata(time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC), ""),
		},
		States: states,
		Now:    time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC),
	}
	var b bytes.Buffer
	gomega.Expect(RenderREADME(&b, in)).To(gomega.Succeed())
	return b.String()
}

var backtickedRE = regexp.MustCompile("`([^`]+)`")

// artifactTableNames returns every backticked name in the table rows
// of README's artifact section, in row order: the per-state rows' two
// archives, the national rows' one, the metadata rows' one.
func artifactTableNames(readme string) []string {
	ginkgo.GinkgoHelper()
	start := strings.Index(readme, "\n## 2. The artifact set\n")
	gomega.Expect(start).To(gomega.BeNumerically(">=", 0))
	section := readme[start+1:]
	if end := strings.Index(section[1:], "\n## "); end >= 0 {
		section = section[:end+1]
	}
	var names []string
	for _, line := range strings.Split(section, "\n") {
		if !strings.HasPrefix(line, "| ") || strings.HasPrefix(line, "|---") {
			continue
		}
		for _, m := range backtickedRE.FindAllStringSubmatch(line, -1) {
			names = append(names, m[1])
		}
	}
	return names
}

func sortedNames(names []string) []string {
	out := slices.Clone(names)
	slices.Sort(out)
	return out
}

var _ = ginkgo.Describe("README.md's artifact set against plannedNames", func() {
	ginkgo.DescribeTable("names exactly the top-level files a full run over the states plans, each once",
		func(codes []string) {
			states := statesOf(codes...)
			got := artifactTableNames(readmeOver(states))
			want := plannedNames(states, Groups(), plannedSnapshot)
			gomega.Expect(sortedNames(got)).To(gomega.Equal(sortedNames(want)))
			gomega.Expect(got).To(gomega.HaveLen(2*len(states) + 4 + 1 + 10))
		},
		ginkgo.Entry("one state", []string{"YUC"}),
		ginkgo.Entry("three states", []string{"AGS", "DF", "YUC"}),
		ginkgo.Entry("every CONAGUA state", allStateCodes()),
	)

	ginkgo.It("plans 79 files for the 32-state deposit, under the file cap, and the README names all 79", func() {
		states := statesOf(allStateCodes()...)
		gomega.Expect(states).To(gomega.HaveLen(32))
		gomega.Expect(plannedNames(states, Groups(), plannedSnapshot)).To(gomega.HaveLen(79))
		gomega.Expect(artifactTableNames(readmeOver(states))).To(gomega.HaveLen(79))
		gomega.Expect(79).To(gomega.BeNumerically("<=", MaxTopLevelFiles))
	})

	ginkgo.It("lists the per-state rows in the states' order — official name, code, tabular then json — the national rows in build order, the metadata rows in artifact-set order", func() {
		readme := readmeOver(statesOf("AGS", "DF", "YUC"))
		gomega.Expect(readme).To(gomega.ContainSubstring(
			"| Aguascalientes | AGS | `ags-tabular.zip` | `ags-json.zip` |\n" +
				"| Ciudad de México | DF | `df-tabular.zip` | `df-json.zip` |\n" +
				"| Yucatán | YUC | `yuc-tabular.zip` | `yuc-json.zip` |\n"))
		got := artifactTableNames(readme)
		gomega.Expect(got[:6]).To(gomega.Equal([]string{
			"ags-tabular.zip", "ags-json.zip", "df-tabular.zip", "df-json.zip", "yuc-tabular.zip", "yuc-json.zip",
		}))
		gomega.Expect(got[6:11]).To(gomega.Equal([]string{
			"national-csv.zip", "national-parquet.zip", "national-json.zip", "national-sqlite.zip",
			"conagua-raw-" + plannedSnapshot + ".zip",
		}))
		gomega.Expect(got[11:]).To(gomega.Equal(DocsFiles()))
	})

	ginkgo.It("names a superset of what a --state docs-only run plans: the README describes the deposit, not the run's scope", func() {
		subset := plannedNames(statesOf("YUC"), []Group{GroupDocs}, plannedSnapshot)
		gomega.Expect(subset).To(gomega.HaveLen(10))
		gomega.Expect(sortedNames(subset)).To(gomega.Equal(sortedNames(DocsFiles())))
		whole := artifactTableNames(readmeOver(statesOf("AGS", "YUC")))
		for _, name := range subset {
			gomega.Expect(whole).To(gomega.ContainElement(name))
		}
		gomega.Expect(whole).To(gomega.ContainElements("ags-tabular.zip", "ags-json.zip", "yuc-tabular.zip", "yuc-json.zip", "national-sqlite.zip"))
		gomega.Expect(len(whole)).To(gomega.BeNumerically(">", len(subset)))
	})
})
