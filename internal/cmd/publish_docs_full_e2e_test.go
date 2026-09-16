package cmd_test

// The docs group on the real binary at full scope: `publish --only
// docs` with no --state over the seeded two-state DB writes the ten
// top-level files and no archive, and its README.md names exactly the
// directory a full run of the same DB leaves behind — the by-hand
// listing the full-run e2e holds — while the DB-describing files come
// out byte-identical to a `--state yuc --only docs` run's: the docs
// describe the database, not the run's scope. Every one of the ten
// files passes the identity sweep.

import (
	"encoding/json"
	"path/filepath"
	"regexp"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
)

var backtickedNameRE = regexp.MustCompile("`([^`]+)`")

// artifactSetOf returns the backticked names of the table rows of
// README's artifact section, in row order.
func artifactSetOf(readme string) []string {
	var names []string
	for _, row := range mdTableRows(mdSection(readme, "## 2. The artifact set")) {
		for _, m := range backtickedNameRE.FindAllStringSubmatch(row, -1) {
			names = append(names, m[1])
		}
	}
	return names
}

var _ = Describe("conagua-etl publish --only docs at full scope", Ordered, func() {
	var (
		dbPath, out, stdout string
		docs                map[string]string
	)

	BeforeAll(func() {
		dbPath = seedPublishDB()
		out = filepath.Join(GinkgoT().TempDir(), "docs-full")
		session := runPublishToExit(dbPath, out, "--only", "docs")
		Expect(session.ExitCode()).To(Equal(0))
		stdout = string(session.Out.Contents())
		docs = map[string]string{}
		for _, name := range wantDocsDir {
			docs[name] = string(readBytes(filepath.Join(out, name)))
		}
	})

	It("writes the ten top-level files for both states and no archive, and lists both states in the manifest with no artifact", func() {
		Expect(stdout).To(ContainSubstring("  states               : AGS,YUC\n"))
		Expect(stdout).To(ContainSubstring("  groups               : docs\n"))
		Expect(stdout).To(ContainSubstring("  artifacts            : 8 ok / 0 failed / 8 attempted\n"))
		Expect(stdout).To(ContainSubstring("  top-level files      : 10\n"))
		Expect(listDir(out)).To(Equal(wantDocsDir))
		expectNoTempResidue(out)
		m, _ := readManifest(out)
		Expect(m.States).To(Equal([]publish.ManifestState{
			{Code: "AGS", Name: "Aguascalientes", Artifacts: []string{}},
			{Code: "YUC", Name: "Yucatán", Artifacts: []string{}},
		}))
		Expect(m.National).To(BeEmpty())
	})

	It("README.md names exactly the files a full run of this database leaves in its directory", func() {
		names := artifactSetOf(docs["README.md"])
		Expect(names).To(HaveLen(len(wantFullRunDir)))
		want := slices.Clone(wantFullRunDir)
		slices.Sort(names)
		slices.Sort(want)
		Expect(names).To(Equal(want))
	})

	It("renders the database-describing files byte-identical to a --state yuc --only docs run", func() {
		sub := filepath.Join(GinkgoT().TempDir(), "docs-yuc")
		Expect(runPublishToExit(dbPath, sub, "--state", "yuc", "--only", "docs").ExitCode()).To(Equal(0))
		for _, name := range []string{"README.md", "NOTICE", "DATA-DICTIONARY.md", "DATA-DICTIONARY.json", "QA-REPORT.md", "LICENSE"} {
			Expect(string(readBytes(filepath.Join(sub, name)))).To(Equal(docs[name]), name)
		}
		Expect(artifactSetOf(string(readBytes(filepath.Join(sub, "README.md"))))).To(Equal(artifactSetOf(docs["README.md"])))
	})

	It("carries exactly the settled creator — name + ORCID — in the files that name one, and no other personal identity fields", func() {
		for _, name := range wantDocsDir {
			text := docs[name]
			for _, forbidden := range []string{"affiliation", "@"} {
				Expect(text).NotTo(ContainSubstring(forbidden), name)
			}
			// Every ORCID-shaped id in any file is the creator's own.
			for _, match := range bareORCIDValueRE.FindAllString(text, -1) {
				Expect(match).To(Equal("0009-0007-4050-494X"), name)
			}
		}
		_, lists, _ := cffDocument(docs["CITATION.cff"])
		Expect(lists["authors"]).To(Equal([]string{`family-names: "Trinidad"`}))
		Expect(docs["CITATION.cff"]).To(ContainSubstring("\n    orcid: https://orcid.org/0009-0007-4050-494X\n"))
		var stub struct {
			Metadata struct {
				Creators []map[string]string `json:"creators"`
			} `json:"metadata"`
		}
		Expect(json.Unmarshal([]byte(docs["zenodo-metadata.json"]), &stub)).To(Succeed())
		Expect(stub.Metadata.Creators).To(Equal([]map[string]string{{"name": "Trinidad, Pablo", "orcid": "0009-0007-4050-494X"}}))
		m, _ := readManifest(out)
		Expect(m.Dataset.Creator).To(Equal(publish.DatasetCreator))
		Expect(docs["manifest.json"]).To(ContainSubstring("\"creator_orcid\": \"0009-0007-4050-494X\""))
	})
})
