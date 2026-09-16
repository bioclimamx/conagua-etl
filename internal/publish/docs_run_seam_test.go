package publish_test

// The docs group at the Run seam, over the seeded three-state DB: two
// docs-only runs on one clock write all ten top-level files
// byte-identical — manifest.json and CHECKSUMS included — and a third
// run on another clock moves only the two build-time stamps
// (date-released, publication_date) and generated_at; every file of the
// deposit carries exactly the dataset's creator — name + ORCID — where a
// creator is named, and no other personal fields; and
// README.md names the whole deposit's artifact set — every state of
// the DB, every group — whatever --state selected, every
// DB-describing file byte-identical between a --state run and a
// full-scope one.

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// docsDepositDir is the out dir of a docs-only run, bytewise-sorted as
// os.ReadDir, CHECKSUMS, and manifest.json list it.
var docsDepositDir = []string{
	"CHECKSUMS", "CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md",
	"README.md", "manifest.json", "zenodo-metadata.json",
}

// unstampedDocs are the docs files no clock reaches.
var unstampedDocs = []string{"README.md", "DATA-DICTIONARY.md", "DATA-DICTIONARY.json", "LICENSE", "NOTICE", "QA-REPORT.md"}

var (
	// bareORCIDRE matches an ORCID-shaped identifier — the identity hard
	// stop permits exactly one, the dataset creator's own.
	bareORCIDRE = regexp.MustCompile(`\b\d{4}-\d{4}-\d{4}-\d{3}[\dX]\b`)
	backtickRE  = regexp.MustCompile("`([^`]+)`")
)

// readDeposit reads every file of dir by name.
func readDeposit(dir string) map[string]string {
	GinkgoHelper()
	files := map[string]string{}
	for _, name := range dirNames(dir) {
		files[name] = string(readFile(filepath.Join(dir, name)))
	}
	return files
}

// readmeArtifactNames returns the backticked names of the table rows
// in README's artifact section, in row order.
func readmeArtifactNames(readme string) []string {
	GinkgoHelper()
	start := strings.Index(readme, "\n## 2. The artifact set\n")
	Expect(start).To(BeNumerically(">=", 0))
	section := readme[start+1:]
	if end := strings.Index(section[1:], "\n## "); end >= 0 {
		section = section[:end+1]
	}
	var names []string
	for _, line := range strings.Split(section, "\n") {
		if !strings.HasPrefix(line, "| ") || strings.HasPrefix(line, "|---") {
			continue
		}
		for _, m := range backtickRE.FindAllStringSubmatch(line, -1) {
			names = append(names, m[1])
		}
	}
	return names
}

func manifestFilesByName(m publish.Manifest) map[string]publish.ManifestFile {
	out := map[string]publish.ManifestFile{}
	for _, f := range m.Files {
		out[f.Name] = f
	}
	return out
}

var _ = Describe("Run's docs group at the seam", func() {
	var db *sql.DB

	BeforeEach(func() {
		db = openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
	})

	// docsRun builds the docs group alone into a fresh out dir on the
	// given clock and holds the run clean and the dir to the ten files.
	docsRun := func(name string, now time.Time, opts publish.Options) (string, *publish.Report) {
		GinkgoHelper()
		out := filepath.Join(GinkgoT().TempDir(), name)
		opts.OutDir = out
		opts.ETLGitSHA = "abc123"
		opts.Only = []string{"docs"}
		opts.Now = func() time.Time { return now }
		report, err := publish.Run(context.Background(), db, opts, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
		Expect(dirNames(out)).To(Equal(docsDepositDir))
		return out, report
	}

	It("renders Options.DOI into the citation of every file that carries one, and refuses a malformed DOI before writing", func() {
		const doi = "10.5281/zenodo.1234567"
		out, _ := docsRun("doi", fixedNow, publish.Options{DOI: doi})
		files := readDeposit(out)
		m := readManifest(out)
		Expect(m.Dataset.DOI).To(Equal(doi))
		Expect(m.Dataset.SuggestedCitation).To(HaveSuffix(" Zenodo. DOI: " + doi))
		Expect(files["CITATION.cff"]).To(ContainSubstring("\ndoi: " + doi + "\n"))
		Expect(files["CITATION.cff"]).NotTo(ContainSubstring("# doi:"))
		Expect(files["README.md"]).To(ContainSubstring("\n> " + m.Dataset.SuggestedCitation + "\n"))
		Expect(files["DATA-DICTIONARY.json"]).To(ContainSubstring(`"doi": "` + doi + `"`))

		bad := filepath.Join(GinkgoT().TempDir(), "bad")
		_, err := publish.Run(context.Background(), db, publish.Options{OutDir: bad, DOI: "https://doi.org/" + doi}, nil)
		Expect(err).To(MatchError(ContainSubstring(`pass the bare DOI, "` + doi + `"`)))
		Expect(bad).NotTo(BeAnExistingFile())
	})

	It("writes all ten top-level files byte-identical across two docs-only runs on one clock", func() {
		outA, report := docsRun("a", fixedNow, publish.Options{})
		Expect(report.Groups).To(Equal([]string{"docs"}))
		Expect(report.States).To(Equal([]string{"AGS", "YUC", "ZAC"}))
		Expect(artifactNames(report)).To(Equal(docsNames))
		outB, _ := docsRun("b", fixedNow, publish.Options{})
		a, b := readDeposit(outA), readDeposit(outB)
		Expect(a).To(HaveLen(10))
		for _, name := range docsDepositDir {
			Expect(a[name]).NotTo(BeEmpty(), name)
			Expect(b[name]).To(Equal(a[name]), name)
		}
	})

	It("moves only date-released, publication_date, and generated_at when the clock moves; every other byte holds", func() {
		later := fixedNow.AddDate(1, 1, 4)
		outA, _ := docsRun("a", fixedNow, publish.Options{})
		outC, _ := docsRun("c", later, publish.Options{})
		a, c := readDeposit(outA), readDeposit(outC)
		for _, name := range unstampedDocs {
			Expect(c[name]).To(Equal(a[name]), name)
		}

		dateA, dateC := fixedNow.UTC().Format("2006-01-02"), later.UTC().Format("2006-01-02")
		Expect(dateC).NotTo(Equal(dateA))
		Expect(c["CITATION.cff"]).NotTo(Equal(a["CITATION.cff"]))
		Expect(c["CITATION.cff"]).To(Equal(strings.ReplaceAll(a["CITATION.cff"],
			"date-released: "+dateA, "date-released: "+dateC)))
		Expect(c["zenodo-metadata.json"]).NotTo(Equal(a["zenodo-metadata.json"]))
		Expect(c["zenodo-metadata.json"]).To(Equal(strings.ReplaceAll(a["zenodo-metadata.json"],
			`"publication_date": "`+dateA+`"`, `"publication_date": "`+dateC+`"`)))

		mA, mC := readManifest(outA), readManifest(outC)
		Expect(mA.GeneratedAt).To(Equal(fixedNow.UTC().Format(time.RFC3339)))
		Expect(mC.GeneratedAt).To(Equal(later.UTC().Format(time.RFC3339)))
		fa, fc := manifestFilesByName(mA), manifestFilesByName(mC)
		Expect(fa).To(HaveLen(len(docsNames)))
		for _, name := range docsNames {
			if slices.Contains(unstampedDocs, name) {
				Expect(fc[name]).To(Equal(fa[name]), name)
			} else {
				Expect(fc[name].SHA256).NotTo(Equal(fa[name].SHA256), name)
			}
		}
		mA.GeneratedAt, mC.GeneratedAt = "", ""
		mA.Files, mC.Files = nil, nil
		Expect(mC).To(Equal(mA))

		sa, sc := readChecksums(outA), readChecksums(outC)
		Expect(sc).To(HaveLen(len(sa)))
		Expect(sa).To(HaveLen(len(docsNames) + 1))
		for i := range sa {
			Expect(sc[i].Name).To(Equal(sa[i].Name))
			if slices.Contains(unstampedDocs, sa[i].Name) {
				Expect(sc[i]).To(Equal(sa[i]))
			} else {
				Expect(sc[i].SHA256).NotTo(Equal(sa[i].SHA256), sa[i].Name)
			}
		}
	})

	It("carries exactly the settled creator in the files that name one, and no other personal identity fields", func() {
		out, _ := docsRun("identity", fixedNow, publish.Options{})
		files := readDeposit(out)
		Expect(files).To(HaveLen(10))
		for name, text := range files {
			for _, forbidden := range []string{"affiliation", "@"} {
				Expect(text).NotTo(ContainSubstring(forbidden), name)
			}
			// Every ORCID-shaped id in any file is the creator's own.
			for _, match := range bareORCIDRE.FindAllString(text, -1) {
				Expect(match).To(Equal("0009-0007-4050-494X"), name)
			}
		}

		scalars, lists, comments := cffFields(files["CITATION.cff"])
		Expect(lists["authors"]).To(Equal([]string{`family-names: "Trinidad"`}))
		Expect(files["CITATION.cff"]).To(ContainSubstring("\n    orcid: https://orcid.org/0009-0007-4050-494X\n"))
		Expect(scalars).NotTo(HaveKey("doi"))
		Expect(comments[0]).To(HavePrefix("# doi: this build carries no DOI"))
		var stub struct {
			Metadata struct {
				Creators []map[string]string `json:"creators"`
			} `json:"metadata"`
		}
		Expect(json.Unmarshal([]byte(files["zenodo-metadata.json"]), &stub)).To(Succeed())
		Expect(stub.Metadata.Creators).To(Equal([]map[string]string{{"name": "Trinidad, Pablo", "orcid": "0009-0007-4050-494X"}}))
		Expect(readManifest(out).Dataset.Creator).To(Equal("Pablo Trinidad"))
		Expect(files["manifest.json"]).To(ContainSubstring("\"creator_orcid\": \"0009-0007-4050-494X\""))
		var dict publish.Dictionary
		Expect(json.Unmarshal([]byte(files["DATA-DICTIONARY.json"]), &dict)).To(Succeed())
		Expect(dict.Dataset.Creator).To(Equal("Pablo Trinidad"))
		Expect(files["README.md"]).To(ContainSubstring("\n> Pablo Trinidad (2026). " + publish.DatasetTitle +
			". Version 0.1. Zenodo.\n"))
		// No artifact of the deposit ships a stand-in for the DOI: a
		// Zenodo file cannot be patched once published.
		for name, text := range files {
			Expect(text).NotTo(ContainSubstring("placeholder"), name)
			Expect(text).NotTo(ContainSubstring("<concept-DOI"), name)
		}
	})

	It("names in README.md the whole deposit's artifact set whatever --state selected, and renders every docs file identically in a --state run and a full-scope one", func() {
		outSub, report := docsRun("yuc", fixedNow, publish.Options{States: []string{"yuc"}})
		Expect(report.States).To(Equal([]string{"YUC"}))
		outAll, _ := docsRun("all", fixedNow, publish.Options{})
		sub, all := readDeposit(outSub), readDeposit(outAll)

		states, err := publish.LoadStates(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(states).To(HaveLen(3))
		var want []string
		for _, g := range publish.Groups() {
			switch {
			case g == publish.GroupDocs:
				want = append(want, publish.DocsFiles()...)
			case g.PerState():
				for _, st := range states {
					want = append(want, st.Slug+"-"+string(g)+".zip")
				}
			case g == publish.GroupRaw:
				want = append(want, publish.RawArchiveName("2026-06-08"))
			default:
				want = append(want, string(g)+".zip")
			}
		}
		Expect(want).To(HaveLen(2*3 + 4 + 1 + 10))
		got := readmeArtifactNames(sub["README.md"])
		slices.Sort(got)
		slices.Sort(want)
		Expect(got).To(Equal(want))
		for _, name := range docsDepositDir {
			Expect(got).To(ContainElement(name))
		}
		Expect(sub["README.md"]).To(ContainSubstring("| Aguascalientes | AGS | `ags-tabular.zip` | `ags-json.zip` |\n"))

		for _, name := range docsNames {
			Expect(all[name]).To(Equal(sub[name]), name)
		}
		mSub, mAll := readManifest(outSub), readManifest(outAll)
		Expect(mSub.States).To(Equal([]publish.ManifestState{{Code: "YUC", Name: "Yucatán", Artifacts: []string{}}}))
		Expect(mAll.States).To(Equal([]publish.ManifestState{
			{Code: "AGS", Name: "Aguascalientes", Artifacts: []string{}},
			{Code: "YUC", Name: "Yucatán", Artifacts: []string{}},
			{Code: "ZAC", Name: "Zacatecas", Artifacts: []string{}},
		}))
		Expect(mSub.National).To(BeEmpty())
		Expect(mAll.National).To(BeEmpty())
	})
})
