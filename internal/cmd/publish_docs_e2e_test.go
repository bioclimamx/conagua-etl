package cmd_test

// The docs group on the real binary: `publish --only docs` on the seeded DB
// writes the eight metadata files in write order, then manifest.json
// and CHECKSUMS — ten top-level files — every one digested in CHECKSUMS
// and listed in the manifest. README.md names every artifact a full run
// of the DB would produce and tabulates the 32 state codes in code
// order;
// DATA-DICTIONARY.md covers every exported column of every file spec
// in export order and DATA-DICTIONARY.json parses to the same column
// sets; LICENSE is the CC BY 4.0 legal code by digest; NOTICE carries
// its four numbered attribution sections, the snapshot date, and the
// seeded power runs' access dates; CITATION.cff is dated with the run's
// UTC date, authored by the dataset creator with their ORCID, with no
// doi key; zenodo-metadata.json has the build-only stub shape; the only
// identity any file carries is that creator's name and ORCID — no
// affiliation, no other ORCID, no e-mail; and a second run renders every
// docs file byte-identical,
// the two stamped files too unless the UTC date rolled between the runs.
//
// Expectations are written by hand from the seed and the artifact set,
// never from the publish package's own lists, so a drift in the
// package's tables cannot pass here.

import (
	"encoding/json"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

var (
	// wantDocsOrder is the docs group's files in write order — the order
	// the file lines appear on stderr and the artifacts in the report.
	wantDocsOrder = []string{
		"README.md", "DATA-DICTIONARY.md", "DATA-DICTIONARY.json", "LICENSE", "NOTICE", "CITATION.cff",
		"QA-REPORT.md", "zenodo-metadata.json",
	}
	// wantDocsDir is the out dir of a docs-only run, bytewise-sorted as
	// os.ReadDir, CHECKSUMS, and manifest.json's files list it.
	wantDocsDir = []string{
		"CHECKSUMS", "CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md",
		"README.md", "manifest.json", "zenodo-metadata.json",
	}
	// wantDictionarySpecs is every flat-file spec the dictionary must
	// cover, in export order.
	wantDictionarySpecs = []publish.FileSpec{
		publish.Stations, publish.MonthlyNormals, publish.MonthlyNormalsExtras, publish.DailyObservations,
		publish.Cells, publish.StationCellMap, publish.PowerMonthly, publish.PowerDaily,
		publish.CombinedMonthly.FileSpec, publish.CombinedDaily.FileSpec,
		publish.ProvenanceIngestRuns, publish.ProvenancePowerRuns,
	}
	// bareORCIDValueRE matches an ORCID-shaped identifier — the identity
	// hard stop permits exactly one, the dataset creator's own.
	bareORCIDValueRE = regexp.MustCompile(`\b\d{4}-\d{4}-\d{4}-\d{3}[\dX]\b`)
)

// utcDate is today's date in UTC — the value the dated docs files stamp
// at build time.
func utcDate() string {
	return time.Now().UTC().Format("2006-01-02")
}

// mdSection returns the text of a Markdown document from the heading
// line that starts with from up to the next "## " heading.
func mdSection(doc, from string) string {
	start := strings.Index(doc, "\n"+from)
	ExpectWithOffset(1, start).To(BeNumerically(">=", 0), from)
	rest := doc[start+1:]
	if end := strings.Index(rest[len(from):], "\n## "); end >= 0 {
		return rest[:len(from)+end]
	}
	return rest
}

// mdTableRows returns the table rows of a Markdown text — header rows
// included, separator rows not.
func mdTableRows(text string) []string {
	var rows []string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "| ") && !strings.HasPrefix(line, "|---") {
			rows = append(rows, line)
		}
	}
	return rows
}

// dictionaryMarkdownColumns parses the per-file column tables out of
// DATA-DICTIONARY.md: under each "### <file>" heading, the first cell of
// every row of the table whose header starts with "| column |", the
// backticks stripped.
func dictionaryMarkdownColumns(md string) map[string][]string {
	out := map[string][]string{}
	var file string
	inColumns := false
	for _, line := range strings.Split(md, "\n") {
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

// cffDocument parses a CITATION.cff with a line-based reader — scalars
// as `key: value`, lists as `key:` followed by `  - item` lines,
// comment lines kept apart — so no YAML library is needed.
func cffDocument(text string) (scalars map[string]string, lists map[string][]string, comments []string) {
	scalars, lists = map[string]string{}, map[string][]string{}
	var current string
	for _, line := range strings.Split(strings.TrimRight(text, "\n"), "\n") {
		switch {
		case strings.HasPrefix(line, "#"):
			comments = append(comments, line)
		case strings.HasPrefix(line, "  - "):
			lists[current] = append(lists[current], strings.TrimPrefix(line, "  - "))
		default:
			key, value, found := strings.Cut(line, ":")
			ExpectWithOffset(1, found).To(BeTrue(), line)
			value = strings.TrimPrefix(value, " ")
			if value == "" {
				current = key
				lists[key] = []string{}
			} else {
				scalars[key] = value
			}
		}
	}
	return scalars, lists, comments
}

// jsonObjectKeys returns the keys of the JSON object at path, in file
// order.
func jsonObjectKeys(data []byte, path ...string) []string {
	raw := json.RawMessage(data)
	for _, p := range path {
		var outer map[string]json.RawMessage
		ExpectWithOffset(1, json.Unmarshal(raw, &outer)).To(Succeed())
		raw = outer[p]
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	tok, err := dec.Token()
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	ExpectWithOffset(1, tok).To(Equal(json.Delim('{')))
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		keys = append(keys, tok.(string))
		var skip json.RawMessage
		ExpectWithOffset(1, dec.Decode(&skip)).To(Succeed())
	}
	return keys
}

var _ = Describe("conagua-etl publish --only docs", Ordered, func() {
	var (
		dbPath, dbSHA, out     string
		stdout, stderr         string
		dateBefore, dateAfter  string
		docs                   map[string]string
		stationCount, cellRows int
	)

	BeforeAll(func() {
		dbPath = seedPublishDB()
		dbSHA = fileSHA(dbPath)
		db := openRO(dbPath)
		stationCount = queryInt(db, `SELECT COUNT(*) FROM stations WHERE source = 'conagua_conventional'`)
		cellRows = queryInt(db, `SELECT COUNT(*) FROM nasa_power_grid_cells`)
		out = filepath.Join(GinkgoT().TempDir(), "docs")
		dateBefore = utcDate()
		session := runPublishToExit(dbPath, out, "--state", "yuc", "--only", "docs")
		dateAfter = utcDate()
		Expect(session.ExitCode()).To(Equal(0))
		stdout, stderr = string(session.Out.Contents()), string(session.Err.Contents())
		Expect(fileSHA(dbPath)).To(Equal(dbSHA))
		docs = map[string]string{}
		for _, name := range wantDocsDir {
			docs[name] = string(readBytes(filepath.Join(out, name)))
		}
	})

	It("writes the eight docs files in write order, then manifest.json and CHECKSUMS: ten top-level files, every one digested and listed", func() {
		expectGateLines(stderr, wantSeededGate)
		Expect(publishUnitLineRE.FindAllStringSubmatch(stderr, -1)).To(BeEmpty())
		Expect(publishArtifactLineRE.FindAllStringSubmatch(stderr, -1)).To(BeEmpty())
		files := publishFileLineRE.FindAllStringSubmatch(stderr, -1)
		Expect(files).To(HaveLen(len(wantDocsOrder)))
		for i, name := range wantDocsOrder {
			path := filepath.Join(out, name)
			Expect(files[i][1:5]).To(Equal([]string{"ok", name, strconv.FormatInt(sizeOf(path), 10), fileSHA(path)[:12]}), name)
			Expect(sizeOf(path)).To(BeNumerically(">", 0), name)
		}

		Expect(stdout).To(ContainSubstring("\npublish complete\n"))
		Expect(stdout).To(ContainSubstring("  states               : YUC\n"))
		Expect(stdout).To(ContainSubstring("  groups               : docs\n"))
		Expect(stdout).To(ContainSubstring("  artifacts            : 8 ok / 0 failed / 8 attempted\n"))
		Expect(stdout).To(ContainSubstring("  top-level files      : 10\n"))
		Expect(listDir(out)).To(Equal(wantDocsDir))
		expectNoTempResidue(out)

		// readChecksums recomputes every digest from the bytes on disk.
		sums := readChecksums(out)
		Expect(checksumNames(sums)).To(Equal(wantDocsDir[1:]))
		m, _ := readManifest(out)
		wantFiles := make([]publish.ManifestFile, 0, len(wantDocsOrder))
		for _, s := range sums {
			if s.Name == "manifest.json" {
				continue
			}
			wantFiles = append(wantFiles, publish.ManifestFile{Name: s.Name, SHA256: s.SHA256, Bytes: sizeOf(filepath.Join(out, s.Name))})
		}
		Expect(m.Files).To(Equal(wantFiles))
		Expect(m.SnapshotDate).To(Equal(publishSnapshotDate))
		Expect(m.States).To(Equal([]publish.ManifestState{{Code: "YUC", Name: "Yucatán", Artifacts: []string{}}}))
		Expect(m.National).To(BeEmpty())
	})

	It("README.md names every artifact a full run of this database would produce, tabulates the 32 state codes, and states the seed's numbers", func() {
		readme := docs["README.md"]
		Expect(readme).To(HavePrefix("# " + publish.DatasetTitle + "\n\nVersion 0.1. Snapshot `" + publishSnapshotDate +
			"`, schema version " + strconv.Itoa(schema.Version) + ", ETL git SHA "))

		// The seeded DB holds two states: two archives each, the four
		// national archives, the raw snapshot, and the ten docs files.
		artifacts := mdSection(readme, "## 2. The artifact set")
		for _, name := range []string{
			"ags-tabular.zip", "ags-json.zip", "yuc-tabular.zip", "yuc-json.zip",
			"national-csv.zip", "national-parquet.zip", "national-json.zip", "national-sqlite.zip",
			"conagua-raw-" + publishSnapshotDate + ".zip",
			"README.md", "DATA-DICTIONARY.md", "DATA-DICTIONARY.json", "LICENSE", "NOTICE", "CITATION.cff",
			"manifest.json", "CHECKSUMS", "QA-REPORT.md", "zenodo-metadata.json",
		} {
			Expect(artifacts).To(ContainSubstring("`"+name+"`"), name)
		}
		Expect(artifacts).To(ContainSubstring("| Aguascalientes | AGS | `ags-tabular.zip` | `ags-json.zip` |\n"))
		Expect(artifacts).To(ContainSubstring("| Yucatán | YUC | `yuc-tabular.zip` | `yuc-json.zip` |\n"))
		Expect(mdTableRows(artifacts)).To(HaveLen(1 + 2 + 1 + 5 + 1 + 10))

		states := mdSection(readme, "## 5. State codes")
		rows := mdTableRows(states)
		Expect(rows).To(HaveLen(1 + 32))
		Expect(rows[0]).To(Equal("| code | file-name slug | state |"))
		codes := slices.Clone(conagua.AllStates)
		slices.Sort(codes)
		for i, c := range codes {
			Expect(rows[i+1]).To(Equal("| " + strings.ToUpper(string(c)) + " | `" + string(c) + "` | " + c.DisplayName() + " |"))
		}
		Expect(states).To(ContainSubstring("| YUC | `yuc` | Yucatán |\n"))
		Expect(states).To(ContainSubstring("| DF | `df` | Ciudad de México |\n"))

		// Every number is the DB's.
		flat := strings.Join(strings.Fields(readme), " ")
		Expect(flat).To(ContainSubstring("**Stations.** " + strconv.Itoa(stationCount) + " CONAGUA conventional weather stations"))
		Expect(flat).To(ContainSubstring(strconv.Itoa(cellRows) + " cells,"))
		Expect(flat).To(ContainSubstring("consulted on " + publishSnapshotDate + "."))
		Expect(flat).To(ContainSubstring("| power-daily-1981-2026 | daily | 1981-01-01 to 2026-06-08 | 2026-07-02T00:00:00Z | complete | null |"))
		Expect(flat).To(ContainSubstring("| power-monthly-1981-2010 | monthly | 1981 to 2010 | 2026-07-01T00:00:00Z | complete | p0w3r |"))
		Expect(flat).To(ContainSubstring("| power-monthly-1991-2020 | monthly | 1991 to 2020 | 2026-07-01T07:00:00Z | complete | p0w3r |"))
		Expect(flat).To(ContainSubstring(`> **"Combined" / "Augmented" = a CONAGUA-spine record augmented with its ` +
			`station's POWER-cell reanalysis on matching keys — a LEFT join from the CONAGUA side. Never a temporal union.**`))
		Expect(flat).To(ContainSubstring("The NASA POWER API version is not recorded"))
		Expect(readme).To(HaveSuffix("\n"))
		Expect(readme).NotTo(HaveSuffix("\n\n"))
	})

	It("DATA-DICTIONARY.md covers every exported column of every file spec in export order, and DATA-DICTIONARY.json parses to the same column sets", func() {
		fromMarkdown := dictionaryMarkdownColumns(docs["DATA-DICTIONARY.md"])
		var dict publish.Dictionary
		Expect(json.Unmarshal([]byte(docs["DATA-DICTIONARY.json"]), &dict)).To(Succeed())
		Expect(dict.Files).To(HaveLen(len(wantDictionarySpecs)))
		Expect(fromMarkdown).To(HaveLen(len(wantDictionarySpecs)))
		for i, spec := range wantDictionarySpecs {
			want := make([]string, 0, len(spec.Columns))
			for _, c := range spec.Columns {
				want = append(want, c.Name)
			}
			Expect(dict.Files[i].Name).To(Equal(spec.Name))
			got := make([]string, 0, len(dict.Files[i].Columns))
			for _, c := range dict.Files[i].Columns {
				got = append(got, c.Name)
			}
			Expect(got).To(Equal(want), spec.Name)
			Expect(fromMarkdown[spec.Name]).To(Equal(want), spec.Name)
		}
		Expect(dict.SchemaVersion).To(Equal(schema.Version))
		Expect(dict.SnapshotDate).To(Equal(publishSnapshotDate))
		Expect(dict.Dataset.Title).To(Equal(publish.DatasetTitle))
		Expect(dict.Dataset.Creator).To(Equal(publish.DatasetCreator))
		Expect(dict.Power).To(HaveLen(31))
		Expect(docs["DATA-DICTIONARY.md"]).To(HavePrefix("# Data dictionary — " + publish.DatasetTitle + "\n"))
		Expect(docs["DATA-DICTIONARY.json"]).To(HaveSuffix("}\n"))
	})

	It("LICENSE is the CC BY 4.0 legal code, pinned by sha256 and length", func() {
		Expect(fileSHA(filepath.Join(out, "LICENSE"))).To(Equal("9ba9550ad48438d0836ddab3da480b3b69ffa0aac7b7878b5a0039e7ab429411"))
		Expect(len(docs["LICENSE"])).To(Equal(18657))
		Expect(docs["LICENSE"]).To(HavePrefix("Attribution 4.0 International\n"))
	})

	It("NOTICE carries the four NOTICE elements in order, the snapshot date as the consultation date, and the seeded power runs' access dates", func() {
		notice := docs["NOTICE"]
		flat := strings.Join(strings.Fields(notice), " ")
		positions := []int{
			strings.Index(notice, "\n1. CONAGUA / Servicio Meteorológico Nacional — Términos de Libre Uso MX\n"),
			strings.Index(notice, "\n2. NASA POWER\n"),
			strings.Index(notice, "\n3. License scope — CC BY 4.0 on the compilation only\n"),
			strings.Index(notice, "\n4. Transparency note — CONAGUA's terms\n"),
			strings.Index(notice, "\nKnown gaps\n"),
		}
		for i, p := range positions {
			Expect(p).To(BeNumerically(">", 0), strconv.Itoa(i))
			if i > 0 {
				Expect(p).To(BeNumerically(">", positions[i-1]), strconv.Itoa(i))
			}
		}
		Expect(notice).To(HavePrefix("NOTICE — " + publish.DatasetTitle + "\n\nVersion 0.1. Snapshot " + publishSnapshotDate + ".\n"))
		Expect(notice).To(ContainSubstring("Source:             CONAGUA / Servicio Meteorológico Nacional\n"))
		Expect(notice).To(ContainSubstring("Consultation date:  " + publishSnapshotDate + "\n"))
		Expect(flat).To(ContainSubstring("https://datos.gob.mx/libreusomx"))
		Expect(flat).To(ContainSubstring("No utilizar la información con objeto de engañar o confundir a la población"))
		// The seeded provenance runs started on 2026-07-01 (monthly, twice)
		// and 2026-07-02 (daily); the aborted run is not provenance.
		Expect(flat).To(ContainSubstring("The data was obtained from the POWER Project's Daily and Monthly " +
			"(API version as served on the access dates; the version string was not recorded by the ETL — " +
			"see Known gaps) version on 2026/07/01 and 2026/07/02."))
		Expect(flat).To(ContainSubstring("The data was obtained from National Aeronautics and Space Administration " +
			"(NASA) Langley Research Center's Prediction Of Worldwide Energy Resources (POWER) project funded " +
			"through the NASA Earth Science Division."))
		Expect(flat).To(ContainSubstring("Creative Commons Attribution 4.0 International (CC BY 4.0; SPDX CC-BY-4.0). The legal code is in LICENSE."))
		Expect(flat).To(ContainSubstring("That license covers the compilation only."))
		Expect(flat).To(ContainSubstring("https://www.gob.mx/terminos"))
		Expect(flat).To(ContainSubstring("takes the Libre Uso MX position"))
		Expect(flat).To(ContainSubstring("The NASA POWER API version string is not recorded"))
		Expect(notice).To(HaveSuffix("\n"))
		Expect(notice).NotTo(HaveSuffix("\n\n"))
	})

	It("CITATION.cff is dated with the run's UTC date, authored by the settled personal creator with their ORCID URL, with no doi key", func() {
		scalars, lists, comments := cffDocument(docs["CITATION.cff"])
		Expect(scalars["cff-version"]).To(Equal("1.2.0"))
		Expect(scalars["type"]).To(Equal("dataset"))
		Expect(scalars["title"]).To(Equal(`"` + publish.DatasetTitle + `"`))
		Expect(scalars["version"]).To(Equal(`"0.1"`))
		Expect(scalars["license"]).To(Equal("CC-BY-4.0"))
		Expect(scalars["repository-code"]).To(Equal(`"https://github.com/bioclimamx/conagua-etl"`))
		Expect(scalars["date-released"]).To(Or(Equal(dateBefore), Equal(dateAfter)))
		Expect(lists["authors"]).To(Equal([]string{`family-names: "Trinidad"`}))
		Expect(docs["CITATION.cff"]).To(ContainSubstring("\n    orcid: https://orcid.org/0009-0007-4050-494X\n"))
		Expect(scalars).NotTo(HaveKey("doi"))
		Expect(lists).NotTo(HaveKey("doi"))
		Expect(comments).To(HaveLen(1))
		Expect(comments[0]).To(HavePrefix("# doi: this build carries no DOI"))
		var abstract string
		Expect(json.Unmarshal([]byte(scalars["abstract"]), &abstract)).To(Succeed())
		Expect(abstract).To(HavePrefix("Daily observations and published climatological normals from " +
			strconv.Itoa(stationCount) + " CONAGUA conventional weather stations across Mexico"))
		Expect(lists["keywords"]).To(ContainElements(`"CONAGUA"`, `"NASA POWER"`, `"Mexico"`))
	})

	It("zenodo-metadata.json is the build-only Zenodo stub: Zenodo's fields inside metadata, in order, the build note beside them", func() {
		data := []byte(docs["zenodo-metadata.json"])
		Expect(jsonObjectKeys(data)).To(Equal([]string{"metadata", "build_note"}))
		Expect(jsonObjectKeys(data, "metadata")).To(Equal([]string{
			"upload_type", "title", "creators", "description", "access_right", "license", "keywords",
			"version", "publication_date",
		}))
		var stub struct {
			Metadata struct {
				UploadType  string              `json:"upload_type"`
				Title       string              `json:"title"`
				Creators    []map[string]string `json:"creators"`
				Description string              `json:"description"`
				AccessRight string              `json:"access_right"`
				License     string              `json:"license"`
				Keywords    []string            `json:"keywords"`
				Version     string              `json:"version"`
				PubDate     string              `json:"publication_date"`
			} `json:"metadata"`
			BuildNote string `json:"build_note"`
		}
		Expect(json.Unmarshal(data, &stub)).To(Succeed())
		Expect(stub.Metadata.UploadType).To(Equal("dataset"))
		Expect(stub.Metadata.Title).To(Equal(publish.DatasetTitle))
		Expect(stub.Metadata.Creators).To(Equal([]map[string]string{{"name": "Trinidad, Pablo", "orcid": "0009-0007-4050-494X"}}))
		Expect(stub.Metadata.AccessRight).To(Equal("open"))
		Expect(stub.Metadata.License).To(Equal("cc-by-4.0"))
		Expect(stub.Metadata.Version).To(Equal("0.1"))
		Expect(stub.Metadata.PubDate).To(Or(Equal(dateBefore), Equal(dateAfter)))
		Expect(stub.BuildNote).To(Equal("build-only stub; no deposit performed"))
		// The description is the citation's abstract.
		scalars, _, _ := cffDocument(docs["CITATION.cff"])
		var abstract string
		Expect(json.Unmarshal([]byte(scalars["abstract"]), &abstract)).To(Succeed())
		Expect(stub.Metadata.Description).To(Equal(abstract))
	})

	It("carries exactly the settled creator identity — name + ORCID — and no personal-name fields in any of the ten files", func() {
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
	})

	It("renders every docs file byte-identical on a second run — the two stamped files too unless the UTC date rolled — and manifest.json differs only in generated_at", func() {
		again := filepath.Join(GinkgoT().TempDir(), "again")
		session := runPublishToExit(dbPath, again, "--state", "yuc", "--only", "docs")
		dateAgain := utcDate()
		Expect(session.ExitCode()).To(Equal(0))
		Expect(listDir(again)).To(Equal(wantDocsDir))
		sameDate := dateBefore == dateAgain
		for _, name := range wantDocsOrder {
			second := string(readBytes(filepath.Join(again, name)))
			stamped := name == "CITATION.cff" || name == "zenodo-metadata.json"
			if !stamped || sameDate {
				Expect(second).To(Equal(docs[name]), name)
				continue
			}
			// The date rolled between the runs: the stamped files differ
			// in that one value alone.
			normalize := func(s string) string {
				return strings.ReplaceAll(strings.ReplaceAll(s, dateAgain, "<date>"), dateBefore, "<date>")
			}
			Expect(normalize(second)).To(Equal(normalize(docs[name])), name)
		}
		if sameDate {
			_, first := readManifest(out)
			_, secondManifest := readManifest(again)
			Expect(generatedAtRE.ReplaceAllString(secondManifest, "")).To(Equal(generatedAtRE.ReplaceAllString(first, "")))
			// CHECKSUMS: the same digest for every docs file; only
			// manifest.json's line may move with generated_at.
			firstSums, secondSums := readChecksums(out), readChecksums(again)
			Expect(secondSums).To(HaveLen(len(firstSums)))
			for i := range firstSums {
				if firstSums[i].Name != "manifest.json" {
					Expect(secondSums[i]).To(Equal(firstSums[i]), firstSums[i].Name)
				}
			}
		}
		Expect(fileSHA(dbPath)).To(Equal(dbSHA))
	})
})
