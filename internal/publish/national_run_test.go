package publish_test

// Specs at the Run seam for the deposit-wide groups: a full run builds,
// after every state's archives, the four national archives and the raw
// snapshot in build order — each held to its own assembler's rendering
// (the copy-built archives byte for byte, the Parquet files entry by
// entry, the national database table by table against the source, the
// raw archive file by file against the snapshot directory) — lists them
// in manifest.json's national list and in CHECKSUMS, and reports their
// units; a copy-built national archive fails soft, naming the state
// whose source archive failed, while the regenerated ones build and a
// prior run's copy is removed; the raw group is refused before anything
// is written when its snapshot directory is missing; a national or raw
// group is refused with --state, and a copy-built group without its
// source; and two runs yield byte-identical national archives, the
// SQLite one's bytes reported rather than claimed.

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// nationalNames is the deposit-wide archives of a full run, in build
// order, for the seeded snapshot.
var nationalNames = []string{
	"national-csv.zip", "national-parquet.zip", "national-json.zip", "national-sqlite.zip", "conagua-raw-2026-06-08.zip",
}

// docsNames is the docs group's files in write order — by hand from the
// artifact set, never from DocsFiles, so a drift in the group's order
// or membership cannot pass here.
var docsNames = []string{
	"README.md", "DATA-DICTIONARY.md", "DATA-DICTIONARY.json", "LICENSE", "NOTICE", "CITATION.cff",
	"QA-REPORT.md", "zenodo-metadata.json",
}

// docsNamesSorted is docsNames in the bytewise order the out dir,
// manifest.json's files, and CHECKSUMS list them.
var docsNamesSorted = []string{
	"CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md", "README.md",
	"zenodo-metadata.json",
}

// writeAssembly writes a copy-built assembly into a fresh directory
// under the seeded snapshot's timestamp and returns the zip's bytes.
func writeAssembly(ctx context.Context, name string, n *publish.NationalEntries) []byte {
	GinkgoHelper()
	defer func() { Expect(n.Close()).To(Succeed()) }()
	path := filepath.Join(GinkgoT().TempDir(), name)
	_, err := archive.WriteZipMixed(ctx, path, n.Entries, archive.ZipOptions{Modified: seededSnapshot})
	Expect(err).NotTo(HaveOccurred())
	data, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	return data
}

var _ = Describe("Run with the deposit-wide groups", func() {
	var (
		ctx    context.Context
		db     *sql.DB
		ids    map[string]int64
		root   string
		raw    map[string]string
		out    string
		rec    *recorder
		states []publish.State
		runs   publish.Runs
	)

	BeforeEach(func() {
		ctx = context.Background()
		db = openTempDB()
		ids = seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		seedNational(db, ids)
		root = GinkgoT().TempDir()
		raw = seedRawSnapshot(root)
		out = filepath.Join(GinkgoT().TempDir(), "publish")
		rec = &recorder{}
		var err error
		states, err = publish.LoadStates(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		Expect(codesOf(states)).To(Equal([]string{"AGS", "YUC", "ZAC"}))
		runs, err = publish.LoadRuns(ctx, db)
		Expect(err).NotTo(HaveOccurred())
	})

	run := func(opts publish.Options) (*publish.Report, error) {
		opts.OutDir = out
		opts.SnapshotRoot = root
		if opts.Now == nil {
			opts.Now = fixedClock
		}
		return publish.Run(ctx, db, opts, rec.record)
	}

	stateArchives := func(group string) []publish.StateArchive {
		archives := make([]publish.StateArchive, len(states))
		for i, st := range states {
			archives[i] = publish.StateArchive{State: st, Path: filepath.Join(out, st.Slug+"-"+group+".zip")}
		}
		return archives
	}

	It("builds the four national archives and the raw snapshot after every state's archives, in build order, each as its assembler renders it, listed in the manifest's national list and in CHECKSUMS", func() {
		report, err := run(publish.Options{ETLGitSHA: "abc123"})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
		Expect(report.Groups).To(Equal([]string{
			"tabular", "json", "national-csv", "national-parquet", "national-json", "national-sqlite", "raw", "docs",
		}))
		Expect(artifactNames(report)).To(Equal(append(append([]string{
			"ags-tabular.zip", "ags-json.zip", "yuc-tabular.zip", "yuc-json.zip", "zac-tabular.zip", "zac-json.zip",
		}, nationalNames...), docsNames...)))
		Expect(report.TopLevelFiles).To(Equal(21))
		Expect(dirNames(out)).To(Equal([]string{
			"CHECKSUMS", "CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md",
			"README.md", "ags-json.zip", "ags-tabular.zip", "conagua-raw-2026-06-08.zip", "manifest.json",
			"national-csv.zip", "national-json.zip", "national-parquet.zip", "national-sqlite.zip",
			"yuc-json.zip", "yuc-tabular.zip", "zac-json.zip", "zac-tabular.zip", "zenodo-metadata.json",
		}))

		// The copy-built archives are the assemblies over the run's own
		// state archives, byte for byte.
		csvAssembly, err := publish.NationalCSVEntries(ctx, db, stateArchives("tabular"), runs, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(readFile(filepath.Join(out, "national-csv.zip"))).To(Equal(writeAssembly(ctx, "national-csv.zip", csvAssembly)))
		jsonAssembly, err := publish.NationalJSONEntries(ctx, db, stateArchives("json"), runs, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(readFile(filepath.Join(out, "national-json.zip"))).To(Equal(writeAssembly(ctx, "national-json.zip", jsonAssembly)))

		// The Parquet archive is the ten national files, each the entry's
		// own bytes.
		parquet, err := publish.NationalParquetEntries(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		names, contents := zipContents(filepath.Join(out, "national-parquet.zip"))
		Expect(names).To(Equal(entryPaths(parquet)))
		for _, e := range parquet {
			Expect(contents[e.Path]).To(Equal(render(e)), e.Path)
		}

		// The SQLite archive is the full canonical database, every table
		// the source's, shipped in rollback-journal mode and stamped.
		names, contents = zipContents(filepath.Join(out, "national-sqlite.zip"))
		Expect(names).To(Equal([]string{"bioclima.db"}))
		got := openRO(writeDB(contents["bioclima.db"], "bioclima.db"))
		for _, t := range ddlTableNames {
			Expect(selectAll(got, t, "")).To(Equal(selectAll(db, t, "")), t)
		}
		Expect(pragmaText(got, "journal_mode")).To(Equal("delete"))
		Expect(pragmaText(got, "integrity_check")).To(Equal("ok"))

		// The raw archive is the snapshot directory, file for file.
		names, contents = zipContents(filepath.Join(out, "conagua-raw-2026-06-08.zip"))
		var wantRaw []string
		for name := range raw {
			wantRaw = append(wantRaw, name)
		}
		slices.Sort(wantRaw)
		Expect(names).To(Equal(wantRaw))
		for name, body := range raw {
			Expect(string(contents[name])).To(Equal(body), name)
		}

		m := readManifest(out)
		Expect(m.States).To(HaveLen(3))
		Expect(m.National).To(Equal([]publish.ManifestNational{
			{Group: "national-csv", Artifacts: []string{"national-csv.zip"}},
			{Group: "national-parquet", Artifacts: []string{"national-parquet.zip"}},
			{Group: "national-json", Artifacts: []string{"national-json.zip"}},
			{Group: "national-sqlite", Artifacts: []string{"national-sqlite.zip"}},
			{Group: "raw", Artifacts: []string{"conagua-raw-2026-06-08.zip"}},
		}))
		var fileNames, sumNames []string
		for _, f := range m.Files {
			fileNames = append(fileNames, f.Name)
			a := artifactByName(report, f.Name)
			Expect(f.SHA256).To(Equal(a.SHA256), f.Name)
			Expect(f.Bytes).To(Equal(a.Bytes), f.Name)
		}
		for _, s := range readChecksums(out) {
			sumNames = append(sumNames, s.Name)
		}
		Expect(fileNames).To(Equal([]string{
			"CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md", "README.md",
			"ags-json.zip", "ags-tabular.zip", "conagua-raw-2026-06-08.zip",
			"national-csv.zip", "national-json.zip", "national-parquet.zip", "national-sqlite.zip",
			"yuc-json.zip", "yuc-tabular.zip", "zac-json.zip", "zac-tabular.zip", "zenodo-metadata.json",
		}))
		Expect(sumNames).To(Equal(slices.Insert(slices.Clone(fileNames), 10, "manifest.json")))
		Expect(tempDirs(out)).To(BeEmpty())
	})

	It("reports the national archives' units: one per state archive consumed by a copy, none for the Parquet files, the database once, one per snapshot directory child", func() {
		_, err := run(publish.Options{})
		Expect(err).NotTo(HaveOccurred())
		// A copy's unit fires after the last entry copied from its archive
		// in path order: Zacatecas has no cell file, so its last is under
		// conagua/; Yucatán's own cell sorts before Aguascalientes' under
		// nasa_power/daily/.
		Expect(eventsOf(rec, "national-csv.zip")).To(Equal(append(
			unitEvents("national-csv.zip", "zac-tabular.zip", "yuc-tabular.zip", "ags-tabular.zip"),
			publish.ProgressEvent{Artifact: "national-csv.zip"})))
		Expect(eventsOf(rec, "national-parquet.zip")).To(Equal([]publish.ProgressEvent{{Artifact: "national-parquet.zip"}}))
		Expect(eventsOf(rec, "national-json.zip")).To(Equal(append(
			unitEvents("national-json.zip", "ags-json.zip", "yuc-json.zip", "zac-json.zip"),
			publish.ProgressEvent{Artifact: "national-json.zip"})))
		Expect(eventsOf(rec, "national-sqlite.zip")).To(Equal(append(
			unitEvents("national-sqlite.zip", "bioclima.db"),
			publish.ProgressEvent{Artifact: "national-sqlite.zip"})))
		Expect(eventsOf(rec, "conagua-raw-2026-06-08.zip")).To(Equal(append(
			unitEvents("conagua-raw-2026-06-08.zip",
				"_index.json", "_progress.json", "daily/", "extremes/", "monthly/", "normals_1961_1990/", "normals_1991_2020/"),
			publish.ProgressEvent{Artifact: "conagua-raw-2026-06-08.zip"})))
		// The national archives' events follow every state's, which
		// follow the gate's.
		last := slices.IndexFunc(rec.events, func(ev publish.ProgressEvent) bool { return ev.Artifact == "national-csv.zip" })
		Expect(rec.events[:len(gateRuleIDs)]).To(Equal(gateEvents(rec)))
		for _, ev := range rec.events[len(gateRuleIDs):last] {
			Expect(ev.Artifact).To(HaveSuffix(".zip"))
			Expect(ev.Artifact).NotTo(HavePrefix("national-"))
		}
	})

	It("fails a copy-built national archive soft, naming the state whose source archive failed, while the archives that do not depend on it build, and removes a prior run's copy", func() {
		report, err := run(publish.Options{})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
		Expect(dirNames(out)).To(ContainElement("national-json.zip"))

		// A daily date the profile refuses fails the state's JSON archive
		// only; the flat files carry the text verbatim.
		insertDaily(db, ids["conv/31002"], "2020/01/01", 1.0, 1.0, 0.0, 1.0)
		rec.events = nil
		report, err = run(publish.Options{})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(Equal(2))
		Expect(artifactByName(report, "yuc-json.zip").Err).To(ContainSubstring(`date "2020/01/01" is not YYYY-MM-DD`))
		Expect(artifactByName(report, "national-json.zip").Err).To(Equal(
			"state YUC archive yuc-json.zip failed: national-json.zip needs every state's json archive"))
		for _, name := range []string{"national-csv.zip", "national-parquet.zip", "national-sqlite.zip", "conagua-raw-2026-06-08.zip"} {
			Expect(artifactByName(report, name).Err).To(BeEmpty(), name)
		}
		Expect(report.TopLevelFiles).To(Equal(19))
		Expect(dirNames(out)).NotTo(ContainElements("yuc-json.zip", "national-json.zip"))
		Expect(eventsOf(rec, "national-json.zip")).To(HaveLen(1))
		Expect(eventsOf(rec, "national-json.zip")[0].Err).To(MatchError(artifactByName(report, "national-json.zip").Err))
		m := readManifest(out)
		Expect(m.States[1].Artifacts).To(Equal([]string{"yuc-tabular.zip"}))
		Expect(m.National[2]).To(Equal(publish.ManifestNational{Group: "national-json", Artifacts: []string{}}))
		var sumNames []string
		for _, s := range readChecksums(out) {
			sumNames = append(sumNames, s.Name)
		}
		Expect(sumNames).NotTo(ContainElements("yuc-json.zip", "national-json.zip"))
	})

	It("names every state whose source archive failed", func() {
		insertDaily(db, ids["conv/31002"], "2020/01/01", 1.0, 1.0, 0.0, 1.0)
		insertDaily(db, ids["conv/32001"], "2020/01/02", 1.0, 1.0, 0.0, 1.0)
		report, err := run(publish.Options{Only: []string{"json", "national-json"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(artifactByName(report, "national-json.zip").Err).To(Equal(
			"state YUC archive yuc-json.zip failed, state ZAC archive zac-json.zip failed: " +
				"national-json.zip needs every state's json archive"))
	})

	It("refuses the raw group before anything is written when the snapshot directory is missing, naming the path and --root", func() {
		empty := GinkgoT().TempDir()
		report, err := publish.Run(ctx, db, publish.Options{OutDir: out, SnapshotRoot: empty, Now: fixedClock}, rec.record)
		Expect(err).To(MatchError(ContainSubstring("snapshot directory " + filepath.Join(empty, "conagua-raw", "2026-06-08") + " does not exist")))
		Expect(err).To(MatchError(ContainSubstring("--root")))
		Expect(report.SnapshotDate).To(Equal("2026-06-08"))
		Expect(report.Artifacts).To(BeEmpty())
		Expect(rec.events).To(HaveLen(len(gateRuleIDs)), "the gate's lines only")
		_, statErr := os.Stat(out)
		Expect(os.IsNotExist(statErr)).To(BeTrue())

		// Without the raw group the directory is never consulted.
		report, err = publish.Run(ctx, db, publish.Options{OutDir: out, SnapshotRoot: empty, Now: fixedClock,
			Only: []string{"tabular", "national-sqlite"}}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
		Expect(artifactNames(report)).To(Equal([]string{"ags-tabular.zip", "yuc-tabular.zip", "zac-tabular.zip", "national-sqlite.zip"}))
	})

	It("refuses a national or raw group with a state subset, and a copy-built group without its source, before touching the directory", func() {
		report, err := run(publish.Options{States: []string{"yuc"}, Only: []string{"tabular", "national-csv"}})
		Expect(err).To(MatchError(`artifact group "national-csv" is built only in a full run: drop --state`))
		Expect(report.Groups).To(BeEmpty())
		report, err = run(publish.Options{States: []string{"yuc"}, Only: []string{"raw"}})
		Expect(err).To(MatchError(`artifact group "raw" is built only in a full run: drop --state`))
		Expect(report.Groups).To(BeEmpty())
		report, err = run(publish.Options{Only: []string{"national-csv"}})
		Expect(err).To(MatchError(`artifact group "national-csv" copies from the tabular archives: add tabular to --only`))
		Expect(report.Groups).To(BeEmpty())
		Expect(rec.events).To(BeEmpty())
		_, statErr := os.Stat(out)
		Expect(os.IsNotExist(statErr)).To(BeTrue())

		// A state subset builds the per-state groups alone: the manifest's
		// national list is empty, never null.
		report, err = run(publish.Options{States: []string{"yuc"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Groups).To(Equal([]string{"tabular", "json"}))
		Expect(readManifest(out).National).To(Equal([]publish.ManifestNational{}))
	})

	It("refuses an out dir holding a national archive a subset build does not produce", func() {
		_, err := run(publish.Options{})
		Expect(err).NotTo(HaveOccurred())
		_, err = run(publish.Options{States: []string{"ags", "yuc", "zac"}})
		Expect(err).To(MatchError("out dir " + out + " holds entries this run does not produce: " +
			"CITATION.cff, DATA-DICTIONARY.json, DATA-DICTIONARY.md, LICENSE, NOTICE, QA-REPORT.md, README.md, " +
			"conagua-raw-2026-06-08.zip, national-csv.zip, national-json.zip, national-parquet.zip, national-sqlite.zip, " +
			"zenodo-metadata.json (remove them or use a fresh --out)"))
	})

	It("builds byte-identical national archives on a second run (byte-reproducible), the SQLite one's bytes reported", func() {
		_, err := run(publish.Options{ETLGitSHA: "abc123"})
		Expect(err).NotTo(HaveOccurred())
		first := out
		out = filepath.Join(GinkgoT().TempDir(), "again")
		_, err = run(publish.Options{ETLGitSHA: "abc123"})
		Expect(err).NotTo(HaveOccurred())

		for _, name := range nationalNames {
			a, b := readFile(filepath.Join(first, name)), readFile(filepath.Join(out, name))
			if name == "national-sqlite.zip" {
				// SQLite is content-reproducible only: the tables are
				// held equal, the bytes reported.
				_, ca := zipContents(filepath.Join(first, name))
				_, cb := zipContents(filepath.Join(out, name))
				x := openRO(writeDB(ca["bioclima.db"], "a.db"))
				y := openRO(writeDB(cb["bioclima.db"], "b.db"))
				for _, t := range ddlTableNames {
					Expect(selectAll(y, t, "")).To(Equal(selectAll(x, t, "")), t)
				}
				GinkgoWriter.Printf("national-sqlite.zip twice: bytes identical = %t (%d bytes)\n", bytes.Equal(a, b), len(a))
				continue
			}
			Expect(b).To(Equal(a), name)
		}
	})
})

func readFile(path string) []byte {
	GinkgoHelper()
	data, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	return data
}
