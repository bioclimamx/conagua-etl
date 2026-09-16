package publish_test

// Specs for the dependent national archives when a per-state source of
// the tabular group fails — the mirror of the JSON-direction spec: a
// referenced cell_id that cannot name a file fails one state's tabular
// archive only, so national-csv.zip fails soft naming that state while
// national-json.zip, national-sqlite.zip, and the raw archive build;
// the same guard fails national-parquet.zip on its own terms, since it
// reads the national cell list; a prior run's national-csv.zip is
// removed; and with a tabular-only fault in one state and a JSON-only
// fault in another, each copy-built archive names only the states whose
// source of its own group failed.

import (
	"context"
	"database/sql"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
)

var _ = Describe("the copy-built national archives when a tabular source fails", func() {
	const badCell = `21.0N/89.0000W`
	var (
		ctx  context.Context
		db   *sql.DB
		ids  map[string]int64
		root string
		out  string
		rec  *recorder
	)

	BeforeEach(func() {
		ctx = context.Background()
		db = openTempDB()
		ids = seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		seedNational(db, ids)
		root = GinkgoT().TempDir()
		seedRawSnapshot(root)
		out = filepath.Join(GinkgoT().TempDir(), "publish")
		rec = &recorder{}
	})

	run := func(only ...string) *publish.Report {
		GinkgoHelper()
		report, err := publish.Run(ctx, db, publish.Options{OutDir: out, SnapshotRoot: root, Now: fixedClock, Only: only}, rec.record)
		Expect(err).NotTo(HaveOccurred())
		return report
	}

	It("fails national-csv.zip soft naming the state whose tabular archive failed, national-parquet.zip on its own guard, and builds the rest; a prior run's national-csv.zip is removed", func() {
		report := run()
		Expect(report.Failed).To(BeZero())
		Expect(dirNames(out)).To(ContainElements("national-csv.zip", "national-parquet.zip", "yuc-tabular.zip"))

		insertCell(db, badCell, 21.0, -89.0)
		insertStationCell(db, ids["conv/31002"], badCell, 1.0)
		rec.events = nil
		report = run()
		Expect(report.Failed).To(Equal(3))
		Expect(artifactNames(report)).To(Equal(append([]string{
			"ags-tabular.zip", "ags-json.zip", "yuc-tabular.zip", "yuc-json.zip", "zac-tabular.zip", "zac-json.zip",
			"national-csv.zip", "national-parquet.zip", "national-json.zip", "national-sqlite.zip", "conagua-raw-2026-06-08.zip",
		}, docsNames...)))
		Expect(artifactByName(report, "yuc-tabular.zip").Err).To(Equal(
			`list cells of YUC: cell_id "21.0N/89.0000W" is not a POWER cell key`))
		Expect(artifactByName(report, "national-csv.zip").Err).To(Equal(
			"state YUC archive yuc-tabular.zip failed: national-csv.zip needs every state's tabular archive"))
		Expect(artifactByName(report, "national-parquet.zip").Err).To(Equal(
			`list cells of all states: cell_id "21.0N/89.0000W" is not a POWER cell key`))
		for _, name := range []string{"ags-tabular.zip", "ags-json.zip", "yuc-json.zip", "zac-tabular.zip", "zac-json.zip",
			"national-json.zip", "national-sqlite.zip", "conagua-raw-2026-06-08.zip"} {
			Expect(artifactByName(report, name).Err).To(BeEmpty(), name)
			Expect(artifactByName(report, name).Bytes).To(BeNumerically(">", 0), name)
		}

		Expect(report.TopLevelFiles).To(Equal(18))
		Expect(dirNames(out)).To(Equal([]string{
			"CHECKSUMS", "CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md",
			"README.md", "ags-json.zip", "ags-tabular.zip", "conagua-raw-2026-06-08.zip", "manifest.json",
			"national-json.zip", "national-sqlite.zip", "yuc-json.zip", "zac-json.zip", "zac-tabular.zip", "zenodo-metadata.json",
		}))
		Expect(tempDirs(out)).To(BeEmpty())

		// The dependent archive fails before any copy: one event, the
		// failure, no unit.
		Expect(eventsOf(rec, "national-csv.zip")).To(HaveLen(1))
		Expect(eventsOf(rec, "national-csv.zip")[0].Err).To(MatchError(artifactByName(report, "national-csv.zip").Err))
		Expect(eventsOf(rec, "national-parquet.zip")).To(HaveLen(1))
		Expect(eventsOf(rec, "national-json.zip")).To(HaveLen(4), "three copies and the artifact line")

		m := readManifest(out)
		Expect(m.States[1]).To(Equal(publish.ManifestState{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-json.zip"}}))
		Expect(m.National).To(Equal([]publish.ManifestNational{
			{Group: "national-csv", Artifacts: []string{}},
			{Group: "national-parquet", Artifacts: []string{}},
			{Group: "national-json", Artifacts: []string{"national-json.zip"}},
			{Group: "national-sqlite", Artifacts: []string{"national-sqlite.zip"}},
			{Group: "raw", Artifacts: []string{"conagua-raw-2026-06-08.zip"}},
		}))
		var sumNames []string
		for _, s := range readChecksums(out) {
			sumNames = append(sumNames, s.Name)
		}
		Expect(sumNames).To(Equal([]string{
			"CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md", "README.md",
			"ags-json.zip", "ags-tabular.zip", "conagua-raw-2026-06-08.zip", "manifest.json",
			"national-json.zip", "national-sqlite.zip", "yuc-json.zip", "zac-json.zip", "zac-tabular.zip", "zenodo-metadata.json",
		}))
		Expect(m.Files).To(HaveLen(16))
	})

	It("names, per copy-built archive, only the states whose source of its own group failed", func() {
		insertCell(db, badCell, 21.0, -89.0)
		insertStationCell(db, ids["conv/31002"], badCell, 1.0)
		insertDaily(db, ids["conv/32001"], "2020/01/01", 1.0, 1.0, 0.0, 1.0)

		report := run("tabular", "json", "national-csv", "national-json")
		Expect(report.Failed).To(Equal(4))
		Expect(artifactByName(report, "yuc-tabular.zip").Err).To(ContainSubstring("is not a POWER cell key"))
		Expect(artifactByName(report, "yuc-json.zip").Err).To(BeEmpty())
		Expect(artifactByName(report, "zac-tabular.zip").Err).To(BeEmpty())
		Expect(artifactByName(report, "zac-json.zip").Err).To(ContainSubstring(`date "2020/01/01" is not YYYY-MM-DD`))
		Expect(artifactByName(report, "national-csv.zip").Err).To(Equal(
			"state YUC archive yuc-tabular.zip failed: national-csv.zip needs every state's tabular archive"))
		Expect(artifactByName(report, "national-json.zip").Err).To(Equal(
			"state ZAC archive zac-json.zip failed: national-json.zip needs every state's json archive"))
		Expect(dirNames(out)).To(Equal([]string{
			"CHECKSUMS", "ags-json.zip", "ags-tabular.zip", "manifest.json", "yuc-json.zip", "zac-tabular.zip",
		}))
		Expect(report.TopLevelFiles).To(Equal(6))
	})
})
