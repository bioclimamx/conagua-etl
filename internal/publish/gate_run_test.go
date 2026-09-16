package publish_test

// Specs for the publish gate at the Run seam: the gate runs over the
// same read-only handle right after the snapshot resolves and ahead of
// every write — one progress event per
// rule in execution order, its counts the gate's own; a clean DB passes
// with the result in the Report; an error-severity finding refuses the
// run as a GateError naming every finding, before the out dir exists,
// with the refusal ahead of the state, out-dir, and provenance checks;
// a warn-severity finding never blocks; the message lists at most
// gateRefusalCap findings; and the DB's bytes are unchanged by a run
// that was refused and by one that passed. And for the docs group at
// the same seam: QA-REPORT.md written atomically into the out dir,
// listed in manifest.json and CHECKSUMS with the digest of its bytes,
// its numbers the loader's over the same DB, byte-identical across
// runs and across group selections, its progress event a File
// artifact line, and its failure one fail-soft artifact.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// gateRuleIDs is the gate's rule set in execution order — the events
// every Run emits ahead of its first artifact.
var gateRuleIDs = func() []string {
	rules := validate.GateRules()
	ids := make([]string, len(rules))
	for i, r := range rules {
		ids[i] = r.ID
	}
	return ids
}()

// gateEvents extracts the gate lines of a recording, in order.
func gateEvents(rec *recorder) []publish.ProgressEvent {
	var out []publish.ProgressEvent
	for _, ev := range rec.events {
		if ev.Rule != "" {
			out = append(out, ev)
		}
	}
	return out
}

func sha256OfFile(path string) string {
	GinkgoHelper()
	data, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

var _ = Describe("Run behind the gate", func() {
	var (
		dbPath string
		db     *sql.DB
		ids    map[string]int64
		out    string
		rec    *recorder
	)

	BeforeEach(func() {
		// The DB lives at a known path so its bytes can be hashed around
		// a run; the writer handle stays open for the corruptions the
		// specs inject, and Run reads through the same handle.
		dbPath = filepath.Join(GinkgoT().TempDir(), "gate.db")
		var err error
		db, err = schema.Open(dbPath)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = db.Close() })
		ids = seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		out = filepath.Join(GinkgoT().TempDir(), "publish")
		rec = &recorder{}
	})

	run := func(opts publish.Options) (*publish.Report, error) {
		opts.OutDir = out
		opts.Now = fixedClock
		return publish.Run(context.Background(), db, opts, rec.record)
	}

	It("evaluates every rule in execution order ahead of the first artifact, each event the gate's own counts, and carries the result in the Report", func() {
		report, err := run(publish.Options{Only: stateGroups})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Gate).NotTo(BeNil())
		Expect(report.Gate.Errors).To(BeZero())
		Expect(report.Gate.Rules).To(HaveLen(len(gateRuleIDs)))

		// The gate's events come first, one per rule, before any unit or
		// artifact event.
		Expect(len(rec.events)).To(BeNumerically(">", len(gateRuleIDs)))
		Expect(gateEvents(rec)).To(HaveLen(len(gateRuleIDs)))
		for i, id := range gateRuleIDs {
			ev := rec.events[i]
			rr := report.Gate.Rules[i]
			Expect(rr.ID).To(Equal(id))
			Expect(ev).To(Equal(publish.ProgressEvent{
				Rule: id, Scanned: rr.Scanned, Warnings: rr.Warnings, Errors: rr.Errors,
			}), id)
		}
		Expect(rec.events[len(gateRuleIDs)].Rule).To(BeEmpty())

		// The counts are the gate's over this DB: an independent
		// evaluation agrees rule for rule.
		direct, err := validate.Gate(context.Background(), db, nil)
		Expect(err).NotTo(HaveOccurred())
		for i := range direct.Rules {
			got, want := report.Gate.Rules[i], direct.Rules[i]
			got.Elapsed, want.Elapsed = 0, 0
			Expect(got).To(Equal(want), want.ID)
		}
		// The fixture's stations without coordinates are warn findings,
		// reported and never blocking.
		Expect(report.Gate.Warnings).To(Equal(gateEvents(rec)[0].Warnings))
		Expect(report.Gate.Warnings).To(BeNumerically(">", 0))
		Expect(report.Failed).To(BeZero())
		Expect(report.TopLevelFiles).To(Equal(3*2 + 2))
	})

	It("refuses an impossible coordinate as a GateError naming the station, before the out dir exists, the DB's bytes unchanged", func() {
		mustExec(db, `UPDATE stations SET lat = 0, lon = 0 WHERE id = ?`, ids["conv/31001"])
		before := sha256OfFile(dbPath)

		report, err := run(publish.Options{Only: stateGroups})
		var gateErr *publish.GateError
		Expect(errors.As(err, &gateErr)).To(BeTrue(), err)
		Expect(err).To(MatchError("gate refused: 1 error finding\n" +
			`  bbox — station conagua_conventional/31001 ("Mérida, \"La Plancha\"") at impossible lat=0.0000 lon=0.0000 — error blocks publish`))
		Expect(gateErr.Report).To(BeIdenticalTo(report.Gate))
		Expect(report.Gate.Errors).To(Equal(1))
		Expect(report.SnapshotDate).To(Equal("2026-06-08"))
		// Refused ahead of the state resolution and the out-dir check.
		Expect(report.States).To(BeEmpty())
		Expect(report.Artifacts).To(BeEmpty())
		Expect(report.TopLevelFiles).To(BeZero())
		_, statErr := os.Stat(out)
		Expect(os.IsNotExist(statErr)).To(BeTrue(), "nothing may be written behind a refusal")

		// Every rule still reported — the gate evaluates the whole set —
		// with bbox the one FAIL.
		events := gateEvents(rec)
		Expect(events).To(HaveLen(len(gateRuleIDs)))
		Expect(rec.events).To(HaveLen(len(gateRuleIDs)))
		Expect(events[0].Rule).To(Equal("bbox"))
		Expect(events[0].Errors).To(Equal(1))
		for _, ev := range events[1:] {
			Expect(ev.Errors).To(BeZero(), ev.Rule)
		}
		Expect(sha256OfFile(dbPath)).To(Equal(before))
	})

	It("refuses a run in flight and a fill leak together, every finding on its own line in rule order", func() {
		mustExec(db, `UPDATE power_runs SET status = 'running' WHERE id = ?`, seededMonthlyRunID)
		mustExec(db, `UPDATE daily_supplement SET t2m_c = -999 WHERE cell_id = ? AND date = '2020-01-01'`, cellShared)

		report, err := run(publish.Options{})
		Expect(err).To(MatchError("gate refused: 2 error findings\n" +
			"  runs-in-flight — power_runs.id=1 started 2026-07-01T00:00:00Z, status='running' — a run in flight or stranded " +
			"cannot be vouched for (finish it, or reconcile through validate)\n" +
			"  fill-leak — daily_supplement.t2m_c: 1 row holding POWER's -999 fill (" + cellShared + "/2020-01-01)"))
		Expect(report.Gate.Errors).To(Equal(2))
		Expect(report.Artifacts).To(BeEmpty())
		_, statErr := os.Stat(out)
		Expect(os.IsNotExist(statErr)).To(BeTrue())
	})

	It("refuses ahead of the raw-snapshot, state, out-dir, and provenance checks", func() {
		// Each of these would refuse the run on its own; the gate's
		// refusal comes first.
		Expect(os.MkdirAll(out, 0o755)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(out, "stray"), []byte("x"), 0o600)).To(Succeed())
		dup := insertPowerRun(db, powerRunSeed{
			startedAt: "2026-07-04T00:00:00Z", status: "complete",
			endpoint: "u", parameters: "T2M", community: "AG", startYear: 1981, endYear: 2010,
			gridResolution: "0.5x0.625", temporalMode: "monthly", counters: make([]*int64, 4),
		})
		insertMonthlySupplement(db, "n20.75_w88.125", "1981-2010", 1, dup)
		mustExec(db, `UPDATE stations SET lat = 91 WHERE id = ?`, ids["conv/1001"])

		_, err := run(publish.Options{States: []string{"xyz"}, SnapshotRoot: filepath.Join(out, "no-such-root")})
		Expect(err).To(MatchError(HavePrefix("gate refused: 2 error findings\n  bbox — station conagua_conventional/1001")))
		Expect(err.Error()).To(ContainSubstring("\n  run-label-unique — power_runs 1, "))
		Expect(dirNames(out)).To(Equal([]string{"stray"}))
	})

	It("lists at most 20 findings, counting the rest", func() {
		for i := range 25 {
			upsertStation(db, ingest.StationUpsert{
				Source: ingest.SourceConaguaConventional, ExternalID: fmt.Sprintf("z%02d", i), Name: "zero",
				State: "YUC", Lat: f64(0), Lon: f64(0),
			})
		}
		report, err := run(publish.Options{Only: stateGroups})
		Expect(report.Gate.Errors).To(Equal(25))
		lines := strings.Split(err.Error(), "\n")
		Expect(lines[0]).To(Equal("gate refused: 25 error findings"))
		Expect(lines).To(HaveLen(1 + 20 + 1))
		for _, l := range lines[1:21] {
			Expect(l).To(HavePrefix("  bbox — station conagua_conventional/z"))
		}
		Expect(lines[21]).To(Equal("  … and 5 more"))
		Expect(report.Gate.ErrorFindings()).To(HaveLen(25), "the report keeps every finding")
	})

	It("leaves the DB's bytes untouched across a run that passed", func() {
		before := sha256OfFile(dbPath)
		_, err := run(publish.Options{Only: stateGroups})
		Expect(err).NotTo(HaveOccurred())
		Expect(sha256OfFile(dbPath)).To(Equal(before))
	})

	It("aborts on a rule's own error, the Report carrying the rules that completed and nothing written", func() {
		mustExec(db, `DROP TABLE nasa_power_grid_cells`)
		report, err := run(publish.Options{Only: stateGroups})
		Expect(err).To(MatchError(HavePrefix("gate: rule cell-refs: scan monthly_supplement: ")))
		Expect(err).To(MatchError(ContainSubstring("no such table: nasa_power_grid_cells")))
		var gateErr *publish.GateError
		Expect(errors.As(err, &gateErr)).To(BeFalse(), "a rule error is not a refusal")
		Expect(report.Gate).NotTo(BeNil())
		Expect(report.Gate.Errors).To(BeZero())
		Expect(report.Gate.Rules).To(HaveLen(4))
		Expect(report.Gate.Rules[3].ID).To(Equal("station-refs"))
		Expect(gateEvents(rec)).To(HaveLen(4))
		Expect(report.States).To(BeEmpty())
		Expect(report.Artifacts).To(BeEmpty())
		_, statErr := os.Stat(out)
		Expect(os.IsNotExist(statErr)).To(BeTrue())
	})
})

var _ = Describe("Run with the docs group", func() {
	var (
		db  *sql.DB
		ids map[string]int64
		out string
		rec *recorder
	)

	BeforeEach(func() {
		db = openTempDB()
		ids = seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		out = filepath.Join(GinkgoT().TempDir(), "publish")
		rec = &recorder{}
	})

	run := func(opts publish.Options) (*publish.Report, error) {
		if opts.OutDir == "" {
			opts.OutDir = out
		}
		opts.Now = fixedClock
		if opts.ETLGitSHA == "" {
			opts.ETLGitSHA = "abc123"
		}
		return publish.Run(context.Background(), db, opts, rec.record)
	}

	// expectedQA renders the report the loader produces over the same
	// inputs Run resolves, so the file on disk is held to the loader
	// byte for byte.
	expectedQA := func(gate *validate.GateReport) []byte {
		GinkgoHelper()
		ctx := context.Background()
		runs, err := publish.LoadRuns(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		counts, err := publish.LoadCounts(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		states, err := publish.LoadStates(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		q, err := publish.LoadQA(ctx, db, "2026-06-08", runs, counts, gate, states)
		Expect(err).NotTo(HaveOccurred())
		var buf bytes.Buffer
		Expect(publish.RenderQA(&buf, q, publish.ProfileMeta{
			SchemaVersion: schema.Version, ETLGitSHA: "abc123", SnapshotDate: "2026-06-08",
			Runs: runs, Dataset: publish.DatasetMetadata(seededSnapshot, ""),
		})).To(Succeed())
		return buf.Bytes()
	}

	// docsDir is the out dir of a yuc json + docs run, bytewise-sorted.
	docsDir := []string{
		"CHECKSUMS", "CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md",
		"README.md", "manifest.json", "yuc-json.zip", "zenodo-metadata.json",
	}

	It("writes the docs group after the archives — one File artifact per file, in write order, each listed in manifest.json and CHECKSUMS with the digest of its bytes — the QA report the loader's over the same DB", func() {
		report, err := run(publish.Options{States: []string{"yuc"}, Only: []string{"json", "docs"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
		Expect(report.Groups).To(Equal([]string{"json", "docs"}))
		Expect(artifactNames(report)).To(Equal(append([]string{"yuc-json.zip"}, docsNames...)))
		for _, a := range report.Artifacts[1:] {
			Expect(a.Err).To(BeEmpty(), a.Name)
			Expect(a.Entries).To(BeZero(), a.Name)
			hexsum, n, err := archive.SHA256File(filepath.Join(out, a.Name))
			Expect(err).NotTo(HaveOccurred())
			Expect(a.SHA256).To(Equal(hexsum), a.Name)
			Expect(a.Bytes).To(Equal(n), a.Name)
			Expect(n).To(BeNumerically(">", 0), a.Name)
		}
		Expect(report.TopLevelFiles).To(Equal(11))
		Expect(dirNames(out)).To(Equal(docsDir))

		data, err := os.ReadFile(filepath.Join(out, "QA-REPORT.md"))
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal(expectedQA(report.Gate)))

		// The gate section is this run's gate, and the report names the
		// state selection's whole DB, not the one state.
		text := string(data)
		Expect(text).To(ContainSubstring("| bbox | Lat/lon plausibility | "))
		Expect(text).To(ContainSubstring(fmt.Sprintf("Rules: %d. Error findings: 0. Warn findings: %d.\n",
			len(gateRuleIDs), report.Gate.Warnings)))
		Expect(text).To(ContainSubstring("| AGS | Aguascalientes | "))
		Expect(text).To(ContainSubstring("ETL git SHA `abc123`."))

		// The README and the citation read the same run's inputs: the
		// whole DB's states, the snapshot, the fixed clock's UTC date.
		readme, err := os.ReadFile(filepath.Join(out, "README.md"))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(readme)).To(ContainSubstring("| Aguascalientes | AGS | `ags-tabular.zip` | `ags-json.zip` |\n"))
		Expect(string(readme)).To(ContainSubstring("| Yucatán | YUC | `yuc-tabular.zip` | `yuc-json.zip` |\n"))
		citation, err := os.ReadFile(filepath.Join(out, "CITATION.cff"))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(citation)).To(ContainSubstring("\ndate-released: " + fixedNow.Format("2006-01-02") + "\n"))

		m := readManifest(out)
		wantFiles := make([]publish.ManifestFile, 0, len(report.Artifacts))
		for _, a := range report.Artifacts {
			wantFiles = append(wantFiles, publish.ManifestFile{Name: a.Name, SHA256: a.SHA256, Bytes: a.Bytes})
		}
		slices.SortFunc(wantFiles, func(a, b publish.ManifestFile) int { return strings.Compare(a.Name, b.Name) })
		Expect(m.Files).To(Equal(wantFiles))
		Expect(m.States).To(Equal([]publish.ManifestState{{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-json.zip"}}}))
		Expect(m.National).To(BeEmpty())
		var sumNames []string
		for _, s := range readChecksums(out) {
			sumNames = append(sumNames, s.Name)
			if s.Name != "manifest.json" {
				Expect(s.SHA256).To(Equal(artifactByName(report, s.Name).SHA256), s.Name)
			}
		}
		Expect(sumNames).To(Equal(docsDir[1:]))

		// Each artifact event is a File line: no entries, the digest and
		// size of the bytes on disk; the group's last file closes the run.
		var fileEvents []publish.ProgressEvent
		for _, ev := range rec.events {
			if ev.File {
				fileEvents = append(fileEvents, ev)
			}
		}
		wantEvents := make([]publish.ProgressEvent, 0, len(docsNames))
		for _, a := range report.Artifacts[1:] {
			wantEvents = append(wantEvents, publish.ProgressEvent{Artifact: a.Name, File: true, Bytes: a.Bytes, SHA256: a.SHA256})
		}
		Expect(fileEvents).To(Equal(wantEvents))
		Expect(rec.events[len(rec.events)-1]).To(Equal(fileEvents[len(fileEvents)-1]), "written after the archives")
	})

	It("renders the same bytes for every docs file on a second run, alone or beside every archive of a full run", func() {
		_, err := run(publish.Options{States: []string{"yuc"}, Only: []string{"docs"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(dirNames(out)).To(Equal(slices.DeleteFunc(slices.Clone(docsDir), func(s string) bool { return s == "yuc-json.zip" })))
		first := map[string][]byte{}
		for _, name := range docsNames {
			first[name] = readFile(filepath.Join(out, name))
		}

		again := filepath.Join(GinkgoT().TempDir(), "again")
		root := GinkgoT().TempDir()
		seedRawSnapshot(root)
		report, err := run(publish.Options{OutDir: again, SnapshotRoot: root})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Groups).To(HaveLen(8))
		Expect(artifactNames(report)[len(report.Artifacts)-len(docsNames):]).To(Equal(docsNames))
		for _, name := range docsNames {
			Expect(readFile(filepath.Join(again, name))).To(Equal(first[name]), name)
		}
	})

	It("is not selected by an empty request under --state, and a docs file from a wider build is foreign to such a run", func() {
		_, err := run(publish.Options{States: []string{"yuc"}, Only: []string{"docs"}})
		Expect(err).NotTo(HaveOccurred())
		rec.events = nil
		report, err := run(publish.Options{States: []string{"yuc"}})
		Expect(err).To(MatchError("out dir " + out + " holds entries this run does not produce: " +
			strings.Join(docsNamesSorted, ", ") + " (remove them or use a fresh --out)"))
		Expect(report.Groups).To(Equal([]string{"tabular", "json"}))
	})

	It("fails the files that read a docs input that could not be loaded soft — each recorded, reported, its prior copy removed — while the files that need no DB read, the archives, and the manifest ship", func() {
		_, err := run(publish.Options{States: []string{"yuc"}, Only: []string{"json", "docs"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(dirNames(out)).To(Equal(docsDir))

		// A stations.state the code table does not know is a row the
		// loader describes rather than refuses, so the group needs a
		// fault of its own: a column only the QA report's warnings
		// summary reads — the JSON archive and the gate never touch it —
		// dropped from under it fails the QA load, and with it every
		// file rendered over the DB-loaded inputs, and nothing else.
		mustExec(db, `ALTER TABLE parsing_warnings DROP COLUMN source_file`)
		rec.events = nil
		report, err := run(publish.Options{States: []string{"yuc"}, Only: []string{"json", "docs"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(artifactNames(report)).To(Equal(append([]string{"yuc-json.zip"}, docsNames...)))
		Expect(report.Failed).To(Equal(5))
		Expect(report.Artifacts[0].Err).To(BeEmpty())
		shipped := []string{"DATA-DICTIONARY.md", "DATA-DICTIONARY.json", "LICENSE"}
		for _, a := range report.Artifacts[1:] {
			if slices.Contains(shipped, a.Name) {
				Expect(a.Err).To(BeEmpty(), a.Name)
				Expect(a.Bytes).To(BeNumerically(">", 0), a.Name)
				continue
			}
			Expect(a.Err).To(HavePrefix("write file "+filepath.Join(out, a.Name)+": load warnings by source: "), a.Name)
			Expect(a.Bytes).To(BeZero(), a.Name)
		}
		Expect(report.TopLevelFiles).To(Equal(6))
		Expect(dirNames(out)).To(Equal([]string{
			"CHECKSUMS", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "manifest.json", "yuc-json.zip",
		}))
		m := readManifest(out)
		var listed []string
		for _, f := range m.Files {
			listed = append(listed, f.Name)
		}
		Expect(listed).To(Equal([]string{"DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "yuc-json.zip"}))
		last := rec.events[len(rec.events)-1]
		Expect(last.Artifact).To(Equal("zenodo-metadata.json"))
		Expect(last.File).To(BeTrue())
		Expect(last.Err).To(MatchError(artifactByName(report, "zenodo-metadata.json").Err))
	})
})
