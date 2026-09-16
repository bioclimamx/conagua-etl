package publish_test

// Specs for Run — the spine: DB → curated CSV → per-state zip →
// manifest.json → CHECKSUMS, proven at each seam it crosses (the zip
// entries against TabularEntries — the four scope folders merged —
// CHECKSUMS against the bytes on disk, manifest.json against the
// loaders), with the byte-reproducibility claim, the archive-wide unit
// numbering, --state scoping, the no-snapshot precondition, the out-dir
// discipline (a foreign entry refuses the build; a state that fails soft
// loses its prior archive), per-state fail-soft, the two abort paths,
// and the provenance fail-fast before any archive is written.

import (
	"archive/zip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

var (
	fixedNow = time.Date(2026, 8, 29, 12, 34, 56, 0, time.UTC)
	// seededSnapshot is the snapshot_date of seedRuns' latest complete
	// ingest run, as the time every archive entry is stamped with.
	seededSnapshot = time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
)

func fixedClock() time.Time { return fixedNow }

// stateGroups selects the two per-state groups by name — the scope of
// the specs that hold Run's per-state archives to their builders; the
// deposit-wide groups have their own Run specs.
var stateGroups = []string{"tabular", "json"}

// recorder collects progress events with the wall-clock Elapsed zeroed
// so sequences compare exactly.
type recorder struct {
	events []publish.ProgressEvent
}

func (r *recorder) record(ev publish.ProgressEvent) {
	ev.Elapsed = 0
	r.events = append(r.events, ev)
}

func readManifest(dir string) publish.Manifest {
	GinkgoHelper()
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	Expect(err).NotTo(HaveOccurred())
	var m publish.Manifest
	Expect(json.Unmarshal(data, &m)).To(Succeed())
	return m
}

func readChecksums(dir string) []archive.Checksum {
	GinkgoHelper()
	f, err := os.Open(filepath.Join(dir, "CHECKSUMS"))
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = f.Close() }()
	sums, err := archive.ParseChecksums(f)
	Expect(err).NotTo(HaveOccurred())
	return sums
}

func dirNames(dir string) []string {
	GinkgoHelper()
	entries, err := os.ReadDir(dir)
	Expect(err).NotTo(HaveOccurred())
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// isTabularUnit reports whether a tabular archive entry is a progress
// unit: a per-station or per-cell daily file, or the state database at
// the archive root.
func isTabularUnit(name string) bool {
	for _, dir := range []string{"conagua/daily_observations/", "combined/combined_daily/", "nasa_power/daily/"} {
		if strings.HasPrefix(name, dir) {
			return true
		}
	}
	return strings.HasSuffix(name, ".db") && !strings.Contains(name, "/")
}

// unitEvents renders the unit events the recorder expects for one
// artifact: its per-station and per-cell files and the state database
// by entry path, in write order, numbered across the archive.
func unitEvents(artifact string, units ...string) []publish.ProgressEvent {
	out := make([]publish.ProgressEvent, len(units))
	for i, u := range units {
		out[i] = publish.ProgressEvent{Artifact: artifact, Unit: u, Index: i + 1, Total: len(units)}
	}
	return out
}

var _ = Describe("Run", func() {
	var (
		db  *sql.DB
		ids map[string]int64
		out string
		rec *recorder
	)

	BeforeEach(func() {
		db = openTempDB()
		ids = seedTwoStates(db)
		seedRuns(db)
		out = filepath.Join(GinkgoT().TempDir(), "publish")
		rec = &recorder{}
	})

	run := func(opts publish.Options) (*publish.Report, error) {
		opts.OutDir = out
		if opts.Now == nil {
			opts.Now = fixedClock
		}
		return publish.Run(context.Background(), db, opts, rec.record)
	}

	It("builds every state's archives in code order, tabular then JSON per state, then manifest.json and CHECKSUMS", func() {
		report, err := run(publish.Options{ETLGitSHA: "abc123", Only: stateGroups})
		Expect(err).NotTo(HaveOccurred())

		Expect(report.SnapshotDate).To(Equal("2026-06-08"))
		Expect(report.OutDir).To(Equal(out))
		Expect(report.States).To(Equal([]string{"AGS", "YUC"}))
		Expect(report.Groups).To(Equal([]string{"tabular", "json"}))
		Expect(report.Failed).To(BeZero())
		Expect(report.TopLevelFiles).To(Equal(6))
		Expect(report.StartedAt).To(Equal(fixedNow))
		Expect(report.FinishedAt).To(Equal(fixedNow))
		Expect(report.Artifacts).To(HaveLen(4))
		// Per state, the tabular archive — the nine per-table CSVs (three
		// conagua/, three nasa_power/, combined_monthly, two provenance/)
		// plus two daily files per station and one per referenced cell
		// (none here), the ten Parquet twins, and the state database —
		// then the JSON one: two files per station plus the two
		// provenance files.
		agsTabular, agsJSON, yucTabular, yucJSON := report.Artifacts[0], report.Artifacts[1], report.Artifacts[2], report.Artifacts[3]
		Expect(agsTabular.Name).To(Equal("ags-tabular.zip"))
		Expect(agsTabular.Entries).To(Equal(9 + 2*1 + 10 + 1))
		Expect(agsJSON.Name).To(Equal("ags-json.zip"))
		Expect(agsJSON.Entries).To(Equal(2 + 2*1))
		Expect(yucTabular.Name).To(Equal("yuc-tabular.zip"))
		Expect(yucTabular.Entries).To(Equal(9 + 2*3 + 10 + 1))
		Expect(yucJSON.Name).To(Equal("yuc-json.zip"))
		Expect(yucJSON.Entries).To(Equal(2 + 2*3))
		for _, a := range report.Artifacts {
			Expect(a.Err).To(BeEmpty())
			Expect(a.Bytes).To(BeNumerically(">", 0))
			Expect(a.SHA256).To(MatchRegexp(`^[0-9a-f]{64}$`))
		}
		Expect(dirNames(out)).To(Equal([]string{
			"CHECKSUMS", "ags-json.zip", "ags-tabular.zip", "manifest.json", "yuc-json.zip", "yuc-tabular.zip",
		}))
	})

	It("writes each archive's entries exactly as TabularEntries assembles the four scope folders, their Parquet twins, and the state database, stamped with the snapshot date", func() {
		_, err := run(publish.Options{Only: stateGroups})
		Expect(err).NotTo(HaveOccurred())

		runs, err := publish.LoadRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		entries, units, err := publish.TabularEntries(context.Background(), db, yucatan, runs, GinkgoT().TempDir(), nil)
		Expect(err).NotTo(HaveOccurred())
		// Two daily files per station, none per cell (no station has
		// one), and the state database.
		Expect(units).To(Equal(2*3 + 1))
		var paths []string
		for _, e := range entries {
			paths = append(paths, e.Path)
		}
		Expect(paths).To(Equal([]string{
			"combined/combined_daily.parquet",
			"combined/combined_daily/yuc/daily-31001.csv",
			"combined/combined_daily/yuc/daily-31002.csv",
			"combined/combined_daily/yuc/daily-3101.csv",
			"combined/combined_monthly.csv",
			"combined/combined_monthly.parquet",
			"conagua/daily_observations.parquet",
			"conagua/daily_observations/yuc/daily-31001.csv",
			"conagua/daily_observations/yuc/daily-31002.csv",
			"conagua/daily_observations/yuc/daily-3101.csv",
			"conagua/monthly_normals.csv",
			"conagua/monthly_normals.parquet",
			"conagua/monthly_normals_extras.csv",
			"conagua/monthly_normals_extras.parquet",
			"conagua/stations.csv",
			"conagua/stations.parquet",
			"nasa_power/cells.csv",
			"nasa_power/cells.parquet",
			"nasa_power/daily.parquet",
			"nasa_power/monthly.csv",
			"nasa_power/monthly.parquet",
			"nasa_power/station_cell_map.csv",
			"nasa_power/station_cell_map.parquet",
			"provenance/ingest_runs.csv",
			"provenance/power_runs.csv",
			"yuc.db",
		}))
		zr, err := zip.OpenReader(filepath.Join(out, "yuc-tabular.zip"))
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = zr.Close() }()
		Expect(zr.File).To(HaveLen(len(entries)))
		for i, f := range zr.File {
			Expect(f.Name).To(Equal(entries[i].Path))
			Expect(f.Modified.UTC()).To(Equal(seededSnapshot))
			rc, err := f.Open()
			Expect(err).NotTo(HaveOccurred())
			got, err := io.ReadAll(rc)
			Expect(err).NotTo(HaveOccurred())
			Expect(rc.Close()).To(Succeed())
			Expect(got).To(Equal(render(entries[i])), f.Name)
		}
	})

	It("lists in CHECKSUMS the sha256 of every top-level file it wrote, manifest.json included, itself excluded", func() {
		report, err := run(publish.Options{Only: stateGroups})
		Expect(err).NotTo(HaveOccurred())

		sums := readChecksums(out)
		var names []string
		for _, s := range sums {
			names = append(names, s.Name)
			hexsum, _, err := archive.SHA256File(filepath.Join(out, s.Name))
			Expect(err).NotTo(HaveOccurred())
			Expect(s.SHA256).To(Equal(hexsum), s.Name)
		}
		Expect(names).To(Equal([]string{"ags-json.zip", "ags-tabular.zip", "manifest.json", "yuc-json.zip", "yuc-tabular.zip"}))
		Expect(sums[0].SHA256).To(Equal(report.Artifacts[1].SHA256))
		Expect(sums[1].SHA256).To(Equal(report.Artifacts[0].SHA256))
		Expect(sums[3].SHA256).To(Equal(report.Artifacts[3].SHA256))
		Expect(sums[4].SHA256).To(Equal(report.Artifacts[2].SHA256))
	})

	It("writes a manifest.json that indexes the run: identity, states, provenance, counts, files", func() {
		report, err := run(publish.Options{ETLGitSHA: "abc123", Only: stateGroups})
		Expect(err).NotTo(HaveOccurred())

		got := readManifest(out)
		Expect(got.SchemaVersion).To(Equal(schema.Version))
		Expect(got.ETLGitSHA).To(Equal("abc123"))
		Expect(got.GeneratedAt).To(Equal("2026-08-29T12:34:56Z"))
		Expect(got.SnapshotDate).To(Equal("2026-06-08"))
		Expect(got.Dataset).To(Equal(publish.DatasetMetadata(seededSnapshot, "")))
		Expect(got.States).To(Equal([]publish.ManifestState{
			{Code: "AGS", Name: "Aguascalientes", Artifacts: []string{"ags-tabular.zip", "ags-json.zip"}},
			{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-tabular.zip", "yuc-json.zip"}},
		}))
		Expect(got.Runs.Ingest).To(Equal(wantIngestRuns))
		expectPowerRunsEqual(got.Runs.Power, wantPowerRuns)
		Expect(got.PowerParameters).To(Equal(publish.PowerParameters()))
		counts, err := publish.LoadCounts(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Counts).To(Equal(counts))
		// Files are listed by name, not in build order.
		Expect(got.Files).To(Equal([]publish.ManifestFile{
			{Name: "ags-json.zip", SHA256: report.Artifacts[1].SHA256, Bytes: report.Artifacts[1].Bytes},
			{Name: "ags-tabular.zip", SHA256: report.Artifacts[0].SHA256, Bytes: report.Artifacts[0].Bytes},
			{Name: "yuc-json.zip", SHA256: report.Artifacts[3].SHA256, Bytes: report.Artifacts[3].Bytes},
			{Name: "yuc-tabular.zip", SHA256: report.Artifacts[2].SHA256, Bytes: report.Artifacts[2].Bytes},
		}))
	})

	It("reports one event per unit as it lands, numbered across its archive, and one per artifact on completion, archive after archive", func() {
		_, err := run(publish.Options{Only: stateGroups})
		Expect(err).NotTo(HaveOccurred())

		// The state database is a unit at its Path-sorted position: first
		// for ags (before combined/), last for yuc (after provenance/).
		var want []publish.ProgressEvent
		want = append(want, unitEvents("ags-tabular.zip",
			"ags.db",
			"combined/combined_daily/ags/daily-1001.csv",
			"conagua/daily_observations/ags/daily-1001.csv",
		)...)
		want = append(want, publish.ProgressEvent{Artifact: "ags-tabular.zip"})
		want = append(want, unitEvents("ags-json.zip", "combined/ags/1001/profile.json")...)
		want = append(want, publish.ProgressEvent{Artifact: "ags-json.zip"})
		want = append(want, unitEvents("yuc-tabular.zip",
			"combined/combined_daily/yuc/daily-31001.csv",
			"combined/combined_daily/yuc/daily-31002.csv",
			"combined/combined_daily/yuc/daily-3101.csv",
			"conagua/daily_observations/yuc/daily-31001.csv",
			"conagua/daily_observations/yuc/daily-31002.csv",
			"conagua/daily_observations/yuc/daily-3101.csv",
			"yuc.db",
		)...)
		want = append(want, publish.ProgressEvent{Artifact: "yuc-tabular.zip"})
		want = append(want, unitEvents("yuc-json.zip",
			"combined/yuc/31001/profile.json",
			"combined/yuc/31002/profile.json",
			"combined/yuc/3101/profile.json",
		)...)
		want = append(want, publish.ProgressEvent{Artifact: "yuc-json.zip"})
		// The artifact lines carry the archive's size and digest; the
		// gate's lines come before every one of them.
		events := rec.events[len(gateRuleIDs):]
		for i := range events {
			Expect(events[i].Rule).To(BeEmpty())
			if events[i].Unit == "" {
				Expect(events[i].Bytes).To(BeNumerically(">", 0))
				Expect(events[i].SHA256).To(HaveLen(64))
				Expect(events[i].Err).NotTo(HaveOccurred())
				events[i].Bytes, events[i].SHA256, events[i].Entries = 0, "", 0
			}
		}
		Expect(events).To(Equal(want))
	})

	It("counts a referenced cell's daily file as a unit too: every per-station and per-cell file, in write order, with the archive total from the first event", func() {
		seedPower(db, ids)
		report, err := run(publish.Options{States: []string{"yuc"}})
		Expect(err).NotTo(HaveOccurred())
		// Four stations now (31003 joined), two referenced cells, the ten
		// Parquet twins, and the state database.
		Expect(report.Artifacts[0].Entries).To(Equal(9 + 2*4 + 2 + 10 + 1))

		// The unit events are exactly the archive's entries under the
		// three per-unit folders plus the state database, in the zip's
		// own order, 1..Total.
		zr, err := zip.OpenReader(filepath.Join(out, "yuc-tabular.zip"))
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = zr.Close() }()
		var wantUnits []string
		for _, f := range zr.File {
			if isTabularUnit(f.Name) {
				wantUnits = append(wantUnits, f.Name)
			}
		}
		Expect(wantUnits).To(Equal([]string{
			"combined/combined_daily/yuc/daily-31001.csv",
			"combined/combined_daily/yuc/daily-31002.csv",
			"combined/combined_daily/yuc/daily-31003.csv",
			"combined/combined_daily/yuc/daily-3101.csv",
			"conagua/daily_observations/yuc/daily-31001.csv",
			"conagua/daily_observations/yuc/daily-31002.csv",
			"conagua/daily_observations/yuc/daily-31003.csv",
			"conagua/daily_observations/yuc/daily-3101.csv",
			"nasa_power/daily/daily-21.0N_89.6250W.csv",
			"nasa_power/daily/daily-21.5N_89.3750W.csv",
			"yuc.db",
		}))
		want := append(unitEvents("yuc-tabular.zip", wantUnits...), publish.ProgressEvent{Artifact: "yuc-tabular.zip"})
		Expect(eventsOf(rec, "yuc-tabular.zip")).To(Equal(want))
	})

	It("produces byte-identical archives on a second build; only generated_at moves (byte-reproducible)", func() {
		first, err := run(publish.Options{ETLGitSHA: "abc123", Only: stateGroups})
		Expect(err).NotTo(HaveOccurred())
		firstOut := out
		firstZip, err := os.ReadFile(filepath.Join(firstOut, "yuc-tabular.zip"))
		Expect(err).NotTo(HaveOccurred())
		firstManifest, err := os.ReadFile(filepath.Join(firstOut, "manifest.json"))
		Expect(err).NotTo(HaveOccurred())

		out = filepath.Join(GinkgoT().TempDir(), "again")
		later := fixedNow.Add(90 * time.Minute)
		second, err := run(publish.Options{ETLGitSHA: "abc123", Only: stateGroups, Now: func() time.Time { return later }})
		Expect(err).NotTo(HaveOccurred())
		secondZip, err := os.ReadFile(filepath.Join(out, "yuc-tabular.zip"))
		Expect(err).NotTo(HaveOccurred())
		secondManifest, err := os.ReadFile(filepath.Join(out, "manifest.json"))
		Expect(err).NotTo(HaveOccurred())

		Expect(secondZip).To(Equal(firstZip))
		firstJSON, err := os.ReadFile(filepath.Join(firstOut, "yuc-json.zip"))
		Expect(err).NotTo(HaveOccurred())
		secondJSON, err := os.ReadFile(filepath.Join(out, "yuc-json.zip"))
		Expect(err).NotTo(HaveOccurred())
		Expect(secondJSON).To(Equal(firstJSON))
		for i := range first.Artifacts {
			Expect(second.Artifacts[i].SHA256).To(Equal(first.Artifacts[i].SHA256))
			Expect(second.Artifacts[i].Bytes).To(Equal(first.Artifacts[i].Bytes))
		}
		Expect(string(secondManifest)).To(Equal(strings.Replace(string(firstManifest),
			`"generated_at": "2026-08-29T12:34:56Z"`, `"generated_at": "2026-08-29T14:04:56Z"`, 1)))
		Expect(string(secondManifest)).NotTo(Equal(string(firstManifest)))
	})

	It("scopes to the requested states, matched case-insensitively, kept in code order", func() {
		report, err := run(publish.Options{States: []string{"yuc", "AGS", "Yuc"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.States).To(Equal([]string{"AGS", "YUC"}))

		out = filepath.Join(GinkgoT().TempDir(), "one")
		report, err = run(publish.Options{States: []string{"yuc"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.States).To(Equal([]string{"YUC"}))
		Expect(report.TopLevelFiles).To(Equal(4))
		Expect(dirNames(out)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-json.zip", "yuc-tabular.zip"}))
		Expect(readManifest(out).States).To(Equal([]publish.ManifestState{
			{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-tabular.zip", "yuc-json.zip"}},
		}))
	})

	It("rejects an unknown state before writing anything", func() {
		report, err := run(publish.Options{States: []string{"xyz"}})
		Expect(err).To(MatchError(ContainSubstring(`unknown state "xyz" (valid: AGS, YUC)`)))
		Expect(report).NotTo(BeNil())
		Expect(report.SnapshotDate).To(Equal("2026-06-08"))
		_, statErr := os.Stat(out)
		Expect(os.IsNotExist(statErr)).To(BeTrue())
	})

	It("refuses a DB with no complete ingest run: there is no snapshot to publish", func() {
		mustExec(db, `UPDATE ingest_runs SET status = 'aborted' WHERE status = 'complete'`)
		report, err := run(publish.Options{Only: stateGroups})
		Expect(err).To(MatchError("no complete ingest run: nothing to publish"))
		Expect(report).NotTo(BeNil())
		Expect(report.SnapshotDate).To(BeEmpty())
		_, statErr := os.Stat(out)
		Expect(os.IsNotExist(statErr)).To(BeTrue())
	})

	It("requires OutDir", func() {
		report, err := publish.Run(context.Background(), db, publish.Options{}, nil)
		Expect(err).To(MatchError("publish: OutDir is required"))
		Expect(report).To(BeNil())
	})

	It("stamps generated_at from the wall clock when Now is nil", func() {
		before := time.Now().UTC().Truncate(time.Second)
		report, err := publish.Run(context.Background(), db, publish.Options{OutDir: out, Only: stateGroups}, nil)
		Expect(err).NotTo(HaveOccurred())
		stamp, err := time.Parse(time.RFC3339, readManifest(out).GeneratedAt)
		Expect(err).NotTo(HaveOccurred())
		Expect(stamp).To(BeTemporally(">=", before))
		Expect(stamp).To(BeTemporally("<=", time.Now().UTC()))
		Expect(report.StartedAt).To(BeTemporally(">=", before))
		Expect(report.FinishedAt).To(BeTemporally(">=", report.StartedAt))
	})

	It("fails soft per archive: a broken state's archives are recorded and the rest still ship", func() {
		// An infinite REAL cannot be written (checkFinite refuses it in the
		// Parquet twin, the CSV cell, and the daily.json row alike), so
		// both AGS archives fail inside their zip writes while YUC is
		// untouched. The fault is one the gate does not judge — an
		// impossible coordinate would have refused the run up front.
		mustExec(db, `UPDATE daily_observations SET tmax = 9e999 WHERE station_id = ?`, ids["conv/1001"])
		report, err := run(publish.Options{Only: stateGroups})
		Expect(err).NotTo(HaveOccurred())

		Expect(report.Failed).To(Equal(2))
		Expect(report.TopLevelFiles).To(Equal(4))
		Expect(report.Artifacts).To(HaveLen(4))
		Expect(report.Artifacts[0].Name).To(Equal("ags-tabular.zip"))
		// The archive is named once, by the zip write; the entry path
		// carries the station.
		Expect(report.Artifacts[0].Err).To(HavePrefix("write zip " + filepath.Join(out, "ags-tabular.zip") +
			`: entry "combined/combined_daily.parquet": station 1001: row 1: column tmax_c: non-finite value +Inf`))
		Expect(strings.Count(report.Artifacts[0].Err, "ags-tabular.zip")).To(Equal(1))
		Expect(report.Artifacts[0].Bytes).To(BeZero())
		Expect(report.Artifacts[0].SHA256).To(BeEmpty())
		Expect(report.Artifacts[1].Name).To(Equal("ags-json.zip"))
		Expect(report.Artifacts[1].Err).To(HavePrefix("write zip " + filepath.Join(out, "ags-json.zip") +
			`: entry "combined/ags/1001/daily.json": `))
		Expect(report.Artifacts[1].Err).To(HaveSuffix("column tmax_c: non-finite value +Inf"))
		Expect(strings.Count(report.Artifacts[1].Err, "ags-json.zip")).To(Equal(1))
		Expect(report.Artifacts[1].Bytes).To(BeZero())
		Expect(report.Artifacts[2].Name).To(Equal("yuc-tabular.zip"))
		Expect(report.Artifacts[2].Err).To(BeEmpty())
		Expect(report.Artifacts[3].Name).To(Equal("yuc-json.zip"))
		Expect(report.Artifacts[3].Err).To(BeEmpty())

		// Nothing partial at the failed paths; the survivors are complete.
		Expect(dirNames(out)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-json.zip", "yuc-tabular.zip"}))
		m := readManifest(out)
		Expect(m.States).To(Equal([]publish.ManifestState{
			{Code: "AGS", Name: "Aguascalientes", Artifacts: []string{}},
			{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-tabular.zip", "yuc-json.zip"}},
		}))
		Expect(m.Files).To(HaveLen(2))
		Expect(m.Files[0].Name).To(Equal("yuc-json.zip"))
		Expect(m.Files[1].Name).To(Equal("yuc-tabular.zip"))
		var names []string
		for _, s := range readChecksums(out) {
			names = append(names, s.Name)
		}
		Expect(names).To(Equal([]string{"manifest.json", "yuc-json.zip", "yuc-tabular.zip"}))

		// After the gate's lines: the state database (its copy carries
		// the infinite REAL as stored), the archive's first entry, still
		// reports; the failure is on the artifact line, at the Parquet
		// twin that follows it; the JSON archive fails on its first
		// station's daily.json, before any unit; and the loop went on.
		events := rec.events[len(gateRuleIDs):]
		Expect(events[:3]).To(Equal([]publish.ProgressEvent{
			{Artifact: "ags-tabular.zip", Unit: "ags.db", Index: 1, Total: 3},
			{Artifact: "ags-tabular.zip", Err: events[1].Err},
			{Artifact: "ags-json.zip", Err: events[2].Err},
		}))
		Expect(events[1].Err).To(MatchError(ContainSubstring("non-finite value +Inf")))
		Expect(events[2].Err).To(MatchError(ContainSubstring("non-finite value +Inf")))
		Expect(rec.events[len(rec.events)-1].Artifact).To(Equal("yuc-json.zip"))
		Expect(rec.events[len(rec.events)-1].Err).NotTo(HaveOccurred())
	})

	It("aborts on cancellation between archives, report populated, no manifest or CHECKSUMS written", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var events []publish.ProgressEvent
		report, err := publish.Run(ctx, db, publish.Options{OutDir: out, Now: fixedClock, Only: stateGroups},
			func(ev publish.ProgressEvent) {
				events = append(events, ev)
				if ev.Unit == "" && ev.Rule == "" {
					// The first archive just completed; the operator interrupts.
					cancel()
				}
			})
		Expect(errors.Is(err, context.Canceled)).To(BeTrue(), err)
		Expect(err).To(MatchError(HavePrefix("aborted after 1 of 4 artifacts: ")))
		Expect(report.States).To(Equal([]string{"AGS", "YUC"}))
		Expect(report.Artifacts).To(HaveLen(1))
		Expect(report.Artifacts[0].Err).To(BeEmpty())
		Expect(report.Failed).To(BeZero())
		Expect(report.TopLevelFiles).To(BeZero())
		Expect(report.FinishedAt).To(Equal(fixedNow))
		// The gate's lines, then the first archive's three units (its
		// database and two daily files) and its artifact line; the
		// state's JSON archive is never started.
		Expect(events).To(HaveLen(len(gateRuleIDs) + 4))
		Expect(dirNames(out)).To(Equal([]string{"ags-tabular.zip"}))
	})

	It("aborts when cancellation surfaces from inside an archive write, counting the casualty", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		report, err := publish.Run(ctx, db, publish.Options{OutDir: out, Now: fixedClock, Only: stateGroups},
			func(ev publish.ProgressEvent) {
				if ev.Artifact == "yuc-tabular.zip" && ev.Unit == "conagua/daily_observations/yuc/daily-31001.csv" {
					cancel()
				}
			})
		Expect(errors.Is(err, context.Canceled)).To(BeTrue(), err)
		Expect(err).To(MatchError(HavePrefix("aborted after 3 of 4 artifacts: write zip ")))
		Expect(report.Artifacts).To(HaveLen(3))
		Expect(report.Artifacts[2].Name).To(Equal("yuc-tabular.zip"))
		Expect(report.Artifacts[2].Err).To(ContainSubstring("context canceled"))
		Expect(report.Failed).To(Equal(1))
		Expect(dirNames(out)).To(Equal([]string{"ags-json.zip", "ags-tabular.zip"}))
	})

	It("fails the run on a power run_label collision before any archive is written", func() {
		dup := insertPowerRun(db, powerRunSeed{
			startedAt: "2026-07-04T00:00:00Z", status: "complete",
			endpoint: "u", parameters: "T2M", community: "AG", startYear: 1981, endYear: 2010,
			gridResolution: "0.5x0.625", temporalMode: "monthly", counters: make([]*int64, 4),
		})
		insertMonthlySupplement(db, "n20.75_w88.125", "1981-2010", 1, dup)

		// The gate's run-label-unique anchor names the collision ahead of
		// the provenance loader's own refusal, which stands behind it.
		report, err := run(publish.Options{Only: stateGroups})
		var gateErr *publish.GateError
		Expect(errors.As(err, &gateErr)).To(BeTrue(), err)
		Expect(err).To(MatchError(ContainSubstring("\n  run-label-unique — power_runs 1, " + fmt.Sprint(dup) +
			" share temporal_mode/period (monthly 1981-2010)")))
		_, loadErr := publish.LoadRuns(context.Background(), db)
		Expect(loadErr).To(MatchError(ContainSubstring(`share run_label "power-monthly-1981-2010"`)))
		Expect(report.States).To(BeEmpty())
		Expect(report.Artifacts).To(BeEmpty())
		Expect(rec.events).To(HaveLen(len(gateRuleIDs)), "the gate's lines only")
		_, statErr := os.Stat(out)
		Expect(os.IsNotExist(statErr)).To(BeTrue(), "the integrity signal must abort before the first archive")
	})

	It("refuses an out dir holding an entry this run does not produce, before writing anything", func() {
		report, err := run(publish.Options{States: []string{"yuc"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.TopLevelFiles).To(Equal(4))

		// A subset build into the same directory: the earlier state's
		// archives would end up unlisted by the new manifest and CHECKSUMS.
		before := dirNames(out)
		yucBytes, err := os.ReadFile(filepath.Join(out, "yuc-tabular.zip"))
		Expect(err).NotTo(HaveOccurred())
		rec.events = nil
		report, err = run(publish.Options{States: []string{"ags"}})
		Expect(err).To(MatchError("out dir " + out + " holds entries this run does not produce: " +
			"yuc-json.zip, yuc-tabular.zip (remove them or use a fresh --out)"))
		Expect(report.States).To(Equal([]string{"AGS"}))
		Expect(report.Artifacts).To(BeEmpty())
		Expect(report.TopLevelFiles).To(BeZero())
		Expect(rec.events).To(HaveLen(len(gateRuleIDs)), "the gate's lines only")
		Expect(dirNames(out)).To(Equal(before))
		got, err := os.ReadFile(filepath.Join(out, "yuc-tabular.zip"))
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(yucBytes))

		// Anything foreign — here a crashed run's temp residue and a
		// stray directory — is named in the error, sorted as listed.
		Expect(os.WriteFile(filepath.Join(out, ".yuc-tabular.zip.tmp-123"), []byte("x"), 0o600)).To(Succeed())
		Expect(os.Mkdir(filepath.Join(out, "notes"), 0o755)).To(Succeed())
		_, err = run(publish.Options{Only: stateGroups})
		Expect(err).To(MatchError("out dir " + out + " holds entries this run does not produce: " +
			".yuc-tabular.zip.tmp-123, notes (remove them or use a fresh --out)"))

		// The full set into the same directory is the converging re-run.
		Expect(os.Remove(filepath.Join(out, ".yuc-tabular.zip.tmp-123"))).To(Succeed())
		Expect(os.Remove(filepath.Join(out, "notes"))).To(Succeed())
		report, err = run(publish.Options{Only: stateGroups})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.TopLevelFiles).To(Equal(6))
		Expect(dirNames(out)).To(Equal([]string{
			"CHECKSUMS", "ags-json.zip", "ags-tabular.zip", "manifest.json", "yuc-json.zip", "yuc-tabular.zip",
		}))
	})

	It("removes a prior run's archives for a state that fails soft, so CHECKSUMS covers exactly the directory", func() {
		report, err := run(publish.Options{Only: stateGroups})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
		Expect(dirNames(out)).To(Equal([]string{
			"CHECKSUMS", "ags-json.zip", "ags-tabular.zip", "manifest.json", "yuc-json.zip", "yuc-tabular.zip",
		}))

		mustExec(db, `UPDATE daily_observations SET tmax = 9e999 WHERE station_id = ?`, ids["conv/1001"])
		report, err = run(publish.Options{Only: stateGroups})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(Equal(2))
		Expect(report.Artifacts[0].Name).To(Equal("ags-tabular.zip"))
		Expect(report.Artifacts[0].Err).To(ContainSubstring("non-finite value +Inf"))
		Expect(report.Artifacts[1].Name).To(Equal("ags-json.zip"))
		Expect(report.Artifacts[1].Err).To(ContainSubstring("non-finite value +Inf"))
		Expect(report.TopLevelFiles).To(Equal(4))

		// The stale AGS archives are gone: the directory is exactly what
		// CHECKSUMS lists plus CHECKSUMS itself.
		Expect(dirNames(out)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-json.zip", "yuc-tabular.zip"}))
		var names []string
		for _, s := range readChecksums(out) {
			names = append(names, s.Name)
		}
		Expect(names).To(Equal([]string{"manifest.json", "yuc-json.zip", "yuc-tabular.zip"}))
		Expect(readManifest(out).States).To(Equal([]publish.ManifestState{
			{Code: "AGS", Name: "Aguascalientes", Artifacts: []string{}},
			{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-tabular.zip", "yuc-json.zip"}},
		}))
	})

	It("counts the top-level files from the directory itself and refuses one CHECKSUMS does not list", func() {
		// A writer that slips a file in mid-run: the only way the
		// deposit-ready invariant can break once the pre-flight passed.
		var planted bool
		report, err := publish.Run(context.Background(), db, publish.Options{OutDir: out, Now: fixedClock, Only: stateGroups},
			func(ev publish.ProgressEvent) {
				if ev.Unit == "" && ev.Rule == "" && !planted {
					planted = true
					Expect(os.WriteFile(filepath.Join(out, "README.md"), []byte("later"), 0o600)).To(Succeed())
				}
			})
		Expect(err).To(MatchError("out dir " + out + " holds entries CHECKSUMS does not list: README.md"))
		Expect(report.Failed).To(BeZero())
		Expect(report.TopLevelFiles).To(BeZero())
	})
})
