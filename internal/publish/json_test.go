package publish_test

// Specs for the JSON archive: JSONEntries' entry list (the daily.json +
// profile.json pair of every station of the one station list, the
// provenance pair, Path-sorted), the station as the unit — fired once,
// after both files of its pair — and each entry held to its builder's
// own rendering; then the seam with Run: the tabular and JSON archives
// built per state in that order, both listed in manifest.json and
// CHECKSUMS, the JSON archive's bytes held to JSONEntries, its unit
// events, the --only selection and its refusal, the out-dir discipline
// across groups, each archive failing soft on its own, and byte
// identity whether the archive is built alone or beside the tabular one.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// runMeta is the ProfileMeta Run builds for a seedRuns DB: the schema
// version, the build's SHA, the seeded snapshot, the runs LoadRuns
// returns, and the dataset identity.
func runMeta(db *sql.DB, gitSHA string) publish.ProfileMeta {
	GinkgoHelper()
	runs, err := publish.LoadRuns(context.Background(), db)
	Expect(err).NotTo(HaveOccurred())
	return publish.ProfileMeta{
		SchemaVersion: schema.Version, ETLGitSHA: gitSHA, SnapshotDate: "2026-06-08",
		Runs: runs, Dataset: publish.DatasetMetadata(seededSnapshot, ""),
	}
}

func entryPaths(entries []archive.Entry) []string {
	paths := make([]string, len(entries))
	for i, e := range entries {
		paths[i] = e.Path
	}
	return paths
}

func artifactNames(report *publish.Report) []string {
	names := make([]string, len(report.Artifacts))
	for i, a := range report.Artifacts {
		names[i] = a.Name
	}
	return names
}

func artifactByName(report *publish.Report, name string) publish.ArtifactResult {
	GinkgoHelper()
	i := slices.IndexFunc(report.Artifacts, func(a publish.ArtifactResult) bool { return a.Name == name })
	Expect(i).To(BeNumerically(">=", 0), "no artifact %s", name)
	return report.Artifacts[i]
}

// eventsOf keeps the recorder's events of one artifact, the artifact
// line's size, digest, and entry count zeroed so sequences compare
// exactly.
func eventsOf(rec *recorder, artifact string) []publish.ProgressEvent {
	var out []publish.ProgressEvent
	for _, ev := range rec.events {
		if ev.Artifact != artifact {
			continue
		}
		if ev.Unit == "" {
			ev.Bytes, ev.SHA256, ev.Entries = 0, "", 0
		}
		out = append(out, ev)
	}
	return out
}

var yucJSONPaths = []string{
	"combined/yuc/31001/daily.json",
	"combined/yuc/31001/profile.json",
	"combined/yuc/31002/daily.json",
	"combined/yuc/31002/profile.json",
	"combined/yuc/31003/daily.json",
	"combined/yuc/31003/profile.json",
	"combined/yuc/3101/daily.json",
	"combined/yuc/3101/profile.json",
	"provenance/ingest_runs.json",
	"provenance/power_runs.json",
}

var yucJSONUnits = []string{
	"combined/yuc/31001/profile.json",
	"combined/yuc/31002/profile.json",
	"combined/yuc/31003/profile.json",
	"combined/yuc/3101/profile.json",
}

var _ = Describe("JSONEntries", func() {
	var (
		db      *sql.DB
		runs    publish.Runs
		meta    publish.ProfileMeta
		entries []archive.Entry
		units   int
	)

	BeforeEach(func() {
		db = openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		var err error
		runs, err = publish.LoadRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		meta = runMeta(db, "abc123")
		entries, units, err = publish.JSONEntries(context.Background(), db, yucatan, runs, meta, nil)
		Expect(err).NotTo(HaveOccurred())
	})

	It("lists, Path-sorted, the daily.json + profile.json pair of every station of the state and the provenance pair, the unit count the station count", func() {
		Expect(entryPaths(entries)).To(Equal(yucJSONPaths))
		Expect(units).To(Equal(4))
	})

	It("fires the unit once per station, after both files of its pair are written, in write order", func() {
		var sequence []string
		withUnits, _, err := publish.JSONEntries(context.Background(), db, yucatan, runs, meta,
			func(path string) { sequence = append(sequence, "unit "+path) })
		Expect(err).NotTo(HaveOccurred())
		for _, e := range withUnits {
			render(e)
			sequence = append(sequence, "wrote "+e.Path)
		}
		Expect(sequence).To(Equal([]string{
			"wrote combined/yuc/31001/daily.json",
			"unit combined/yuc/31001/profile.json", "wrote combined/yuc/31001/profile.json",
			"wrote combined/yuc/31002/daily.json",
			"unit combined/yuc/31002/profile.json", "wrote combined/yuc/31002/profile.json",
			"wrote combined/yuc/31003/daily.json",
			"unit combined/yuc/31003/profile.json", "wrote combined/yuc/31003/profile.json",
			"wrote combined/yuc/3101/daily.json",
			"unit combined/yuc/3101/profile.json", "wrote combined/yuc/3101/profile.json",
			"wrote provenance/ingest_runs.json",
			"wrote provenance/power_runs.json",
		}))
	})

	It("does not fire the unit for a profile whose write fails", func() {
		mustExec(db, `UPDATE stations SET lat = 9e999 WHERE external_id = '31002' AND source = 'conagua_conventional'`)
		var fired []string
		withUnits, _, err := publish.JSONEntries(context.Background(), db, yucatan, runs, meta,
			func(path string) { fired = append(fired, path) })
		Expect(err).NotTo(HaveOccurred())
		Expect(entryByPath(withUnits, "combined/yuc/31001/profile.json").Write(discardWriter{})).To(Succeed())
		err = entryByPath(withUnits, "combined/yuc/31002/profile.json").Write(discardWriter{})
		Expect(err).To(MatchError(ContainSubstring("non-finite value +Inf")))
		Expect(fired).To(Equal([]string{"combined/yuc/31001/profile.json"}))
	})

	It("gives two stations on one cell identical power_monthly and reanalysis blocks, the cell read once per archive", func() {
		// 31001 and 3101 share cellShared (seedPower); the memo serves the
		// second from the first's read, so the two blocks are one value.
		first := decodeProfile(render(entryByPath(entries, "combined/yuc/31001/profile.json")))
		second := decodeProfile(render(entryByPath(entries, "combined/yuc/3101/profile.json")))
		Expect(obj(first, "power_cell")["cell_id"]).To(Equal(cellShared))
		Expect(obj(second, "power_cell")["cell_id"]).To(Equal(cellShared))
		Expect(obj(second, "power_monthly")).To(Equal(obj(first, "power_monthly")))
		Expect(obj(second, "daily_summary", "coverage")["reanalysis"]).To(Equal(obj(first, "daily_summary", "coverage")["reanalysis"]))
		Expect(obj(first, "daily_summary", "coverage")["reanalysis"]).To(Equal(map[string]any{
			"first_date": "1999-12-31", "last_date": "2020-01-02", "days": num("3"),
		}))
		// The distance is the station's, not the cell's.
		Expect(obj(first, "power_cell")["distance_km"]).To(Equal(num("27.252")))
		Expect(obj(second, "power_cell")["distance_km"]).To(Equal(num("0.000")))
	})

	It("renders each entry as its builder does: profiles as ProfileEntries, series as DailyJSONEntries, provenance as ProvenanceJSONEntries", func() {
		profiles, err := publish.ProfileEntries(context.Background(), db, yucatan, meta)
		Expect(err).NotTo(HaveOccurred())
		series, err := publish.DailyJSONEntries(context.Background(), db, yucatan)
		Expect(err).NotTo(HaveOccurred())
		want := slices.Concat(profiles, series, publish.ProvenanceJSONEntries(runs))
		Expect(want).To(HaveLen(len(entries)))
		for _, w := range want {
			Expect(render(entryByPath(entries, w.Path))).To(Equal(render(w)), w.Path)
		}
	})

	It("names the same station set as the tabular per-station folders, station for station", func() {
		conagua, err := publish.ConaguaEntries(context.Background(), db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())
		var csvStations, jsonStations []string
		for _, e := range conagua {
			if rest, ok := strings.CutPrefix(e.Path, "conagua/daily_observations/yuc/daily-"); ok {
				csvStations = append(csvStations, strings.TrimSuffix(rest, ".csv"))
			}
		}
		for _, e := range entries {
			if rest, ok := strings.CutPrefix(e.Path, "combined/yuc/"); ok && strings.HasSuffix(rest, "/profile.json") {
				jsonStations = append(jsonStations, strings.TrimSuffix(rest, "/profile.json"))
			}
		}
		Expect(jsonStations).To(Equal(csvStations))
		Expect(jsonStations).To(HaveLen(units))
	})

	It("lists only the provenance pair for the other state's stations never in this state", func() {
		ags, n, err := publish.JSONEntries(context.Background(), db, aguascalientes, runs, meta, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(entryPaths(ags)).To(Equal([]string{
			"combined/ags/1001/daily.json",
			"combined/ags/1001/profile.json",
			"provenance/ingest_runs.json",
			"provenance/power_runs.json",
		}))
		Expect(n).To(Equal(1))
	})
})

// discardWriter accepts every write, so a failing entry's error is the
// builder's, never the sink's.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

var _ = Describe("Run with the JSON group", func() {
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
		opts.OutDir = out
		if opts.Now == nil {
			opts.Now = fixedClock
		}
		return publish.Run(context.Background(), db, opts, rec.record)
	}

	It("builds, per state, the tabular archive then the JSON one, lists both in manifest.json and CHECKSUMS, and writes each JSON archive exactly as JSONEntries assembles it", func() {
		report, err := run(publish.Options{ETLGitSHA: "abc123", Only: stateGroups})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
		Expect(report.Groups).To(Equal([]string{"tabular", "json"}))
		Expect(artifactNames(report)).To(Equal([]string{
			"ags-tabular.zip", "ags-json.zip", "yuc-tabular.zip", "yuc-json.zip", "zac-tabular.zip", "zac-json.zip",
		}))
		Expect(report.TopLevelFiles).To(Equal(8))
		Expect(dirNames(out)).To(Equal([]string{
			"CHECKSUMS", "ags-json.zip", "ags-tabular.zip", "manifest.json",
			"yuc-json.zip", "yuc-tabular.zip", "zac-json.zip", "zac-tabular.zip",
		}))

		m := readManifest(out)
		Expect(m.States).To(Equal([]publish.ManifestState{
			{Code: "AGS", Name: "Aguascalientes", Artifacts: []string{"ags-tabular.zip", "ags-json.zip"}},
			{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-tabular.zip", "yuc-json.zip"}},
			{Code: "ZAC", Name: "Zacatecas", Artifacts: []string{"zac-tabular.zip", "zac-json.zip"}},
		}))
		var fileNames, sumNames []string
		for _, f := range m.Files {
			fileNames = append(fileNames, f.Name)
		}
		for _, s := range readChecksums(out) {
			sumNames = append(sumNames, s.Name)
		}
		Expect(fileNames).To(Equal([]string{
			"ags-json.zip", "ags-tabular.zip", "yuc-json.zip", "yuc-tabular.zip", "zac-json.zip", "zac-tabular.zip",
		}))
		Expect(sumNames).To(Equal([]string{
			"ags-json.zip", "ags-tabular.zip", "manifest.json", "yuc-json.zip", "yuc-tabular.zip", "zac-json.zip", "zac-tabular.zip",
		}))

		states, err := publish.LoadStates(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		runs, err := publish.LoadRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		provenance := map[string][][]byte{}
		for _, st := range states {
			want, units, err := publish.JSONEntries(context.Background(), db, st, runs, runMeta(db, "abc123"), nil)
			Expect(err).NotTo(HaveOccurred())
			a := artifactByName(report, st.Slug+"-json.zip")
			Expect(a.Err).To(BeEmpty())
			Expect(a.Entries).To(Equal(len(want)))
			// Two files per station and the two provenance files.
			Expect(a.Entries).To(Equal(2*units + 2))
			names, contents := zipContents(filepath.Join(out, a.Name))
			Expect(names).To(Equal(entryPaths(want)), st.Code)
			for _, e := range want {
				Expect(contents[e.Path]).To(Equal(render(e)), "%s: %s", st.Code, e.Path)
			}
			for _, p := range []string{"provenance/ingest_runs.json", "provenance/power_runs.json"} {
				provenance[p] = append(provenance[p], contents[p])
			}
		}
		// The provenance pair is the same bytes in every JSON archive.
		for p, renderings := range provenance {
			Expect(renderings).To(HaveLen(3), p)
			Expect(renderings[1]).To(Equal(renderings[0]), p)
			Expect(renderings[2]).To(Equal(renderings[0]), p)
		}
	})

	It("stamps every profile with the build's meta: the SHA, the snapshot, the runs by label", func() {
		_, err := run(publish.Options{ETLGitSHA: "abc123", Only: stateGroups})
		Expect(err).NotTo(HaveOccurred())
		_, contents := zipContents(filepath.Join(out, "yuc-json.zip"))
		got := decodeProfile(contents["combined/yuc/31001/profile.json"])
		Expect(obj(got, "meta")).To(Equal(map[string]any{
			"schema_version": num(fmt.Sprint(schema.Version)), "etl_git_sha": "abc123", "snapshot_date": "2026-06-08",
			"runs": map[string]any{
				"ingest": []any{"2026-05-01", "2026-06-08"},
				"power":  []any{"power-daily-1981-2026", "power-monthly-1981-2010"},
			},
			"license":            "CC-BY-4.0",
			"suggested_citation": publish.DatasetMetadata(seededSnapshot, "").SuggestedCitation,
		}))
		Expect(string(contents["combined/yuc/31001/profile.json"])).NotTo(ContainSubstring("generated"))
	})

	It("reports the JSON archive's units — one per station, labelled by its profile.json, numbered across the archive — after the tabular archive's events", func() {
		_, err := run(publish.Options{States: []string{"yuc"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(eventsOf(rec, "yuc-json.zip")).To(Equal(append(
			unitEvents("yuc-json.zip", yucJSONUnits...), publish.ProgressEvent{Artifact: "yuc-json.zip"})))
		tabularDone := slices.IndexFunc(rec.events, func(ev publish.ProgressEvent) bool {
			return ev.Artifact == "yuc-tabular.zip" && ev.Unit == ""
		})
		firstJSON := slices.IndexFunc(rec.events, func(ev publish.ProgressEvent) bool {
			return ev.Artifact == "yuc-json.zip"
		})
		Expect(tabularDone).To(BeNumerically(">=", 0))
		Expect(firstJSON).To(Equal(tabularDone + 1))
		Expect(rec.events[len(rec.events)-1].Artifact).To(Equal("yuc-json.zip"))
	})

	It("builds only the selected groups, in build order however they were named", func() {
		report, err := run(publish.Options{Only: []string{"JSON"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Groups).To(Equal([]string{"json"}))
		Expect(artifactNames(report)).To(Equal([]string{"ags-json.zip", "yuc-json.zip", "zac-json.zip"}))
		Expect(report.TopLevelFiles).To(Equal(5))
		Expect(dirNames(out)).To(Equal([]string{"CHECKSUMS", "ags-json.zip", "manifest.json", "yuc-json.zip", "zac-json.zip"}))
		Expect(readManifest(out).States[1]).To(Equal(publish.ManifestState{
			Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-json.zip"},
		}))
		for _, ev := range rec.events[len(gateRuleIDs):] {
			Expect(ev.Artifact).To(HaveSuffix("-json.zip"))
		}

		out = filepath.Join(GinkgoT().TempDir(), "tabular")
		report, err = run(publish.Options{Only: []string{"tabular"}, States: []string{"yuc"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Groups).To(Equal([]string{"tabular"}))
		Expect(artifactNames(report)).To(Equal([]string{"yuc-tabular.zip"}))
		Expect(dirNames(out)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-tabular.zip"}))

		out = filepath.Join(GinkgoT().TempDir(), "both")
		report, err = run(publish.Options{Only: []string{"json", "Tabular", "json"}, States: []string{"yuc"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Groups).To(Equal([]string{"tabular", "json"}))
		Expect(artifactNames(report)).To(Equal([]string{"yuc-tabular.zip", "yuc-json.zip"}))
	})

	It("refuses an unknown group before touching the DB or the directory", func() {
		report, err := run(publish.Options{Only: []string{"tabular", "xyz"}})
		Expect(err).To(MatchError(`unknown artifact group "xyz" (valid: ` + validGroups + `)`))
		Expect(report).NotTo(BeNil())
		Expect(report.Groups).To(BeEmpty())
		Expect(report.SnapshotDate).To(BeEmpty())
		Expect(report.Artifacts).To(BeEmpty())
		Expect(rec.events).To(BeEmpty())
		_, statErr := os.Stat(out)
		Expect(os.IsNotExist(statErr)).To(BeTrue())
	})

	It("writes the same JSON archive bytes whether built alone or beside the tabular one, and identical bytes on a later build (byte-reproducible)", func() {
		first, err := run(publish.Options{ETLGitSHA: "abc123", States: []string{"yuc"}})
		Expect(err).NotTo(HaveOccurred())
		firstZip, err := os.ReadFile(filepath.Join(out, "yuc-json.zip"))
		Expect(err).NotTo(HaveOccurred())

		out = filepath.Join(GinkgoT().TempDir(), "again")
		later := fixedNow.Add(90 * time.Minute)
		second, err := run(publish.Options{
			ETLGitSHA: "abc123", States: []string{"yuc"}, Only: []string{"json"},
			Now: func() time.Time { return later },
		})
		Expect(err).NotTo(HaveOccurred())
		secondZip, err := os.ReadFile(filepath.Join(out, "yuc-json.zip"))
		Expect(err).NotTo(HaveOccurred())

		Expect(secondZip).To(Equal(firstZip))
		Expect(artifactByName(second, "yuc-json.zip").SHA256).To(Equal(artifactByName(first, "yuc-json.zip").SHA256))
		Expect(artifactByName(second, "yuc-json.zip").Bytes).To(Equal(artifactByName(first, "yuc-json.zip").Bytes))
	})

	It("refuses an out dir holding the other group's archive from a subset build, and converges on the full set", func() {
		_, err := run(publish.Options{States: []string{"yuc"}, Only: []string{"tabular"}})
		Expect(err).NotTo(HaveOccurred())
		before := dirNames(out)

		report, err := run(publish.Options{States: []string{"yuc"}, Only: []string{"json"}})
		Expect(err).To(MatchError("out dir " + out + " holds entries this run does not produce: " +
			"yuc-tabular.zip (remove them or use a fresh --out)"))
		Expect(report.Artifacts).To(BeEmpty())
		Expect(dirNames(out)).To(Equal(before))

		report, err = run(publish.Options{States: []string{"yuc"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.TopLevelFiles).To(Equal(4))
		Expect(dirNames(out)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-json.zip", "yuc-tabular.zip"}))
	})

	It("fails the tabular archive soft on its own: a referenced cell_id that cannot name a file ships the state's JSON archive, which carries the id as a value", func() {
		const bad = `21.0N/89.0000W`
		insertCell(db, bad, 21.0, -89.0)
		insertStationCell(db, ids["conv/31002"], bad, 1.0)

		report, err := run(publish.Options{States: []string{"yuc"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(Equal(1))
		Expect(report.TopLevelFiles).To(Equal(3))
		Expect(artifactNames(report)).To(Equal([]string{"yuc-tabular.zip", "yuc-json.zip"}))
		Expect(report.Artifacts[0].Err).To(Equal(`list cells of YUC: cell_id "21.0N/89.0000W" is not a POWER cell key`))
		Expect(report.Artifacts[1].Err).To(BeEmpty())
		Expect(dirNames(out)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-json.zip"}))
		Expect(readManifest(out).States).To(Equal([]publish.ManifestState{
			{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-json.zip"}},
		}))
		_, contents := zipContents(filepath.Join(out, "yuc-json.zip"))
		Expect(obj(decodeProfile(contents["combined/yuc/31002/profile.json"]), "power_cell")["cell_id"]).To(Equal(bad))
		Expect(eventsOf(rec, "yuc-json.zip")).To(Equal(append(
			unitEvents("yuc-json.zip", yucJSONUnits...), publish.ProgressEvent{Artifact: "yuc-json.zip"})))
	})

	It("fails the JSON archive soft on its own: a daily date the profile refuses ships the state's tabular archive, which carries the text verbatim, the failed archive's units stopping at the casualty", func() {
		insertDaily(db, ids["conv/31002"], "2020/01/01", 1.0, 1.0, 0.0, 1.0)

		report, err := run(publish.Options{States: []string{"yuc"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(Equal(1))
		Expect(report.TopLevelFiles).To(Equal(3))
		Expect(artifactNames(report)).To(Equal([]string{"yuc-tabular.zip", "yuc-json.zip"}))
		Expect(report.Artifacts[0].Err).To(BeEmpty())
		// The archive is named once, by the zip write; the entry path
		// carries the station, the block the failing check.
		Expect(report.Artifacts[1].Err).To(HavePrefix("write zip " + filepath.Join(out, "yuc-json.zip") +
			`: entry "combined/yuc/31002/profile.json": daily_summary: daily_observations: date "2020/01/01" is not YYYY-MM-DD`))
		Expect(report.Artifacts[1].Bytes).To(BeZero())
		Expect(dirNames(out)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-tabular.zip"}))
		Expect(readManifest(out).States).To(Equal([]publish.ManifestState{
			{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-tabular.zip"}},
		}))
		_, contents := zipContents(filepath.Join(out, "yuc-tabular.zip"))
		Expect(string(contents["conagua/daily_observations/yuc/daily-31002.csv"])).To(ContainSubstring("\n31002,2020/01/01,1.00,1.00,0.00,1.00\n"))
		got := eventsOf(rec, "yuc-json.zip")
		Expect(got).To(HaveLen(2))
		Expect(got[0]).To(Equal(publish.ProgressEvent{
			Artifact: "yuc-json.zip", Unit: "combined/yuc/31001/profile.json", Index: 1, Total: 4,
		}))
		Expect(got[1].Unit).To(BeEmpty())
		Expect(got[1].Err).To(MatchError(report.Artifacts[1].Err))
	})

	It("is refused at the gate on a cell reference with no grid row, before either archive is attempted", func() {
		// The state database and the station's profile would each refuse
		// the reference at their own entry; the gate's cell-refs anchor
		// names it first, and nothing is written.
		const orphan = "20.0N_90.0000W"
		insertStationCell(db, ids["conv/31002"], orphan, 2.0)

		report, err := run(publish.Options{States: []string{"yuc"}})
		Expect(err).To(MatchError("gate refused: 1 error finding\n" +
			"  cell-refs — station_power_cell: 1 row with a cell_id naming no nasa_power_grid_cells row (1 distinct: " + orphan + ")"))
		Expect(report.Artifacts).To(BeEmpty())
		Expect(report.TopLevelFiles).To(BeZero())
		_, statErr := os.Stat(out)
		Expect(os.IsNotExist(statErr)).To(BeTrue())
	})

	It("removes a prior run's JSON archive for a state whose JSON archive fails soft", func() {
		report, err := run(publish.Options{States: []string{"yuc"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
		Expect(dirNames(out)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-json.zip", "yuc-tabular.zip"}))

		insertDaily(db, ids["conv/31002"], "2020/01/01", 1.0, 1.0, 0.0, 1.0)
		report, err = run(publish.Options{States: []string{"yuc"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(Equal(1))
		Expect(dirNames(out)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-tabular.zip"}))
		var names []string
		for _, s := range readChecksums(out) {
			names = append(names, s.Name)
		}
		Expect(names).To(Equal([]string{"manifest.json", "yuc-tabular.zip"}))
	})
})
