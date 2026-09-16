package publish_test

// Specs at the seams the Run specs cover only transitively: the report
// and the artifact progress events against the archives on disk, the
// manifest's serialized key contract (the member names a reader of the
// provenance index parses — and their order at the top level, which
// byte-reproducible output makes the writer's to fix), and the code →
// official-name lookup across the whole 32-state catalog through the
// real DB → LoadStates → Run path rather than the three codes the other
// specs use.

import (
	"archive/zip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// jsonKeysAt returns the member names, in document order, of the object
// reached by walking path from the root of text — each step an object
// key or an array index — so a spec can pin the serialized contract
// without decoding through the very struct tags under test.
func jsonKeysAt(text string, path ...string) []string {
	GinkgoHelper()
	dec := json.NewDecoder(strings.NewReader(text))
	for _, step := range path {
		tok, err := dec.Token()
		Expect(err).NotTo(HaveOccurred())
		switch tok {
		case json.Delim('{'):
			found := false
			for dec.More() {
				key, err := dec.Token()
				Expect(err).NotTo(HaveOccurred())
				if key == step {
					found = true
					break
				}
				skipJSONValue(dec)
			}
			Expect(found).To(BeTrue(), "no member %q on the path %v", step, path)
		case json.Delim('['):
			n, err := strconv.Atoi(step)
			Expect(err).NotTo(HaveOccurred(), "array step %q", step)
			for range n {
				skipJSONValue(dec)
			}
		default:
			Fail("path step " + step + " reached a scalar")
		}
	}
	tok, err := dec.Token()
	Expect(err).NotTo(HaveOccurred())
	Expect(tok).To(Equal(json.Delim('{')), "path %v does not end at an object", path)
	var keys []string
	for dec.More() {
		key, err := dec.Token()
		Expect(err).NotTo(HaveOccurred())
		keys = append(keys, key.(string))
		skipJSONValue(dec)
	}
	return keys
}

func skipJSONValue(dec *json.Decoder) {
	GinkgoHelper()
	var v json.RawMessage
	Expect(dec.Decode(&v)).To(Succeed())
}

var _ = Describe("Run against the bytes on disk", func() {
	It("describes each archive exactly — size, sha256, entry count — in the report, the artifact event, and manifest.json", func() {
		db := openTempDB()
		seedTwoStates(db)
		seedRuns(db)
		out := filepath.Join(GinkgoT().TempDir(), "publish")
		rec := &recorder{}

		report, err := publish.Run(context.Background(), db, publish.Options{OutDir: out, Now: fixedClock, Only: stateGroups}, rec.record)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Artifacts).To(HaveLen(4))

		m := readManifest(out)
		Expect(m.Files).To(HaveLen(4))
		for _, a := range report.Artifacts {
			path := filepath.Join(out, a.Name)
			sum, n, err := archive.SHA256File(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(a.SHA256).To(Equal(sum), a.Name)
			Expect(a.Bytes).To(Equal(n), a.Name)
			zr, err := zip.OpenReader(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(zr.File).To(HaveLen(a.Entries), a.Name)
			Expect(zr.Close()).To(Succeed())

			j := slices.IndexFunc(rec.events, func(ev publish.ProgressEvent) bool {
				return ev.Artifact == a.Name && ev.Unit == ""
			})
			Expect(j).To(BeNumerically(">=", 0), a.Name)
			ev := rec.events[j]
			Expect(ev.Err).NotTo(HaveOccurred())
			Expect(ev.Bytes).To(Equal(n))
			Expect(ev.SHA256).To(Equal(sum))
			Expect(ev.Entries).To(Equal(a.Entries))

			Expect(m.Files).To(ContainElement(publish.ManifestFile{Name: a.Name, SHA256: sum, Bytes: n}))
		}
	})
})

var _ = Describe("manifest.json key contract", func() {
	var text string

	BeforeEach(func() {
		db := openTempDB()
		seedTwoStates(db)
		seedRuns(db)
		root := GinkgoT().TempDir()
		seedRawSnapshot(root)
		out := filepath.Join(GinkgoT().TempDir(), "publish")
		_, err := publish.Run(context.Background(), db, publish.Options{OutDir: out, SnapshotRoot: root, Now: fixedClock}, nil)
		Expect(err).NotTo(HaveOccurred())
		data, err := os.ReadFile(filepath.Join(out, "manifest.json"))
		Expect(err).NotTo(HaveOccurred())
		text = string(data)
	})

	It("writes the index's members in one fixed order (byte-reproducible)", func() {
		Expect(jsonKeysAt(text)).To(Equal([]string{
			"schema_version", "etl_git_sha", "generated_at", "snapshot_date",
			"dataset", "states", "national", "runs", "power_parameters", "counts", "files",
		}))
		Expect(jsonKeysAt(text, "power_parameters", "0")).To(Equal([]string{
			"id", "column", "power_unit", "stored_unit", "factor",
		}))
		Expect(jsonKeysAt(text, "dataset")).To(Equal([]string{
			"title", "version", "license", "creator", "creator_orcid", "suggested_citation",
		}))
		Expect(jsonKeysAt(text, "states", "0")).To(Equal([]string{"code", "name", "artifacts"}))
		Expect(jsonKeysAt(text, "national", "0")).To(Equal([]string{"group", "artifacts"}))
		Expect(jsonKeysAt(text, "runs")).To(Equal([]string{"ingest", "power"}))
		Expect(jsonKeysAt(text, "files", "0")).To(Equal([]string{"name", "sha256", "bytes"}))
		// One member per table, in DDL order.
		Expect(jsonKeysAt(text, "counts")).To(Equal([]string{
			"stations", "monthly_normals", "monthly_normals_extras", "daily_observations", "parsing_warnings",
			"ingest_runs", "power_runs", "nasa_power_grid_cells", "station_power_cell",
			"monthly_supplement", "daily_supplement",
		}))
	})

	It("names every provenance column of a run in the published export order, under the natural label, never the surrogate id", func() {
		// The order is pinned, not just the set: a JSON rendering of the
		// provenance files must carry the same pinned column order, so
		// the bytes are reproducible.
		Expect(jsonKeysAt(text, "runs", "ingest", "0")).To(Equal([]string{
			"snapshot_date", "started_at", "finished_at", "sink_kind", "etl_git_sha", "status",
			"stations_attempted", "stations_succeeded", "stations_failed",
			"daily_rows", "normals_rows", "extras_rows", "warnings_total",
		}))
		Expect(jsonKeysAt(text, "runs", "power", "0")).To(Equal([]string{
			"run_label", "started_at", "finished_at", "status", "endpoint_url", "parameters", "community",
			"period_start_year", "period_end_year", "grid_resolution", "unit_conversions", "temporal_mode",
			"period_start_date", "period_end_date",
			"cells_attempted", "cells_succeeded", "cells_failed", "supplement_rows", "etl_git_sha",
		}))
		// The surrogate id and the documented constant appear nowhere in
		// the provenance index (the registry block's "id" is the POWER
		// parameter id — a natural key).
		var top map[string]json.RawMessage
		Expect(json.Unmarshal([]byte(text), &top)).To(Succeed())
		Expect(string(top["runs"])).NotTo(ContainSubstring(`"id":`))
		Expect(text).NotTo(ContainSubstring("solar_conversion"))
	})
})

var _ = Describe("the 32-state catalog through LoadStates and Run", func() {
	var want []publish.State

	// catalogStates is what the full CONAGUA catalog must resolve to:
	// the uppercase code as ingest stores it, the lowercase slug,
	// the official name — in bytewise code order, as LoadStates lists.
	catalogStates := func() []publish.State {
		var states []publish.State
		for _, c := range conagua.AllStates {
			states = append(states, publish.State{
				Code: strings.ToUpper(string(c)), Slug: string(c), Name: c.DisplayName(),
			})
		}
		slices.SortFunc(states, func(a, b publish.State) int { return strings.Compare(a.Code, b.Code) })
		return states
	}

	BeforeEach(func() {
		want = catalogStates()
		Expect(want).To(HaveLen(32))
	})

	It("resolves every code to a slug that is a safe path component and a non-empty official name", func() {
		db := openTempDB()
		for _, c := range conagua.AllStates {
			upsertStation(db, ingest.StationUpsert{
				Source: ingest.SourceConaguaConventional, ExternalID: "s-" + string(c), Name: "S",
				State: strings.ToUpper(string(c)),
			})
		}
		states, err := publish.LoadStates(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(states).To(Equal(want))
		for _, s := range states {
			Expect(s.Slug).To(MatchRegexp(`^[a-z]+$`), s.Code)
			Expect(s.Name).NotTo(BeEmpty(), s.Code)
		}
	})

	It("publishes the whole artifact set of a full run — two archives per state, the four national archives, the raw snapshot, manifest.json, CHECKSUMS — under the cap, and ships the code → name lookup", func() {
		db := openTempDB()
		for _, c := range conagua.AllStates {
			upsertStation(db, ingest.StationUpsert{
				Source: ingest.SourceConaguaConventional, ExternalID: "s-" + string(c), Name: "S",
				State: strings.ToUpper(string(c)),
			})
		}
		seedRuns(db)
		root := GinkgoT().TempDir()
		seedRawSnapshot(root)
		out := filepath.Join(GinkgoT().TempDir(), "publish")

		report, err := publish.Run(context.Background(), db, publish.Options{OutDir: out, SnapshotRoot: root, Now: fixedClock}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
		// 64 per-state archives, then the five deposit-wide ones in build
		// order, then the docs group's eight files in write order; with
		// manifest.json and CHECKSUMS, the 79 files of the full artifact
		// set, under the cap.
		Expect(report.Artifacts).To(HaveLen(64 + 5 + 8))
		Expect(artifactNames(report)[64:]).To(Equal(append([]string{
			"national-csv.zip", "national-parquet.zip", "national-json.zip", "national-sqlite.zip", "conagua-raw-2026-06-08.zip",
		}, docsNames...)))
		Expect(report.TopLevelFiles).To(Equal(79))
		Expect(report.TopLevelFiles).To(Equal(2*len(conagua.AllStates) + 4 + 1 + len(publish.DocsFiles())))
		Expect(report.TopLevelFiles).To(BeNumerically("<=", publish.MaxTopLevelFiles), "Zenodo's per-record file cap")

		// The deposit's file list and CHECKSUMS are bytewise-sorted, so
		// manifest.json and the docs files land among the archives, not
		// after them.
		wantSums := []string{"manifest.json"}
		var wantStates []publish.ManifestState
		for _, s := range want {
			wantSums = append(wantSums, s.Slug+"-tabular.zip", s.Slug+"-json.zip")
			wantStates = append(wantStates, publish.ManifestState{
				Code: s.Code, Name: s.Name, Artifacts: []string{s.Slug + "-tabular.zip", s.Slug + "-json.zip"},
			})
		}
		wantSums = append(wantSums, artifactNames(report)[64:]...)
		slices.Sort(wantSums)
		wantDir := append([]string{"CHECKSUMS"}, wantSums...)
		slices.Sort(wantDir)
		Expect(dirNames(out)).To(Equal(wantDir))
		m := readManifest(out)
		Expect(m.States).To(Equal(wantStates))
		Expect(m.National).To(Equal([]publish.ManifestNational{
			{Group: "national-csv", Artifacts: []string{"national-csv.zip"}},
			{Group: "national-parquet", Artifacts: []string{"national-parquet.zip"}},
			{Group: "national-json", Artifacts: []string{"national-json.zip"}},
			{Group: "national-sqlite", Artifacts: []string{"national-sqlite.zip"}},
			{Group: "raw", Artifacts: []string{"conagua-raw-2026-06-08.zip"}},
		}))
		var sumNames []string
		for _, s := range readChecksums(out) {
			sumNames = append(sumNames, s.Name)
		}
		Expect(sumNames).To(Equal(wantSums))
		// The national CSV archive consumed every state's tabular
		// archive: one unit each, in state order.
		Expect(artifactByName(report, "national-csv.zip").Entries).To(Equal(32*2 + 7 + 2))
	})
})
