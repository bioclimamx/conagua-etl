package publish_test

// Specs for the copy-built national archives: national-csv.zip built
// from real state tabular archives — every per-station and per-cell
// file copied raw with its record intact, a cell two states share once
// from the first state in state order, the seven whole-scope tables the
// union of the states' rows in primary-key order, provenance/ once, the
// unit one per state archive consumed, the bytes identical across two
// builds; national-json.zip likewise from the state JSON archives; the
// refusals — a missing state, a foreign or misordered archive, a shared
// file whose copies disagree, another state's shard, an unknown entry,
// an archive with nothing to copy, cancellation; and the artifact-set
// arithmetic against Zenodo's cap.

import (
	"archive/zip"
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// zipFiles opens the zip at path, scheduling its close, and indexes its
// entries by name.
func zipFiles(path string) map[string]*zip.File {
	GinkgoHelper()
	zr, err := zip.OpenReader(path)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = zr.Close() })
	files := map[string]*zip.File{}
	for _, f := range zr.File {
		files[f.Name] = f
	}
	return files
}

// rawRecord reads f as stored — the compressed bytes.
func rawRecord(f *zip.File) []byte {
	GinkgoHelper()
	r, err := f.OpenRaw()
	Expect(err).NotTo(HaveOccurred())
	data, err := io.ReadAll(r)
	Expect(err).NotTo(HaveOccurred())
	return data
}

// writeFixtureZip writes a hand-built archive of the given files under
// the seeded snapshot's timestamp.
func writeFixtureZip(path string, files map[string]string) {
	GinkgoHelper()
	var entries []archive.Entry
	for name, content := range files {
		entries = append(entries, archive.Entry{Path: name, Write: func(w io.Writer) error {
			_, err := io.WriteString(w, content)
			return err
		}})
	}
	slices.SortFunc(entries, func(a, b archive.Entry) int { return strings.Compare(a.Path, b.Path) })
	_, err := archive.WriteZip(context.Background(), path, entries, archive.ZipOptions{Modified: seededSnapshot})
	Expect(err).NotTo(HaveOccurred())
}

// isPerUnit reports whether a tabular archive entry is a per-station or
// per-cell file — what the national CSV archive copies.
func isPerUnit(name string) bool {
	for _, dir := range []string{"conagua/daily_observations/", "combined/combined_daily/", "nasa_power/daily/"} {
		if strings.HasPrefix(name, dir) {
			return true
		}
	}
	return false
}

// firstKeyOf is a CSV row's first key cell.
func firstKeyOf(rows [][]string) []string {
	keys := make([]string, len(rows))
	for i, r := range rows {
		keys[i] = r[0]
	}
	return keys
}

var _ = Describe("the copy-built national archives", func() {
	var (
		ctx    context.Context
		db     *sql.DB
		runs   publish.Runs
		states []publish.State
		out    string
	)

	// stateArchives lists the run's archives of one group in state order.
	stateArchives := func(group string) []publish.StateArchive {
		archives := make([]publish.StateArchive, len(states))
		for i, st := range states {
			archives[i] = publish.StateArchive{State: st, Path: filepath.Join(out, st.Slug+"-"+group+".zip")}
		}
		return archives
	}

	// A DB whose three states share one cell between Yucatán and
	// Aguascalientes, its archives built by Run into out.
	BeforeEach(func() {
		ctx = context.Background()
		db = openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		ids["conv/1002"] = upsertStation(db, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "1002", Name: "Calvillo", State: "AGS",
		})
		insertStationCell(db, ids["conv/1002"], cellShared, 40.0)
		var err error
		runs, err = publish.LoadRuns(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		states, err = publish.LoadStates(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		Expect(codesOf(states)).To(Equal([]string{"AGS", "YUC", "ZAC"}))
		out = filepath.Join(GinkgoT().TempDir(), "publish")
		report, err := publish.Run(ctx, db, publish.Options{OutDir: out, Now: fixedClock, Only: []string{"tabular", "json"}}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
	})

	Describe("NationalCSVEntries", func() {
		var (
			path  string
			units []string
			n     *publish.NationalEntries
		)

		BeforeEach(func() {
			units = nil
			var err error
			n, err = publish.NationalCSVEntries(ctx, db, stateArchives("tabular"), runs,
				func(label string) { units = append(units, label) })
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(n.Close()).To(Succeed()) })
			path = filepath.Join(GinkgoT().TempDir(), "national-csv.zip")
			_, err = archive.WriteZipMixed(ctx, path, n.Entries, archive.ZipOptions{Modified: seededSnapshot})
			Expect(err).NotTo(HaveOccurred())
		})

		It("copies every state's per-station and per-cell files raw — the record of the first state carrying each — beside the seven national tables and provenance/, Path-sorted, nothing else", func() {
			national := zipFiles(path)
			owner := map[string]string{}
			var want []string
			for _, st := range states {
				for name, f := range zipFiles(filepath.Join(out, st.Slug+"-tabular.zip")) {
					if !isPerUnit(name) {
						continue
					}
					if _, seen := owner[name]; seen {
						continue
					}
					owner[name] = st.Slug
					want = append(want, name)
					Expect(national).To(HaveKey(name))
					got := national[name]
					Expect(got.CRC32).To(Equal(f.CRC32), name)
					Expect(got.CompressedSize64).To(Equal(f.CompressedSize64), name)
					Expect(got.UncompressedSize64).To(Equal(f.UncompressedSize64), name)
					Expect(got.Modified.Unix()).To(Equal(seededSnapshot.Unix()), name)
					Expect(rawRecord(got)).To(Equal(rawRecord(f)), name)
				}
			}
			// The shared cell's file is in both carrying archives and once
			// nationally, from Aguascalientes — the first in state order.
			shared := "nasa_power/daily/daily-" + cellShared + ".csv"
			Expect(zipFiles(filepath.Join(out, "yuc-tabular.zip"))).To(HaveKey(shared))
			Expect(zipFiles(filepath.Join(out, "ags-tabular.zip"))).To(HaveKey(shared))
			Expect(owner[shared]).To(Equal("ags"))

			for _, e := range publish.NationalTableEntries(ctx, db) {
				want = append(want, e.Path)
			}
			for _, e := range publish.ProvenanceEntries(runs) {
				want = append(want, e.Path)
			}
			slices.Sort(want)
			names, _ := zipContents(path)
			Expect(names).To(Equal(want))
			Expect(n.Entries).To(HaveLen(len(want)))
			Expect(names).NotTo(ContainElement(HaveSuffix(".parquet")))
			Expect(names).NotTo(ContainElement(HaveSuffix(".db")))
		})

		It("regenerates each whole-scope table as the union of the states' rows, each row once, in primary-key order, and provenance/ as the run's", func() {
			_, national := zipContents(path)
			for _, e := range publish.NationalTableEntries(ctx, db) {
				name := e.Path
				got := records(national[name])
				var header []string
				union := map[string][]string{}
				for _, st := range states {
					_, contents := zipContents(filepath.Join(out, st.Slug+"-tabular.zip"))
					rows := records(contents[name])
					header = rows[0]
					for _, r := range rows[1:] {
						union[strings.Join(r, "\x00")] = r
					}
				}
				Expect(got[0]).To(Equal(header), name)
				body := got[1:]
				Expect(body).To(HaveLen(len(union)), name)
				for _, r := range body {
					Expect(union).To(HaveKey(strings.Join(r, "\x00")), name)
				}
				Expect(slices.IsSorted(firstKeyOf(body))).To(BeTrue(), name)
				Expect(national[name]).To(Equal(render(e)), name)
			}
			// Every state's stations are in the national stations file.
			stations := records(national["conagua/stations.csv"])
			Expect(firstKeyOf(stations[1:])).To(Equal([]string{"1001", "1002", "31001", "31002", "31003", "3101", "32001"}))
			for _, e := range publish.ProvenanceEntries(runs) {
				Expect(national[e.Path]).To(Equal(render(e)), e.Path)
			}
		})

		It("counts one unit per state archive, fired after the last entry copied from it, labelled by its file name", func() {
			Expect(n.Units).To(Equal(len(states)))
			ownerOf := map[string]string{}
			for _, st := range states {
				for name := range zipFiles(filepath.Join(out, st.Slug+"-tabular.zip")) {
					if isPerUnit(name) {
						if _, seen := ownerOf[name]; !seen {
							ownerOf[name] = st.Slug + "-tabular.zip"
						}
					}
				}
			}
			last := map[string]int{}
			for i, e := range n.Entries {
				if e.From != nil {
					last[ownerOf[e.Path]] = i
				}
			}
			var want []string
			for label := range last {
				want = append(want, label)
			}
			slices.SortFunc(want, func(a, b string) int { return last[a] - last[b] })
			Expect(units).To(Equal(want))
			Expect(units).To(ConsistOf("ags-tabular.zip", "yuc-tabular.zip", "zac-tabular.zip"))
		})

		It("builds byte-identical archives on a second assembly (byte-reproducible)", func() {
			again, err := publish.NationalCSVEntries(ctx, db, stateArchives("tabular"), runs, nil)
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(again.Close()).To(Succeed()) }()
			p2 := filepath.Join(GinkgoT().TempDir(), "national-csv.zip")
			_, err = archive.WriteZipMixed(ctx, p2, again.Entries, archive.ZipOptions{Modified: seededSnapshot})
			Expect(err).NotTo(HaveOccurred())
			b1, err := os.ReadFile(path)
			Expect(err).NotTo(HaveOccurred())
			b2, err := os.ReadFile(p2)
			Expect(err).NotTo(HaveOccurred())
			Expect(b2).To(Equal(b1))
		})
	})

	Describe("NationalJSONEntries", func() {
		It("copies every state's station files raw, regenerates provenance/ once, one unit per state archive", func() {
			var units []string
			n, err := publish.NationalJSONEntries(ctx, db, stateArchives("json"), runs,
				func(label string) { units = append(units, label) })
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(n.Close()).To(Succeed()) }()
			path := filepath.Join(GinkgoT().TempDir(), "national-json.zip")
			_, err = archive.WriteZipMixed(ctx, path, n.Entries, archive.ZipOptions{Modified: seededSnapshot})
			Expect(err).NotTo(HaveOccurred())

			national := zipFiles(path)
			var want []string
			for _, st := range states {
				for name, f := range zipFiles(filepath.Join(out, st.Slug+"-json.zip")) {
					if !strings.HasPrefix(name, "combined/") {
						continue
					}
					want = append(want, name)
					Expect(national).To(HaveKey(name))
					Expect(national[name].CRC32).To(Equal(f.CRC32), name)
					Expect(rawRecord(national[name])).To(Equal(rawRecord(f)), name)
				}
			}
			want = append(want, "provenance/ingest_runs.json", "provenance/power_runs.json")
			slices.Sort(want)
			names, contents := zipContents(path)
			Expect(names).To(Equal(want))
			// provenance/ is global — the same bytes every state archive carries.
			_, yuc := zipContents(filepath.Join(out, "yuc-json.zip"))
			Expect(contents["provenance/ingest_runs.json"]).To(Equal(yuc["provenance/ingest_runs.json"]))
			Expect(contents["provenance/power_runs.json"]).To(Equal(yuc["provenance/power_runs.json"]))

			Expect(n.Units).To(Equal(3))
			// Each state's files are contiguous under combined/<slug>/, so
			// the units fire in state order.
			Expect(units).To(Equal([]string{"ags-json.zip", "yuc-json.zip", "zac-json.zip"}))
		})
	})

	Describe("refusals", func() {
		var dir string
		perStation := func(st publish.State, id string) string {
			return "conagua/daily_observations/" + st.Slug + "/daily-" + id + ".csv"
		}
		// fixtures writes one hand-built tabular-shaped archive per state,
		// each with one per-station file, plus overrides.
		fixtures := func(extra map[string]map[string]string) []publish.StateArchive {
			archives := make([]publish.StateArchive, len(states))
			for i, st := range states {
				files := map[string]string{perStation(st, "1"): "station_id,date\n1,2020-01-01\n"}
				for name, content := range extra[st.Code] {
					files[name] = content
				}
				p := filepath.Join(dir, st.Slug+"-tabular.zip")
				writeFixtureZip(p, files)
				archives[i] = publish.StateArchive{State: st, Path: p}
			}
			return archives
		}

		BeforeEach(func() {
			dir = GinkgoT().TempDir()
		})

		It("refuses a state with no archive, naming it", func() {
			_, err := publish.NationalCSVEntries(ctx, db, stateArchives("tabular")[:2], runs, nil)
			Expect(err).To(MatchError("no tabular archive for ZAC: the national archive needs every state's"))
		})

		It("refuses an archive for a state the database does not carry, and archives out of state order", func() {
			foreign := append(stateArchives("tabular"), publish.StateArchive{
				State: publish.State{Code: "NL", Slug: "nl", Name: "Nuevo León"}, Path: filepath.Join(dir, "nl-tabular.zip"),
			})
			_, err := publish.NationalCSVEntries(ctx, db, foreign, runs, nil)
			Expect(err).To(MatchError("tabular archive nl-tabular.zip is for NL, which is not a state of the database"))

			reversed := slices.Clone(stateArchives("json"))
			slices.Reverse(reversed)
			_, err = publish.NationalJSONEntries(ctx, db, reversed, runs, nil)
			Expect(err).To(MatchError("json archives are not in state order (ZAC, YUC, AGS; the states are AGS, YUC, ZAC)"))
		})

		It("refuses a shared file whose copies differ between two states, naming the file and both archives", func() {
			shared := "nasa_power/daily/daily-" + cellShared + ".csv"
			archives := fixtures(map[string]map[string]string{
				"AGS": {shared: "cell_id,date\n" + cellShared + ",2020-01-01\n"},
				"YUC": {shared: "cell_id,date\n" + cellShared + ",2020-01-02\n"},
			})
			_, err := publish.NationalCSVEntries(ctx, db, archives, runs, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(HavePrefix(`entry "` + shared + `" differs between ags-tabular.zip (crc32 `))
			Expect(err.Error()).To(ContainSubstring(" and yuc-tabular.zip (crc32 "))
		})

		It("accepts a shared file whose copies agree, copying it once", func() {
			shared := "nasa_power/daily/daily-" + cellShared + ".csv"
			same := "cell_id,date\n" + cellShared + ",2020-01-01\n"
			archives := fixtures(map[string]map[string]string{"AGS": {shared: same}, "YUC": {shared: same}})
			n, err := publish.NationalCSVEntries(ctx, db, archives, runs, nil)
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(n.Close()).To(Succeed()) }()
			var copies []string
			for _, e := range n.Entries {
				if e.From != nil {
					copies = append(copies, e.Path)
				}
			}
			Expect(copies).To(Equal([]string{
				perStation(states[0], "1"), perStation(states[1], "1"), perStation(states[2], "1"), shared,
			}))
		})

		It("refuses an entry under another state's shard, an entry the layout does not account for, and an archive with nothing to copy", func() {
			_, err := publish.NationalCSVEntries(ctx, db, fixtures(map[string]map[string]string{
				"YUC": {perStation(states[0], "9"): "station_id,date\n"},
			}), runs, nil)
			Expect(err).To(MatchError(`state archive yuc-tabular.zip: entry "conagua/daily_observations/ags/daily-9.csv" is under another state's shard`))

			_, err = publish.NationalCSVEntries(ctx, db, fixtures(map[string]map[string]string{
				"ZAC": {"README.txt": "stray"},
			}), runs, nil)
			Expect(err).To(MatchError(`state archive zac-tabular.zip: entry "README.txt" is not part of a state tabular archive`))

			empty := fixtures(nil)
			writeFixtureZip(empty[1].Path, map[string]string{"conagua/stations.csv": "station_id\n", "yuc.db": "", "conagua/stations.parquet": ""})
			_, err = publish.NationalCSVEntries(ctx, db, empty, runs, nil)
			Expect(err).To(MatchError("state archive yuc-tabular.zip holds no entry to copy"))
		})

		It("refuses, in a JSON archive, an entry outside combined/", func() {
			archives := make([]publish.StateArchive, len(states))
			for i, st := range states {
				p := filepath.Join(dir, st.Slug+"-json.zip")
				files := map[string]string{"combined/" + st.Slug + "/1/profile.json": "{}"}
				if st.Code == "YUC" {
					files["conagua/stations.csv"] = "station_id\n"
				}
				writeFixtureZip(p, files)
				archives[i] = publish.StateArchive{State: st, Path: p}
			}
			_, err := publish.NationalJSONEntries(ctx, db, archives, runs, nil)
			Expect(err).To(MatchError(`state archive yuc-json.zip: entry "conagua/stations.csv" is not part of a state JSON archive`))
		})

		It("refuses a missing archive file and a cancelled context", func() {
			missing := stateArchives("tabular")
			missing[2].Path = filepath.Join(dir, "zac-tabular.zip")
			_, err := publish.NationalCSVEntries(ctx, db, missing, runs, nil)
			Expect(err).To(MatchError(os.ErrNotExist))
			Expect(err.Error()).To(HavePrefix("open state archive: "))

			cctx, cancel := context.WithCancel(ctx)
			cancel()
			_, err = publish.NationalCSVEntries(cctx, db, stateArchives("tabular"), runs, nil)
			Expect(err).To(MatchError(context.Canceled))
		})
	})
})

var _ = Describe("the artifact set against Zenodo's cap", func() {
	It("holds 2 × 32 per-state + 4 national + 1 raw + 10 docs = 79 top-level files, under the 100-file cap", func() {
		var perState, national, raw, docsGroups int
		for _, g := range publish.Groups() {
			switch {
			case g.PerState():
				perState++
			case g == publish.GroupRaw:
				raw++
			case g == publish.GroupDocs:
				docsGroups++
			default:
				national++
			}
		}
		Expect(perState).To(Equal(2), "{state}-tabular.zip, {state}-json.zip")
		Expect(national).To(Equal(4), "csv, parquet, json, sqlite")
		Expect(raw).To(Equal(1), "conagua-raw-<date>.zip")
		Expect(docsGroups).To(Equal(1), "the metadata group, DocsFiles() whole")
		Expect(conagua.AllStates).To(HaveLen(32))
		Expect(publish.DocsFiles()).To(Equal([]string{
			"README.md", "DATA-DICTIONARY.md", "DATA-DICTIONARY.json", "LICENSE", "NOTICE", "CITATION.cff",
			"manifest.json", "CHECKSUMS", "QA-REPORT.md", "zenodo-metadata.json",
		}))
		docs := publish.DocsFiles()
		docs[0] = "tampered"
		Expect(publish.DocsFiles()[0]).To(Equal("README.md"), "a caller's copy cannot reorder or truncate the set")
		total := perState*len(conagua.AllStates) + national + raw + len(publish.DocsFiles())
		Expect(total).To(Equal(79))
		Expect(total).To(BeNumerically("<=", publish.MaxTopLevelFiles))
		Expect(publish.MaxTopLevelFiles).To(Equal(100))
	})
})
