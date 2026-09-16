package publish_test

// Specs at the Run → archive seam for a build whose states carry every
// folder's content: each archive's entry set and order held to the
// union of the four folder builders' paths and each entry's bytes to
// the builder's own rendering; the provenance/ pair byte-identical
// across archives; a state with no station_power_cell row shipped with
// header-only nasa_power/ tables, no per-cell file, and an empty join
// context; and a referenced cell_id that cannot name a file failing its
// state soft — before any of that state's entries exist — while the
// others ship.

import (
	"archive/zip"
	"context"
	"database/sql"
	"io"
	"path/filepath"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// zipContents reads every entry of the zip at path, in archive order.
func zipContents(path string) (names []string, contents map[string][]byte) {
	GinkgoHelper()
	zr, err := zip.OpenReader(path)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = zr.Close() }()
	contents = map[string][]byte{}
	for _, f := range zr.File {
		rc, err := f.Open()
		Expect(err).NotTo(HaveOccurred())
		data, err := io.ReadAll(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Close()).To(Succeed())
		names = append(names, f.Name)
		contents[f.Name] = data
	}
	return names, contents
}

// folderEntries assembles a state's archive from the six builders
// separately — the four CSV folders, the Parquet twins, and the state
// database built under tmpDir — never through TabularEntries, so the
// seam spec holds Run to the builders and not to the function Run
// itself calls.
func folderEntries(db *sql.DB, st publish.State, runs publish.Runs, tmpDir string) []archive.Entry {
	GinkgoHelper()
	ctx := context.Background()
	conagua, err := publish.ConaguaEntries(ctx, db, st, nil)
	Expect(err).NotTo(HaveOccurred())
	nasaPower, err := publish.PowerEntries(ctx, db, st, nil)
	Expect(err).NotTo(HaveOccurred())
	combined, err := publish.CombinedEntries(ctx, db, st, nil)
	Expect(err).NotTo(HaveOccurred())
	parquet, err := publish.ParquetEntries(ctx, db, st)
	Expect(err).NotTo(HaveOccurred())
	all := slices.Concat(conagua, nasaPower, combined, parquet, publish.ProvenanceEntries(runs),
		[]archive.Entry{publish.StateDBEntry(ctx, db, st, tmpDir)})
	slices.SortFunc(all, func(a, b archive.Entry) int { return strings.Compare(a.Path, b.Path) })
	return all
}

var _ = Describe("Run's archives against the folder builders", func() {
	var (
		db     *sql.DB
		ids    map[string]int64
		out    string
		rec    *recorder
		states []publish.State
	)

	BeforeEach(func() {
		db = openTempDB()
		ids = seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		// Zacatecas: a station with rows in every spine table and no cell.
		insertNormals(db, ids["conv/32001"], "1981-2010", 3, 21.0, 6.0, 13.5, 9.5, 155.5)
		insertNormals(db, ids["conv/32001"], "1961-1990", 3, 20.5, 5.5, 13.0, 8.0, 150.0)
		insertDaily(db, ids["conv/32001"], "2020-01-01", 18.0, 2.0, 0.0, 4.4)
		insertDaily(db, ids["conv/32001"], "1990-07-04", 25.5, 12.5, 30.1, nil)
		var err error
		states, err = publish.LoadStates(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(codesOf(states)).To(Equal([]string{"AGS", "YUC", "ZAC"}))
		out = filepath.Join(GinkgoT().TempDir(), "publish")
		rec = &recorder{}
	})

	It("writes, per state, exactly the union of the six builders' entries, Path-sorted, each entry's bytes as its builder renders them", func() {
		report, err := publish.Run(context.Background(), db, publish.Options{OutDir: out, Now: fixedClock, Only: stateGroups}, rec.record)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
		// Per state, the tabular archive then the JSON one.
		Expect(report.Artifacts).To(HaveLen(6))

		runs, err := publish.LoadRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		provenance := map[string][][]byte{}
		for i, st := range states {
			want := folderEntries(db, st, runs, GinkgoT().TempDir())
			var wantPaths []string
			for _, e := range want {
				wantPaths = append(wantPaths, e.Path)
			}
			Expect(wantPaths).To(HaveLen(len(slices.Compact(slices.Clone(wantPaths)))), "%s: a duplicate path", st.Code)

			tabular := report.Artifacts[2*i]
			Expect(tabular.Name).To(Equal(st.Slug + "-tabular.zip"))
			Expect(tabular.Entries).To(Equal(len(want)))
			names, contents := zipContents(filepath.Join(out, tabular.Name))
			Expect(names).To(Equal(wantPaths), st.Code)
			for _, e := range want {
				Expect(contents[e.Path]).To(Equal(render(e)), "%s: %s", st.Code, e.Path)
			}
			for _, p := range []string{"provenance/ingest_runs.csv", "provenance/power_runs.csv"} {
				provenance[p] = append(provenance[p], contents[p])
			}
		}
		// The provenance pair is the same bytes in every archive.
		for p, renderings := range provenance {
			Expect(renderings).To(HaveLen(3), p)
			Expect(renderings[1]).To(Equal(renderings[0]), p)
			Expect(renderings[2]).To(Equal(renderings[0]), p)
		}
	})

	It("holds the builders' unit folders and the state database to Run's unit events, per archive, in write order", func() {
		_, err := publish.Run(context.Background(), db, publish.Options{OutDir: out, Now: fixedClock, Only: stateGroups}, rec.record)
		Expect(err).NotTo(HaveOccurred())
		runs, err := publish.LoadRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())

		var want []publish.ProgressEvent
		for _, st := range states {
			var units []string
			for _, e := range folderEntries(db, st, runs, GinkgoT().TempDir()) {
				if isTabularUnit(e.Path) {
					units = append(units, e.Path)
				}
			}
			want = append(want, unitEvents(st.Slug+"-tabular.zip", units...)...)
			want = append(want, publish.ProgressEvent{Artifact: st.Slug + "-tabular.zip"})
		}
		// The JSON archives' events interleave per state; the tabular
		// archives' are held here.
		var got []publish.ProgressEvent
		for _, ev := range rec.events {
			if !strings.HasSuffix(ev.Artifact, "-tabular.zip") {
				continue
			}
			if ev.Unit == "" {
				Expect(ev.Err).NotTo(HaveOccurred())
				ev.Bytes, ev.SHA256, ev.Entries = 0, "", 0
			}
			got = append(got, ev)
		}
		Expect(got).To(Equal(want))
	})

	It("ships a state whose stations reference no cell with header-only nasa_power/ tables, no per-cell file, and an empty join context", func() {
		report, err := publish.Run(context.Background(), db, publish.Options{OutDir: out, Now: fixedClock, States: []string{"zac"}}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
		Expect(report.Artifacts[0].Entries).To(Equal(11 + 10 + 1))

		names, contents := zipContents(filepath.Join(out, "zac-tabular.zip"))
		Expect(names).To(Equal([]string{
			"combined/combined_daily.parquet",
			"combined/combined_daily/zac/daily-32001.csv",
			"combined/combined_monthly.csv",
			"combined/combined_monthly.parquet",
			"conagua/daily_observations.parquet",
			"conagua/daily_observations/zac/daily-32001.csv",
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
			"zac.db",
		}))
		Expect(records(contents["nasa_power/cells.csv"])).To(Equal([][]string{publish.Cells.Header()}))
		Expect(records(contents["nasa_power/monthly.csv"])).To(Equal([][]string{publish.PowerMonthly.Header()}))
		Expect(records(contents["nasa_power/station_cell_map.csv"])).To(Equal([][]string{publish.StationCellMap.Header()}))
		// The Parquet twins of the header-only tables are schema-only
		// files, and the per-cell twin has no cell to carry.
		for _, name := range []string{"nasa_power/cells.parquet", "nasa_power/monthly.parquet",
			"nasa_power/station_cell_map.parquet", "nasa_power/daily.parquet"} {
			Expect(openParquet(contents[name]).NumRows()).To(BeZero(), name)
		}
		Expect(records(contents["combined/combined_monthly.csv"])).To(Equal([][]string{
			publish.CombinedMonthly.Header(),
			withPower([]string{"32001", "1961-1990", "3", "", "", "20.5", "5.5", "13.0", "8.0", "150.0"}, noPower),
			withPower([]string{"32001", "1981-2010", "3", "", "", "21.0", "6.0", "13.5", "9.5", "155.5"}, noPower),
		}))
		Expect(records(contents["combined/combined_daily/zac/daily-32001.csv"])).To(Equal([][]string{
			publish.CombinedDaily.Header(),
			withPower([]string{"32001", "1990-07-04", "", "", "25.50", "12.50", "30.10", ""}, noPower),
			withPower([]string{"32001", "2020-01-01", "", "", "18.00", "2.00", "0.00", "4.40"}, noPower),
		}))
		// The provenance pair is global: the same rows as any other state.
		Expect(records(contents["provenance/ingest_runs.csv"])).To(Equal(wantIngestRunsCSV))
		Expect(records(contents["provenance/power_runs.csv"])).To(Equal(wantPowerRunsCSV))
	})

	It("fails a state soft on a referenced cell_id that cannot name a file, before any of its entries exist, and ships the others", func() {
		const bad = `21.0N/89.0000W`
		insertCell(db, bad, 21.0, -89.0)
		insertStationCell(db, ids["conv/31002"], bad, 1.0)

		report, err := publish.Run(context.Background(), db, publish.Options{OutDir: out, Now: fixedClock, Only: stateGroups}, rec.record)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(Equal(1))
		Expect(report.TopLevelFiles).To(Equal(7))
		Expect(report.Artifacts).To(HaveLen(6))
		Expect(report.Artifacts[2].Name).To(Equal("yuc-tabular.zip"))
		Expect(report.Artifacts[2].Err).To(Equal(`list cells of YUC: cell_id "21.0N/89.0000W" is not a POWER cell key`))
		Expect(report.Artifacts[2].Bytes).To(BeZero())
		for _, i := range []int{0, 1, 3, 4, 5} {
			Expect(report.Artifacts[i].Err).To(BeEmpty(), report.Artifacts[i].Name)
		}
		// The guard is the tabular archive's — the id is a path segment
		// there and a value in the profile — so the state's JSON archive
		// ships.
		Expect(dirNames(out)).To(Equal([]string{
			"CHECKSUMS", "ags-json.zip", "ags-tabular.zip", "manifest.json", "yuc-json.zip", "zac-json.zip", "zac-tabular.zip",
		}))
		Expect(readManifest(out).States[1]).To(Equal(publish.ManifestState{
			Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-json.zip"},
		}))

		// No unit of Yucatán's was written: the guard fires while the
		// entry list is assembled, ahead of the zip write.
		var yucEvents []publish.ProgressEvent
		for _, ev := range rec.events {
			if ev.Artifact == "yuc-tabular.zip" {
				yucEvents = append(yucEvents, ev)
			}
		}
		Expect(yucEvents).To(HaveLen(1))
		Expect(yucEvents[0].Unit).To(BeEmpty())
		Expect(yucEvents[0].Err).To(MatchError(report.Artifacts[2].Err))
	})
})

func codesOf(states []publish.State) []string {
	out := make([]string, len(states))
	for i, s := range states {
		out[i] = s.Code
	}
	return out
}
