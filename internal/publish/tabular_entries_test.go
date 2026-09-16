package publish_test

// Specs for the tabular archive at the seam where its Parquet twins and
// its state database join the CSV folders: the entry list held to the
// union of the six builders and Path-sorted across the three suffixes,
// each Parquet twin beside its CSV; the state database the one non-file
// unit, at its sorted position in either direction; a Parquet twin's
// refusal and the database's refusal each failing the state soft with
// the archive discarded whole and no residue — the first entry of the
// archive in one case, the last in the other; and the run-scoped temp
// directory: one per run, inside OutDir, gone before the post-check,
// and refused as a crashed run's residue by the pre-flight.

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// tempDirs lists the run-scoped temp directories present in dir.
func tempDirs(dir string) []string {
	GinkgoHelper()
	var names []string
	for _, n := range dirNames(dir) {
		if strings.HasPrefix(n, ".publish.tmp-") {
			names = append(names, n)
		}
	}
	return names
}

var _ = Describe("TabularEntries with the Parquet twins and the state database", func() {
	var (
		ctx = context.Background()
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

	tabularOnly := func(states ...string) publish.Options {
		return publish.Options{OutDir: out, Now: fixedClock, Only: []string{"tabular"}, States: states}
	}

	It("lists exactly the union of the six builders' entries, Path-sorted across .csv, .parquet, and .db names, each Parquet twin beside its CSV, the database one unit", func() {
		runs, err := publish.LoadRuns(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		tmpDir := GinkgoT().TempDir()
		entries, units, err := publish.TabularEntries(ctx, db, yucatan, runs, tmpDir, nil)
		Expect(err).NotTo(HaveOccurred())

		var paths, wantPaths []string
		for _, e := range entries {
			paths = append(paths, e.Path)
		}
		for _, e := range folderEntries(db, yucatan, runs, tmpDir) {
			wantPaths = append(wantPaths, e.Path)
		}
		Expect(paths).To(Equal(wantPaths))
		Expect(slices.IsSorted(paths)).To(BeTrue())
		// Four stations with two daily files each, two referenced cells,
		// and the database; the Parquet twins are not units.
		Expect(units).To(Equal(2*4 + 2 + 1))

		// A whole-table twin follows its CSV immediately ('.csv' < '.parquet');
		// a sharded table's twin precedes its shard folder ('.' < '/');
		// provenance/ has no twin; the database sorts last for this slug.
		for _, name := range []string{
			"conagua/stations", "conagua/monthly_normals", "conagua/monthly_normals_extras",
			"nasa_power/cells", "nasa_power/station_cell_map", "nasa_power/monthly", "combined/combined_monthly",
		} {
			i := slices.Index(paths, name+".csv")
			Expect(i).To(BeNumerically(">=", 0), name)
			Expect(paths[i+1]).To(Equal(name+".parquet"), name)
		}
		for _, name := range []string{"conagua/daily_observations", "nasa_power/daily", "combined/combined_daily"} {
			i := slices.Index(paths, name+".parquet")
			Expect(i).To(BeNumerically(">=", 0), name)
			Expect(paths[i+1]).To(HavePrefix(name+"/"), name)
		}
		Expect(paths).NotTo(ContainElement(And(HavePrefix("provenance/"), HaveSuffix(".parquet"))))
		Expect(paths[len(paths)-1]).To(Equal("yuc.db"))
	})

	It("fires the state database as one unit at its Path-sorted position — first for ags, last for yuc — and ships the published row set inside the zip", func() {
		_, err := publish.Run(ctx, db, tabularOnly(), rec.record)
		Expect(err).NotTo(HaveOccurred())

		ags := eventsOf(rec, "ags-tabular.zip")
		Expect(ags[0]).To(Equal(publish.ProgressEvent{Artifact: "ags-tabular.zip", Unit: "ags.db", Index: 1, Total: len(ags) - 1}))
		yuc := eventsOf(rec, "yuc-tabular.zip")
		Expect(yuc[len(yuc)-2]).To(Equal(publish.ProgressEvent{
			Artifact: "yuc-tabular.zip", Unit: "yuc.db", Index: len(yuc) - 1, Total: len(yuc) - 1,
		}))
		Expect(yuc[len(yuc)-1]).To(Equal(publish.ProgressEvent{Artifact: "yuc-tabular.zip"}))

		_, contents := zipContents(filepath.Join(out, "yuc-tabular.zip"))
		got := openRO(writeDB(contents["yuc.db"], "yuc.db"))
		Expect(pragmaText(got, "integrity_check")).To(Equal("ok"))
		Expect(pragmaText(got, "journal_mode")).To(Equal("delete"))
		var stations []string
		rows, err := got.Query(`SELECT external_id FROM stations ORDER BY external_id`)
		Expect(err).NotTo(HaveOccurred())
		for rows.Next() {
			var id string
			Expect(rows.Scan(&id)).To(Succeed())
			stations = append(stations, id)
		}
		Expect(rows.Close()).To(Succeed())
		Expect(stations).To(Equal([]string{"31001", "31002", "31003", "3101"}))
		Expect(count(got, `SELECT COUNT(*) FROM stations WHERE state <> 'YUC'`)).To(BeZero())
		Expect(count(got, `SELECT COUNT(*) FROM nasa_power_grid_cells WHERE cell_id IN (?, ?)`, cellAGS, cellNone)).To(BeZero())
		Expect(count(got, `SELECT COUNT(*) FROM nasa_power_grid_cells`)).To(Equal(2))
	})

	It("fails a state soft on a Parquet twin's refusal — the archive's first entry, before any unit — discarding the archive whole with no residue", func() {
		// An infinite REAL is refused by column; the combined_daily twin
		// is the first entry of the archive, ahead of every CSV that
		// would refuse the same value.
		mustExec(db, `UPDATE daily_observations SET tmax = 9e999 WHERE station_id = ? AND date = '2020-01-02'`,
			ids["conv/31001"])
		report, err := publish.Run(ctx, db, tabularOnly(), rec.record)
		Expect(err).NotTo(HaveOccurred())

		Expect(report.Failed).To(Equal(1))
		Expect(report.Artifacts).To(HaveLen(3))
		Expect(report.Artifacts[0].Name).To(Equal("ags-tabular.zip"))
		Expect(report.Artifacts[0].Err).To(BeEmpty())
		Expect(report.Artifacts[1].Name).To(Equal("yuc-tabular.zip"))
		Expect(report.Artifacts[1].Err).To(Equal("write zip " + filepath.Join(out, "yuc-tabular.zip") +
			`: entry "combined/combined_daily.parquet": station 31001: row 3: column tmax_c: non-finite value +Inf`))
		Expect(report.Artifacts[1].Bytes).To(BeZero())
		Expect(report.Artifacts[2].Name).To(Equal("zac-tabular.zip"))
		Expect(report.Artifacts[2].Err).To(BeEmpty())
		events := eventsOf(rec, "yuc-tabular.zip")
		Expect(events).To(HaveLen(1))
		Expect(events[0].Unit).To(BeEmpty())
		Expect(events[0].Err).To(MatchError(report.Artifacts[1].Err))
		Expect(report.TopLevelFiles).To(Equal(4))
		Expect(dirNames(out)).To(Equal([]string{"CHECKSUMS", "ags-tabular.zip", "manifest.json", "zac-tabular.zip"}))
	})

	It("fails a state soft on its database's refusal — the archive's last entry, after every per-file unit — discarding the archive whole with no residue", func() {
		// A source column the schema lacks: the CSV and Parquet queries
		// name their columns and never see it; the database copy holds
		// the source's shape to the DDL and refuses.
		mustExec(db, `ALTER TABLE stations ADD COLUMN extra TEXT`)
		report, err := publish.Run(ctx, db, tabularOnly("yuc"), rec.record)
		Expect(err).NotTo(HaveOccurred())

		Expect(report.Failed).To(Equal(1))
		Expect(report.Artifacts).To(HaveLen(1))
		Expect(report.Artifacts[0].Err).To(HavePrefix("write zip " + filepath.Join(out, "yuc-tabular.zip") +
			`: entry "yuc.db": build state database: source table stations has columns [`))
		Expect(report.Artifacts[0].Err).To(ContainSubstring(" extra], the schema has ["))
		events := eventsOf(rec, "yuc-tabular.zip")
		want := []publish.ProgressEvent{
			{Artifact: "yuc-tabular.zip", Unit: "combined/combined_daily/yuc/daily-31001.csv", Index: 1, Total: 11},
			{Artifact: "yuc-tabular.zip", Unit: "combined/combined_daily/yuc/daily-31002.csv", Index: 2, Total: 11},
			{Artifact: "yuc-tabular.zip", Unit: "combined/combined_daily/yuc/daily-31003.csv", Index: 3, Total: 11},
			{Artifact: "yuc-tabular.zip", Unit: "combined/combined_daily/yuc/daily-3101.csv", Index: 4, Total: 11},
			{Artifact: "yuc-tabular.zip", Unit: "conagua/daily_observations/yuc/daily-31001.csv", Index: 5, Total: 11},
			{Artifact: "yuc-tabular.zip", Unit: "conagua/daily_observations/yuc/daily-31002.csv", Index: 6, Total: 11},
			{Artifact: "yuc-tabular.zip", Unit: "conagua/daily_observations/yuc/daily-31003.csv", Index: 7, Total: 11},
			{Artifact: "yuc-tabular.zip", Unit: "conagua/daily_observations/yuc/daily-3101.csv", Index: 8, Total: 11},
			{Artifact: "yuc-tabular.zip", Unit: "nasa_power/daily/daily-" + cellSingle + ".csv", Index: 9, Total: 11},
			{Artifact: "yuc-tabular.zip", Unit: "nasa_power/daily/daily-" + cellShared + ".csv", Index: 10, Total: 11},
			{Artifact: "yuc-tabular.zip", Err: events[len(events)-1].Err},
		}
		Expect(events).To(Equal(want))
		Expect(events[len(events)-1].Err).To(MatchError(report.Artifacts[0].Err))
		Expect(dirNames(out)).To(Equal([]string{"CHECKSUMS", "manifest.json"}))
	})

	It("keeps one run-scoped temp directory inside OutDir for the whole run, empty between entries, and removes it before the post-check", func() {
		var during [][]string
		var residue [][]string
		_, err := publish.Run(ctx, db, tabularOnly(), func(ev publish.ProgressEvent) {
			if !strings.HasSuffix(ev.Unit, ".db") {
				return
			}
			dirs := tempDirs(out)
			during = append(during, dirs)
			for _, d := range dirs {
				residue = append(residue, dirNames(filepath.Join(out, d)))
			}
		})
		Expect(err).NotTo(HaveOccurred())
		// One directory, the same for all three states' databases, and
		// the entry's own build directory already gone when its unit fires.
		Expect(during).To(HaveLen(3))
		Expect(during[0]).To(HaveLen(1))
		Expect(during[1]).To(Equal(during[0]))
		Expect(during[2]).To(Equal(during[0]))
		Expect(residue).To(Equal([][]string{nil, nil, nil}))
		Expect(tempDirs(out)).To(BeEmpty())
		Expect(dirNames(out)).To(Equal([]string{
			"CHECKSUMS", "ags-tabular.zip", "manifest.json", "yuc-tabular.zip", "zac-tabular.zip",
		}))
	})

	It("refuses a crashed run's temp residue in OutDir before writing anything", func() {
		Expect(os.MkdirAll(filepath.Join(out, ".publish.tmp-crashed"), 0o755)).To(Succeed())
		report, err := publish.Run(ctx, db, tabularOnly(), rec.record)
		Expect(err).To(MatchError("out dir " + out +
			" holds entries this run does not produce: .publish.tmp-crashed (remove them or use a fresh --out)"))
		Expect(report.Artifacts).To(BeEmpty())
		Expect(rec.events).To(HaveLen(len(gateRuleIDs)), "the gate's lines only")
		Expect(dirNames(out)).To(Equal([]string{".publish.tmp-crashed"}))
	})
})
