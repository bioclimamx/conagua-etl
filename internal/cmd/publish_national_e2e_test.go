package cmd_test

// The full-run e2e story: the real binary, given no --state, builds the
// whole deposit from the seeded two-state DB and a fixture snapshot tree
// under --root — the two per-state archives of each state, then
// national-csv.zip, national-parquet.zip, national-json.zip,
// national-sqlite.zip, and conagua-raw-<date>.zip, then the docs group's
// eight files, then manifest.json and CHECKSUMS: nineteen top-level
// files. The specs hold each national
// archive to its sources exactly as an operator could check them:
// national-csv's per-station and per-cell entries are the state
// archives' records copied raw (CRC-32 and compressed bytes equal, the
// cell two states share once) beside the seven whole-scope tables held
// cell for cell to the union of the states' hand-written rows in
// national order and provenance/ once; national-json likewise from the
// state JSON archives; national-parquet's ten files decoded and held
// cell for cell to national-csv; national-sqlite's bioclima.db extracted
// and held table by table, every column, to the source, stamped and in
// rollback-journal mode, the source's bytes unchanged; the raw archive
// held file by file to the fixture tree. manifest.json lists the states
// and the national groups, CHECKSUMS every top-level file. Two full runs
// yield byte-identical archives. A national group with --state, and
// the raw group without its snapshot directory, exit 1 before anything
// is written; a state whose archives fail degrades the run to exit 2,
// the copy-built national archives failing soft naming the state while
// the SQLite and raw archives still build.

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// seedPublishSnapshot lays out a fixture snapshot of publishSnapshotDate
// under root as pull does — <root>/conagua-raw/<date>/<kind>/<id>.txt
// plus the two metadata files — and returns the files by slash path.
// The bodies carry what CONAGUA's do: a BOM, a Latin-1 byte, CRLF line
// endings, a missing-value token; one body is empty.
func seedPublishSnapshot(root string) map[string]string {
	files := map[string]string{
		"daily/31001.txt":             "\xef\xbb\xbfESTACI\xd3N : 31001\r\n01/01/1981\t31.0\t14.5\tNULO\t0.0\r\n",
		"daily/1001.txt":              "ESTACI\xd3N : 1001\r\n01/01/1981\t20.0\t5.0\t0.0\t6.0\r\n",
		"monthly/31001.txt":           "MENSUALES 31001\r\n",
		"extremes/1001.txt":           "",
		"normals_1981_2010/31001.txt": "NORMALES 1981-2010\r\nTEMPERATURA M\xc1XIMA\r\n",
		"_index.json":                 `{"snapshot_date":"` + publishSnapshotDate + `","stations":[]}` + "\n",
		"_progress.json":              `{"snapshot_date":"` + publishSnapshotDate + `"}` + "\n",
	}
	dir := filepath.Join(root, "conagua-raw", publishSnapshotDate)
	for name, body := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		ExpectWithOffset(1, os.MkdirAll(filepath.Dir(p), 0o755)).To(Succeed())
		ExpectWithOffset(1, os.WriteFile(p, []byte(body), 0o600)).To(Succeed())
	}
	return files
}

// sortedKeys is a map's keys, sorted.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// zipRecords opens the archive at path, scheduling its close, and
// indexes its entries by name — the records a raw copy is compared by.
func zipRecords(path string) map[string]*zip.File {
	zr, err := zip.OpenReader(path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = zr.Close() })
	files := map[string]*zip.File{}
	for _, f := range zr.File {
		files[f.Name] = f
	}
	return files
}

// rawRecordOf reads f as stored — the compressed bytes.
func rawRecordOf(f *zip.File) []byte {
	r, err := f.OpenRaw()
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	data, err := io.ReadAll(r)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return data
}

// expectRawCopy holds one national entry to the state archive's record
// it was copied from: the same CRC-32, sizes, timestamp, and compressed
// bytes — a copy, not a re-encoding.
func expectRawCopy(national, source map[string]*zip.File, name string) {
	ExpectWithOffset(1, national).To(HaveKey(name))
	ExpectWithOffset(1, source).To(HaveKey(name))
	got, want := national[name], source[name]
	ExpectWithOffset(1, got.CRC32).To(Equal(want.CRC32), name)
	ExpectWithOffset(1, got.CompressedSize64).To(Equal(want.CompressedSize64), name)
	ExpectWithOffset(1, got.UncompressedSize64).To(Equal(want.UncompressedSize64), name)
	ExpectWithOffset(1, got.Modified.UTC()).To(Equal(want.Modified.UTC()), name)
	ExpectWithOffset(1, rawRecordOf(got)).To(Equal(rawRecordOf(want)), name)
}

// The national archives' entry lists, written by hand: national-csv is
// both states' per-station and per-cell files — the cell both states
// carry once — the seven whole-scope tables, and provenance/;
// national-json both states' station pairs and provenance/.
var (
	wantNationalCSVPaths = []string{
		"combined/combined_daily/ags/daily-1001.csv",
		"combined/combined_daily/ags/daily-1002.csv",
		"combined/combined_daily/ags/daily-1010.csv",
		"combined/combined_daily/yuc/daily-31001.csv",
		"combined/combined_daily/yuc/daily-31002.csv",
		"combined/combined_daily/yuc/daily-3101.csv",
		"combined/combined_daily/yuc/daily-31019.csv",
		"combined/combined_monthly.csv",
		"conagua/daily_observations/ags/daily-1001.csv",
		"conagua/daily_observations/ags/daily-1002.csv",
		"conagua/daily_observations/ags/daily-1010.csv",
		"conagua/daily_observations/yuc/daily-31001.csv",
		"conagua/daily_observations/yuc/daily-31002.csv",
		"conagua/daily_observations/yuc/daily-3101.csv",
		"conagua/daily_observations/yuc/daily-31019.csv",
		"conagua/monthly_normals.csv",
		"conagua/monthly_normals_extras.csv",
		"conagua/stations.csv",
		"nasa_power/cells.csv",
		"nasa_power/daily/daily-" + publishCellYUC + ".csv",
		"nasa_power/daily/daily-" + publishCellAGS + ".csv",
		"nasa_power/monthly.csv",
		"nasa_power/station_cell_map.csv",
		"provenance/ingest_runs.csv",
		"provenance/power_runs.csv",
	}
	wantNationalJSONPaths = []string{
		"combined/ags/1001/daily.json",
		"combined/ags/1001/profile.json",
		"combined/ags/1002/daily.json",
		"combined/ags/1002/profile.json",
		"combined/ags/1010/daily.json",
		"combined/ags/1010/profile.json",
		"combined/yuc/31001/daily.json",
		"combined/yuc/31001/profile.json",
		"combined/yuc/31002/daily.json",
		"combined/yuc/31002/profile.json",
		"combined/yuc/3101/daily.json",
		"combined/yuc/3101/profile.json",
		"combined/yuc/31019/daily.json",
		"combined/yuc/31019/profile.json",
		"provenance/ingest_runs.json",
		"provenance/power_runs.json",
	}

	// The seven whole-scope tables at national scope: the union of the
	// two states' rows in primary-key order — every Aguascalientes key
	// sorts before every Yucatán key bytewise, and the cells and the
	// POWER monthly rows in cell_id order are what ags-tabular.zip
	// already lists (its stations reference both cells).
	wantNationalStations        = slices.Concat(wantAgsStations, wantYucStations[1:])
	wantNationalNormals         = slices.Concat(wantAgsNormals, wantYucNormals[1:])
	wantNationalExtras          = slices.Concat(wantAgsExtras, wantYucExtras[1:])
	wantNationalCells           = wantAgsCells
	wantNationalCellMap         = slices.Concat(wantAgsCellMap, wantYucCellMap[1:])
	wantNationalPowerMonthly    = wantAgsPowerMonthly
	wantNationalCombinedMonthly = slices.Concat(wantAgsCombinedMonthly, wantYucCombinedMonthly[1:])

	// The unit lines of the national archives: a copy's unit fires after
	// the last entry copied from its archive in path order — Yucatán's
	// last is under conagua/ (its cell's file is Aguascalientes' copy,
	// the first state to carry it), Aguascalientes' under
	// nasa_power/daily/; a JSON archive's station folders are contiguous
	// under combined/<slug>/. The Parquet archive has no unit; the
	// database is one; the raw archive's are the snapshot directory's
	// children in path order.
	wantNationalCSVUnits    = []string{"yuc-tabular.zip", "ags-tabular.zip"}
	wantNationalJSONUnits   = []string{"ags-json.zip", "yuc-json.zip"}
	wantNationalSQLiteUnits = []string{"bioclima.db"}
	wantRawUnits            = []string{"_index.json", "_progress.json", "daily/", "extremes/", "monthly/", "normals_1981_2010/"}

	wantFullRunGroups = "tabular,json,national-csv,national-parquet,national-json,national-sqlite,raw,docs"
	wantFullRunDir    = []string{
		"CHECKSUMS", "CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md",
		"README.md", "ags-json.zip", "ags-tabular.zip", "conagua-raw-" + publishSnapshotDate + ".zip",
		"manifest.json", "national-csv.zip", "national-json.zip", "national-parquet.zip", "national-sqlite.zip",
		"yuc-json.zip", "yuc-tabular.zip", "zenodo-metadata.json",
	}
)

// expectNationalDB holds an extracted bioclima.db to the source at
// srcPath: the stamp, rollback-journal mode, integrity, every FK
// resolvable, all eleven tables, and every table's rows — every column,
// the sequence table too — equal to the source's, unfiltered.
func expectNationalDB(entries []zipEntry, srcPath string) {
	got, dir := openArchiveDB(entries, "bioclima.db")
	ExpectWithOffset(1, listDir(dir)).To(Equal([]string{"bioclima.db"}))
	ExpectWithOffset(1, queryInt(got, "PRAGMA user_version")).To(Equal(schema.Version))
	ExpectWithOffset(1, queryStrings(got, "PRAGMA journal_mode")).To(Equal([]string{"delete"}))
	ExpectWithOffset(1, queryStrings(got, "PRAGMA integrity_check")).To(Equal([]string{"ok"}))
	ExpectWithOffset(1, queryInt(got, "SELECT COUNT(*) FROM pragma_foreign_key_check")).To(BeZero())
	tables := []string{
		"daily_observations", "daily_supplement", "ingest_runs", "monthly_normals", "monthly_normals_extras",
		"monthly_supplement", "nasa_power_grid_cells", "parsing_warnings", "power_runs", "station_power_cell", "stations",
	}
	ExpectWithOffset(1, queryStrings(got,
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)).To(Equal(tables))

	src, err := schema.OpenReadOnly(srcPath)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer func() { _ = src.Close() }()
	for _, t := range append(tables, "sqlite_sequence") {
		ExpectWithOffset(1, tableRows(got, t, "")).To(Equal(tableRows(src, t, "")), t)
	}
	// The full DB, not a state's: both states, every cell, the surrogate
	// plumbing the flat files drop, the aborted runs.
	ExpectWithOffset(1, queryStrings(got, `SELECT DISTINCT state FROM stations ORDER BY state`)).To(Equal([]string{"AGS", "YUC"}))
	ExpectWithOffset(1, queryInt(got, `SELECT COUNT(*) FROM stations`)).To(Equal(int(wantPublishCounts.Stations)))
	ExpectWithOffset(1, queryInt(got, `SELECT COUNT(*) FROM power_runs WHERE status = 'aborted'`)).To(Equal(1))
	ExpectWithOffset(1, queryInt(got, `SELECT COUNT(*) FROM daily_supplement WHERE power_run_id IS NULL`)).To(BeZero())
}

var _ = Describe("conagua-etl publish end-to-end, the full run", func() {
	Context("against the seeded two-state DB and a fixture snapshot tree", Ordered, func() {
		var (
			dbPath, dbSHA string
			root          string
			rawFiles      map[string]string
			outRoot, out  string
			session       *gexec.Session
			ags, yuc      []zipEntry
			agsJSON       []zipEntry
			nationalCSV   []zipEntry
			nationalJSON  []zipEntry
			nationalPq    []zipEntry
		)

		BeforeAll(func() {
			dbPath = seedPublishDB()
			dbSHA = fileSHA(dbPath)
			var err error
			outRoot, err = os.MkdirTemp("", "publish-e2e-full-*")
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() error { return os.RemoveAll(outRoot) })
			root = filepath.Join(outRoot, "snapshots")
			rawFiles = seedPublishSnapshot(root)
			out = filepath.Join(outRoot, "all")
			session = runPublishToExit(dbPath, out, "--root", root)
		})

		It("exits 0 with every state's archives, the four national archives, and the raw snapshot: unit lines per archive, one artifact line each, the summary, nineteen files", func() {
			Expect(session.ExitCode()).To(Equal(0))
			Expect(fileSHA(dbPath)).To(Equal(dbSHA))

			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring("db=" + dbPath + " (read-only)  out=" + out + "  root=" + root + "  states=all  only=all  doi=none\n"))
			Expect(stderr).NotTo(ContainSubstring("FAIL"))
			// Per state, the tabular archive's units then the JSON one's;
			// then the national archives' in build order.
			units := publishUnitLineRE.FindAllStringSubmatch(stderr, -1)
			next := 0
			for _, a := range []struct {
				name  string
				units []string
			}{
				{"ags-tabular.zip", wantAgsUnits}, {"ags-json.zip", wantAgsJSONUnits},
				{"yuc-tabular.zip", wantYucUnits}, {"yuc-json.zip", wantYucJSONUnits},
				{"national-csv.zip", wantNationalCSVUnits},
				{"national-json.zip", wantNationalJSONUnits},
				{"national-sqlite.zip", wantNationalSQLiteUnits},
				{"conagua-raw-" + publishSnapshotDate + ".zip", wantRawUnits},
			} {
				Expect(len(units)).To(BeNumerically(">=", next+len(a.units)), a.name)
				expectUnitLines(units[next:next+len(a.units)], a.name, a.units, len(a.units))
				next += len(a.units)
			}
			Expect(units).To(HaveLen(next))
			artifacts := publishArtifactLineRE.FindAllStringSubmatch(stderr, -1)
			Expect(artifacts).To(HaveLen(9))
			for i, want := range []struct {
				name    string
				entries int
			}{
				{"ags-tabular.zip", len(wantAgsPaths)}, {"ags-json.zip", len(wantAgsJSONPaths)},
				{"yuc-tabular.zip", len(wantYucPaths)}, {"yuc-json.zip", len(wantYucJSONPaths)},
				{"national-csv.zip", len(wantNationalCSVPaths)}, {"national-parquet.zip", len(wantParquetTables)},
				{"national-json.zip", len(wantNationalJSONPaths)}, {"national-sqlite.zip", 1},
				{"conagua-raw-" + publishSnapshotDate + ".zip", len(rawFiles)},
			} {
				Expect(artifacts[i][1:4]).To(Equal([]string{"ok", want.name, strconv.Itoa(want.entries)}), want.name)
				zipPath := filepath.Join(out, want.name)
				Expect(artifacts[i][4]).To(Equal(strconv.FormatInt(sizeOf(zipPath), 10)), want.name)
				Expect(artifacts[i][5]).To(Equal(fileSHA(zipPath)[:12]), want.name)
			}

			stdout := string(session.Out.Contents())
			Expect(stdout).To(ContainSubstring("\npublish complete\n"))
			Expect(stdout).To(ContainSubstring("  states               : AGS,YUC\n"))
			Expect(stdout).To(ContainSubstring("  groups               : " + wantFullRunGroups + "\n"))
			Expect(stdout).To(ContainSubstring("  artifacts            : 17 ok / 0 failed / 17 attempted\n"))
			Expect(stdout).To(ContainSubstring("  top-level files      : 19\n"))
			Expect(stdout).NotTo(ContainSubstring("failed artifacts"))
			Expect(listDir(out)).To(Equal(wantFullRunDir))
			expectNoTempResidue(out)
			// The docs group lands last, after the raw archive, one file
			// line per docs file in write order — no entry count — with
			// the digest and size of the bytes on disk.
			files := publishFileLineRE.FindAllStringSubmatch(stderr, -1)
			Expect(files).To(HaveLen(len(wantDocsOrder)))
			for i, name := range wantDocsOrder {
				path := filepath.Join(out, name)
				Expect(files[i][1:5]).To(Equal([]string{"ok", name, strconv.FormatInt(sizeOf(path), 10), fileSHA(path)[:12]}), name)
			}
			Expect(strings.LastIndex(stderr, "] ok   conagua-raw-")).To(BeNumerically("<", strings.Index(stderr, "] ok   README.md")))
		})

		It("scopes the other state's archives to its own stations and cells: the shared cell's file in both tabular archives, provenance identical", func() {
			ags = readZipEntries(filepath.Join(out, "ags-tabular.zip"))
			yuc = readZipEntries(filepath.Join(out, "yuc-tabular.zip"))
			Expect(entryNames(ags)).To(Equal(wantAgsPaths))
			expectEntryCSV(ags, "conagua/stations.csv", wantAgsStations)
			expectEntryCSV(ags, "conagua/monthly_normals.csv", wantAgsNormals)
			expectEntryCSV(ags, "conagua/monthly_normals_extras.csv", wantAgsExtras)
			expectEntryCSV(ags, "conagua/daily_observations/ags/daily-1001.csv", wantAgsDaily)
			expectEntryCSV(ags, "conagua/daily_observations/ags/daily-1002.csv", [][]string{wantDailyHeader})
			expectEntryCSV(ags, "conagua/daily_observations/ags/daily-1010.csv", [][]string{wantDailyHeader})
			expectEntryCSV(ags, "nasa_power/cells.csv", wantAgsCells)
			expectEntryCSV(ags, "nasa_power/station_cell_map.csv", wantAgsCellMap)
			expectEntryCSV(ags, "nasa_power/monthly.csv", wantAgsPowerMonthly)
			expectEntryCSV(ags, "nasa_power/daily/daily-"+publishCellAGS+".csv", wantAgsPowerDaily)
			// The cell both states carry ships in each archive identically.
			expectEntryCSV(ags, "nasa_power/daily/daily-"+publishCellYUC+".csv", wantYucPowerDaily)
			Expect(entryContent(ags, "nasa_power/daily/daily-"+publishCellYUC+".csv")).To(Equal(
				entryContent(yuc, "nasa_power/daily/daily-"+publishCellYUC+".csv")))
			expectEntryCSV(ags, "combined/combined_monthly.csv", wantAgsCombinedMonthly)
			expectEntryCSV(ags, "combined/combined_daily/ags/daily-1001.csv", wantAgsCombinedDaily)
			expectEntryCSV(ags, "combined/combined_daily/ags/daily-1002.csv", [][]string{wantCombinedDailyHeader})
			expectEntryCSV(ags, "combined/combined_daily/ags/daily-1010.csv", [][]string{wantCombinedDailyHeader})
			for _, name := range []string{"provenance/ingest_runs.csv", "provenance/power_runs.csv"} {
				Expect(entryContent(ags, name)).To(Equal(entryContent(yuc, name)), name)
			}
			// No Yucatán value and no row keyed by a Yucatán station.
			for _, e := range csvEntries(ags) {
				Expect(e.Content).NotTo(ContainSubstring("YUC"), e.Name)
				for _, id := range []string{"31001", "31002", "3101", "31019"} {
					Expect(e.Content).NotTo(ContainSubstring("\n"+id+","), e.Name)
				}
			}
			expectParquetTwinsEqualCSV(ags)
			expectStateDB(ags, dbPath, "ags", "AGS")
			agsDB, _ := openArchiveDB(ags, "ags.db")
			Expect(queryStrings(agsDB, `SELECT external_id FROM stations ORDER BY external_id`)).To(Equal([]string{"1001", "1002", "1010"}))
			Expect(queryStrings(agsDB, `SELECT cell_id FROM nasa_power_grid_cells ORDER BY cell_id`)).To(Equal([]string{publishCellYUC, publishCellAGS}))
			Expect(queryInt(agsDB, `SELECT COUNT(*) FROM parsing_warnings`)).To(BeZero())

			agsJSON = readZipEntries(filepath.Join(out, "ags-json.zip"))
			Expect(entryNames(agsJSON)).To(Equal(wantAgsJSONPaths))
			yucJSON := readZipEntries(filepath.Join(out, "yuc-json.zip"))
			for _, name := range []string{"provenance/ingest_runs.json", "provenance/power_runs.json"} {
				Expect(entryContent(agsJSON, name)).To(Equal(entryContent(yucJSON, name)), name)
			}
			for _, e := range agsJSON {
				Expect(e.Content).NotTo(ContainSubstring("YUC"), e.Name)
			}
			Expect(decodeJSONDoc(entryContent(agsJSON, "combined/ags/1001/profile.json"))["power_cell"]).To(Equal(map[string]any{
				"source": "bioclima_derived", "cell_id": publishCellAGS,
				"lat": num("21.750"), "lon": num("-102.500"), "distance_km": num("12.346"),
			}))
			Expect(decodeJSONDoc(entryContent(agsJSON, "combined/ags/1002/profile.json"))["power_cell"]).To(Equal(map[string]any{
				"source": "bioclima_derived", "cell_id": publishCellYUC,
				"lat": num("20.750"), "lon": num("-89.375"), "distance_km": num("1337.500"),
			}))
			Expect(entryContent(agsJSON, "combined/ags/1001/daily.json")).To(Equal(dailyDocFromCombined(wantAgsCombinedDaily)))
			// The trace rain day survives into the JSON pair too: 0.01,
			// the value the shipped SQLite holds, not a rounded 0.0.
			Expect(entryContent(agsJSON, "combined/ags/1001/daily.json")).To(ContainSubstring(`"precip_mm":0.01,`))
		})

		It("copies every per-station and per-cell entry of both tabular archives raw into national-csv.zip — the shared cell once, from the first state — beside the seven national tables and provenance/", func() {
			nationalCSV = readZipEntries(filepath.Join(out, "national-csv.zip"))
			Expect(entryNames(nationalCSV)).To(Equal(wantNationalCSVPaths))
			Expect(slices.IsSorted(entryNames(nationalCSV))).To(BeTrue())
			midnight := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
			for _, e := range nationalCSV {
				Expect(e.Modified).To(Equal(midnight), e.Name)
			}

			national := zipRecords(filepath.Join(out, "national-csv.zip"))
			agsRecords := zipRecords(filepath.Join(out, "ags-tabular.zip"))
			yucRecords := zipRecords(filepath.Join(out, "yuc-tabular.zip"))
			for _, name := range wantAgsPaths {
				if strings.HasPrefix(name, "combined/combined_daily/") || strings.HasPrefix(name, "conagua/daily_observations/") ||
					strings.HasPrefix(name, "nasa_power/daily/") {
					expectRawCopy(national, agsRecords, name)
				}
			}
			for _, name := range wantYucPaths {
				if name == "nasa_power/daily/daily-"+publishCellYUC+".csv" {
					continue
				}
				if strings.HasPrefix(name, "combined/combined_daily/") || strings.HasPrefix(name, "conagua/daily_observations/") ||
					strings.HasPrefix(name, "nasa_power/daily/") {
					expectRawCopy(national, yucRecords, name)
				}
			}
			// The shared cell's file: Aguascalientes' record, and Yucatán's
			// copy is the same record byte for byte.
			shared := "nasa_power/daily/daily-" + publishCellYUC + ".csv"
			expectRawCopy(national, agsRecords, shared)
			Expect(yucRecords[shared].CRC32).To(Equal(agsRecords[shared].CRC32))
			Expect(rawRecordOf(yucRecords[shared])).To(Equal(rawRecordOf(agsRecords[shared])))
			expectEntryCSV(nationalCSV, shared, wantYucPowerDaily)
			expectEntryCSV(nationalCSV, "conagua/daily_observations/yuc/daily-31001.csv", wantMeridaDaily)
			expectEntryCSV(nationalCSV, "combined/combined_daily/ags/daily-1001.csv", wantAgsCombinedDaily)
			// CONAGUA's trace ("inappreciable") rain, 0.01 mm, reaches
			// the national files as 0.01 — a wet day — in the observed
			// file and through the combined join alike, and never as the
			// dry 0.0 a one-decimal export published.
			for _, name := range []string{
				"conagua/daily_observations/ags/daily-1001.csv",
				"combined/combined_daily/ags/daily-1001.csv",
			} {
				Expect(entryContent(nationalCSV, name)).To(ContainSubstring(",20.00,5.00,0.01,6.00"), name)
				Expect(entryContent(nationalCSV, name)).NotTo(ContainSubstring(",20.0,5.0,0.0,6.0"), name)
			}
		})

		It("regenerates the seven national tables as the union of both states' rows in national order, every column, and provenance/ once", func() {
			expectEntryCSV(nationalCSV, "conagua/stations.csv", wantNationalStations)
			expectEntryCSV(nationalCSV, "conagua/monthly_normals.csv", wantNationalNormals)
			expectEntryCSV(nationalCSV, "conagua/monthly_normals_extras.csv", wantNationalExtras)
			expectEntryCSV(nationalCSV, "nasa_power/cells.csv", wantNationalCells)
			expectEntryCSV(nationalCSV, "nasa_power/station_cell_map.csv", wantNationalCellMap)
			expectEntryCSV(nationalCSV, "nasa_power/monthly.csv", wantNationalPowerMonthly)
			expectEntryCSV(nationalCSV, "combined/combined_monthly.csv", wantNationalCombinedMonthly)
			expectEntryCSV(nationalCSV, "provenance/ingest_runs.csv", wantIngestRunsCSV)
			expectEntryCSV(nationalCSV, "provenance/power_runs.csv", wantPowerRunsCSV)
			// The stations of both states in one bytewise order.
			stations := entryContent(nationalCSV, "conagua/stations.csv")
			Expect(stations).To(HavePrefix(strings.Join(wantStationsHeader, ",") + "\n1001,"))
			Expect(stations).To(HaveSuffix("\n31019,PROGRESO,YUC" + strings.Repeat(",", 15) + "\n"))
			for _, e := range csvEntries(nationalCSV) {
				Expect(e.Content).NotTo(HavePrefix("\xef\xbb\xbf"), e.Name)
				Expect(e.Content).NotTo(ContainSubstring("\r"), e.Name)
				Expect(e.Content).To(HaveSuffix("\n"), e.Name)
			}
		})

		It("copies every station pair of both JSON archives raw into national-json.zip beside provenance/ once", func() {
			nationalJSON = readZipEntries(filepath.Join(out, "national-json.zip"))
			Expect(entryNames(nationalJSON)).To(Equal(wantNationalJSONPaths))
			national := zipRecords(filepath.Join(out, "national-json.zip"))
			agsRecords := zipRecords(filepath.Join(out, "ags-json.zip"))
			yucRecords := zipRecords(filepath.Join(out, "yuc-json.zip"))
			for _, name := range wantAgsJSONPaths {
				if strings.HasPrefix(name, "combined/") {
					expectRawCopy(national, agsRecords, name)
				}
			}
			for _, name := range wantYucJSONPaths {
				if strings.HasPrefix(name, "combined/") {
					expectRawCopy(national, yucRecords, name)
				}
			}
			for _, name := range []string{"provenance/ingest_runs.json", "provenance/power_runs.json"} {
				Expect(entryContent(nationalJSON, name)).To(Equal(entryContent(agsJSON, name)), name)
			}
			Expect(decodeJSONDoc(entryContent(nationalJSON, "combined/yuc/31001/profile.json"))).To(Equal(wantMeridaProfile()))
			Expect(entryContent(nationalJSON, "combined/yuc/31001/daily.json")).To(Equal(dailyDocFromCombined(wantMeridaCombinedDaily)))
		})

		It("regenerates national-parquet.zip's ten files, every column of every row equal to national-csv.zip's tables", func() {
			nationalPq = readZipEntries(filepath.Join(out, "national-parquet.zip"))
			var want []string
			for _, t := range wantParquetTables {
				want = append(want, t+".parquet")
			}
			slices.Sort(want)
			Expect(entryNames(nationalPq)).To(Equal(want))
			expectParquetEqualCSV(nationalPq, nationalCSV)
			// Pinned by hand: both states' stations in one order, the
			// per-cell table's cells in cell order.
			header, rows := parquetCells("conagua/stations.parquet", entryContent(nationalPq, "conagua/stations.parquet"))
			Expect(header).To(Equal(wantStationsHeader))
			Expect(rows).To(Equal(wantNationalStations[1:]))
			header, rows = parquetCells("nasa_power/daily.parquet", entryContent(nationalPq, "nasa_power/daily.parquet"))
			Expect(header).To(Equal(wantPowerDailyHeader))
			Expect(rows).To(Equal(slices.Concat(wantYucPowerDaily[1:], wantAgsPowerDaily[1:])))
		})

		It("ships bioclima.db in national-sqlite.zip: the full canonical database, every table equal to the source, stamped, in rollback-journal mode, the source's bytes unchanged", func() {
			entries := readZipEntries(filepath.Join(out, "national-sqlite.zip"))
			Expect(entryNames(entries)).To(Equal([]string{"bioclima.db"}))
			expectNationalDB(entries, dbPath)
			Expect(fileSHA(dbPath)).To(Equal(dbSHA))
		})

		It("ships the snapshot directory verbatim in conagua-raw-<date>.zip: exactly the fixture files, their bytes as on disk, snapshot-stamped", func() {
			entries := readZipEntries(filepath.Join(out, "conagua-raw-"+publishSnapshotDate+".zip"))
			Expect(entryNames(entries)).To(Equal(sortedKeys(rawFiles)))
			midnight := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
			for _, e := range entries {
				Expect(e.Content).To(Equal(rawFiles[e.Name]), e.Name)
				Expect(e.Modified).To(Equal(midnight), e.Name)
			}
		})

		It("writes a manifest.json listing the states, the national groups, and every archive, and a CHECKSUMS covering every top-level file", func() {
			m, text := readManifest(out)
			Expect(m.SnapshotDate).To(Equal(publishSnapshotDate))
			Expect(m.States).To(Equal([]publish.ManifestState{
				{Code: "AGS", Name: "Aguascalientes", Artifacts: []string{"ags-tabular.zip", "ags-json.zip"}},
				{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-tabular.zip", "yuc-json.zip"}},
			}))
			Expect(m.National).To(Equal([]publish.ManifestNational{
				{Group: "national-csv", Artifacts: []string{"national-csv.zip"}},
				{Group: "national-parquet", Artifacts: []string{"national-parquet.zip"}},
				{Group: "national-json", Artifacts: []string{"national-json.zip"}},
				{Group: "national-sqlite", Artifacts: []string{"national-sqlite.zip"}},
				{Group: "raw", Artifacts: []string{"conagua-raw-" + publishSnapshotDate + ".zip"}},
			}))
			Expect(m.Counts).To(Equal(wantPublishCounts))
			Expect(m.Runs.Ingest).To(Equal(wantPublishIngestRuns))
			expectPowerRuns(m.Runs.Power, wantPublishPowerRuns)
			sums := readChecksums(out)
			wantSums := slices.DeleteFunc(slices.Clone(wantFullRunDir), func(n string) bool { return n == "CHECKSUMS" })
			Expect(checksumNames(sums)).To(Equal(wantSums))
			var files []publish.ManifestFile
			for _, s := range sums {
				if s.Name == "manifest.json" {
					continue
				}
				files = append(files, publish.ManifestFile{Name: s.Name, SHA256: s.SHA256, Bytes: sizeOf(filepath.Join(out, s.Name))})
			}
			Expect(m.Files).To(Equal(files))
			Expect(strings.ToLower(text)).To(ContainSubstring("\"creator\": \"pablo trinidad\""))
			Expect(strings.ToLower(text)).To(ContainSubstring("\"creator_orcid\": \"0009-0007-4050-494x\""))
		})

		It("builds byte-identical archives on a second full run, the national SQLite archive included (byte-reproducible); manifest.json differs only in generated_at", func() {
			again := filepath.Join(outRoot, "again")
			Expect(runPublishToExit(dbPath, again, "--root", root).ExitCode()).To(Equal(0))
			Expect(fileSHA(dbPath)).To(Equal(dbSHA))
			Expect(listDir(again)).To(Equal(wantFullRunDir))
			for _, name := range wantFullRunDir {
				if !strings.HasSuffix(name, ".zip") {
					continue
				}
				first, second := readBytes(filepath.Join(out, name)), readBytes(filepath.Join(again, name))
				if name == "national-sqlite.zip" {
					GinkgoWriter.Printf("national-sqlite.zip twice: bytes identical = %t (%d bytes)\n", bytes.Equal(first, second), len(first))
				}
				Expect(second).To(Equal(first), name)
			}
			firstManifest := string(readBytes(filepath.Join(out, "manifest.json")))
			secondManifest := string(readBytes(filepath.Join(again, "manifest.json")))
			Expect(generatedAtRE.ReplaceAllLiteralString(secondManifest, "")).To(Equal(
				generatedAtRE.ReplaceAllLiteralString(firstManifest, "")))
		})

		It("exits 1 on a national group with --state, writing nothing", func() {
			bad := filepath.Join(outRoot, "national-subset")
			session := runPublishToExit(dbPath, bad, "--only", "national-csv", "--state", "yuc", "--root", root)
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(
				`error: artifact group "national-csv" is built only in a full run: drop --state` + "\n"))
			Expect(string(session.Out.Contents())).To(ContainSubstring("\npublish aborted\n"))
			_, err := os.Stat(bad)
			Expect(os.IsNotExist(err)).To(BeTrue())
			Expect(fileSHA(dbPath)).To(Equal(dbSHA))
		})

		It("exits 1 on the raw group when the snapshot directory is missing under --root, naming the path and the flag, writing nothing", func() {
			empty := filepath.Join(outRoot, "no-snapshots")
			bad := filepath.Join(outRoot, "raw-missing")
			session := runPublishToExit(dbPath, bad, "--only", "raw", "--root", empty)
			Expect(session.ExitCode()).To(Equal(1))
			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring("error: snapshot directory " + filepath.Join(empty, "conagua-raw", publishSnapshotDate) +
				" does not exist: the raw artifact ships the local pull output of snapshot " + publishSnapshotDate +
				" (--root points at the local snapshot root)\n"))
			Expect(publishArtifactLineRE.FindAllStringSubmatch(stderr, -1)).To(BeEmpty())
			Expect(string(session.Out.Contents())).To(ContainSubstring("\npublish aborted\n"))
			_, err := os.Stat(bad)
			Expect(os.IsNotExist(err)).To(BeTrue())
		})

		It("exits 1 on a subset build into the full deposit, naming the national archives among the foreign entries", func() {
			session := runPublishToExit(dbPath, out, "--state", "ags", "--root", root)
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(
				"error: out dir " + out + " holds entries this run does not produce: " +
					"CITATION.cff, DATA-DICTIONARY.json, DATA-DICTIONARY.md, LICENSE, NOTICE, QA-REPORT.md, README.md, " +
					"conagua-raw-" + publishSnapshotDate + ".zip, national-csv.zip, national-json.zip, national-parquet.zip, " +
					"national-sqlite.zip, yuc-json.zip, yuc-tabular.zip, zenodo-metadata.json (remove them or use a fresh --out)\n"))
			Expect(listDir(out)).To(Equal(wantFullRunDir))
		})
	})

	Context("when one state's archives cannot be written", func() {
		It("ships the other state, fails the copy-built national archives soft naming the state, still builds the SQLite and raw archives, and exits 2", func() {
			dbPath := seedPublishDB()
			// An infinite REAL cannot be written (the decimal formatter
			// refuses it in the CSV, the Parquet writer, and the daily.json
			// row alike), so both AGS archives fail inside their zip writes,
			// and so does the national Parquet archive at its first file;
			// the national copies have no AGS archive to copy from; the
			// SQLite copy carries the value as stored and the raw archive
			// reads no database. The fault sits in an observed value the
			// gate does not judge — an impossible coordinate would have
			// refused the run up front.
			db, err := schema.Open(dbPath)
			Expect(err).NotTo(HaveOccurred())
			_, err = db.Exec(`UPDATE daily_observations SET tmax = 9e999
			  WHERE station_id = (SELECT id FROM stations WHERE external_id = '1001')`)
			Expect(err).NotTo(HaveOccurred())
			Expect(db.Close()).To(Succeed())
			root := filepath.Join(GinkgoT().TempDir(), "snapshots")
			seedPublishSnapshot(root)

			out := filepath.Join(GinkgoT().TempDir(), "degraded")
			session := runPublishToExit(dbPath, out, "--root", root)
			Expect(session.ExitCode()).To(Equal(2))

			stderr := string(session.Err.Contents())
			expectGateLines(stderr, wantSeededGate)
			artifacts := publishArtifactLineRE.FindAllStringSubmatch(stderr, -1)
			Expect(artifacts).To(HaveLen(9))
			Expect(artifacts[0][1:3]).To(Equal([]string{"FAIL", "ags-tabular.zip"}))
			Expect(artifacts[0][6]).To(ContainSubstring(`entry "combined/combined_daily.parquet": station 1001: row 1: column tmax_c: non-finite value +Inf`))
			Expect(artifacts[1][1:3]).To(Equal([]string{"FAIL", "ags-json.zip"}))
			Expect(artifacts[1][6]).To(ContainSubstring(`entry "combined/ags/1001/daily.json": `))
			Expect(artifacts[1][6]).To(ContainSubstring("column tmax_c: non-finite value +Inf"))
			Expect(artifacts[2][1:3]).To(Equal([]string{"ok", "yuc-tabular.zip"}))
			Expect(artifacts[3][1:3]).To(Equal([]string{"ok", "yuc-json.zip"}))
			Expect(artifacts[4][1:3]).To(Equal([]string{"FAIL", "national-csv.zip"}))
			Expect(artifacts[4][6]).To(Equal("state AGS archive ags-tabular.zip failed: national-csv.zip needs every state's tabular archive"))
			Expect(artifacts[5][1:3]).To(Equal([]string{"FAIL", "national-parquet.zip"}))
			Expect(artifacts[5][6]).To(ContainSubstring(`entry "combined/combined_daily.parquet": `))
			Expect(artifacts[5][6]).To(ContainSubstring("column tmax_c: non-finite value +Inf"))
			Expect(artifacts[6][1:3]).To(Equal([]string{"FAIL", "national-json.zip"}))
			Expect(artifacts[6][6]).To(Equal("state AGS archive ags-json.zip failed: national-json.zip needs every state's json archive"))
			Expect(artifacts[7][1:3]).To(Equal([]string{"ok", "national-sqlite.zip"}))
			Expect(artifacts[8][1:3]).To(Equal([]string{"ok", "conagua-raw-" + publishSnapshotDate + ".zip"}))
			files := publishFileLineRE.FindAllStringSubmatch(stderr, -1)
			Expect(files).To(HaveLen(len(wantDocsOrder)))
			for i, name := range wantDocsOrder {
				Expect(files[i][1:3]).To(Equal([]string{"ok", name}), name)
			}
			Expect(stderr).To(ContainSubstring("error: publish degraded: 5 of 17 artifacts failed (see report)\n"))
			// ags.db, the entry ahead of the Parquet twin that fails, still
			// reports over the archive's full unit count; Yucatán's
			// archives report in full; the copies never start; the
			// database and the snapshot children report.
			units := publishUnitLineRE.FindAllStringSubmatch(stderr, -1)
			Expect(units).To(HaveLen(1 + len(wantYucUnits) + len(wantYucJSONUnits) + 1 + len(wantRawUnits)))
			expectUnitLines(units[:1], "ags-tabular.zip", wantAgsUnits[:1], len(wantAgsUnits))
			next := 1
			expectUnitLines(units[next:next+len(wantYucUnits)], "yuc-tabular.zip", wantYucUnits, len(wantYucUnits))
			next += len(wantYucUnits)
			expectUnitLines(units[next:next+len(wantYucJSONUnits)], "yuc-json.zip", wantYucJSONUnits, len(wantYucJSONUnits))
			next += len(wantYucJSONUnits)
			expectUnitLines(units[next:next+1], "national-sqlite.zip", wantNationalSQLiteUnits, 1)
			next++
			expectUnitLines(units[next:], "conagua-raw-"+publishSnapshotDate+".zip", wantRawUnits, len(wantRawUnits))

			stdout := string(session.Out.Contents())
			Expect(stdout).To(ContainSubstring("\npublish complete\n"))
			Expect(stdout).To(ContainSubstring("  gate                 : passed · 9 rules · 2 warn · 0 error\n"))
			Expect(stdout).To(ContainSubstring("  artifacts            : 12 ok / 5 failed / 17 attempted\n"))
			Expect(stdout).To(ContainSubstring("  top-level files      : 14\n"))
			Expect(stdout).To(ContainSubstring("  failed artifacts:\n    ags-tabular.zip: write zip " +
				filepath.Join(out, "ags-tabular.zip") + `: entry "combined/combined_daily.parquet": station 1001: row 1: `))
			Expect(stdout).To(ContainSubstring("\n    national-csv.zip: state AGS archive ags-tabular.zip failed: " +
				"national-csv.zip needs every state's tabular archive\n"))
			Expect(stdout).To(ContainSubstring("\n    national-json.zip: state AGS archive ags-json.zip failed: " +
				"national-json.zip needs every state's json archive\n"))

			// Nothing partial at the failed paths; the manifest is honest
			// about the state and the groups that did not ship.
			Expect(listDir(out)).To(Equal([]string{
				"CHECKSUMS", "CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md",
				"README.md", "conagua-raw-" + publishSnapshotDate + ".zip", "manifest.json", "national-sqlite.zip",
				"yuc-json.zip", "yuc-tabular.zip", "zenodo-metadata.json",
			}))
			expectNoTempResidue(out)
			Expect(checksumNames(readChecksums(out))).To(Equal([]string{
				"CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md",
				"README.md", "conagua-raw-" + publishSnapshotDate + ".zip", "manifest.json", "national-sqlite.zip",
				"yuc-json.zip", "yuc-tabular.zip", "zenodo-metadata.json",
			}))
			m, _ := readManifest(out)
			Expect(m.States).To(Equal([]publish.ManifestState{
				{Code: "AGS", Name: "Aguascalientes", Artifacts: []string{}},
				{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-tabular.zip", "yuc-json.zip"}},
			}))
			Expect(m.National).To(Equal([]publish.ManifestNational{
				{Group: "national-csv", Artifacts: []string{}},
				{Group: "national-parquet", Artifacts: []string{}},
				{Group: "national-json", Artifacts: []string{}},
				{Group: "national-sqlite", Artifacts: []string{"national-sqlite.zip"}},
				{Group: "raw", Artifacts: []string{"conagua-raw-" + publishSnapshotDate + ".zip"}},
			}))
			Expect(m.Files).To(HaveLen(4 + len(wantDocsOrder)))
			// The shipped database carries the infinite value as stored.
			entries := readZipEntries(filepath.Join(out, "national-sqlite.zip"))
			expectNationalDB(entries, dbPath)
		})
	})
})
