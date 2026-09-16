package publish_test

// Specs for national order when station ids and cell ids interleave
// across states bytewise: an Aguascalientes station whose id sorts after
// every Yucatán and Zacatecas id, a Zacatecas one after that, and an
// Aguascalientes cell that sorts before every Yucatán cell. The national
// station and cell lists, the seven whole-scope tables, and the ten
// Parquet files follow the exported primary key bytewise — AGS, AGS,
// YUC ×4, ZAC, AGS, ZAC — never grouped by state, with every column of
// the added rows round-tripped by hand; the copied per-unit CSV files
// stay under their state shard, so path order and key order differ by
// design, and the copy units fire in the archives' last-copy order.

import (
	"context"
	"database/sql"
	"path/filepath"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// cellRincon is the Aguascalientes cell that sorts first nationally:
// "20.5N…" precedes every "21.…" Yucatán cell and the "22.0N…" cell.
const cellRincon = "20.5N_102.1250W"

// seedInterleaved adds to seedTwoStates + seedPower + seedNational the
// rows whose keys interleave the states: Aguascalientes station 4001
// (every column set, one normals period over two months, an extras
// row, two daily rows, its own cell with a monthly and a daily row) and
// Zacatecas station 9001 (a normals row and a daily row, no cell).
func seedInterleaved(db *sql.DB, ids map[string]int64) {
	GinkgoHelper()
	conv := ingest.SourceConaguaConventional
	ids["conv/4001"] = upsertStation(db, ingest.StationUpsert{
		Source: conv, ExternalID: "4001", Name: "Rincón de Romos", State: "AGS",
		Municipality: "Rincón de Romos", Lat: f64(22.2333), Lon: f64(-102.3167), AltitudeM: f64(1950.5),
		Status: "operating",
	})
	mustExec(db, `UPDATE stations SET first_year = 1970, last_year = 2010, wmo_completeness_bin_1981_2010 = 0.75
	  WHERE id = ?`, ids["conv/4001"])
	ids["conv/9001"] = upsertStation(db, ingest.StationUpsert{
		Source: conv, ExternalID: "9001", Name: "Sombrerete", State: "ZAC",
	})

	insertCell(db, cellRincon, 20.5, -102.125)
	insertStationCell(db, ids["conv/4001"], cellRincon, 7.25)
	insertPowerMonthly(db, cellRincon, "1981-2010", 1, powerSeqRow(50))
	insertPowerDaily(db, cellRincon, "2020-01-01", powerSeqRow(150))

	insertNormals(db, ids["conv/4001"], "1981-2010", 12, 24.1, 3.2, 13.6, 5.0, 120.4)
	insertNormals(db, ids["conv/4001"], "1981-2010", 1, 27.5, 8.5, 18.0, 12.3, 175.0)
	insertNormals(db, ids["conv/9001"], "1971-2000", 7, 26.0, 12.0, 19.0, 88.8, 160.0)
	insertExtras(db, ids["conv/4001"], "1981-2010", 1,
		36.0, 1990, 39.5, "1990-05-20",
		-2.0, 1985, -5.5, "1985-01-10",
		150.0, 1992, 80.0, "1992-08-01",
		25, 25, 24, 28, 20, 3.0, 27)
	insertDaily(db, ids["conv/4001"], "2020-01-02", 22.0, 4.0, 0.0, 5.5)
	insertDaily(db, ids["conv/4001"], "2020-01-01", 21.5, 3.5, 2.5, nil)
	insertDaily(db, ids["conv/9001"], "1988-08-08", 30.0, 15.0, 10.0, 8.0)
}

// The national key order the interleaving fixture must yield.
var (
	interleavedStations = []string{"1001", "1002", "31001", "31002", "31003", "3101", "32001", "4001", "9001"}
	interleavedCells    = []string{cellRincon, cellSingle, cellShared, cellAGS}
)

var _ = Describe("national order across interleaving states", func() {
	var (
		ctx = context.Background()
		db  *sql.DB
		out string
		rec *recorder
	)

	BeforeEach(func() {
		db = openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		seedNational(db, ids)
		seedInterleaved(db, ids)
		out = filepath.Join(GinkgoT().TempDir(), "publish")
		rec = &recorder{}
		report, err := publish.Run(ctx, db, publish.Options{OutDir: out, Now: fixedClock,
			Only: []string{"tabular", "national-csv", "national-parquet"}}, rec.record)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
	})

	It("lists the stations and cells in one bytewise order, the states interleaved", func() {
		stations, err := publish.NationalStationIDs(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		Expect(stations).To(Equal(interleavedStations))
		cells, err := publish.NationalCellIDs(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		Expect(cells).To(Equal(interleavedCells))
		// The fixture interleaves: a grouping by state would put 4001
		// beside 1002 and Rincón's cell beside Aguascalientes'.
		stateOf := map[string]string{"1001": "AGS", "1002": "AGS", "31001": "YUC", "31002": "YUC", "31003": "YUC",
			"3101": "YUC", "32001": "ZAC", "4001": "AGS", "9001": "ZAC"}
		var sequence []string
		for _, id := range stations {
			sequence = append(sequence, stateOf[id])
		}
		Expect(sequence).To(Equal([]string{"AGS", "AGS", "YUC", "YUC", "YUC", "YUC", "ZAC", "AGS", "ZAC"}))
	})

	It("regenerates the seven national tables in that order, every column of the added rows round-tripped", func() {
		_, national := zipContents(filepath.Join(out, "national-csv.zip"))

		Expect(records(national["conagua/stations.csv"])).To(Equal([][]string{
			publish.Stations.Header(),
			{"1001", "Aguascalientes (OBS)", "AGS", "", "21.880000", "-102.300000", "1878.0", "operating",
				"", "", "", "", "", "", "", "", "", ""},
			{"1002", "Calvillo", "AGS", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
			{"31001", `Mérida, "La Plancha"`, "YUC", "Mérida", "21.850278", "-89.375000", "9.0", "operating",
				"1951", "2026", "0.9722", "0.5000", "", "1.0000", "0.9324", "0.0000", "", "0.0009"},
			{"31002", "Tizimín", "YUC", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
			{"31003", "Progreso", "YUC", "", "21.280000", "-89.660000", "", "", "", "", "", "", "", "", "", "", "", ""},
			{"3101", "Valladolid", "YUC", "Valladolid", "20.689100", "-88.201100", "25.5", "suspended",
				"1961", "1995", "0.2500", "", "", "", "", "", "", ""},
			{"32001", "Zacatecas (OBS)", "ZAC", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
			{"4001", "Rincón de Romos", "AGS", "Rincón de Romos", "22.233300", "-102.316700", "1950.5", "operating",
				"1970", "2010", "", "", "0.7500", "", "", "", "", ""},
			{"9001", "Sombrerete", "ZAC", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
		}))
		Expect(records(national["conagua/monthly_normals.csv"])).To(Equal([][]string{
			publish.MonthlyNormals.Header(),
			{"1001", "1981-2010", "1", "29.9", "9.9", "19.9", "20.0", "200.0"},
			{"1002", "1981-2010", "1", "28.0", "12.0", "20.0", "15.0", "100.0"},
			{"31001", "1981-2010", "1", "33.4", "17.9", "25.7", "28.3", "141.6"},
			{"31001", "1981-2010", "12", "30.1", "16.2", "", "24.5", "110.3"},
			{"31001", "1991-2020", "1", "33.0", "18.3", "25.8", "0.0", "150.2"},
			{"31001", "1991-2020", "12", "", "", "", "", ""},
			{"3101", "1961-1990", "6", "35.9", "22.1", "29.0", "152.7", "190.4"},
			{"32001", "1981-2010", "3", "21.0", "6.0", "13.5", "9.5", "155.5"},
			{"4001", "1981-2010", "1", "27.5", "8.5", "18.0", "12.3", "175.0"},
			{"4001", "1981-2010", "12", "24.1", "3.2", "13.6", "5.0", "120.4"},
			{"9001", "1971-2000", "7", "26.0", "12.0", "19.0", "88.8", "160.0"},
		}))
		extras := records(national["conagua/monthly_normals_extras.csv"])
		Expect(extras[0]).To(Equal(publish.MonthlyNormalsExtras.Header()))
		Expect(keyCells(extras[1:], 3)).To(Equal([][]string{
			{"1001", "1981-2010", "1"},
			{"31001", "1981-2010", "1"}, {"31001", "1981-2010", "12"}, {"31001", "1991-2020", "1"},
			{"3101", "1961-1990", "6"},
			{"4001", "1981-2010", "1"},
		}))
		// The monthly extremes and rain days terminate at one decimal;
		// the three *_daily_extreme columns are single daily readings and
		// carry the daily grain's two.
		Expect(extras[6]).To(Equal([]string{"4001", "1981-2010", "1",
			"36.0", "1990", "39.50", "1990-05-20",
			"-2.0", "1985", "-5.50", "1985-01-10",
			"150.0", "1992", "80.00", "1992-08-01",
			"25", "25", "24", "28", "20", "3.0", "27"}))

		Expect(records(national["nasa_power/cells.csv"])).To(Equal([][]string{
			publish.Cells.Header(),
			{cellRincon, "20.500", "-102.125"},
			{cellSingle, "21.000", "-89.625"},
			{cellShared, "21.500", "-89.375"},
			{cellAGS, "22.000", "-102.500"},
		}))
		Expect(records(national["nasa_power/station_cell_map.csv"])).To(Equal([][]string{
			publish.StationCellMap.Header(),
			{"1001", cellAGS, "3.000"},
			{"1002", cellShared, "40.000"},
			{"31001", cellShared, "27.252"},
			{"31003", cellSingle, "12.500"},
			{"3101", cellShared, "0.000"},
			{"4001", cellRincon, "7.250"},
		}))
		Expect(records(national["nasa_power/monthly.csv"])).To(Equal([][]string{
			publish.PowerMonthly.Header(),
			withPower([]string{cellRincon, "1981-2010", "1"}, powerWant(powerSeqRow(50))),
			withPower([]string{cellSingle, "1981-2010", "6"}, powerWant(noRHPowerRow)),
			withPower([]string{cellShared, "1981-2010", "1"}, powerWant(fullPowerRow)),
			withPower([]string{cellShared, "1981-2010", "12"}, powerWant(allNullPowerRow)),
			withPower([]string{cellShared, "1991-2020", "1"}, powerWant(mixedPowerRow)),
			withPower([]string{cellAGS, "1981-2010", "1"}, powerWant(fullPowerRow)),
		}))
		Expect(records(national["combined/combined_monthly.csv"])).To(Equal([][]string{
			publish.CombinedMonthly.Header(),
			withPower([]string{"1001", "1981-2010", "1", cellAGS, "3.000", "29.9", "9.9", "19.9", "20.0", "200.0"}, powerWant(fullPowerRow)),
			withPower([]string{"1002", "1981-2010", "1", cellShared, "40.000", "28.0", "12.0", "20.0", "15.0", "100.0"}, powerWant(fullPowerRow)),
			withPower([]string{"31001", "1981-2010", "1", cellShared, "27.252", "33.4", "17.9", "25.7", "28.3", "141.6"}, powerWant(fullPowerRow)),
			withPower([]string{"31001", "1981-2010", "12", cellShared, "27.252", "30.1", "16.2", "", "24.5", "110.3"}, powerWant(allNullPowerRow)),
			withPower([]string{"31001", "1991-2020", "1", cellShared, "27.252", "33.0", "18.3", "25.8", "0.0", "150.2"}, powerWant(mixedPowerRow)),
			withPower([]string{"31001", "1991-2020", "12", cellShared, "27.252", "", "", "", "", ""}, noPower),
			withPower([]string{"3101", "1961-1990", "6", cellShared, "0.000", "35.9", "22.1", "29.0", "152.7", "190.4"}, noPower),
			withPower([]string{"32001", "1981-2010", "3", "", "", "21.0", "6.0", "13.5", "9.5", "155.5"}, noPower),
			withPower([]string{"4001", "1981-2010", "1", cellRincon, "7.250", "27.5", "8.5", "18.0", "12.3", "175.0"}, powerWant(powerSeqRow(50))),
			withPower([]string{"4001", "1981-2010", "12", cellRincon, "7.250", "24.1", "3.2", "13.6", "5.0", "120.4"}, noPower),
			withPower([]string{"9001", "1971-2000", "7", "", "", "26.0", "12.0", "19.0", "88.8", "160.0"}, noPower),
		}))
	})

	It("keeps each state's own tables to its rows, in the same bytewise order within the state", func() {
		_, ags := zipContents(filepath.Join(out, "ags-tabular.zip"))
		Expect(firstKeyOf(records(ags["conagua/stations.csv"])[1:])).To(Equal([]string{"1001", "1002", "4001"}))
		Expect(firstKeyOf(records(ags["nasa_power/cells.csv"])[1:])).To(Equal([]string{cellRincon, cellShared, cellAGS}))
		_, zac := zipContents(filepath.Join(out, "zac-tabular.zip"))
		Expect(firstKeyOf(records(zac["conagua/stations.csv"])[1:])).To(Equal([]string{"32001", "9001"}))
		Expect(records(zac["nasa_power/cells.csv"])).To(Equal([][]string{publish.Cells.Header()}))
	})

	It("copies the per-unit files under their state shards — path order, not key order — and fires the units in last-copy order", func() {
		names, _ := zipContents(filepath.Join(out, "national-csv.zip"))
		var copied []string
		for _, n := range names {
			if isPerUnit(n) {
				copied = append(copied, n)
			}
		}
		Expect(copied).To(Equal([]string{
			"combined/combined_daily/ags/daily-1001.csv",
			"combined/combined_daily/ags/daily-1002.csv",
			"combined/combined_daily/ags/daily-4001.csv",
			"combined/combined_daily/yuc/daily-31001.csv",
			"combined/combined_daily/yuc/daily-31002.csv",
			"combined/combined_daily/yuc/daily-31003.csv",
			"combined/combined_daily/yuc/daily-3101.csv",
			"combined/combined_daily/zac/daily-32001.csv",
			"combined/combined_daily/zac/daily-9001.csv",
			"conagua/daily_observations/ags/daily-1001.csv",
			"conagua/daily_observations/ags/daily-1002.csv",
			"conagua/daily_observations/ags/daily-4001.csv",
			"conagua/daily_observations/yuc/daily-31001.csv",
			"conagua/daily_observations/yuc/daily-31002.csv",
			"conagua/daily_observations/yuc/daily-31003.csv",
			"conagua/daily_observations/yuc/daily-3101.csv",
			"conagua/daily_observations/zac/daily-32001.csv",
			"conagua/daily_observations/zac/daily-9001.csv",
			"nasa_power/daily/daily-" + cellRincon + ".csv",
			"nasa_power/daily/daily-" + cellSingle + ".csv",
			"nasa_power/daily/daily-" + cellShared + ".csv",
			"nasa_power/daily/daily-" + cellAGS + ".csv",
		}))
		// Zacatecas' last copy is under conagua/, Yucatán's is its own
		// cell, Aguascalientes' is the cell that sorts last.
		Expect(eventsOf(rec, "national-csv.zip")).To(Equal(append(
			unitEvents("national-csv.zip", "zac-tabular.zip", "yuc-tabular.zip", "ags-tabular.zip"),
			publish.ProgressEvent{Artifact: "national-csv.zip"})))
	})

	It("regenerates the ten Parquet files in national key order, the per-unit tables the shards in that order", func() {
		_, pq := zipContents(filepath.Join(out, "national-parquet.zip"))
		_, csv := zipContents(filepath.Join(out, "national-csv.zip"))
		table := func(name string, spec publish.FileSpec) [][]string {
			GinkgoHelper()
			var rows [][]string
			for _, row := range readParquetRows(openParquet(pq[name])) {
				rows = append(rows, parquetCells(spec, row))
			}
			return rows
		}
		Expect(keyCells(table("conagua/stations.parquet", publish.Stations), 1)).To(Equal(
			keyCells(records(csv["conagua/stations.csv"])[1:], 1)))
		Expect(firstKeyOf(table("conagua/stations.parquet", publish.Stations))).To(Equal(interleavedStations))
		Expect(firstKeyOf(table("nasa_power/cells.parquet", publish.Cells))).To(Equal(interleavedCells))

		daily := table("conagua/daily_observations.parquet", publish.DailyObservations)
		Expect(keyCells(daily, 2)).To(Equal([][]string{
			{"1001", "2020-01-01"}, {"1002", "2020-01-01"},
			{"31001", "1999-12-31"}, {"31001", "2020-01-01"}, {"31001", "2020-01-02"},
			{"3101", "1975-06-15"}, {"32001", "2020-01-01"},
			{"4001", "2020-01-01"}, {"4001", "2020-01-02"},
			{"9001", "1988-08-08"},
		}))
		Expect(daily[7]).To(Equal([]string{"4001", "2020-01-01", "21.50", "3.50", "2.50", ""}))
		Expect(daily[9]).To(Equal([]string{"9001", "1988-08-08", "30.00", "15.00", "10.00", "8.00"}))

		powerDaily := table("nasa_power/daily.parquet", publish.PowerDaily)
		Expect(keyCells(powerDaily, 2)).To(Equal([][]string{
			{cellRincon, "2020-01-01"},
			{cellShared, "1999-12-31"}, {cellShared, "2020-01-01"}, {cellShared, "2020-01-02"},
			{cellAGS, "2020-01-01"},
		}))
		Expect(powerDaily[0]).To(Equal(withPower([]string{cellRincon, "2020-01-01"}, powerWant(powerSeqRow(150)))))

		combined := table("combined/combined_daily.parquet", publish.CombinedDaily.FileSpec)
		Expect(keyCells(combined, 2)).To(Equal(keyCells(daily, 2)))
		Expect(combined[7]).To(Equal(withPower(
			[]string{"4001", "2020-01-01", cellRincon, "7.250", "21.50", "3.50", "2.50", ""}, powerWant(powerSeqRow(150)))))
		Expect(combined[8]).To(Equal(withPower(
			[]string{"4001", "2020-01-02", cellRincon, "7.250", "22.00", "4.00", "0.00", "5.50"}, noPower)))
		Expect(combined[9]).To(Equal(withPower(
			[]string{"9001", "1988-08-08", "", "", "30.00", "15.00", "10.00", "8.00"}, noPower)))

		// The copied CSV shards, read back in national unit order, are
		// the Parquet rows.
		var shards [][]string
		for _, id := range interleavedStations {
			slug := map[string]string{"1001": "ags", "1002": "ags", "4001": "ags", "32001": "zac", "9001": "zac"}[id]
			if slug == "" {
				slug = "yuc"
			}
			shards = append(shards, records(csv["combined/combined_daily/"+slug+"/daily-"+id+".csv"])[1:]...)
		}
		Expect(combined).To(Equal(shards))
		Expect(slices.IsSorted(firstKeyOf(combined))).To(BeTrue())
		Expect(strings.Compare("31001", "3101")).To(BeNumerically("<", 0), "bytewise, not numeric")
	})
})
