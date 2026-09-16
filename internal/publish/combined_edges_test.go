package publish_test

// Specs at the edges of the combined/ LEFT join: a station on a cell
// that has no supplement row at all, or none in the station's own
// dates; a normals key that other cells carry a POWER row for but this
// station's cell does not; POWER dates the station never observed
// — which must never surface, the spine being observed dates only, so
// the row count is held to the observed count read back from the DB;
// and a state whose stations reference no cell, whose combined/ files
// keep every spine row with the join context empty throughout.

import (
	"context"
	"database/sql"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// cellBare is a cell with no row in either supplement table.
const cellBare = "20.5N_90.0000W"

// countRows returns the one integer a COUNT(*) query yields.
func countRows(db *sql.DB, query string, args ...any) int {
	GinkgoHelper()
	var n int
	Expect(db.QueryRowContext(context.Background(), query, args...).Scan(&n)).To(Succeed())
	return n
}

// column returns one column of the data rows (header excluded).
func column(rows [][]string, i int) []string {
	out := make([]string, 0, len(rows)-1)
	for _, r := range rows[1:] {
		out = append(out, r[i])
	}
	return out
}

var _ = Describe("CombinedEntries at the combined LEFT join edges", func() {
	var (
		db      *sql.DB
		ids     map[string]int64
		entries []archive.Entry
	)

	BeforeEach(func() {
		db = openTempDB()
		ids = seedTwoStates(db)
		seedPower(db, ids)

		// 31003 sits on cellSingle — one monthly row, (1981-2010, 6), and
		// no daily row — and observes three dates cellShared has rows
		// for, so a join on date alone would land another cell's rows.
		insertDaily(db, ids["conv/31003"], "2020-01-02", 29.0, 18.0, 1.5, 3.3)
		insertDaily(db, ids["conv/31003"], "2020-01-01", 28.5, 17.5, 0.0, 3.0)
		insertDaily(db, ids["conv/31003"], "1999-12-31", 27.0, 16.0, 10.2, 2.1)
		insertNormals(db, ids["conv/31003"], "1981-2010", 6, 34.0, 23.0, 28.5, 120.0, 170.0)
		insertNormals(db, ids["conv/31003"], "1981-2010", 1, 31.0, 19.0, 25.0, 30.0, 140.0)

		// 31004 sits on a cell with no supplement row of either kind.
		ids["conv/31004"] = upsertStation(db, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "31004", Name: "Motul", State: "YUC",
		})
		insertCell(db, cellBare, 20.5, -90.0)
		insertStationCell(db, ids["conv/31004"], cellBare, 8.125)
		insertNormals(db, ids["conv/31004"], "1991-2020", 1, 32.5, 18.0, 25.2, 26.0, 130.0)
		insertNormals(db, ids["conv/31004"], "1981-2010", 1, 32.0, 17.5, 24.8, 25.0, 128.0)
		insertDaily(db, ids["conv/31004"], "2020-01-01", 30.0, 17.0, 0.0, 5.0)

		// cellShared gains daily rows on dates none of its stations
		// observed: before, between, and after the observed ones.
		insertPowerDaily(db, cellShared, "2020-01-03", fullPowerRow)
		insertPowerDaily(db, cellShared, "1999-12-30", fullPowerRow)
		insertPowerDaily(db, cellShared, "1981-01-01", fullPowerRow)

		var err error
		entries, err = publish.CombinedEntries(context.Background(), db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())
	})

	It("keeps every observed row of a station whose cell has no daily row, join context set and POWER-31 empty, the count held to the observed count", func() {
		// The premise: the cell has no daily row, while another cell has
		// rows on exactly these dates.
		Expect(countRows(db, `SELECT COUNT(*) FROM daily_supplement WHERE cell_id = ?`, cellSingle)).To(BeZero())
		Expect(countRows(db, `SELECT COUNT(*) FROM daily_supplement WHERE cell_id = ?
		  AND date IN ('1999-12-31', '2020-01-01', '2020-01-02')`, cellShared)).To(Equal(3))

		got := records(render(entryByPath(entries, "combined/combined_daily/yuc/daily-31003.csv")))
		Expect(got).To(Equal([][]string{
			publish.CombinedDaily.Header(),
			withPower([]string{"31003", "1999-12-31", cellSingle, "12.500", "27.00", "16.00", "10.20", "2.10"}, noPower),
			withPower([]string{"31003", "2020-01-01", cellSingle, "12.500", "28.50", "17.50", "0.00", "3.00"}, noPower),
			withPower([]string{"31003", "2020-01-02", cellSingle, "12.500", "29.00", "18.00", "1.50", "3.30"}, noPower),
		}))
		Expect(len(got) - 1).To(Equal(countRows(db,
			`SELECT COUNT(*) FROM daily_observations WHERE station_id = ?`, ids["conv/31003"])))
	})

	It("never emits a POWER date the station did not observe: the spine is the observed dates, exactly", func() {
		Expect(countRows(db, `SELECT COUNT(*) FROM daily_supplement WHERE cell_id = ?`, cellShared)).To(Equal(6))

		merida := records(render(entryByPath(entries, "combined/combined_daily/yuc/daily-31001.csv")))
		Expect(merida).To(Equal([][]string{
			publish.CombinedDaily.Header(),
			withPower([]string{"31001", "1999-12-31", cellShared, "27.252", "", "-0.04", "", ""}, powerWant(mixedPowerRow)),
			withPower([]string{"31001", "2020-01-01", cellShared, "27.252", "30.50", "", "12.40", ""}, powerWant(fullPowerRow)),
			withPower([]string{"31001", "2020-01-02", cellShared, "27.252", "31.00", "19.50", "0.00", "4.20"}, powerWant(allNullPowerRow)),
		}))
		Expect(len(merida) - 1).To(Equal(countRows(db,
			`SELECT COUNT(*) FROM daily_observations WHERE station_id = ?`, ids["conv/31001"])))
		Expect(column(merida, 1)).NotTo(ContainElements("1981-01-01", "1999-12-30", "2020-01-03"))

		// The other station on the same cell observed one date, before
		// POWER begins: one row, and none of the cell's six.
		valladolid := records(render(entryByPath(entries, "combined/combined_daily/yuc/daily-3101.csv")))
		Expect(valladolid).To(Equal([][]string{
			publish.CombinedDaily.Header(),
			withPower([]string{"3101", "1975-06-15", cellShared, "0.000", "36.20", "22.00", "45.10", "7.30"}, noPower),
		}))
		Expect(len(valladolid) - 1).To(Equal(countRows(db,
			`SELECT COUNT(*) FROM daily_observations WHERE station_id = ?`, ids["conv/3101"])))
	})

	It("leaves POWER-31 empty on a normals key other cells carry a row for but this station's cell does not, and fills it from the cell's own row", func() {
		// The premise: four cells carry (1981-2010, 1); cellSingle does
		// not, and carries (1981-2010, 6) instead.
		Expect(countRows(db, `SELECT COUNT(*) FROM monthly_supplement WHERE period = '1981-2010' AND month = 1`)).To(Equal(4))
		Expect(countRows(db, `SELECT COUNT(*) FROM monthly_supplement WHERE cell_id = ? AND period = '1981-2010' AND month = 1`,
			cellSingle)).To(BeZero())
		Expect(countRows(db, `SELECT COUNT(*) FROM monthly_supplement WHERE cell_id = ?`, cellBare)).To(BeZero())

		got := records(render(entryByPath(entries, "combined/combined_monthly.csv")))
		Expect(got).To(Equal([][]string{
			publish.CombinedMonthly.Header(),
			withPower([]string{"31001", "1981-2010", "1", cellShared, "27.252", "33.4", "17.9", "25.7", "28.3", "141.6"}, powerWant(fullPowerRow)),
			withPower([]string{"31001", "1981-2010", "12", cellShared, "27.252", "30.1", "16.2", "", "24.5", "110.3"}, powerWant(allNullPowerRow)),
			withPower([]string{"31001", "1991-2020", "1", cellShared, "27.252", "33.0", "18.3", "25.8", "0.0", "150.2"}, powerWant(mixedPowerRow)),
			withPower([]string{"31001", "1991-2020", "12", cellShared, "27.252", "", "", "", "", ""}, noPower),
			withPower([]string{"31003", "1981-2010", "1", cellSingle, "12.500", "31.0", "19.0", "25.0", "30.0", "140.0"}, noPower),
			withPower([]string{"31003", "1981-2010", "6", cellSingle, "12.500", "34.0", "23.0", "28.5", "120.0", "170.0"}, powerWant(noRHPowerRow)),
			withPower([]string{"31004", "1981-2010", "1", cellBare, "8.125", "32.0", "17.5", "24.8", "25.0", "128.0"}, noPower),
			withPower([]string{"31004", "1991-2020", "1", cellBare, "8.125", "32.5", "18.0", "25.2", "26.0", "130.0"}, noPower),
			withPower([]string{"3101", "1961-1990", "6", cellShared, "0.000", "35.9", "22.1", "29.0", "152.7", "190.4"}, noPower),
		}))
		Expect(len(got) - 1).To(Equal(countRows(db, `SELECT COUNT(*) FROM monthly_normals n
		  JOIN stations s ON s.id = n.station_id WHERE s.state = 'YUC' AND s.source = 'conagua_conventional'`)))
	})

	It("carries the join context on every row of a station whose cell has no supplement row of either kind", func() {
		got := records(render(entryByPath(entries, "combined/combined_daily/yuc/daily-31004.csv")))
		Expect(got).To(Equal([][]string{
			publish.CombinedDaily.Header(),
			withPower([]string{"31004", "2020-01-01", cellBare, "8.125", "30.00", "17.00", "0.00", "5.00"}, noPower),
		}))
	})
})

var _ = Describe("a state whose stations reference no cell", func() {
	var (
		db  *sql.DB
		ids map[string]int64
	)
	zacatecas := publish.State{Code: "ZAC", Slug: "zac", Name: "Zacatecas"}

	BeforeEach(func() {
		db = openTempDB()
		ids = seedTwoStates(db)
		seedPower(db, ids)
		insertNormals(db, ids["conv/32001"], "1981-2010", 3, 21.0, 6.0, 13.5, 9.5, 155.5)
		insertNormals(db, ids["conv/32001"], "1961-1990", 3, 20.5, 5.5, 13.0, 8.0, 150.0)
		insertDaily(db, ids["conv/32001"], "2020-01-01", 18.0, 2.0, 0.0, 4.4)
		insertDaily(db, ids["conv/32001"], "1990-07-04", 25.5, 12.5, 30.1, nil)
		Expect(countRows(db, `SELECT COUNT(*) FROM station_power_cell m JOIN stations s ON s.id = m.station_id
		  WHERE s.state = ?`, zacatecas.Code)).To(BeZero())
	})

	It("writes the three nasa_power/ tables header-only and no per-cell file", func() {
		entries, err := publish.PowerEntries(context.Background(), db, zacatecas, nil)
		Expect(err).NotTo(HaveOccurred())
		var paths []string
		for _, e := range entries {
			paths = append(paths, e.Path)
		}
		Expect(paths).To(Equal([]string{
			"nasa_power/cells.csv",
			"nasa_power/monthly.csv",
			"nasa_power/station_cell_map.csv",
		}))
		Expect(records(render(entries[0]))).To(Equal([][]string{publish.Cells.Header()}))
		Expect(records(render(entries[1]))).To(Equal([][]string{publish.PowerMonthly.Header()}))
		Expect(records(render(entries[2]))).To(Equal([][]string{publish.StationCellMap.Header()}))
	})

	It("keeps every spine row of combined/ with the join context and POWER-31 empty throughout", func() {
		entries, err := publish.CombinedEntries(context.Background(), db, zacatecas, nil)
		Expect(err).NotTo(HaveOccurred())
		var paths []string
		for _, e := range entries {
			paths = append(paths, e.Path)
		}
		Expect(paths).To(Equal([]string{
			"combined/combined_daily/zac/daily-32001.csv",
			"combined/combined_monthly.csv",
		}))
		Expect(records(render(entries[1]))).To(Equal([][]string{
			publish.CombinedMonthly.Header(),
			withPower([]string{"32001", "1961-1990", "3", "", "", "20.5", "5.5", "13.0", "8.0", "150.0"}, noPower),
			withPower([]string{"32001", "1981-2010", "3", "", "", "21.0", "6.0", "13.5", "9.5", "155.5"}, noPower),
		}))
		Expect(records(render(entries[0]))).To(Equal([][]string{
			publish.CombinedDaily.Header(),
			withPower([]string{"32001", "1990-07-04", "", "", "25.50", "12.50", "30.10", ""}, noPower),
			withPower([]string{"32001", "2020-01-01", "", "", "18.00", "2.00", "0.00", "4.40"}, noPower),
		}))
	})
})
