package publish_test

// Specs for the national scope: the station list is every state's
// CONAGUA conventional stations in one external_id order (the EMA
// station out), the cell list every cell those stations reference once
// (a cell two states share once; the EMA-only and the unreferenced
// cells out), and the seven whole-scope CSV tables the national CSV
// archive regenerates — each the union of the state archives' rows in
// primary-key order, every column, with the three files a reader joins
// on pinned by hand against the seeded values; a malformed cell id any
// state's station references refuses the national lists.

import (
	"cmp"
	"context"
	"database/sql"
	"slices"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// nationalStates is every state the two-state fixture seeds, in the
// order the run builds them.
var nationalStates = []publish.State{aguascalientes, yucatan, zacatecas}

// seedNational adds to seedTwoStates + seedPower the cross-state edges
// the national scope must handle: an Aguascalientes station on the cell
// two Yucatán stations share, with a normals row and a daily row so the
// combined/ tables join it to that cell, and rows for the Zacatecas
// station, which has no cell, so a third state reaches every table.
func seedNational(db *sql.DB, ids map[string]int64) {
	GinkgoHelper()
	ids["conv/1002"] = upsertStation(db, ingest.StationUpsert{
		Source: ingest.SourceConaguaConventional, ExternalID: "1002", Name: "Calvillo", State: "AGS",
	})
	insertStationCell(db, ids["conv/1002"], cellShared, 40.0)
	insertNormals(db, ids["conv/1002"], "1981-2010", 1, 28.0, 12.0, 20.0, 15.0, 100.0)
	insertDaily(db, ids["conv/1002"], "2020-01-01", 25.0, 10.0, 1.5, nil)
	insertNormals(db, ids["conv/32001"], "1981-2010", 3, 21.0, 6.0, 13.5, 9.5, 155.5)
	insertDaily(db, ids["conv/32001"], "2020-01-01", 18.0, 2.0, 0.0, 4.4)
}

// keyCompare orders rows by spec's key columns as the queries do:
// integer keys numerically, text keys bytewise.
func keyCompare(spec publish.FileSpec) func(a, b []string) int {
	return func(a, b []string) int {
		for i, c := range spec.Columns {
			if !c.Key {
				continue
			}
			if c.Kind == publish.KindInt {
				x, err := strconv.Atoi(a[i])
				Expect(err).NotTo(HaveOccurred())
				y, err := strconv.Atoi(b[i])
				Expect(err).NotTo(HaveOccurred())
				if d := cmp.Compare(x, y); d != 0 {
					return d
				}
				continue
			}
			if d := strings.Compare(a[i], b[i]); d != 0 {
				return d
			}
		}
		return 0
	}
}

// unionRows concatenates the states' rows of one table, drops the
// duplicates a cell shared across states produces, and sorts by key —
// the row set a national table must equal.
func unionRows(spec publish.FileSpec, perState ...[][]string) [][]string {
	GinkgoHelper()
	seen := map[string]bool{}
	var out [][]string
	for _, rows := range perState {
		for _, r := range rows {
			k := strings.Join(r, "\x00")
			if seen[k] {
				continue
			}
			seen[k] = true
			out = append(out, r)
		}
	}
	slices.SortFunc(out, keyCompare(spec))
	return out
}

// stateCSVRows renders spec's rows of every state, one row set per
// state, from the state CSV builders.
func stateCSVRows(ctx context.Context, db *sql.DB, spec publish.FileSpec) [][][]string {
	GinkgoHelper()
	out := make([][][]string, len(nationalStates))
	for i, st := range nationalStates {
		out[i] = csvTwinRows(csvFolders(ctx, db, st), spec)
	}
	return out
}

var _ = Describe("The national scope", func() {
	var (
		ctx = context.Background()
		db  *sql.DB
		ids map[string]int64
	)

	BeforeEach(func() {
		db = openTempDB()
		ids = seedTwoStates(db)
		seedPower(db, ids)
		seedNational(db, ids)
	})

	It("lists every state's CONAGUA conventional stations in one external_id order, the EMA station out", func() {
		got, err := publish.NationalStationIDs(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]string{"1001", "1002", "31001", "31002", "31003", "3101", "32001"}))

		var concat []string
		for _, st := range nationalStates {
			ids, err := publish.StateStationIDs(ctx, db, st)
			Expect(err).NotTo(HaveOccurred())
			concat = append(concat, ids...)
		}
		slices.Sort(concat)
		Expect(got).To(Equal(concat))
	})

	It("lists every cell the conventional stations reference once, in cell_id order — a cell two states share once, the EMA-only and unreferenced cells out", func() {
		Expect(count(db, `SELECT COUNT(DISTINCT s.state) FROM station_power_cell m JOIN stations s ON s.id = m.station_id WHERE m.cell_id = ?`,
			cellShared)).To(Equal(2), "the fixture shares the cell across YUC and AGS")
		got, err := publish.NationalCellIDs(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]string{cellSingle, cellShared, cellAGS}))
	})

	It("refuses a malformed cell_id a conventional station of any state references", func() {
		insertCell(db, "32.0N/99.0000W", 32.0, -99.0)
		insertStationCell(db, ids["conv/32001"], "32.0N/99.0000W", 1.0)
		_, err := publish.NationalCellIDs(ctx, db)
		Expect(err).To(MatchError(`list cells of all states: cell_id "32.0N/99.0000W" is not a POWER cell key`))
		_, err = publish.NationalParquetEntries(ctx, db)
		Expect(err).To(MatchError(`list cells of all states: cell_id "32.0N/99.0000W" is not a POWER cell key`))
	})

	Describe("the seven whole-scope CSV tables", func() {
		var entries []archive.Entry

		BeforeEach(func() {
			entries = publish.NationalTableEntries(ctx, db)
		})

		It("are the three data folders' per-table files, Path-sorted, none per station or per cell", func() {
			Expect(entryPaths(entries)).To(Equal([]string{
				"combined/combined_monthly.csv",
				"conagua/monthly_normals.csv",
				"conagua/monthly_normals_extras.csv",
				"conagua/stations.csv",
				"nasa_power/cells.csv",
				"nasa_power/monthly.csv",
				"nasa_power/station_cell_map.csv",
			}))
		})

		It("each equal the union of the state archives' rows in primary-key order, every column, a shared cell's rows once", func() {
			for _, spec := range []publish.FileSpec{
				publish.Stations, publish.MonthlyNormals, publish.MonthlyNormalsExtras,
				publish.Cells, publish.StationCellMap, publish.PowerMonthly, publish.CombinedMonthly.FileSpec,
			} {
				perState := stateCSVRows(ctx, db, spec)
				want := unionRows(spec, perState...)
				got := records(render(entryByPath(entries, spec.Name+".csv")))
				Expect(got[0]).To(Equal(spec.Header()), spec.Name)
				Expect(got[1:]).To(Equal(want), spec.Name)
				for i, rows := range perState {
					Expect(len(want)).To(BeNumerically(">", len(rows)), "%s spans more than %s", spec.Name, nationalStates[i].Code)
				}
			}
		})

		It("round-trip the stations, cells, and map every state's rows carry, by hand", func() {
			Expect(records(render(entryByPath(entries, "conagua/stations.csv")))).To(Equal([][]string{
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
			}))
			Expect(records(render(entryByPath(entries, "nasa_power/cells.csv")))).To(Equal([][]string{
				publish.Cells.Header(),
				{cellSingle, "21.000", "-89.625"},
				{cellShared, "21.500", "-89.375"},
				{cellAGS, "22.000", "-102.500"},
			}))
			Expect(records(render(entryByPath(entries, "nasa_power/station_cell_map.csv")))).To(Equal([][]string{
				publish.StationCellMap.Header(),
				{"1001", cellAGS, "3.000"},
				{"1002", cellShared, "40.000"},
				{"31001", cellShared, "27.252"},
				{"31003", cellSingle, "12.500"},
				{"3101", cellShared, "0.000"},
			}))
		})

		It("join the other state's station to the shared cell's POWER row in combined_monthly", func() {
			got := records(render(entryByPath(entries, "combined/combined_monthly.csv")))
			Expect(got).To(ContainElement(withPower(
				[]string{"1002", "1981-2010", "1", cellShared, "40.000", "28.0", "12.0", "20.0", "15.0", "100.0"},
				powerWant(fullPowerRow))))
			Expect(got[1][0]).To(Equal("1001"))
			Expect(got[len(got)-1][0]).To(Equal("32001"))
		})
	})
})
