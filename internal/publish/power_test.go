package publish_test

// Specs for the nasa_power/ entries of a state's archive: the per-state
// cell scoping through station_power_cell (a cell shared by two of the
// state's stations once; another state's cell, an EMA station's cell,
// and an unreferenced cell never), every column of the four files
// round-tripped against seeded values at their pinned decimals, the
// primary-key sorts, the per-cell daily files (header-only for a cell
// with no rows), the cell_id alphabet guard, and the seam with
// archive.WriteZip.

import (
	"archive/zip"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// powerCase is one seeded POWER value (nil seeds NULL) and its
// fixed-precision rendering ("" for NULL).
type powerCase struct {
	in   any
	want string
}

// fullPowerRow seeds one value per POWER-31 column, keyed by column so
// the specs never transcribe the order: every column carries a distinct
// value (a column-order slip cannot pass), and the precision cases ride
// along — the solar conversion tail, wind direction at one decimal with
// an exact half-to-even tie, a sub-precision negative, integral values
// padded.
var fullPowerRow = map[string]powerCase{
	"t2m_c":            {24.475, "24.48"},
	"t2m_max_c":        {31.0, "31.00"},
	"t2m_min_c":        {-0.004, "0.00"},
	"t2m_wet_c":        {19.125, "19.12"},
	"t2m_dew_c":        {15.5, "15.50"},
	"ts_c":             {26.789, "26.79"},
	"ts_max_c":         {40.0, "40.00"},
	"ts_min_c":         {-3.35, "-3.35"},
	"rh2m_pct":         {97.26, "97.26"},
	"qv2m_gkg":         {12.3, "12.30"},
	"ws2m_ms":          {2.0, "2.00"},
	"ws10m_ms":         {3.456, "3.46"},
	"ws50m_ms":         {5.0, "5.00"},
	"wd2m_deg":         {300.3, "300.3"},
	"wd10m_deg":        {180.25, "180.2"},
	"solar_ghi_wm2":    {228.472222222222, "228.47"},
	"solar_dhi_wm2":    {80.555555555556, "80.56"},
	"solar_dni_wm2":    {150.0, "150.00"},
	"solar_clrsky_wm2": {260.115740740741, "260.12"},
	"clearness_index":  {0.69, "0.69"},
	"par_wm2":          {100.1, "100.10"},
	"uva_wm2":          {10.0, "10.00"},
	"uvb_wm2":          {0.25, "0.25"},
	"lw_dwn_wm2":       {400.0, "400.00"},
	"cloud_amt_pct":    {55.5, "55.50"},
	"ps_kpa":           {101.3, "101.30"},
	"precip_mmpd":      {3.7, "3.70"},
	"evland_mmpd":      {2.22, "2.22"},
	"gwet_top":         {0.5, "0.50"},
	"gwet_root":        {0.72, "0.72"},
	"gwet_prof":        {1.0, "1.00"},
}

// withNulls copies row with the named columns seeded NULL.
func withNulls(row map[string]powerCase, cols ...string) map[string]powerCase {
	out := maps.Clone(row)
	for _, c := range cols {
		out[c] = powerCase{nil, ""}
	}
	return out
}

var (
	mixedPowerRow   = withNulls(fullPowerRow, "t2m_c", "wd2m_deg", "solar_ghi_wm2", "gwet_prof")
	noRHPowerRow    = withNulls(fullPowerRow, "rh2m_pct")
	allNullPowerRow = withNulls(fullPowerRow, registryColumns()...)
)

// registryColumns is the POWER-31 column list in registry order — the
// order the seeded INSERTs and the expected records both follow.
func registryColumns() []string {
	cols := make([]string, len(power.Registry))
	for i, p := range power.Registry {
		cols[i] = p.Column
	}
	return cols
}

func powerValues(row map[string]powerCase) []any {
	GinkgoHelper()
	Expect(row).To(HaveLen(len(power.Registry)))
	out := make([]any, 0, len(row))
	for _, c := range registryColumns() {
		Expect(row).To(HaveKey(c))
		out = append(out, row[c].in)
	}
	return out
}

func powerWant(row map[string]powerCase) []string {
	GinkgoHelper()
	out := make([]string, 0, len(row))
	for _, c := range registryColumns() {
		out = append(out, row[c].want)
	}
	return out
}

// withPower builds one expected CSV record: a row's leading cells then
// its POWER-31 cells — the suite's one concatenation helper.
func withPower(cells []string, power []string) []string {
	return append(slices.Clone(cells), power...)
}

// noPower is POWER-31 as it appears where the LEFT join matched nothing:
// 31 empty fields, the same bytes an all-NULL row renders to — the specs
// name the two cases apart on purpose.
var noPower = make([]string, len(power.Registry))

// powerSeqRow seeds base, base+1, …, base+30 down the registry — a
// distinct value per column and per base, so a join that lands the
// wrong row is caught column by column — rendered at two decimals, the
// two wind directions at one, written out here independently of
// schema.Precision.
func powerSeqRow(base float64) map[string]powerCase {
	row := make(map[string]powerCase, len(power.Registry))
	for i, p := range power.Registry {
		v := base + float64(i)
		want := fmt.Sprintf("%.2f", v)
		if p.Column == "wd2m_deg" || p.Column == "wd10m_deg" {
			want = fmt.Sprintf("%.1f", v)
		}
		row[p.Column] = powerCase{v, want}
	}
	return row
}

// insertPowerMonthly and insertPowerDaily seed one supplement row each,
// tied to seedRuns' monthly or daily run by id (seededMonthlyRunID,
// seededDailyRunID) so a DB seeded with both passes the gate's run-refs
// anchor; the reference is by constant because seedPower usually runs
// ahead of seedRuns, and foreign keys are off at runtime.
func insertPowerMonthly(db *sql.DB, cellID, period string, month int, row map[string]powerCase) {
	GinkgoHelper()
	cols := registryColumns()
	mustExec(db, `INSERT INTO monthly_supplement (cell_id, period, month, `+strings.Join(cols, ", ")+
		`, power_run_id) VALUES (?, ?, ?, `+strings.TrimSuffix(strings.Repeat("?, ", len(cols)), ", ")+`, ?)`,
		append(append([]any{cellID, period, month}, powerValues(row)...), seededMonthlyRunID)...)
}

func insertPowerDaily(db *sql.DB, cellID, date string, row map[string]powerCase) {
	GinkgoHelper()
	cols := registryColumns()
	mustExec(db, `INSERT INTO daily_supplement (cell_id, date, `+strings.Join(cols, ", ")+
		`, power_run_id) VALUES (?, ?, `+strings.TrimSuffix(strings.Repeat("?, ", len(cols)), ", ")+`, ?)`,
		append(append([]any{cellID, date}, powerValues(row)...), seededDailyRunID)...)
}

func insertStationCell(db *sql.DB, stationID int64, cellID string, distanceKm float64) {
	GinkgoHelper()
	mustExec(db, `INSERT INTO station_power_cell (station_id, cell_id, distance_km) VALUES (?, ?, ?)`,
		stationID, cellID, distanceKm)
}

// The seeded cells, named by role.
const (
	cellShared = "21.5N_89.3750W"  // two Yucatán stations
	cellSingle = "21.0N_89.6250W"  // one Yucatán station, no daily rows
	cellAGS    = "22.0N_102.5000W" // the other state's only cell
	cellEMA    = "21.5N_88.1250W"  // referenced by the EMA station only
	cellNone   = "19.0N_99.3750W"  // referenced by no station
)

// seedPower adds the POWER side to seedTwoStates' DB: a fourth Yucatán
// station and a Zacatecas one; five cells inserted out of key order;
// station_power_cell rows that share one cell between two Yucatán
// stations, leave 31002 and the Zacatecas station without a cell, and
// tie the EMA station to a cell of its own; monthly rows for two periods
// (a full row, a mixed-NULL row, an all-NULL row inserted ahead of its
// month-1 sibling) and daily rows for three dates inserted out of order;
// rows for the AGS, EMA, and unreferenced cells that the Yucatán files
// must never carry.
func seedPower(db *sql.DB, ids map[string]int64) {
	GinkgoHelper()
	ids["conv/31003"] = upsertStation(db, ingest.StationUpsert{
		Source: ingest.SourceConaguaConventional, ExternalID: "31003", Name: "Progreso", State: "YUC",
		Lat: f64(21.28), Lon: f64(-89.66),
	})
	ids["conv/32001"] = upsertStation(db, ingest.StationUpsert{
		Source: ingest.SourceConaguaConventional, ExternalID: "32001", Name: "Zacatecas (OBS)", State: "ZAC",
	})

	insertCell(db, cellShared, 21.5, -89.375)
	insertCell(db, cellNone, 19.0, -99.375)
	insertCell(db, cellEMA, 21.5, -88.125)
	insertCell(db, cellAGS, 22.0, -102.5)
	insertCell(db, cellSingle, 21.0, -89.625)

	insertStationCell(db, ids["conv/3101"], cellShared, 0.0)
	insertStationCell(db, ids["conv/31001"], cellShared, 27.2524061152165)
	insertStationCell(db, ids["conv/31003"], cellSingle, 12.5)
	insertStationCell(db, ids["conv/1001"], cellAGS, 3.0)
	insertStationCell(db, ids["ema/31001"], cellEMA, 1.0)

	insertPowerMonthly(db, cellShared, "1981-2010", 12, allNullPowerRow)
	insertPowerMonthly(db, cellShared, "1991-2020", 1, mixedPowerRow)
	insertPowerMonthly(db, cellShared, "1981-2010", 1, fullPowerRow)
	insertPowerMonthly(db, cellSingle, "1981-2010", 6, noRHPowerRow)
	insertPowerMonthly(db, cellAGS, "1981-2010", 1, fullPowerRow)
	insertPowerMonthly(db, cellEMA, "1981-2010", 1, fullPowerRow)
	insertPowerMonthly(db, cellNone, "1981-2010", 1, fullPowerRow)

	insertPowerDaily(db, cellShared, "2020-01-02", allNullPowerRow)
	insertPowerDaily(db, cellShared, "2020-01-01", fullPowerRow)
	insertPowerDaily(db, cellShared, "1999-12-31", mixedPowerRow)
	insertPowerDaily(db, cellAGS, "2020-01-01", fullPowerRow)
	insertPowerDaily(db, cellEMA, "2020-01-01", fullPowerRow)
	insertPowerDaily(db, cellNone, "2020-01-01", fullPowerRow)
}

var _ = Describe("PowerEntries", func() {
	var (
		db      *sql.DB
		ids     map[string]int64
		entries []archive.Entry
		units   []string
	)

	BeforeEach(func() {
		db = openTempDB()
		ids = seedTwoStates(db)
		seedPower(db, ids)
		units = nil
		var err error
		entries, err = publish.PowerEntries(context.Background(), db, yucatan,
			func(path string) { units = append(units, path) })
		Expect(err).NotTo(HaveOccurred())
	})

	It("lists the three table files and one daily file per referenced cell, Path-sorted", func() {
		var paths []string
		for _, e := range entries {
			paths = append(paths, e.Path)
		}
		Expect(paths).To(Equal([]string{
			"nasa_power/cells.csv",
			"nasa_power/daily/daily-21.0N_89.6250W.csv",
			"nasa_power/daily/daily-21.5N_89.3750W.csv",
			"nasa_power/monthly.csv",
			"nasa_power/station_cell_map.csv",
		}))
	})

	It("round-trips every cells column for the state's referenced cells, each once, by cell_id", func() {
		got := records(render(entryByPath(entries, "nasa_power/cells.csv")))
		Expect(got).To(Equal([][]string{
			publish.Cells.Header(),
			{cellSingle, "21.000", "-89.625"},
			{cellShared, "21.500", "-89.375"},
		}))
	})

	It("round-trips every station_cell_map column, bytewise by station_id, only for stations that have a cell", func() {
		got := records(render(entryByPath(entries, "nasa_power/station_cell_map.csv")))
		Expect(got).To(Equal([][]string{
			publish.StationCellMap.Header(),
			{"31001", cellShared, "27.252"},
			{"31003", cellSingle, "12.500"},
			{"3101", cellShared, "0.000"},
		}))
	})

	It("round-trips every monthly column for the state's cells in (cell_id, period, month) order", func() {
		got := records(render(entryByPath(entries, "nasa_power/monthly.csv")))
		Expect(got).To(Equal([][]string{
			publish.PowerMonthly.Header(),
			withPower([]string{cellSingle, "1981-2010", "6"}, powerWant(noRHPowerRow)),
			withPower([]string{cellShared, "1981-2010", "1"}, powerWant(fullPowerRow)),
			withPower([]string{cellShared, "1981-2010", "12"}, powerWant(allNullPowerRow)),
			withPower([]string{cellShared, "1991-2020", "1"}, powerWant(mixedPowerRow)),
		}))
	})

	It("round-trips each cell's daily rows by date, header-only for a cell without rows", func() {
		got := records(render(entryByPath(entries, "nasa_power/daily/daily-21.5N_89.3750W.csv")))
		Expect(got).To(Equal([][]string{
			publish.PowerDaily.Header(),
			withPower([]string{cellShared, "1999-12-31"}, powerWant(mixedPowerRow)),
			withPower([]string{cellShared, "2020-01-01"}, powerWant(fullPowerRow)),
			withPower([]string{cellShared, "2020-01-02"}, powerWant(allNullPowerRow)),
		}))
		Expect(string(render(entryByPath(entries, "nasa_power/daily/daily-21.0N_89.6250W.csv")))).To(Equal(
			strings.Join(publish.PowerDaily.Header(), ",") + "\n"))
	})

	It("writes an all-NULL POWER block as a run of bare commas and a full one at the fixed export precision (raw bytes)", func() {
		data := string(render(entryByPath(entries, "nasa_power/daily/daily-21.5N_89.3750W.csv")))
		Expect(data).NotTo(HavePrefix("\xef\xbb\xbf"))
		Expect(data).NotTo(ContainSubstring("\r"))
		lines := strings.Split(strings.TrimSuffix(data, "\n"), "\n")
		Expect(lines).To(HaveLen(4))
		Expect(lines[2]).To(Equal(cellShared + ",2020-01-01," + strings.Join(powerWant(fullPowerRow), ",")))
		Expect(lines[3]).To(Equal(cellShared + ",2020-01-02" + strings.Repeat(",", len(power.Registry))))
	})

	It("fires onUnit with the entry path once per cell, in write order, as each daily entry is written", func() {
		for _, e := range entries {
			render(e)
		}
		Expect(units).To(Equal([]string{
			"nasa_power/daily/daily-" + cellSingle + ".csv",
			"nasa_power/daily/daily-" + cellShared + ".csv",
		}))
	})

	It("scopes another state to its own cell", func() {
		ags := publish.State{Code: "AGS", Slug: "ags", Name: "Aguascalientes"}
		agsEntries, err := publish.PowerEntries(context.Background(), db, ags, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(agsEntries).To(HaveLen(4))
		Expect(agsEntries[1].Path).To(Equal("nasa_power/daily/daily-22.0N_102.5000W.csv"))
		Expect(records(render(entryByPath(agsEntries, "nasa_power/cells.csv")))).To(Equal([][]string{
			publish.Cells.Header(),
			{cellAGS, "22.000", "-102.500"},
		}))
		Expect(records(render(entryByPath(agsEntries, "nasa_power/station_cell_map.csv")))).To(Equal([][]string{
			publish.StationCellMap.Header(),
			{"1001", cellAGS, "3.000"},
		}))
		Expect(records(render(entryByPath(agsEntries, "nasa_power/monthly.csv")))).To(Equal([][]string{
			publish.PowerMonthly.Header(),
			withPower([]string{cellAGS, "1981-2010", "1"}, powerWant(fullPowerRow)),
		}))
		Expect(records(render(agsEntries[1]))).To(Equal([][]string{
			publish.PowerDaily.Header(),
			withPower([]string{cellAGS, "2020-01-01"}, powerWant(fullPowerRow)),
		}))
	})

	It("yields the three header-only table files and no daily file for a state whose stations have no cell", func() {
		zac := publish.State{Code: "ZAC", Slug: "zac", Name: "Zacatecas"}
		zacEntries, err := publish.PowerEntries(context.Background(), db, zac, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(zacEntries).To(HaveLen(3))
		Expect(records(render(zacEntries[0]))).To(Equal([][]string{publish.Cells.Header()}))
		Expect(records(render(zacEntries[1]))).To(Equal([][]string{publish.PowerMonthly.Header()}))
		Expect(records(render(zacEntries[2]))).To(Equal([][]string{publish.StationCellMap.Header()}))
	})

	DescribeTable("refuses a referenced cell_id outside the POWER cell-key alphabet, before any entry exists",
		func(cellID string) {
			insertCell(db, cellID, 21.0, -89.0)
			insertStationCell(db, ids["conv/31002"], cellID, 1.0)
			bad, err := publish.PowerEntries(context.Background(), db, yucatan, nil)
			Expect(bad).To(BeNil())
			Expect(err).To(MatchError(fmt.Sprintf("list cells of YUC: cell_id %q is not a POWER cell key", cellID)))
		},
		Entry("empty", ""),
		Entry("a slash", "21.0N/89.0000W"),
		Entry("a backslash", `21.0N\89.0000W`),
		Entry("a colon", "21.0N:89.0000W"),
		Entry("an asterisk", "21.0N*89.0000W"),
		Entry("a question mark", "21.0N?89.0000W"),
		Entry("a double quote", `21.0N"89.0000W`),
		Entry("angle brackets", "<21.0N_89.0000W>"),
		Entry("a pipe", "21.0N|89.0000W"),
		Entry("a space", "21.0N 89.0000W"),
		Entry("a non-ASCII letter", "21.0Ñ_89.0000W"),
	)

	It("accepts every id power.CellID emits, whichever hemispheres it names", func() {
		for _, c := range []struct{ lat, lon float64 }{{21.35, -89.6}, {-33.9, 151.2}, {0, 0}, {89.9, -179.9}} {
			id := power.CellID(c.lat, c.lon)
			insertCell(db, id, c.lat, c.lon)
			insertStationCell(db, ids["conv/31002"], id, 1.0)
			entries, err := publish.PowerEntries(context.Background(), db, yucatan, nil)
			Expect(err).NotTo(HaveOccurred(), id)
			Expect(entryByPath(entries, "nasa_power/daily/daily-"+id+".csv").Path).NotTo(BeEmpty())
			mustExec(db, `DELETE FROM station_power_cell WHERE cell_id = ?`, id)
		}
	})

	It("honours cancellation at write time", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancelable, err := publish.PowerEntries(ctx, db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())
		cancel()
		err = entryByPath(cancelable, "nasa_power/monthly.csv").Write(io.Discard)
		Expect(errors.Is(err, context.Canceled)).To(BeTrue(), err)
	})

	It("refuses to write a non-finite REAL rather than emit +Inf", func() {
		mustExec(db, `UPDATE nasa_power_grid_cells SET lat = 9e999 WHERE cell_id = ?`, cellShared)
		err := entryByPath(entries, "nasa_power/cells.csv").Write(io.Discard)
		Expect(err).To(MatchError(ContainSubstring("nasa_power/cells: row 2: column lat: non-finite value +Inf")))
	})
})

var _ = Describe("PowerEntries and ProvenanceEntries through archive.WriteZip", func() {
	It("writes the entries — cell ids as path segments included — into a zip whose bytes are identical on a second build (byte-reproducible)", func() {
		db := openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		runs, err := publish.LoadRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		entries, err := publish.PowerEntries(context.Background(), db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())
		entries = append(entries, publish.ProvenanceEntries(runs)...)
		slices.SortFunc(entries, func(a, b archive.Entry) int { return strings.Compare(a.Path, b.Path) })

		dir := GinkgoT().TempDir()
		opts := archive.ZipOptions{Modified: time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)}
		first, err := archive.WriteZip(context.Background(), filepath.Join(dir, "a.zip"), entries, opts)
		Expect(err).NotTo(HaveOccurred())
		second, err := archive.WriteZip(context.Background(), filepath.Join(dir, "b.zip"), entries, opts)
		Expect(err).NotTo(HaveOccurred())
		Expect(second).To(Equal(first))
		Expect(first.Entries).To(Equal(7))

		zr, err := zip.OpenReader(filepath.Join(dir, "a.zip"))
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = zr.Close() }()
		Expect(zr.File).To(HaveLen(len(entries)))
		for i, f := range zr.File {
			Expect(f.Name).To(Equal(entries[i].Path))
			rc, err := f.Open()
			Expect(err).NotTo(HaveOccurred())
			got, err := io.ReadAll(rc)
			Expect(err).NotTo(HaveOccurred())
			Expect(rc.Close()).To(Succeed())
			Expect(got).To(Equal(render(entries[i])), f.Name)
		}
	})
})

var _ = Describe("the nasa_power queries' plans", func() {
	var db *sql.DB

	BeforeEach(func() {
		db = openTempDB()
	})

	It("walk a cell's daily rows on the (cell_id, date) primary key, never a scan", func() {
		details := planDetails(db, publish.PowerDailySQL, cellShared)
		Expect(details).To(ContainElement(MatchRegexp(
			`^SEARCH d USING (INDEX sqlite_autoindex_daily_supplement_1|PRIMARY KEY) \(cell_id=\?\)`)))
		for _, d := range details {
			Expect(d).NotTo(HavePrefix("SCAN"), d)
		}
	})

	It("probe the monthly supplement on its primary key per cell of a once-materialized cell list, never a scan", func() {
		details := planDetails(db, publish.PowerMonthlySQL, "YUC", "conagua_conventional")
		Expect(details).To(ContainElement(MatchRegexp(
			`^SEARCH p USING (INDEX sqlite_autoindex_monthly_supplement_1|PRIMARY KEY) \(cell_id=\?\)`)))
		Expect(details).To(ContainElement(HavePrefix("LIST SUBQUERY")))
		for _, d := range details {
			Expect(d).NotTo(MatchRegexp(`^SCAN (p|m)\b`), d)
		}
	})
})
