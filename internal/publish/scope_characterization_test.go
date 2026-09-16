package publish_test

// Characterization of the state archive across the scope refactor: the
// Yucatán tabular archive's entry list and the bytes of its scope-bound
// CSVs, pinned as literals written from the seed, so a change in what a
// state archive holds cannot ride in unnoticed; and the cross-scope
// invariant the refactor exists for — every state's whole-scope table
// is exactly the national table's rows keyed to that state's stations
// or cells, row for row and column for column, for all three states.

import (
	"context"
	"database/sql"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// power31Header is the POWER-31 block of a CSV header line, in registry
// order, written out so a literal below stands on its own.
const power31Header = "t2m_c,t2m_max_c,t2m_min_c,t2m_wet_c,t2m_dew_c,ts_c,ts_max_c,ts_min_c,rh2m_pct,qv2m_gkg," +
	"ws2m_ms,ws10m_ms,ws50m_ms,wd2m_deg,wd10m_deg,solar_ghi_wm2,solar_dhi_wm2,solar_dni_wm2,solar_clrsky_wm2," +
	"clearness_index,par_wm2,uva_wm2,uvb_wm2,lw_dwn_wm2,cloud_amt_pct,ps_kpa,precip_mmpd,evland_mmpd," +
	"gwet_top,gwet_root,gwet_prof"

// stateKeyOracle lists, straight from the DB, the station ids and the
// cell ids a state's whole-scope tables may carry — the oracle the
// filtered national rows are held to, independent of the package's own
// scope queries.
func stateKeyOracle(db *sql.DB, st publish.State) (stations, cells map[string]bool) {
	GinkgoHelper()
	stations, cells = map[string]bool{}, map[string]bool{}
	rows, err := db.Query(`SELECT external_id FROM stations WHERE state = ? AND source = 'conagua_conventional'`, st.Code)
	Expect(err).NotTo(HaveOccurred())
	for rows.Next() {
		var id string
		Expect(rows.Scan(&id)).To(Succeed())
		stations[id] = true
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	Expect(rows.Close()).To(Succeed())
	rows, err = db.Query(`SELECT DISTINCT m.cell_id FROM station_power_cell m JOIN stations s ON s.id = m.station_id
	  WHERE s.state = ? AND s.source = 'conagua_conventional'`, st.Code)
	Expect(err).NotTo(HaveOccurred())
	for rows.Next() {
		var id string
		Expect(rows.Scan(&id)).To(Succeed())
		cells[id] = true
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	Expect(rows.Close()).To(Succeed())
	return stations, cells
}

var _ = Describe("the Yucatán tabular archive across the scope refactor", func() {
	var (
		ctx     = context.Background()
		db      *sql.DB
		entries []archive.Entry
		units   int
	)

	BeforeEach(func() {
		db = openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		runs, err := publish.LoadRuns(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		entries, units, err = publish.TabularEntries(ctx, db, yucatan, runs, GinkgoT().TempDir(), nil)
		Expect(err).NotTo(HaveOccurred())
	})

	It("lists the thirty entries of the archive, Path-sorted, and eleven units", func() {
		Expect(entryPaths(entries)).To(Equal([]string{
			"combined/combined_daily.parquet",
			"combined/combined_daily/yuc/daily-31001.csv",
			"combined/combined_daily/yuc/daily-31002.csv",
			"combined/combined_daily/yuc/daily-31003.csv",
			"combined/combined_daily/yuc/daily-3101.csv",
			"combined/combined_monthly.csv",
			"combined/combined_monthly.parquet",
			"conagua/daily_observations.parquet",
			"conagua/daily_observations/yuc/daily-31001.csv",
			"conagua/daily_observations/yuc/daily-31002.csv",
			"conagua/daily_observations/yuc/daily-31003.csv",
			"conagua/daily_observations/yuc/daily-3101.csv",
			"conagua/monthly_normals.csv",
			"conagua/monthly_normals.parquet",
			"conagua/monthly_normals_extras.csv",
			"conagua/monthly_normals_extras.parquet",
			"conagua/stations.csv",
			"conagua/stations.parquet",
			"nasa_power/cells.csv",
			"nasa_power/cells.parquet",
			"nasa_power/daily.parquet",
			"nasa_power/daily/daily-21.0N_89.6250W.csv",
			"nasa_power/daily/daily-21.5N_89.3750W.csv",
			"nasa_power/monthly.csv",
			"nasa_power/monthly.parquet",
			"nasa_power/station_cell_map.csv",
			"nasa_power/station_cell_map.parquet",
			"provenance/ingest_runs.csv",
			"provenance/power_runs.csv",
			"yuc.db",
		}))
		Expect(units).To(Equal(11))
	})

	It("writes the scope-bound tables byte for byte as before the refactor", func() {
		Expect(string(render(entryByPath(entries, "conagua/stations.csv")))).To(Equal(
			"station_id,name,state,municipality,lat,lon,altitude_m,status,first_year,last_year," +
				"wmo_completeness_bin_1961_1990,wmo_completeness_bin_1971_2000,wmo_completeness_bin_1981_2010,wmo_completeness_bin_1991_2020," +
				"wmo_completeness_cont_1961_1990,wmo_completeness_cont_1971_2000,wmo_completeness_cont_1981_2010,wmo_completeness_cont_1991_2020\n" +
				`31001,"Mérida, ""La Plancha""",YUC,Mérida,21.850278,-89.375000,9.0,operating,1951,2026,0.9722,0.5000,,1.0000,0.9324,0.0000,,0.0009` + "\n" +
				"31002,Tizimín,YUC,,,,,,,,,,,,,,,\n" +
				"31003,Progreso,YUC,,21.280000,-89.660000,,,,,,,,,,,,\n" +
				"3101,Valladolid,YUC,Valladolid,20.689100,-88.201100,25.5,suspended,1961,1995,0.2500,,,,,,,\n"))
		Expect(string(render(entryByPath(entries, "conagua/monthly_normals.csv")))).To(Equal(
			"station_id,period,month,tmax_c,tmin_c,tmean_c,precip_mm,evap_mm\n" +
				"31001,1981-2010,1,33.4,17.9,25.7,28.3,141.6\n" +
				"31001,1981-2010,12,30.1,16.2,,24.5,110.3\n" +
				"31001,1991-2020,1,33.0,18.3,25.8,0.0,150.2\n" +
				"31001,1991-2020,12,,,,,\n" +
				"3101,1961-1990,6,35.9,22.1,29.0,152.7,190.4\n"))
		Expect(string(render(entryByPath(entries, "nasa_power/cells.csv")))).To(Equal(
			"cell_id,lat,lon\n" +
				"21.0N_89.6250W,21.000,-89.625\n" +
				"21.5N_89.3750W,21.500,-89.375\n"))
		Expect(string(render(entryByPath(entries, "nasa_power/station_cell_map.csv")))).To(Equal(
			"station_id,cell_id,distance_km\n" +
				"31001,21.5N_89.3750W,27.252\n" +
				"31003,21.0N_89.6250W,12.500\n" +
				"3101,21.5N_89.3750W,0.000\n"))
		Expect(records(render(entryByPath(entries, "nasa_power/monthly.csv")))).To(Equal([][]string{
			publish.PowerMonthly.Header(),
			withPower([]string{cellSingle, "1981-2010", "6"}, powerWant(noRHPowerRow)),
			withPower([]string{cellShared, "1981-2010", "1"}, powerWant(fullPowerRow)),
			withPower([]string{cellShared, "1981-2010", "12"}, powerWant(allNullPowerRow)),
			withPower([]string{cellShared, "1991-2020", "1"}, powerWant(mixedPowerRow)),
		}))
		Expect(records(render(entryByPath(entries, "combined/combined_monthly.csv")))).To(Equal([][]string{
			publish.CombinedMonthly.Header(),
			withPower([]string{"31001", "1981-2010", "1", cellShared, "27.252", "33.4", "17.9", "25.7", "28.3", "141.6"}, powerWant(fullPowerRow)),
			withPower([]string{"31001", "1981-2010", "12", cellShared, "27.252", "30.1", "16.2", "", "24.5", "110.3"}, powerWant(allNullPowerRow)),
			withPower([]string{"31001", "1991-2020", "1", cellShared, "27.252", "33.0", "18.3", "25.8", "0.0", "150.2"}, powerWant(mixedPowerRow)),
			withPower([]string{"31001", "1991-2020", "12", cellShared, "27.252", "", "", "", "", ""}, noPower),
			withPower([]string{"3101", "1961-1990", "6", cellShared, "0.000", "35.9", "22.1", "29.0", "152.7", "190.4"}, noPower),
		}))
	})

	It("writes the per-unit files byte for byte as before the refactor", func() {
		Expect(string(render(entryByPath(entries, "conagua/daily_observations/yuc/daily-31001.csv")))).To(Equal(
			"station_id,date,tmax_c,tmin_c,precip_mm,evap_mm\n" +
				"31001,1999-12-31,,-0.04,,\n" +
				"31001,2020-01-01,30.50,,12.40,\n" +
				"31001,2020-01-02,31.00,19.50,0.00,4.20\n"))
		Expect(string(render(entryByPath(entries, "conagua/daily_observations/yuc/daily-31003.csv")))).To(Equal(
			"station_id,date,tmax_c,tmin_c,precip_mm,evap_mm\n"))
		Expect(string(render(entryByPath(entries, "nasa_power/daily/daily-21.0N_89.6250W.csv")))).To(Equal(
			"cell_id,date," + power31Header + "\n"))
		Expect(string(render(entryByPath(entries, "combined/combined_daily/yuc/daily-3101.csv")))).To(Equal(
			"station_id,date,cell_id,distance_km,tmax_c,tmin_c,precip_mm,evap_mm," + power31Header + "\n" +
				"3101,1975-06-15,21.5N_89.3750W,0.000,36.20,22.00,45.10,7.30" + strings.Repeat(",", 31) + "\n"))
	})
})

var _ = Describe("every state's whole-scope tables against the national ones", func() {
	It("equal, for each of the three states, the national rows keyed to that state's stations or cells — row for row, column for column", func() {
		ctx := context.Background()
		db := openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedNational(db, ids)
		national := publish.NationalTableEntries(ctx, db)

		covered := 0
		for _, st := range nationalStates {
			stations, cells := stateKeyOracle(db, st)
			Expect(stations).NotTo(BeEmpty(), st.Code)
			state := csvFolders(ctx, db, st)
			for _, spec := range []publish.FileSpec{
				publish.Stations, publish.MonthlyNormals, publish.MonthlyNormalsExtras,
				publish.Cells, publish.StationCellMap, publish.PowerMonthly, publish.CombinedMonthly.FileSpec,
			} {
				keyed := stations
				if spec.Columns[0].Name == "cell_id" {
					keyed = cells
				}
				all := records(render(entryByPath(national, spec.Name+".csv")))
				want := [][]string{all[0]}
				for _, r := range all[1:] {
					if keyed[r[0]] {
						want = append(want, r)
					}
				}
				got := records(render(entryByPath(state, spec.Name+".csv")))
				Expect(got).To(Equal(want), "%s at %s", spec.Name, st.Code)
				Expect(slices.IsSorted(firstKeyOf(got[1:]))).To(BeTrue(), "%s at %s", spec.Name, st.Code)
				covered++
			}
		}
		Expect(covered).To(Equal(3 * 7))
		// The oracle bites: Zacatecas has no cell, Yucatán and
		// Aguascalientes share one.
		_, zacCells := stateKeyOracle(db, zacatecas)
		Expect(zacCells).To(BeEmpty())
		_, yucCells := stateKeyOracle(db, yucatan)
		_, agsCells := stateKeyOracle(db, aguascalientes)
		Expect(yucCells).To(HaveKey(cellShared))
		Expect(agsCells).To(HaveKey(cellShared))
	})
})
