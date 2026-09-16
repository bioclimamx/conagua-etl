package publish_test

import (
	"archive/zip"
	"context"
	"database/sql"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// seedCombined builds on seedTwoStates the POWER side of the join:
// two cells; 31001 and 3101 sharing one cell, 1001 on the other, 31002
// with no cell, and the EMA 31001 on the shared cell (its rows must
// never surface); normals for all four periods on 3101 and a row for
// the cell-less 31002; a pre-1981 observation for 31001 and one for
// 31002; monthly_supplement rows for 1981-2010 and 1991-2020 only (so
// 1961-1990 / 1971-2000 combined rows carry NULL POWER), with a
// 1991-2020 month 12 gap; daily_supplement rows for some post-1981
// dates only, including one date 31001 never observed.
func seedCombined(db *sql.DB) map[string]int64 {
	GinkgoHelper()
	ids := seedTwoStates(db)

	insertCell(db, cellShared, 21.5, -89.375)
	insertCell(db, cellAGS, 22.0, -102.5)
	insertStationCell(db, ids["conv/31001"], cellShared, 27.2524061152165)
	insertStationCell(db, ids["conv/3101"], cellShared, 5.5)
	insertStationCell(db, ids["conv/1001"], cellAGS, 0)
	insertStationCell(db, ids["ema/31001"], cellShared, 1)

	insertNormals(db, ids["conv/31002"], "1981-2010", 1, 28.0, 15.0, 21.5, 30.0, 120.0)
	insertNormals(db, ids["conv/3101"], "1991-2020", 6, 36.1, 22.4, 29.3, 160.2, 188.0)
	insertNormals(db, ids["conv/3101"], "1981-2010", 6, 36.0, 22.2, 29.1, 155.0, 189.5)
	insertNormals(db, ids["conv/3101"], "1971-2000", 6, 35.8, 22.0, 28.9, 150.3, 191.0)

	insertDaily(db, ids["conv/31001"], "1980-12-31", 28.0, 14.5, 0.0, 3.1)
	insertDaily(db, ids["conv/31002"], "2020-01-01", 29.5, 18.0, 2.0, 5.0)

	insertPowerMonthly(db, cellShared, "1981-2010", 12, mixedPowerRow)
	insertPowerMonthly(db, cellShared, "1981-2010", 1, fullPowerRow)
	insertPowerMonthly(db, cellShared, "1981-2010", 6, powerSeqRow(100))
	insertPowerMonthly(db, cellShared, "1991-2020", 1, powerSeqRow(200))
	insertPowerMonthly(db, cellAGS, "1981-2010", 1, powerSeqRow(300))

	insertPowerDaily(db, cellShared, "2020-01-03", powerSeqRow(400))
	insertPowerDaily(db, cellShared, "2020-01-01", fullPowerRow)
	insertPowerDaily(db, cellShared, "1999-12-31", mixedPowerRow)
	insertPowerDaily(db, cellAGS, "2020-01-01", powerSeqRow(300))
	return ids
}

// planDetails returns the detail column of EXPLAIN QUERY PLAN for query.
func planDetails(db *sql.DB, query string, args ...any) []string {
	GinkgoHelper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+query, args...)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	var details []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		Expect(rows.Scan(&id, &parent, &notUsed, &detail)).To(Succeed())
		details = append(details, detail)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return details
}

var power31 = []string{
	"t2m_c", "t2m_max_c", "t2m_min_c", "t2m_wet_c", "t2m_dew_c", "ts_c", "ts_max_c", "ts_min_c",
	"rh2m_pct", "qv2m_gkg",
	"ws2m_ms", "ws10m_ms", "ws50m_ms", "wd2m_deg", "wd10m_deg",
	"solar_ghi_wm2", "solar_dhi_wm2", "solar_dni_wm2", "solar_clrsky_wm2", "clearness_index",
	"par_wm2", "uva_wm2", "uvb_wm2",
	"lw_dwn_wm2",
	"cloud_amt_pct", "ps_kpa",
	"precip_mmpd", "evland_mmpd",
	"gwet_top", "gwet_root", "gwet_prof",
}

var composedSpecs = map[string]publish.ComposedSpec{
	"monthly_supplement": publish.CombinedMonthly,
	"daily_supplement":   publish.CombinedDaily,
}

var _ = Describe("the combined/ file specs", func() {
	It("pins the published header of both combined/ files", func() {
		Expect(publish.CombinedMonthly.Header()).To(Equal(append([]string{
			"station_id", "period", "month", "cell_id", "distance_km",
			"tmax_c", "tmin_c", "tmean_c", "precip_mm", "evap_mm",
		}, power31...)))
		Expect(publish.CombinedDaily.Header()).To(Equal(append([]string{
			"station_id", "date", "cell_id", "distance_km",
			"tmax_c", "tmin_c", "precip_mm", "evap_mm",
		}, power31...)))
		Expect(publish.CombinedMonthly.Name).To(Equal("combined/combined_monthly"))
		Expect(publish.CombinedDaily.Name).To(Equal("combined/combined_daily"))
		Expect(publish.CombinedMonthly.Columns).To(HaveLen(10 + 31))
		Expect(publish.CombinedDaily.Columns).To(HaveLen(8 + 31))
	})

	It("composes each file from the base spec's keys, the join context, the base spec's values, and POWER-31, name by name and decimal by decimal", func() {
		compose := func(base publish.FileSpec, supplement string) (want []publish.Column, sources map[string]string) {
			sources = map[string]string{}
			add := func(c publish.Column, table string) {
				want = append(want, c)
				sources[c.Name] = table
			}
			for _, c := range base.Columns {
				if c.Key {
					add(c, base.Table)
				}
			}
			distance, ok := schema.Decimals("station_power_cell", "distance_km")
			Expect(ok).To(BeTrue())
			add(publish.Column{Name: "cell_id", DB: "cell_id", Kind: publish.KindText}, "station_power_cell")
			add(publish.Column{Name: "distance_km", DB: "distance_km", Kind: publish.KindReal, Decimals: distance},
				"station_power_cell")
			for _, c := range base.Columns {
				if !c.Key {
					add(c, base.Table)
				}
			}
			for _, p := range power.Registry {
				decimals, ok := schema.Decimals(supplement, p.Column)
				Expect(ok).To(BeTrue(), "%s.%s has no Precision entry", supplement, p.Column)
				add(publish.Column{Name: p.Column, DB: p.Column, Kind: publish.KindReal, Decimals: decimals}, supplement)
			}
			return want, sources
		}

		wantMonthly, monthlySources := compose(publish.MonthlyNormals, "monthly_supplement")
		Expect(publish.CombinedMonthly.Columns).To(Equal(wantMonthly))
		Expect(publish.CombinedMonthly.Sources).To(Equal(monthlySources))
		Expect(publish.CombinedMonthly.Table).To(Equal(publish.MonthlyNormals.Table))

		wantDaily, dailySources := compose(publish.DailyObservations, "daily_supplement")
		Expect(publish.CombinedDaily.Columns).To(Equal(wantDaily))
		Expect(publish.CombinedDaily.Sources).To(Equal(dailySources))
		Expect(publish.CombinedDaily.Table).To(Equal(publish.DailyObservations.Table))
	})

	It("pins POWER decimals at two, the wind directions at one, distance_km at three", func() {
		for _, spec := range composedSpecs {
			for _, c := range spec.Columns {
				switch {
				case c.Name == "distance_km":
					Expect(c.Decimals).To(Equal(3), "%s.%s", spec.Name, c.Name)
				case c.Name == "wd2m_deg", c.Name == "wd10m_deg":
					Expect(c.Decimals).To(Equal(1), "%s.%s", spec.Name, c.Name)
				case strings.HasSuffix(spec.Source(c), "_supplement"):
					Expect(c.Decimals).To(Equal(2), "%s.%s", spec.Name, c.Name)
				}
			}
		}
	})

	It("reads every column from a DDL column of its source table, typed by the DDL declaration", func() {
		db := openTempDB()
		for _, spec := range composedSpecs {
			for _, c := range spec.Columns {
				table := spec.Source(c)
				Expect(table).NotTo(BeEmpty(), "%s.%s has no source", spec.Name, c.Name)
				cols := ddlColumns(db, table)
				var ddl ddlColumn
				found := false
				for _, d := range cols {
					if d.Name == c.DB {
						ddl, found = d, true
					}
				}
				Expect(found).To(BeTrue(), "%s reads a column the DDL lacks: %s.%s", spec.Name, table, c.DB)
				switch {
				case c.DB == "station_id":
					Expect(ddl.Type).To(Equal("INTEGER"))
					Expect(c.Kind).To(Equal(publish.KindText))
				case ddl.Type == "INTEGER":
					Expect(c.Kind).To(Equal(publish.KindInt), "%s.%s", spec.Name, c.Name)
				case ddl.Type == "REAL":
					Expect(c.Kind).To(Equal(publish.KindReal), "%s.%s", spec.Name, c.Name)
				case ddl.Type == "TEXT":
					Expect(c.Kind).To(BeElementOf(publish.KindText, publish.KindDate, publish.KindPeriod),
						"%s.%s", spec.Name, c.Name)
				default:
					Fail("unexpected DDL type " + ddl.Type + " for " + table + "." + c.DB)
				}
			}
		}
	})

	It("takes POWER-31 as every supplement DDL column except the keys and the dropped FK, in DDL order", func() {
		db := openTempDB()
		for table, spec := range composedSpecs {
			var wantDDL []string
			for _, c := range ddlColumns(db, table) {
				if _, dropped := publish.Dropped[table][c.Name]; c.PK > 0 || dropped {
					continue
				}
				wantDDL = append(wantDDL, c.Name)
			}
			var got []string
			for _, c := range spec.Columns {
				if spec.Source(c) == table {
					got = append(got, c.DB)
				}
			}
			Expect(got).To(Equal(wantDDL), table)
			Expect(got).To(Equal(power31), table)
		}
	})

	It("keys the sort by the exported natural key of the spine", func() {
		keysOf := func(spec publish.ComposedSpec) []string {
			var keys []string
			for _, c := range spec.Columns {
				if c.Key {
					keys = append(keys, c.Name)
				}
			}
			return keys
		}
		Expect(keysOf(publish.CombinedMonthly)).To(Equal([]string{"station_id", "period", "month"}))
		Expect(keysOf(publish.CombinedDaily)).To(Equal([]string{"station_id", "date"}))
	})
})

var _ = Describe("CombinedEntries", func() {
	var (
		db      *sql.DB
		entries []archive.Entry
		units   []string
	)

	BeforeEach(func() {
		db = openTempDB()
		seedCombined(db)
		units = nil
		var err error
		entries, err = publish.CombinedEntries(context.Background(), db, yucatan,
			func(path string) { units = append(units, path) })
		Expect(err).NotTo(HaveOccurred())
	})

	It("lists combined_monthly and one daily file per station, Path-sorted, under the state slug", func() {
		var paths []string
		for _, e := range entries {
			paths = append(paths, e.Path)
		}
		Expect(paths).To(Equal([]string{
			"combined/combined_daily/yuc/daily-31001.csv",
			"combined/combined_daily/yuc/daily-31002.csv",
			"combined/combined_daily/yuc/daily-3101.csv",
			"combined/combined_monthly.csv",
		}))
	})

	It("round-trips every combined_monthly column: all four periods kept, POWER only where the cell has the (period, month) row", func() {
		got := records(render(entryByPath(entries, "combined/combined_monthly.csv")))
		Expect(got).To(Equal([][]string{
			publish.CombinedMonthly.Header(),
			withPower([]string{"31001", "1981-2010", "1", cellShared, "27.252", "33.4", "17.9", "25.7", "28.3", "141.6"}, powerWant(fullPowerRow)),
			withPower([]string{"31001", "1981-2010", "12", cellShared, "27.252", "30.1", "16.2", "", "24.5", "110.3"}, powerWant(mixedPowerRow)),
			withPower([]string{"31001", "1991-2020", "1", cellShared, "27.252", "33.0", "18.3", "25.8", "0.0", "150.2"}, powerWant(powerSeqRow(200))),
			withPower([]string{"31001", "1991-2020", "12", cellShared, "27.252", "", "", "", "", ""}, noPower),
			withPower([]string{"31002", "1981-2010", "1", "", "", "28.0", "15.0", "21.5", "30.0", "120.0"}, noPower),
			withPower([]string{"3101", "1961-1990", "6", cellShared, "5.500", "35.9", "22.1", "29.0", "152.7", "190.4"}, noPower),
			withPower([]string{"3101", "1971-2000", "6", cellShared, "5.500", "35.8", "22.0", "28.9", "150.3", "191.0"}, noPower),
			withPower([]string{"3101", "1981-2010", "6", cellShared, "5.500", "36.0", "22.2", "29.1", "155.0", "189.5"}, powerWant(powerSeqRow(100))),
			withPower([]string{"3101", "1991-2020", "6", cellShared, "5.500", "36.1", "22.4", "29.3", "160.2", "188.0"}, noPower),
		}))
	})

	It("round-trips each station's combined daily file: the observed spine with POWER only on dates the cell has, never a date the station did not observe", func() {
		Expect(records(render(entryByPath(entries, "combined/combined_daily/yuc/daily-31001.csv")))).To(Equal([][]string{
			publish.CombinedDaily.Header(),
			withPower([]string{"31001", "1980-12-31", cellShared, "27.252", "28.00", "14.50", "0.00", "3.10"}, noPower),
			withPower([]string{"31001", "1999-12-31", cellShared, "27.252", "", "-0.04", "", ""}, powerWant(mixedPowerRow)),
			withPower([]string{"31001", "2020-01-01", cellShared, "27.252", "30.50", "", "12.40", ""}, powerWant(fullPowerRow)),
			withPower([]string{"31001", "2020-01-02", cellShared, "27.252", "31.00", "19.50", "0.00", "4.20"}, noPower),
		}))
		Expect(records(render(entryByPath(entries, "combined/combined_daily/yuc/daily-3101.csv")))).To(Equal([][]string{
			publish.CombinedDaily.Header(),
			withPower([]string{"3101", "1975-06-15", cellShared, "5.500", "36.20", "22.00", "45.10", "7.30"}, noPower),
		}))
	})

	It("leaves the join context and POWER-31 empty on every row of a station with no cell", func() {
		Expect(records(render(entryByPath(entries, "combined/combined_daily/yuc/daily-31002.csv")))).To(Equal([][]string{
			publish.CombinedDaily.Header(),
			withPower([]string{"31002", "2020-01-01", "", "", "29.50", "18.00", "2.00", "5.00"}, noPower),
		}))
		monthly := records(render(entryByPath(entries, "combined/combined_monthly.csv")))
		var cellless [][]string
		for _, row := range monthly[1:] {
			if row[0] == "31002" {
				cellless = append(cellless, row)
			}
		}
		Expect(cellless).To(Equal([][]string{
			withPower([]string{"31002", "1981-2010", "1", "", "", "28.0", "15.0", "21.5", "30.0", "120.0"}, noPower),
		}))
	})

	It("writes a header-only daily file for a station with no observations", func() {
		mustExec(db, `DELETE FROM daily_observations WHERE station_id = (SELECT id FROM stations WHERE external_id = '31002' AND source = 'conagua_conventional')`)
		Expect(string(render(entryByPath(entries, "combined/combined_daily/yuc/daily-31002.csv")))).To(Equal(
			strings.Join(publish.CombinedDaily.Header(), ",") + "\n"))
	})

	It("writes the LEFT join's NULLs as empty fields, UTF-8 without BOM, LF endings", func() {
		data := render(entryByPath(entries, "combined/combined_daily/yuc/daily-31001.csv"))
		Expect(data).NotTo(HavePrefix("\xef\xbb\xbf"))
		Expect(strings.Contains(string(data), "\r")).To(BeFalse())
		lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
		Expect(lines[0]).To(Equal(strings.Join(publish.CombinedDaily.Header(), ",")))
		Expect(lines[1]).To(Equal("31001,1980-12-31," + cellShared + ",27.252,28.00,14.50,0.00,3.10" +
			strings.Repeat(",", len(power.Registry))))
	})

	It("fires onUnit with the entry path once per station daily file, in write order", func() {
		for _, e := range entries {
			render(e)
		}
		Expect(units).To(Equal([]string{
			"combined/combined_daily/yuc/daily-31001.csv",
			"combined/combined_daily/yuc/daily-31002.csv",
			"combined/combined_daily/yuc/daily-3101.csv",
		}))
	})

	It("names the same daily file set as ConaguaEntries, station for station", func() {
		conagua, err := publish.ConaguaEntries(context.Background(), db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())
		perStation := func(list []archive.Entry, prefix string) []string {
			var files []string
			for _, e := range list {
				if strings.HasPrefix(e.Path, prefix) {
					files = append(files, strings.TrimPrefix(e.Path, prefix))
				}
			}
			return files
		}
		Expect(perStation(entries, "combined/combined_daily/yuc/")).To(Equal(
			perStation(conagua, "conagua/daily_observations/yuc/")))
	})

	It("scopes another state to its own stations and cells", func() {
		ags := publish.State{Code: "AGS", Slug: "ags", Name: "Aguascalientes"}
		agsEntries, err := publish.CombinedEntries(context.Background(), db, ags, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(agsEntries).To(HaveLen(2))
		Expect(records(render(entryByPath(agsEntries, "combined/combined_monthly.csv")))).To(Equal([][]string{
			publish.CombinedMonthly.Header(),
			withPower([]string{"1001", "1981-2010", "1", cellAGS, "0.000", "29.9", "9.9", "19.9", "20.0", "200.0"}, powerWant(powerSeqRow(300))),
		}))
		Expect(records(render(entryByPath(agsEntries, "combined/combined_daily/ags/daily-1001.csv")))).To(Equal([][]string{
			publish.CombinedDaily.Header(),
			withPower([]string{"1001", "2020-01-01", cellAGS, "0.000", "20.00", "5.00", "0.00", "6.00"}, powerWant(powerSeqRow(300))),
		}))
	})

	It("yields a header-only combined_monthly and no daily files for a state with no stations", func() {
		none := publish.State{Code: "ZAC", Slug: "zac", Name: "Zacatecas"}
		zacEntries, err := publish.CombinedEntries(context.Background(), db, none, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(zacEntries).To(HaveLen(1))
		Expect(zacEntries[0].Path).To(Equal("combined/combined_monthly.csv"))
		Expect(records(render(zacEntries[0]))).To(Equal([][]string{publish.CombinedMonthly.Header()}))
	})

	It("honours cancellation at write time", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancelable, err := publish.CombinedEntries(ctx, db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())
		cancel()
		err = entryByPath(cancelable, "combined/combined_monthly.csv").Write(io.Discard)
		Expect(errors.Is(err, context.Canceled)).To(BeTrue(), err)
		err = entryByPath(cancelable, "combined/combined_daily/yuc/daily-31001.csv").Write(io.Discard)
		Expect(errors.Is(err, context.Canceled)).To(BeTrue(), err)
	})

	It("refuses to write a non-finite POWER value rather than emit +Inf", func() {
		mustExec(db, `UPDATE daily_supplement SET ps_kpa = 9e999 WHERE cell_id = ? AND date = '2020-01-01'`, cellShared)
		err := entryByPath(entries, "combined/combined_daily/yuc/daily-31001.csv").Write(io.Discard)
		Expect(err).To(MatchError(ContainSubstring("combined/combined_daily: row 3: column ps_kpa: non-finite value +Inf")))
	})
})

var _ = Describe("the combined queries' plans", func() {
	var db *sql.DB

	BeforeEach(func() {
		db = openTempDB()
	})

	It("walk the daily spine's (station_id, date) key and probe the supplement's (cell_id, date) primary key, never a scan", func() {
		details := planDetails(db, publish.CombinedDailySQL,
			"31001", sql.NullString{}, sql.NullFloat64{}, sql.NullString{}, int64(1))
		Expect(details).To(ContainElement(MatchRegexp(
			`^SEARCH d USING (COVERING )?INDEX (idx_daily_station_date|sqlite_autoindex_daily_observations_1) \(station_id=\?\)`)))
		Expect(details).To(ContainElement(MatchRegexp(
			`^SEARCH ds USING (INDEX sqlite_autoindex_daily_supplement_1|PRIMARY KEY) \(cell_id=\? AND date=\?\)`)))
		for _, d := range details {
			Expect(d).NotTo(HavePrefix("SCAN"), d)
		}
	})

	It("probe the monthly supplement on its full (cell_id, period, month) primary key and the cell map on its rowid, never a scan of either", func() {
		details := planDetails(db, publish.CombinedMonthlySQL, "YUC", "conagua_conventional")
		Expect(details).To(ContainElement(MatchRegexp(`^SEARCH spc USING INTEGER PRIMARY KEY \(rowid=\?\)`)))
		Expect(details).To(ContainElement(MatchRegexp(
			`^SEARCH ms USING (INDEX sqlite_autoindex_monthly_supplement_1|PRIMARY KEY) \(cell_id=\? AND period=\? AND month=\?\)`)))
		for _, d := range details {
			Expect(d).NotTo(MatchRegexp(`^SCAN (spc|ms)\b`), d)
		}
	})
})

var _ = Describe("CombinedEntries through archive.WriteZip", func() {
	It("writes the entries into a zip whose bytes are identical on a second build (byte-reproducible)", func() {
		db := openTempDB()
		seedCombined(db)
		entries, err := publish.CombinedEntries(context.Background(), db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())

		dir := GinkgoT().TempDir()
		opts := archive.ZipOptions{Modified: time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)}
		first, err := archive.WriteZip(context.Background(), filepath.Join(dir, "a.zip"), entries, opts)
		Expect(err).NotTo(HaveOccurred())
		second, err := archive.WriteZip(context.Background(), filepath.Join(dir, "b.zip"), entries, opts)
		Expect(err).NotTo(HaveOccurred())
		Expect(second).To(Equal(first))
		Expect(first.Entries).To(Equal(len(entries)))

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
