package parity_test

import (
	"fmt"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// expectClean asserts one table's tally shows the given number of
// aligned rows and no finding in any category.
func expectClean(t parity.TableComparison, rowsCompared int) {
	GinkgoHelper()
	Expect(t.RowsCompared).To(Equal(rowsCompared), "%s rows compared", t.Table)
	Expect(t.RowsIdentical).To(Equal(rowsCompared), "%s rows identical", t.Table)
	Expect(t.ValueDiffs.Total).To(BeZero(), "%s value diffs", t.Table)
	Expect(t.BaseOnly.Total).To(BeZero(), "%s base-only", t.Table)
	Expect(t.NewOnly.Total).To(BeZero(), "%s new-only", t.Table)
}

// expectSingleDiff seeds the baseline pair, applies one mutation to the
// new side, and asserts the comparator reports exactly the wanted diff
// record — key, column, and both rendered values — in the selected
// table, with every other tally clean.
func expectSingleDiff(ctx SpecContext, mutate string,
	pick func(*parity.DBComparison) parity.TableComparison, want parity.DBDiff, rowsCompared int) {
	GinkgoHelper()
	baseDB, newDB := seedPair()
	execSQL(newDB, mutate)

	cmp := mustCompare(ctx, baseDB, newDB)
	t := pick(cmp)
	Expect(t.ValueDiffs.Samples).To(Equal([]parity.DBDiff{want}))
	Expect(t.ValueDiffs.Total).To(Equal(1))
	Expect(t.RowsCompared).To(Equal(rowsCompared))
	Expect(t.RowsIdentical).To(Equal(rowsCompared-1),
		"exactly the diffed row must drop out of the identical count")
	Expect(t.BaseOnly.Total).To(BeZero())
	Expect(t.NewOnly.Total).To(BeZero())
}

func pickStations(c *parity.DBComparison) parity.TableComparison { return c.Stations }
func pickNormals(c *parity.DBComparison) parity.TableComparison  { return c.Normals }
func pickExtras(c *parity.DBComparison) parity.TableComparison   { return c.Extras }
func pickDaily(c *parity.DBComparison) parity.TableComparison    { return c.Daily }

var _ = Describe("CompareDBs", func() {
	It("reports identically-seeded DBs as fully identical despite divergent surrogate ids", func(ctx SpecContext) {
		baseDB, newDB := seedPair()

		cmp := mustCompare(ctx, baseDB, newDB)

		Expect(cmp.Clean()).To(BeTrue())
		expectClean(cmp.Stations, 1)
		expectClean(cmp.Normals, 2)
		expectClean(cmp.Extras, 1)
		expectClean(cmp.Daily, 3)
		expectClean(cmp.Warnings, 2) // two distinct tuples, one of them with multiplicity 2
	})

	It("compares through read-only handles — the seam the harness crosses", func(ctx SpecContext) {
		seedAt := func(path string, stationID int64) {
			db, err := schema.Open(path)
			Expect(err).NotTo(HaveOccurred())
			seedBaseline(db, stationID)
			Expect(db.Close()).To(Succeed())
		}
		basePath := filepath.Join(GinkgoT().TempDir(), "base.db")
		newPath := filepath.Join(GinkgoT().TempDir(), "new.db")
		seedAt(basePath, baseStationID)
		seedAt(newPath, newStationID)

		baseDB, err := schema.OpenReadOnly(basePath)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(baseDB.Close()).To(Succeed()) })
		newDB, err := schema.OpenReadOnly(newPath)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(newDB.Close()).To(Succeed()) })

		cmp := mustCompare(ctx, baseDB, newDB)

		Expect(cmp.Clean()).To(BeTrue())
		expectClean(cmp.Daily, 3)
		Expect(cmp.BaseRun).To(Equal(seededRunCounters(1)))
	})

	It("carries both sides' latest run counters verbatim, round-tripping every column", func(ctx SpecContext) {
		baseDB, newDB := seedPair()

		cmp := mustCompare(ctx, baseDB, newDB)

		Expect(cmp.BaseRun).To(Equal(seededRunCounters(1)))
		Expect(cmp.NewRun).To(Equal(seededRunCounters(1)))
	})

	It("picks the highest-id run and keeps its NULL columns faithful", func(ctx SpecContext) {
		baseDB, newDB := seedPair()
		execSQL(baseDB, `INSERT INTO ingest_runs (id, started_at, snapshot_date, sink_kind, status)
			VALUES (9, '2026-07-19T08:00:00Z', '2026-07-18', 'r2', 'running')`)
		execSQL(newDB, `DELETE FROM ingest_runs`)

		cmp := mustCompare(ctx, baseDB, newDB)

		Expect(cmp.BaseRun).To(Equal(&parity.RunCounters{
			ID:           9,
			StartedAt:    "2026-07-19T08:00:00Z",
			SnapshotDate: "2026-07-18",
			SinkKind:     "r2",
			Status:       "running",
		}))
		Expect(cmp.NewRun).To(BeNil())
	})

	It("lists one-side-only stations without comparing their child rows", func(ctx SpecContext) {
		baseDB, newDB := seedPair()
		execSQL(baseDB, `INSERT INTO stations (id, source, external_id, name) VALUES (3, ?, '2002', 'BASE ONLY')`, seedSource)
		execSQL(baseDB, `INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
			VALUES (3, '1999-12-31', 30, 15, 0, 1)`)
		execSQL(newDB, `INSERT INTO stations (id, source, external_id, name) VALUES (3, ?, '3003', 'NEW ONLY')`, seedSource)

		cmp := mustCompare(ctx, baseDB, newDB)

		Expect(cmp.Stations.BaseOnly.Samples).To(Equal([]parity.DBKey{
			{Source: seedSource, ExternalID: "2002"},
		}))
		Expect(cmp.Stations.NewOnly.Samples).To(Equal([]parity.DBKey{
			{Source: seedSource, ExternalID: "3003"},
		}))
		Expect(cmp.Stations.RowsCompared).To(Equal(1))
		// The base-only station's daily row must not leak into the
		// daily tallies: only common stations' children are compared.
		expectClean(cmp.Daily, 3)
	})

	It("keys the station universe on source too, never external_id alone", func(ctx SpecContext) {
		baseDB, newDB := seedPair()
		// Same external_id as the baseline station under another source:
		// a distinct natural key, so it must not align with it.
		execSQL(baseDB, `INSERT INTO stations (id, source, external_id, name) VALUES (2, 'nasa_power', ?, 'CELL PROXY')`, seedExtID)

		cmp := mustCompare(ctx, baseDB, newDB)

		Expect(cmp.Stations.BaseOnly.Samples).To(Equal([]parity.DBKey{
			{Source: "nasa_power", ExternalID: seedExtID},
		}))
		Expect(cmp.Stations.RowsCompared).To(Equal(1))
		Expect(cmp.Stations.RowsIdentical).To(Equal(1))
	})

	DescribeTable("stations column diffs",
		func(ctx SpecContext, mutate string, want parity.DBDiff) {
			expectSingleDiff(ctx, mutate, pickStations, want, 1)
		},
		Entry("name", `UPDATE stations SET name = 'RENAMED'`,
			parity.DBDiff{Key: dbKey(""), Column: "name", Base: "AGUASCALIENTES (OBS)", New: "RENAMED"}),
		Entry("state", `UPDATE stations SET state = 'JAL'`,
			parity.DBDiff{Key: dbKey(""), Column: "state", Base: "AGS", New: "JAL"}),
		Entry("municipality NULL vs value", `UPDATE stations SET municipality = 'Aguascalientes'`,
			parity.DBDiff{Key: dbKey(""), Column: "municipality", Base: "NULL", New: "Aguascalientes"}),
		Entry("lat value vs NULL", `UPDATE stations SET lat = NULL`,
			parity.DBDiff{Key: dbKey(""), Column: "lat", Base: "21.85027778", New: "NULL"}),
		Entry("lon", `UPDATE stations SET lon = -102.5`,
			parity.DBDiff{Key: dbKey(""), Column: "lon", Base: "-102.2908333", New: "-102.5"}),
		Entry("altitude_m", `UPDATE stations SET altitude_m = 1900.1`,
			parity.DBDiff{Key: dbKey(""), Column: "altitude_m", Base: "1890.8", New: "1900.1"}),
		Entry("status", `UPDATE stations SET status = 'suspended'`,
			parity.DBDiff{Key: dbKey(""), Column: "status", Base: "operating", New: "suspended"}),
		Entry("first_year", `UPDATE stations SET first_year = 1950`,
			parity.DBDiff{Key: dbKey(""), Column: "first_year", Base: "1947", New: "1950"}),
		Entry("last_year", `UPDATE stations SET last_year = 2020`,
			parity.DBDiff{Key: dbKey(""), Column: "last_year", Base: "2016", New: "2020"}),
		Entry("wmo bin 1961-1990", `UPDATE stations SET wmo_completeness_bin_1961_1990 = 0.9`,
			parity.DBDiff{Key: dbKey(""), Column: "wmo_completeness_bin_1961_1990", Base: "0.1", New: "0.9"}),
		Entry("wmo bin 1971-2000", `UPDATE stations SET wmo_completeness_bin_1971_2000 = 0.9`,
			parity.DBDiff{Key: dbKey(""), Column: "wmo_completeness_bin_1971_2000", Base: "0.2", New: "0.9"}),
		Entry("wmo bin 1981-2010", `UPDATE stations SET wmo_completeness_bin_1981_2010 = 0.9`,
			parity.DBDiff{Key: dbKey(""), Column: "wmo_completeness_bin_1981_2010", Base: "0.3", New: "0.9"}),
		Entry("wmo bin 1991-2020", `UPDATE stations SET wmo_completeness_bin_1991_2020 = 0.9`,
			parity.DBDiff{Key: dbKey(""), Column: "wmo_completeness_bin_1991_2020", Base: "0.4", New: "0.9"}),
		Entry("wmo cont 1961-1990", `UPDATE stations SET wmo_completeness_cont_1961_1990 = 0.9`,
			parity.DBDiff{Key: dbKey(""), Column: "wmo_completeness_cont_1961_1990", Base: "0.5", New: "0.9"}),
		Entry("wmo cont 1971-2000", `UPDATE stations SET wmo_completeness_cont_1971_2000 = 0.9`,
			parity.DBDiff{Key: dbKey(""), Column: "wmo_completeness_cont_1971_2000", Base: "0.6", New: "0.9"}),
		Entry("wmo cont 1981-2010", `UPDATE stations SET wmo_completeness_cont_1981_2010 = 0.9`,
			parity.DBDiff{Key: dbKey(""), Column: "wmo_completeness_cont_1981_2010", Base: "0.7", New: "0.9"}),
		Entry("wmo cont 1991-2020", `UPDATE stations SET wmo_completeness_cont_1991_2020 = 0.9`,
			parity.DBDiff{Key: dbKey(""), Column: "wmo_completeness_cont_1991_2020", Base: "0.8", New: "0.9"}),
	)

	DescribeTable("monthly_normals value diffs",
		func(ctx SpecContext, mutate string, want parity.DBDiff) {
			expectSingleDiff(ctx, mutate, pickNormals, want, 2)
		},
		Entry("tmax", `UPDATE monthly_normals SET tmax = 26 WHERE month = 1`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "tmax", Base: "25.5", New: "26"}),
		Entry("tmin value vs NULL", `UPDATE monthly_normals SET tmin = NULL WHERE month = 1`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "tmin", Base: "10.1", New: "NULL"}),
		Entry("tmean", `UPDATE monthly_normals SET tmean = 17.9 WHERE month = 1`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "tmean", Base: "17.8", New: "17.9"}),
		Entry("precip", `UPDATE monthly_normals SET precip = 0.5 WHERE month = 12`,
			parity.DBDiff{Key: dbKey("1991-2020/m12"), Column: "precip", Base: "0", New: "0.5"}),
		Entry("evap", `UPDATE monthly_normals SET evap = 6.5 WHERE month = 1`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "evap", Base: "6.4", New: "6.5"}),
		Entry("tmax NULL vs value", `UPDATE monthly_normals SET tmax = 20 WHERE month = 12`,
			parity.DBDiff{Key: dbKey("1991-2020/m12"), Column: "tmax", Base: "NULL", New: "20"}),
	)

	DescribeTable("monthly_normals_extras value diffs",
		func(ctx SpecContext, mutate string, want parity.DBDiff) {
			expectSingleDiff(ctx, mutate, pickExtras, want, 1)
		},
		Entry("tmax_monthly_extreme", `UPDATE monthly_normals_extras SET tmax_monthly_extreme = 35`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "tmax_monthly_extreme", Base: "34.5", New: "35"}),
		Entry("tmax_monthly_extreme_year", `UPDATE monthly_normals_extras SET tmax_monthly_extreme_year = 1970`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "tmax_monthly_extreme_year", Base: "1969", New: "1970"}),
		Entry("tmax_daily_extreme", `UPDATE monthly_normals_extras SET tmax_daily_extreme = 37.5`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "tmax_daily_extreme", Base: "36", New: "37.5"}),
		Entry("tmax_daily_extreme_date", `UPDATE monthly_normals_extras SET tmax_daily_extreme_date = '1970-01-21'`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "tmax_daily_extreme_date", Base: "1969-01-21", New: "1970-01-21"}),
		Entry("tmin_monthly_extreme", `UPDATE monthly_normals_extras SET tmin_monthly_extreme = -3`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "tmin_monthly_extreme", Base: "-2", New: "-3"}),
		Entry("tmin_monthly_extreme_year", `UPDATE monthly_normals_extras SET tmin_monthly_extreme_year = 1972`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "tmin_monthly_extreme_year", Base: "1971", New: "1972"}),
		Entry("tmin_daily_extreme value vs NULL", `UPDATE monthly_normals_extras SET tmin_daily_extreme = NULL`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "tmin_daily_extreme", Base: "-5.5", New: "NULL"}),
		Entry("tmin_daily_extreme_date", `UPDATE monthly_normals_extras SET tmin_daily_extreme_date = NULL`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "tmin_daily_extreme_date", Base: "1971-01-05", New: "NULL"}),
		Entry("precip_monthly_extreme", `UPDATE monthly_normals_extras SET precip_monthly_extreme = 90`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "precip_monthly_extreme", Base: "88.1", New: "90"}),
		Entry("precip_monthly_extreme_year", `UPDATE monthly_normals_extras SET precip_monthly_extreme_year = 1993`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "precip_monthly_extreme_year", Base: "1992", New: "1993"}),
		Entry("precip_daily_extreme", `UPDATE monthly_normals_extras SET precip_daily_extreme = 41`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "precip_daily_extreme", Base: "40.2", New: "41"}),
		Entry("precip_daily_extreme_date", `UPDATE monthly_normals_extras SET precip_daily_extreme_date = '1992-02-16'`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "precip_daily_extreme_date", Base: "1992-01-16", New: "1992-02-16"}),
		Entry("tmax_years_with_data", `UPDATE monthly_normals_extras SET tmax_years_with_data = 20`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "tmax_years_with_data", Base: "28", New: "20"}),
		Entry("tmin_years_with_data", `UPDATE monthly_normals_extras SET tmin_years_with_data = 21`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "tmin_years_with_data", Base: "27", New: "21"}),
		Entry("tmean_years_with_data", `UPDATE monthly_normals_extras SET tmean_years_with_data = 22`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "tmean_years_with_data", Base: "26", New: "22"}),
		Entry("precip_years_with_data", `UPDATE monthly_normals_extras SET precip_years_with_data = 23`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "precip_years_with_data", Base: "30", New: "23"}),
		Entry("evap_years_with_data", `UPDATE monthly_normals_extras SET evap_years_with_data = 24`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "evap_years_with_data", Base: "25", New: "24"}),
		Entry("rain_days", `UPDATE monthly_normals_extras SET rain_days = 4`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "rain_days", Base: "3.2", New: "4"}),
		Entry("rain_days_years_with_data", `UPDATE monthly_normals_extras SET rain_days_years_with_data = 30`,
			parity.DBDiff{Key: dbKey("1961-1990/m01"), Column: "rain_days_years_with_data", Base: "29", New: "30"}),
	)

	DescribeTable("daily_observations value diffs",
		func(ctx SpecContext, mutate string, want parity.DBDiff) {
			expectSingleDiff(ctx, mutate, pickDaily, want, 3)
		},
		Entry("tmax", `UPDATE daily_observations SET tmax = 20.3 WHERE date = '1985-01-01'`,
			parity.DBDiff{Key: dbKey("1985-01-01"), Column: "tmax", Base: "20.2", New: "20.3"}),
		Entry("tmin value vs NULL", `UPDATE daily_observations SET tmin = NULL WHERE date = '1985-01-01'`,
			parity.DBDiff{Key: dbKey("1985-01-01"), Column: "tmin", Base: "9.8", New: "NULL"}),
		Entry("precip", `UPDATE daily_observations SET precip = 0.1 WHERE date = '1985-01-01'`,
			parity.DBDiff{Key: dbKey("1985-01-01"), Column: "precip", Base: "0", New: "0.1"}),
		Entry("evap NULL vs value", `UPDATE daily_observations SET evap = 4 WHERE date = '1985-01-01'`,
			parity.DBDiff{Key: dbKey("1985-01-01"), Column: "evap", Base: "NULL", New: "4"}),
		Entry("tmax NULL vs value on the all-NULL row", `UPDATE daily_observations SET tmax = 1 WHERE date = '1985-01-03'`,
			parity.DBDiff{Key: dbKey("1985-01-03"), Column: "tmax", Base: "NULL", New: "1"}),
	)

	It("lists one-side-only child rows by natural key", func(ctx SpecContext) {
		baseDB, newDB := seedPair()
		execSQL(newDB, `DELETE FROM daily_observations WHERE date = '1985-01-02'`)
		execSQL(newDB, `INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
			VALUES (?, '1985-01-04', 19, 8, 0, 2)`, newStationID)
		execSQL(newDB, `DELETE FROM monthly_normals WHERE month = 12`)
		execSQL(newDB, `DELETE FROM monthly_normals_extras`)

		cmp := mustCompare(ctx, baseDB, newDB)

		Expect(cmp.Daily.BaseOnly.Samples).To(Equal([]parity.DBKey{dbKey("1985-01-02")}))
		Expect(cmp.Daily.NewOnly.Samples).To(Equal([]parity.DBKey{dbKey("1985-01-04")}))
		Expect(cmp.Daily.RowsCompared).To(Equal(2))
		Expect(cmp.Daily.RowsIdentical).To(Equal(2))
		Expect(cmp.Daily.BaseRows()).To(Equal(3))
		Expect(cmp.Daily.NewRows()).To(Equal(3))

		Expect(cmp.Normals.BaseOnly.Samples).To(Equal([]parity.DBKey{dbKey("1991-2020/m12")}))
		Expect(cmp.Normals.RowsCompared).To(Equal(1))

		Expect(cmp.Extras.BaseOnly.Samples).To(Equal([]parity.DBKey{dbKey("1961-1990/m01")}))
		Expect(cmp.Extras.RowsCompared).To(BeZero())
	})

	It("compares warnings as a multiset keyed by the resolved natural key", func(ctx SpecContext) {
		baseDB, newDB := seedPair()
		// Drop one of the two copies of the station tuple: a
		// multiplicity diff, not a disappearance.
		execSQL(newDB, `DELETE FROM parsing_warnings
			WHERE id = (SELECT id FROM parsing_warnings WHERE issue = 'unparseable TMAX' LIMIT 1)`)
		// Drop the NULL-station tuple entirely and add a new-only one.
		execSQL(newDB, `DELETE FROM parsing_warnings WHERE issue = 'orphan warning'`)
		execSQL(newDB, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
			VALUES (?, 'normals/1001.txt', 3, 'error', 'bad period')`, newStationID)

		cmp := mustCompare(ctx, baseDB, newDB)

		stationTuple := dbKey(`daily/1001.txt:57 warn "unparseable TMAX"`)
		Expect(cmp.Warnings.ValueDiffs.Samples).To(Equal([]parity.DBDiff{
			{Key: stationTuple, Column: "count", Base: "2", New: "1"},
		}))
		Expect(cmp.Warnings.BaseOnly.Samples).To(Equal([]parity.DBKey{
			{Row: `NULL:NULL error "orphan warning"`},
		}))
		Expect(cmp.Warnings.NewOnly.Samples).To(Equal([]parity.DBKey{
			dbKey(`normals/1001.txt:3 error "bad period"`),
		}))
		Expect(cmp.Warnings.RowsCompared).To(Equal(1))
		Expect(cmp.Warnings.RowsIdentical).To(BeZero())
	})

	It("caps diff samples at DBSampleCap in natural-key order while totals stay exact", func(ctx SpecContext) {
		baseDB, newDB := seedPair()
		total := parity.DBSampleCap + 5
		for i := range total {
			date := fmt.Sprintf("1986-01-%02d", i+1)
			execSQL(baseDB, `INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
				VALUES (?, ?, 10, 5, 0, 1)`, baseStationID, date)
			execSQL(newDB, `INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
				VALUES (?, ?, 11, 5, 0, 1)`, newStationID, date)
		}

		cmp := mustCompare(ctx, baseDB, newDB)

		Expect(cmp.Daily.ValueDiffs.Total).To(Equal(total))
		Expect(cmp.Daily.ValueDiffs.Samples).To(HaveLen(parity.DBSampleCap))
		Expect(cmp.Daily.ValueDiffs.Truncated()).To(BeTrue())
		Expect(cmp.Daily.ValueDiffs.Samples[0]).To(Equal(parity.DBDiff{
			Key: dbKey("1986-01-01"), Column: "tmax", Base: "10", New: "11",
		}))
		Expect(cmp.Daily.RowsCompared).To(Equal(3 + total))
		Expect(cmp.Daily.RowsIdentical).To(Equal(3))
	})
})
