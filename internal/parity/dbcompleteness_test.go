// DB-comparator completeness specs, in-package to reach the unexported
// loaders and diff functions (see completeness_test.go for the package
// split).
//
// The gate: each diff function must cover every column of its schema
// table, with the identity/key columns as the only exclusions. One row
// per table is seeded with every column non-NULL, loaded through the
// production loader, and diffed against the zero (all-NULL) row — so
// every covered column records exactly one diff. The expected column
// set comes from pragma_table_info over the applied schema, which makes
// this a tripwire: a column added to schema.sql on a compared table
// fails here until the seed, the loader SELECT, and the diff
// itemization all carry it — it cannot silently escape the parity gate.
package parity

import (
	"database/sql"
	"path/filepath"
	"slices"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// recordedColumns extracts the Column of every recorded diff.
func recordedColumns(diffs []DBDiff) []string {
	cols := make([]string, len(diffs))
	for i, d := range diffs {
		cols[i] = d.Column
	}
	return cols
}

var _ = ginkgo.Describe("DB comparator completeness", func() {
	var db *sql.DB

	mustExec := func(query string, args ...any) {
		ginkgo.GinkgoHelper()
		_, err := db.Exec(query, args...)
		Expect(err).NotTo(HaveOccurred())
	}

	ginkgo.BeforeEach(func() {
		var err error
		db, err = schema.Open(filepath.Join(ginkgo.GinkgoT().TempDir(), "complete.db"))
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { Expect(db.Close()).To(Succeed()) })

		mustExec(`INSERT INTO stations
			(id, source, external_id, name, state, municipality,
			 lat, lon, altitude_m, status, first_year, last_year,
			 wmo_completeness_bin_1961_1990, wmo_completeness_bin_1971_2000,
			 wmo_completeness_bin_1981_2010, wmo_completeness_bin_1991_2020,
			 wmo_completeness_cont_1961_1990, wmo_completeness_cont_1971_2000,
			 wmo_completeness_cont_1981_2010, wmo_completeness_cont_1991_2020)
			VALUES (1, 'conagua_conventional', '1001', 'N', 'S', 'M',
			        1.1, 2.2, 3.3, 'operating', 1947, 2016,
			        0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8)`)
		mustExec(`INSERT INTO monthly_normals (station_id, period, month, tmax, tmin, tmean, precip, evap)
			VALUES (1, '1961-1990', 1, 1, 2, 3, 4, 5)`)
		mustExec(`INSERT INTO monthly_normals_extras
			(station_id, period, month,
			 tmax_monthly_extreme, tmax_monthly_extreme_year,
			 tmax_daily_extreme, tmax_daily_extreme_date,
			 tmin_monthly_extreme, tmin_monthly_extreme_year,
			 tmin_daily_extreme, tmin_daily_extreme_date,
			 precip_monthly_extreme, precip_monthly_extreme_year,
			 precip_daily_extreme, precip_daily_extreme_date,
			 tmax_years_with_data, tmin_years_with_data, tmean_years_with_data,
			 precip_years_with_data, evap_years_with_data,
			 rain_days, rain_days_years_with_data)
			VALUES (1, '1961-1990', 1,
			        1, 2, 3, 'd1', 4, 5, 6, 'd2', 7, 8, 9, 'd3',
			        10, 11, 12, 13, 14, 15, 16)`)
		mustExec(`INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
			VALUES (1, '1985-01-01', 1, 2, 3, 4)`)
	})

	// valueColumns lists a table's columns minus its identity/key
	// columns — the exact set a complete diff must itemize.
	valueColumns := func(table string, keys ...string) []string {
		ginkgo.GinkgoHelper()
		rows, err := db.Query(`SELECT name FROM pragma_table_info(?) ORDER BY cid`, table)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = rows.Close() }()
		var cols []string
		for rows.Next() {
			var name string
			Expect(rows.Scan(&name)).To(Succeed())
			if !slices.Contains(keys, name) {
				cols = append(cols, name)
			}
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		Expect(cols).NotTo(BeEmpty(), "table %s must exist in the schema", table)
		return cols
	}

	ginkgo.It("covers every stations column except identity", func(ctx ginkgo.SpecContext) {
		rows, err := loadStationRows(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		seeded, ok := rows[stationKey{source: "conagua_conventional", externalID: "1001"}]
		Expect(ok).To(BeTrue())

		c := dbDiffCollector{}
		diffStationRow(&c, seeded, stationRow{})
		Expect(recordedColumns(c.diffs)).To(ConsistOf(
			valueColumns("stations", "id", "source", "external_id")))
	})

	ginkgo.It("covers every monthly_normals column except the key", func(ctx ginkgo.SpecContext) {
		rows, err := loadNormalsRows(ctx, db, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(HaveLen(1))

		c := dbDiffCollector{}
		diffNormalsDBRow(&c, rows[0], normalsRow{})
		Expect(recordedColumns(c.diffs)).To(ConsistOf(
			valueColumns("monthly_normals", "station_id", "period", "month")))
	})

	ginkgo.It("covers every monthly_normals_extras column except the key", func(ctx ginkgo.SpecContext) {
		rows, err := loadExtrasRows(ctx, db, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(HaveLen(1))

		c := dbDiffCollector{}
		diffExtrasDBRow(&c, rows[0], extrasRow{})
		Expect(recordedColumns(c.diffs)).To(ConsistOf(
			valueColumns("monthly_normals_extras", "station_id", "period", "month")))
	})

	ginkgo.It("covers every daily_observations column except the key", func(ctx ginkgo.SpecContext) {
		rows, err := loadDailyRows(ctx, db, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(HaveLen(1))

		c := dbDiffCollector{}
		diffDailyDBRow(&c, rows[0], dailyDBRow{})
		Expect(recordedColumns(c.diffs)).To(ConsistOf(
			valueColumns("daily_observations", "station_id", "date")))
	})
})
