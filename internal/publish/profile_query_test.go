package publish_test

// Specs holding every profile query's EXPLAIN QUERY PLAN to a
// primary-key or indexed walk, as the folder queries are held: a
// national build runs each per station (or per cell), so a query that
// degraded to a scan would turn the profile path into a full-table pass
// per station.

import (
	"database/sql"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
)

var _ = Describe("the profile queries' plans", func() {
	var db *sql.DB

	BeforeEach(func() {
		db = openTempDB()
	})

	noScan := func(details []string) {
		GinkgoHelper()
		for _, d := range details {
			Expect(d).NotTo(HavePrefix("SCAN"), d)
		}
	}

	It("read the stations row by its integer primary key", func() {
		details := planDetails(db, publish.StationRowSQL, int64(1))
		Expect(details).To(ContainElement(MatchRegexp(`^SEARCH s USING INTEGER PRIMARY KEY \(rowid=\?\)`)))
		noScan(details)
	})

	It("walk a station's normals and extras on their (station_id, period, month) primary keys", func() {
		details := planDetails(db, publish.NormalsSeriesSQL, int64(1))
		Expect(details).To(ContainElement(MatchRegexp(
			`^SEARCH t USING (COVERING )?(INDEX sqlite_autoindex_monthly_normals_1|PRIMARY KEY) \(station_id=\?\)`)))
		noScan(details)

		details = planDetails(db, publish.ExtrasSeriesSQL, int64(1))
		Expect(details).To(ContainElement(MatchRegexp(
			`^SEARCH t USING (COVERING )?(INDEX sqlite_autoindex_monthly_normals_extras_1|PRIMARY KEY) \(station_id=\?\)`)))
		noScan(details)
	})

	It("walk a cell's monthly climatology on the (cell_id, period, month) primary key", func() {
		details := planDetails(db, publish.PowerSeriesSQL, cellShared)
		Expect(details).To(ContainElement(MatchRegexp(
			`^SEARCH t USING (COVERING )?(INDEX sqlite_autoindex_monthly_supplement_1|PRIMARY KEY) \(cell_id=\?\)`)))
		noScan(details)
	})

	It("read the cell's centroid by the grid cells' primary key", func() {
		details := planDetails(db, publish.GridCellSQL, cellShared)
		Expect(details).To(ContainElement(MatchRegexp(
			`^SEARCH c USING (COVERING )?(INDEX sqlite_autoindex_nasa_power_grid_cells_1|PRIMARY KEY) \(cell_id=\?\)`)))
		noScan(details)
	})

	It("walk a station's daily rows on the (station_id, date) index, never a scan", func() {
		details := planDetails(db, publish.DailySeriesSQL, int64(1))
		Expect(details).To(ContainElement(MatchRegexp(
			`^SEARCH d USING (COVERING )?INDEX (idx_daily_station_date|sqlite_autoindex_daily_observations_1) \(station_id=\?\)`)))
		noScan(details)
	})

	It("aggregate a cell's daily extent over the (cell_id, date) primary key, never a scan", func() {
		details := planDetails(db, publish.ReanalysisCoverageSQL, cellShared)
		Expect(details).To(ContainElement(MatchRegexp(
			`^SEARCH d USING (COVERING )?(INDEX sqlite_autoindex_daily_supplement_1|PRIMARY KEY) \(cell_id=\?\)`)))
		noScan(details)
	})
})
