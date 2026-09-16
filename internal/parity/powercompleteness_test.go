// Power-comparator completeness specs, in-package to reach the
// unexported column derivation, loaders, and diff functions (see
// completeness_test.go for the package split).
//
// Two tripwires. The drift comparator's value-column list — derived
// from the power registry — must equal each supplement table's DDL
// value columns *in order*: the list builds the SELECT and aligns the
// scan targets, so an order regression would silently mis-attribute
// every value. And the cell-math diff functions must cover every
// column of their schema tables, keys excluded — a column added to
// schema.sql fails here until the loader and the diff itemization
// carry it.
package parity

import (
	"database/sql"
	"path/filepath"
	"slices"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// recordedPowerColumns extracts the Column of every recorded diff.
func recordedPowerColumns(diffs []PowerValueDiff) []string {
	cols := make([]string, len(diffs))
	for i, d := range diffs {
		cols[i] = d.Column
	}
	return cols
}

var _ = ginkgo.Describe("Power comparator completeness", func() {
	var db *sql.DB

	mustExec := func(query string, args ...any) {
		ginkgo.GinkgoHelper()
		_, err := db.Exec(query, args...)
		Expect(err).NotTo(HaveOccurred())
	}

	ginkgo.BeforeEach(func() {
		var err error
		db, err = schema.Open(filepath.Join(ginkgo.GinkgoT().TempDir(), "power-complete.db"))
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { Expect(db.Close()).To(Succeed()) })
	})

	// valueColumns lists a table's columns minus the excluded ones, in
	// DDL (cid) order.
	valueColumns := func(table string, excluded ...string) []string {
		ginkgo.GinkgoHelper()
		rows, err := db.Query(`SELECT name FROM pragma_table_info(?) ORDER BY cid`, table)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = rows.Close() }()
		var cols []string
		for rows.Next() {
			var name string
			Expect(rows.Scan(&name)).To(Succeed())
			if !slices.Contains(excluded, name) {
				cols = append(cols, name)
			}
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		Expect(cols).NotTo(BeEmpty(), "table %s must exist in the schema", table)
		return cols
	}

	ginkgo.It("derives exactly monthly_supplement's value columns, in DDL order", func() {
		Expect(supplementValueColumns()).To(Equal(
			valueColumns("monthly_supplement", "cell_id", "period", "month", "power_run_id")))
	})

	ginkgo.It("derives exactly daily_supplement's value columns, in DDL order", func() {
		Expect(supplementValueColumns()).To(Equal(
			valueColumns("daily_supplement", "cell_id", "date", "power_run_id")))
	})

	ginkgo.It("covers every nasa_power_grid_cells column except the key", func(ctx ginkgo.SpecContext) {
		mustExec(`INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution)
			VALUES ('19.5N_99.3750W', 19.5, -99.375, '0.5x0.625')`)
		stored, err := loadGridCellRows(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		Expect(stored).To(HaveKey("19.5N_99.3750W"))

		c := powerDiffCollector{}
		diffGridCellRow(&c, power.Cell{}, stored["19.5N_99.3750W"])
		Expect(recordedPowerColumns(c.diffs)).To(ConsistOf(
			valueColumns("nasa_power_grid_cells", "cell_id")))
	})

	ginkgo.It("covers every station_power_cell column except the station key", func(ctx ginkgo.SpecContext) {
		mustExec(`INSERT INTO station_power_cell (station_id, cell_id, distance_km)
			VALUES (1, '19.5N_99.3750W', 1.5)`)
		stored, err := loadLinkRows(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		Expect(stored).To(HaveKey(int64(1)))

		c := powerDiffCollector{}
		diffLinkRow(&c, "derived-cell", 123.5, stored[1])
		Expect(recordedPowerColumns(c.diffs)).To(ConsistOf(
			valueColumns("station_power_cell", "station_id")))
	})
})
