package publish_test

// Specs for the edges of the {state}.db row set, each pinned by
// hand rather than to the source's own filtering: the warnings of a
// station that has them, none for one that has not, the no-station
// warnings out, the ids as stored; a cell two states share present in
// both databases with the same whole POWER row set and each state's own
// mappings only; and a state with no cell shipping every run whole —
// the unreferenced and the aborted included — beside empty POWER tables.

import (
	"context"
	"database/sql"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// queryAll reads every row of query as the driver's native values.
func queryAll(db *sql.DB, query string, args ...any) [][]any {
	GinkgoHelper()
	rows, err := db.Query(query, args...)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	Expect(err).NotTo(HaveOccurred())
	var out [][]any
	for rows.Next() {
		row := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range row {
			ptrs[i] = &row[i]
		}
		Expect(rows.Scan(ptrs...)).To(Succeed())
		out = append(out, row)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

var _ = Describe("StateDBEntry's row-set edges", func() {
	var (
		ctx     = context.Background()
		srcPath string
		ids     map[string]int64
		tmpDir  string
	)

	stateDB := func(st publish.State) *sql.DB {
		GinkgoHelper()
		data, entryPath := buildStateDB(ctx, srcPath, st, tmpDir)
		Expect(entryPath).To(Equal(st.Slug + ".db"))
		return openRO(writeDB(data, entryPath))
	}

	BeforeEach(func() {
		srcPath, ids = seedStateDBSource()
		tmpDir = filepath.Join(GinkgoT().TempDir(), "out")
	})

	It("carries a station's warnings whole with their stored ids, none for a station without, and keeps the no-station warnings out", func() {
		yuc := stateDB(yucatan)
		Expect(selectAll(yuc, "parsing_warnings", "station_id = ?", ids["conv/31001"])).To(Equal([][]any{
			{int64(1), ids["conv/31001"], "daily/31001.txt", int64(3), "warn", "unparseable value"},
			{int64(2), ids["conv/31001"], "normals/31001.txt", nil, "error", "period mismatch"},
		}))
		Expect(selectAll(yuc, "parsing_warnings", "station_id = ?", ids["conv/3101"])).To(Equal([][]any{
			{int64(3), ids["conv/3101"], "daily/3101.txt", int64(10), "warn", "duplicate date"},
		}))
		for _, id := range []string{"conv/31002", "conv/31003"} {
			Expect(count(yuc, `SELECT COUNT(*) FROM stations WHERE id = ?`, ids[id])).To(Equal(1), id)
			Expect(selectAll(yuc, "parsing_warnings", "station_id = ?", ids[id])).To(BeEmpty(), id)
		}
		Expect(count(yuc, `SELECT COUNT(*) FROM parsing_warnings WHERE station_id IS NULL`)).To(BeZero())
		Expect(count(yuc, `SELECT COUNT(*) FROM parsing_warnings`)).To(Equal(3))
		// The severity index is there for the rows that came over.
		Expect(queryAll(yuc, `SELECT severity, COUNT(*) FROM parsing_warnings GROUP BY severity ORDER BY severity`)).
			To(Equal([][]any{{"error", int64(1)}, {"warn", int64(2)}}))

		// The other state's database: its own station's warning, nothing
		// of Yucatán's, nothing of the EMA station's.
		ags := stateDB(aguascalientes)
		Expect(selectAll(ags, "parsing_warnings", "")).To(Equal([][]any{
			{int64(4), ids["conv/1001"], "daily/1001.txt", int64(1), "warn", "other state"},
		}))
	})

	It("ships a cell two states share in both databases, each with the cell's whole POWER row set and its own stations' mappings only", func() {
		yuc := stateDB(yucatan)
		ags := stateDB(aguascalientes)
		src := openRO(srcPath)

		shared := []any{cellShared, 21.5, -89.375, "0.5x0.625"}
		Expect(selectAll(yuc, "nasa_power_grid_cells", "")).To(Equal([][]any{
			{cellSingle, 21.0, -89.625, "0.5x0.625"},
			shared,
		}))
		Expect(selectAll(ags, "nasa_power_grid_cells", "")).To(Equal([][]any{
			shared,
			{cellAGS, 22.0, -102.5, "0.5x0.625"},
		}))
		for _, t := range []string{"monthly_supplement", "daily_supplement"} {
			want := selectAll(src, t, "cell_id = ?", cellShared)
			Expect(want).To(HaveLen(3), t)
			Expect(selectAll(yuc, t, "cell_id = ?", cellShared)).To(Equal(want), t)
			Expect(selectAll(ags, t, "cell_id = ?", cellShared)).To(Equal(want), t)
		}

		Expect(selectAll(yuc, "station_power_cell", "station_id = ?", ids["conv/31001"])).
			To(Equal([][]any{{ids["conv/31001"], cellShared, 27.2524061152165}}))
		Expect(selectAll(yuc, "station_power_cell", "station_id = ?", ids["conv/3101"])).
			To(Equal([][]any{{ids["conv/3101"], cellShared, 0.0}}))
		Expect(selectAll(yuc, "station_power_cell", "station_id = ?", ids["conv/31003"])).
			To(Equal([][]any{{ids["conv/31003"], cellSingle, 12.5}}))
		Expect(count(yuc, `SELECT COUNT(*) FROM station_power_cell`)).To(Equal(3))
		Expect(selectAll(ags, "station_power_cell", "station_id = ?", ids["conv/1001"])).
			To(Equal([][]any{{ids["conv/1001"], cellAGS, 3.0}}))
		Expect(selectAll(ags, "station_power_cell", "station_id = ?", ids["conv/1002"])).
			To(Equal([][]any{{ids["conv/1002"], cellShared, 40.0}}))
		Expect(count(ags, `SELECT COUNT(*) FROM station_power_cell`)).To(Equal(2))

		// Neither database carries the other's private cell, the EMA
		// station's, or the unreferenced one.
		for _, cell := range []string{cellSingle, cellEMA, cellNone} {
			Expect(count(ags, `SELECT COUNT(*) FROM nasa_power_grid_cells WHERE cell_id = ?`, cell)).To(BeZero(), cell)
			Expect(count(ags, `SELECT COUNT(*) FROM daily_supplement WHERE cell_id = ?`, cell)).To(BeZero(), cell)
		}
		for _, cell := range []string{cellAGS, cellEMA, cellNone} {
			Expect(count(yuc, `SELECT COUNT(*) FROM nasa_power_grid_cells WHERE cell_id = ?`, cell)).To(BeZero(), cell)
			Expect(count(yuc, `SELECT COUNT(*) FROM monthly_supplement WHERE cell_id = ?`, cell)).To(BeZero(), cell)
		}
	})

	It("ships every run whole — the aborted, the running, and the unreferenced included — in a state whose stations reference no cell", func() {
		zac := stateDB(zacatecas)
		Expect(queryAll(zac, `SELECT id, snapshot_date, status, etl_git_sha FROM ingest_runs ORDER BY id`)).To(Equal([][]any{
			{int64(1), "2026-05-01", "complete", "0ld5ha"},
			{int64(2), "2026-06-08", "complete", "f1r5t"},
			{int64(3), "2026-06-08", "complete", nil},
			{int64(4), "2026-06-08", "aborted", "ab0rt"},
			{int64(5), "2026-06-08", "running", "runn1ng"},
		}))
		Expect(queryAll(zac, `SELECT id, temporal_mode, status, period_start_year, period_end_year FROM power_runs ORDER BY id`)).
			To(Equal([][]any{
				{int64(1), "monthly", "complete", int64(1981), int64(2010)},
				{int64(2), "daily", "complete", int64(1981), int64(2026)},
				{int64(3), "monthly", "aborted", int64(1991), int64(2020)},
			}))
		for _, t := range []string{"station_power_cell", "nasa_power_grid_cells", "monthly_supplement", "daily_supplement"} {
			Expect(count(zac, `SELECT COUNT(*) FROM `+t)).To(BeZero(), t)
		}
		Expect(queryAll(zac, `SELECT external_id, state FROM stations`)).To(Equal([][]any{{"32001", "ZAC"}}))
	})
})
