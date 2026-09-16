package publish_test

// Specs at the scope edges of the nasa_power/ folder: a cell that
// stations of two states reference ships in both archives, once each,
// with the same per-cell bytes; the cell centroid rounds to its pinned
// three decimals rather than passing through; a malformed cell_id that
// only an out-of-scope (EMA) station references never reaches the
// state's file set and cannot fail its build.

import (
	"context"
	"database/sql"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

var _ = Describe("PowerEntries at the scope edges", func() {
	var (
		db  *sql.DB
		ids map[string]int64
	)
	aguascalientes := publish.State{Code: "AGS", Slug: "ags", Name: "Aguascalientes"}

	BeforeEach(func() {
		db = openTempDB()
		ids = seedTwoStates(db)
		seedPower(db, ids)
	})

	It("ships a cell referenced from two states in both archives, once each, its rows and daily bytes identical", func() {
		ids["conv/1002"] = upsertStation(db, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "1002", Name: "Calvillo", State: "AGS",
		})
		insertStationCell(db, ids["conv/1002"], cellShared, 40.0)

		ags, err := publish.PowerEntries(context.Background(), db, aguascalientes, nil)
		Expect(err).NotTo(HaveOccurred())
		yuc, err := publish.PowerEntries(context.Background(), db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())

		var agsPaths []string
		for _, e := range ags {
			agsPaths = append(agsPaths, e.Path)
		}
		Expect(agsPaths).To(Equal([]string{
			"nasa_power/cells.csv",
			"nasa_power/daily/daily-" + cellShared + ".csv",
			"nasa_power/daily/daily-" + cellAGS + ".csv",
			"nasa_power/monthly.csv",
			"nasa_power/station_cell_map.csv",
		}))
		Expect(records(render(entryByPath(ags, "nasa_power/cells.csv")))).To(Equal([][]string{
			publish.Cells.Header(),
			{cellShared, "21.500", "-89.375"},
			{cellAGS, "22.000", "-102.500"},
		}))
		Expect(records(render(entryByPath(ags, "nasa_power/station_cell_map.csv")))).To(Equal([][]string{
			publish.StationCellMap.Header(),
			{"1001", cellAGS, "3.000"},
			{"1002", cellShared, "40.000"},
		}))
		Expect(records(render(entryByPath(ags, "nasa_power/monthly.csv")))).To(Equal([][]string{
			publish.PowerMonthly.Header(),
			withPower([]string{cellShared, "1981-2010", "1"}, powerWant(fullPowerRow)),
			withPower([]string{cellShared, "1981-2010", "12"}, powerWant(allNullPowerRow)),
			withPower([]string{cellShared, "1991-2020", "1"}, powerWant(mixedPowerRow)),
			withPower([]string{cellAGS, "1981-2010", "1"}, powerWant(fullPowerRow)),
		}))
		daily := "nasa_power/daily/daily-" + cellShared + ".csv"
		Expect(render(entryByPath(ags, daily))).To(Equal(render(entryByPath(yuc, daily))))
		// Yucatán's scope is unchanged by the other state's reference.
		Expect(records(render(entryByPath(yuc, "nasa_power/cells.csv")))).To(Equal([][]string{
			publish.Cells.Header(),
			{cellSingle, "21.000", "-89.625"},
			{cellShared, "21.500", "-89.375"},
		}))
	})

	It("rounds the cell centroid to three decimals (the fixed export precision), a sub-precision negative to positive zero", func() {
		const cell = "20.7N_89.0000W"
		insertCell(db, cell, 20.6666667, -0.0004)
		insertStationCell(db, ids["conv/31002"], cell, 1.0)
		entries, err := publish.PowerEntries(context.Background(), db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(records(render(entryByPath(entries, "nasa_power/cells.csv")))).To(Equal([][]string{
			publish.Cells.Header(),
			{cell, "20.667", "0.000"},
			{cellSingle, "21.000", "-89.625"},
			{cellShared, "21.500", "-89.375"},
		}))
	})

	It("never lists, and never refuses on, a malformed cell_id only an EMA station references", func() {
		ema := upsertStation(db, ingest.StationUpsert{
			Source: ingest.SourceConaguaEMA, ExternalID: "31099", Name: "EMA Progreso", State: "YUC",
		})
		insertCell(db, "../21.0N_89.0000W", 21.0, -89.0)
		insertStationCell(db, ema, "../21.0N_89.0000W", 1.0)

		entries, err := publish.PowerEntries(context.Background(), db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())
		var paths []string
		for _, e := range entries {
			paths = append(paths, e.Path)
		}
		Expect(paths).To(Equal([]string{
			"nasa_power/cells.csv",
			"nasa_power/daily/daily-" + cellSingle + ".csv",
			"nasa_power/daily/daily-" + cellShared + ".csv",
			"nasa_power/monthly.csv",
			"nasa_power/station_cell_map.csv",
		}))
		Expect(column(records(render(entryByPath(entries, "nasa_power/cells.csv"))), 0)).To(Equal([]string{cellSingle, cellShared}))
	})
})
