package publish_test

// Specs for national-parquet.zip's ten files: the same paths as a
// state's twins; each file equal, cell for cell, to the national CSV
// rendering — the seven whole-scope tables regenerated at national
// scope, the three per-unit tables the state archives' shards
// concatenated in national unit order, a shared cell's shard once with
// identical bytes wherever it ships — and equal to the per-state Parquet
// files' rows in national order; the writer and its determinism are the
// state builder's own.

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"path"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// perUnitSpecs are the three tables the CSV grain shards per station or
// per cell; their national Parquet files are those shards in national
// unit order.
var perUnitSpecs = map[string]bool{
	publish.DailyObservations.Name: true,
	publish.PowerDaily.Name:        true,
	publish.CombinedDaily.Name:     true,
}

// nationalCSVRows renders spec's rows as the national CSV archive
// carries them: a whole-scope table regenerated at national scope; a
// per-unit table as the state archives' shards — each looked up by its
// unit id, a shard two states carry required byte-identical — in the
// national station or cell order.
func nationalCSVRows(ctx context.Context, db *sql.DB, spec publish.FileSpec) [][]string {
	GinkgoHelper()
	if !perUnitSpecs[spec.Name] {
		return csvTwinRows(publish.NationalTableEntries(ctx, db), spec)
	}
	shards := map[string][]byte{}
	for _, st := range nationalStates {
		for _, e := range csvFolders(ctx, db, st) {
			if !strings.HasPrefix(e.Path, spec.Name+"/") {
				continue
			}
			unit := strings.TrimSuffix(strings.TrimPrefix(path.Base(e.Path), "daily-"), ".csv")
			data := render(e)
			if prior, shared := shards[unit]; shared {
				Expect(data).To(Equal(prior), "%s shard %s differs between states", spec.Name, unit)
			}
			shards[unit] = data
		}
	}
	var units []string
	var err error
	if spec.Name == publish.PowerDaily.Name {
		units, err = publish.NationalCellIDs(ctx, db)
	} else {
		units, err = publish.NationalStationIDs(ctx, db)
	}
	Expect(err).NotTo(HaveOccurred())
	Expect(shards).To(HaveLen(len(units)), spec.Name)
	var rows [][]string
	for _, u := range units {
		recs := records(shards[u])
		Expect(recs[0]).To(Equal(spec.Header()), u)
		rows = append(rows, recs[1:]...)
	}
	return rows
}

var _ = Describe("NationalParquetEntries", func() {
	var (
		ctx     = context.Background()
		db      *sql.DB
		ids     map[string]int64
		entries []archive.Entry
	)

	BeforeEach(func() {
		db = openTempDB()
		ids = seedTwoStates(db)
		seedPower(db, ids)
		seedNational(db, ids)
		var err error
		entries, err = publish.NationalParquetEntries(ctx, db)
		Expect(err).NotTo(HaveOccurred())
	})

	It("lists the ten per-table files at the state twins' paths, Path-sorted", func() {
		Expect(entryPaths(entries)).To(Equal(parquetPaths()))
	})

	It("holds every file to the national CSV rendering cell for cell: whole-scope tables regenerated, per-unit tables the state shards in national unit order", func() {
		for _, p := range parquetPaths() {
			spec := parquetSpecs[p]
			got := parquetTable(entryByPath(entries, p), spec)
			Expect(got).NotTo(BeEmpty(), p)
			Expect(got).To(Equal(nationalCSVRows(ctx, db, spec)), p)
		}
		// The per-station files span the three states in one order.
		daily := parquetTable(entryByPath(entries, "conagua/daily_observations.parquet"), publish.DailyObservations)
		Expect(keyCells(daily, 2)).To(Equal([][]string{
			{"1001", "2020-01-01"}, {"1002", "2020-01-01"},
			{"31001", "1999-12-31"}, {"31001", "2020-01-01"}, {"31001", "2020-01-02"},
			{"3101", "1975-06-15"}, {"32001", "2020-01-01"},
		}))
	})

	It("equals the per-state Parquet files' rows concatenated in national order, a cell two states share once", func() {
		for _, p := range parquetPaths() {
			spec := parquetSpecs[p]
			perState := make([][][]string, len(nationalStates))
			for i, st := range nationalStates {
				stateEntries, err := publish.ParquetEntries(ctx, db, st)
				Expect(err).NotTo(HaveOccurred())
				perState[i] = parquetTable(entryByPath(stateEntries, p), spec)
			}
			Expect(parquetTable(entryByPath(entries, p), spec)).To(Equal(unionRows(spec, perState...)), p)
		}
		cells := parquetTable(entryByPath(entries, "nasa_power/cells.parquet"), publish.Cells)
		Expect(keyCells(cells, 1)).To(Equal([][]string{{cellSingle}, {cellShared}, {cellAGS}}))
		daily := parquetTable(entryByPath(entries, "nasa_power/daily.parquet"), publish.PowerDaily)
		Expect(keyCells(daily, 2)).To(Equal([][]string{
			{cellShared, "1999-12-31"}, {cellShared, "2020-01-01"}, {cellShared, "2020-01-02"},
			{cellAGS, "2020-01-01"},
		}))
	})

	It("joins the other state's station to the shared cell in combined_daily, and keeps a station with no cell on its observed values", func() {
		spec := publish.CombinedDaily.FileSpec
		rows := parquetTable(entryByPath(entries, "combined/combined_daily.parquet"), spec)
		Expect(rows).To(ContainElement(withPower(
			[]string{"1002", "2020-01-01", cellShared, "40.000", "25.00", "10.00", "1.50", ""}, powerWant(fullPowerRow))))
		Expect(rows).To(ContainElement(withPower(
			[]string{"32001", "2020-01-01", "", "", "18.00", "2.00", "0.00", "4.40"}, noPower)))
	})

	It("writes byte-identical files on two renders with the pinned created_by", func() {
		for _, p := range parquetPaths() {
			e := entryByPath(entries, p)
			first := render(e)
			Expect(sha256Hex(render(e))).To(Equal(sha256Hex(first)), p)
			Expect(openParquet(first).Metadata().CreatedBy).To(Equal(pinnedCreatedBy), p)
		}
	})

	It("honours cancellation at write time", func() {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancelable, err := publish.NationalParquetEntries(cancelCtx, db)
		Expect(err).NotTo(HaveOccurred())
		cancel()
		err = entryByPath(cancelable, "conagua/daily_observations.parquet").Write(io.Discard)
		Expect(errors.Is(err, context.Canceled)).To(BeTrue(), err)
	})
})
