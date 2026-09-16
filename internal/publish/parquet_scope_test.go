package publish_test

// Specs for the Parquet twins as typed values and as row sets: every
// file held to its CSV twin cell by cell on the values themselves — a
// double bit for bit the CSV cell's parse, an int64 the digits, a string
// the bytes, a null exactly where the cell is empty — rather than
// through re-formatting, which would let an unrounded double pass; a
// state with a station and nothing keyed to it, whose nine other files
// are schema-only twins of header-only CSVs; and the bulk daily files,
// where a station or cell with no rows contributes none and the file is
// the shards' concatenation in unit order.

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/parquet-go/parquet-go"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// csvFolders renders a state's three data folders as CSV entries — the
// twins the Parquet files are held to.
func csvFolders(ctx context.Context, db *sql.DB, st publish.State) []archive.Entry {
	GinkgoHelper()
	conagua, err := publish.ConaguaEntries(ctx, db, st, nil)
	Expect(err).NotTo(HaveOccurred())
	nasaPower, err := publish.PowerEntries(ctx, db, st, nil)
	Expect(err).NotTo(HaveOccurred())
	combined, err := publish.CombinedEntries(ctx, db, st, nil)
	Expect(err).NotTo(HaveOccurred())
	return slices.Concat(conagua, nasaPower, combined)
}

// keyCells projects rows onto their first n cells.
func keyCells(rows [][]string, n int) [][]string {
	out := make([][]string, len(rows))
	for i, r := range rows {
		out[i] = r[:n]
	}
	return out
}

var _ = Describe("Parquet twins as typed values against their CSV twins", func() {
	var (
		ctx        context.Context
		db         *sql.DB
		entries    []archive.Entry
		csvEntries []archive.Entry
	)

	BeforeEach(func() {
		ctx = context.Background()
		db = openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		var err error
		entries, err = publish.ParquetEntries(ctx, db, yucatan)
		Expect(err).NotTo(HaveOccurred())
		csvEntries = csvFolders(ctx, db, yucatan)
	})

	It("holds, in every file, each double bit for bit to the CSV cell's parse, each int64 to its digits, each string to its bytes, and null to the empty cell", func() {
		// The fixture stores no empty string, so an empty CSV cell is a
		// NULL and the null side of the comparison is exact.
		reals, ints, strs, nulls := 0, 0, 0, 0
		for _, path := range parquetPaths() {
			spec := parquetSpecs[path]
			rows := readParquetRows(openParquet(render(entryByPath(entries, path))))
			csv := csvTwinRows(csvEntries, spec)
			Expect(rows).To(HaveLen(len(csv)), path)
			for r, row := range rows {
				Expect(row).To(HaveLen(len(spec.Columns)), path)
				for i, c := range spec.Columns {
					label := fmt.Sprintf("%s row %d column %s", path, r+1, c.Name)
					v, cell := row[i], csv[r][i]
					if cell == "" {
						Expect(v.IsNull()).To(BeTrue(), label)
						nulls++
						continue
					}
					Expect(v.IsNull()).To(BeFalse(), label)
					switch c.Kind {
					case publish.KindReal:
						want, err := strconv.ParseFloat(cell, 64)
						Expect(err).NotTo(HaveOccurred(), label)
						Expect(v.Kind()).To(Equal(parquet.Double), label)
						Expect(bits(v.Double())).To(Equal(bits(want)), label)
						reals++
					case publish.KindInt:
						want, err := strconv.ParseInt(cell, 10, 64)
						Expect(err).NotTo(HaveOccurred(), label)
						Expect(v.Kind()).To(Equal(parquet.Int64), label)
						Expect(v.Int64()).To(Equal(want), label)
						ints++
					default:
						Expect(v.Kind()).To(Equal(parquet.ByteArray), label)
						Expect(string(v.ByteArray())).To(Equal(cell), label)
						strs++
					}
				}
			}
		}
		// Every class was compared, many times over.
		Expect(reals).To(BeNumerically(">", 100))
		Expect(ints).To(BeNumerically(">", 10))
		Expect(strs).To(BeNumerically(">", 10))
		Expect(nulls).To(BeNumerically(">", 100))
	})
})

var _ = Describe("Parquet twins' row scope", func() {
	var (
		ctx context.Context
		db  *sql.DB
	)

	BeforeEach(func() {
		ctx = context.Background()
		db = openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
	})

	It("writes, for a state whose one station has nothing keyed to it, a one-row stations file and nine schema-only files, each the twin of a header-only CSV", func() {
		entries, err := publish.ParquetEntries(ctx, db, zacatecas)
		Expect(err).NotTo(HaveOccurred())
		csvEntries := csvFolders(ctx, db, zacatecas)

		Expect(parquetTable(entryByPath(entries, "conagua/stations.parquet"), publish.Stations)).To(Equal([][]string{
			{"32001", "Zacatecas (OBS)", "ZAC", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
		}))
		Expect(csvTwinRows(csvEntries, publish.Stations)).To(Equal([][]string{
			{"32001", "Zacatecas (OBS)", "ZAC", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
		}))
		for _, path := range parquetPaths() {
			if path == "conagua/stations.parquet" {
				continue
			}
			spec := parquetSpecs[path]
			f := openParquet(render(entryByPath(entries, path)))
			Expect(f.NumRows()).To(BeZero(), path)
			Expect(readParquetRows(f)).To(BeEmpty(), path)
			var names []string
			for _, field := range f.Schema().Fields() {
				names = append(names, field.Name())
			}
			Expect(names).To(Equal(spec.Header()), path)
			// The state has no cell, so the per-cell table has no CSV
			// shard at all; every other twin is header-only.
			if path == "nasa_power/daily.parquet" {
				Expect(entryPaths(csvEntries)).NotTo(ContainElement(HavePrefix("nasa_power/daily/")))
				continue
			}
			Expect(csvTwinRows(csvEntries, spec)).To(BeEmpty(), path)
		}
		// The station's own daily shards exist and are header-only.
		for _, p := range []string{"conagua/daily_observations/zac/daily-32001.csv", "combined/combined_daily/zac/daily-32001.csv"} {
			Expect(records(render(entryByPath(csvEntries, p)))).To(HaveLen(1), p)
		}
	})

	It("carries, in the bulk daily files, only the stations and cells that have rows, in unit then key order — one with none contributes no row and no error", func() {
		entries, err := publish.ParquetEntries(ctx, db, yucatan)
		Expect(err).NotTo(HaveOccurred())
		csvEntries := csvFolders(ctx, db, yucatan)

		// Four stations in the state; 31002 and 31003 have no daily row.
		Expect(keyCells(parquetTable(entryByPath(entries, "conagua/stations.parquet"), publish.Stations), 1)).
			To(Equal([][]string{{"31001"}, {"31002"}, {"31003"}, {"3101"}}))
		// The observed values carry two decimals, so the fixture's
		// sub-tenth -0.04 reaches the file as itself.
		Expect(parquetTable(entryByPath(entries, "conagua/daily_observations.parquet"), publish.DailyObservations)).To(Equal([][]string{
			{"31001", "1999-12-31", "", "-0.04", "", ""},
			{"31001", "2020-01-01", "30.50", "", "12.40", ""},
			{"31001", "2020-01-02", "31.00", "19.50", "0.00", "4.20"},
			{"3101", "1975-06-15", "36.20", "22.00", "45.10", "7.30"},
		}))
		Expect(keyCells(parquetTable(entryByPath(entries, "combined/combined_daily.parquet"), publish.CombinedDaily.FileSpec), 2)).To(Equal([][]string{
			{"31001", "1999-12-31"}, {"31001", "2020-01-01"}, {"31001", "2020-01-02"}, {"3101", "1975-06-15"},
		}))
		for _, id := range []string{"31002", "31003"} {
			for _, dir := range []string{"conagua/daily_observations", "combined/combined_daily"} {
				p := dir + "/yuc/daily-" + id + ".csv"
				Expect(records(render(entryByPath(csvEntries, p)))).To(HaveLen(1), p)
			}
		}

		// Two referenced cells; the single-station one has no daily row.
		Expect(parquetTable(entryByPath(entries, "nasa_power/cells.parquet"), publish.Cells)).To(Equal([][]string{
			{cellSingle, "21.000", "-89.625"},
			{cellShared, "21.500", "-89.375"},
		}))
		Expect(parquetTable(entryByPath(entries, "nasa_power/daily.parquet"), publish.PowerDaily)).To(Equal([][]string{
			withPower([]string{cellShared, "1999-12-31"}, powerWant(mixedPowerRow)),
			withPower([]string{cellShared, "2020-01-01"}, powerWant(fullPowerRow)),
			withPower([]string{cellShared, "2020-01-02"}, powerWant(allNullPowerRow)),
		}))
		Expect(records(render(entryByPath(csvEntries, "nasa_power/daily/daily-"+cellSingle+".csv")))).To(HaveLen(1))
	})
})
