package publish_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// parquetSpecs is the ten logical tables a tabular archive carries as
// Parquet, keyed by in-archive path — one file per logical table, the
// spec's name with the .parquet suffix.
var parquetSpecs = map[string]publish.FileSpec{
	"conagua/stations.parquet":               publish.Stations,
	"conagua/monthly_normals.parquet":        publish.MonthlyNormals,
	"conagua/monthly_normals_extras.parquet": publish.MonthlyNormalsExtras,
	"conagua/daily_observations.parquet":     publish.DailyObservations,
	"nasa_power/cells.parquet":               publish.Cells,
	"nasa_power/station_cell_map.parquet":    publish.StationCellMap,
	"nasa_power/monthly.parquet":             publish.PowerMonthly,
	"nasa_power/daily.parquet":               publish.PowerDaily,
	"combined/combined_monthly.parquet":      publish.CombinedMonthly.FileSpec,
	"combined/combined_daily.parquet":        publish.CombinedDaily.FileSpec,
}

// parquetPaths is parquetSpecs' keys, sorted — the order the entries
// must come in.
func parquetPaths() []string {
	paths := make([]string, 0, len(parquetSpecs))
	for p := range parquetSpecs {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	return paths
}

// openParquet parses a rendered entry with the library's reader.
func openParquet(data []byte) *parquet.File {
	GinkgoHelper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	Expect(err).NotTo(HaveOccurred())
	return f
}

// readParquetRows reads every row of f through the row reader, cloned
// out of the reader's buffers.
func readParquetRows(f *parquet.File) []parquet.Row {
	GinkgoHelper()
	r := parquet.NewReader(f)
	defer func() { _ = r.Close() }()
	var out []parquet.Row
	buf := make([]parquet.Row, 7)
	for {
		n, err := r.ReadRows(buf)
		for _, row := range buf[:n] {
			out = append(out, row.Clone())
		}
		if errors.Is(err, io.EOF) {
			return out
		}
		Expect(err).NotTo(HaveOccurred())
	}
}

// parquetCells renders a row as its CSV cells — NULL as the empty cell,
// a double re-formatted at the column's decimals through the one
// formatter, an int64 as digits, bytes verbatim — checking on the way
// that each value sits at its column's index with the physical type its
// Kind maps to.
func parquetCells(spec publish.FileSpec, row parquet.Row) []string {
	GinkgoHelper()
	Expect(row).To(HaveLen(len(spec.Columns)))
	cells := make([]string, len(row))
	for i, v := range row {
		c := spec.Columns[i]
		Expect(v.Column()).To(Equal(i), c.Name)
		switch {
		case v.IsNull():
			Expect(c.Key).To(BeFalse(), "NULL in key column "+c.Name)
			cells[i] = ""
		case c.Kind == publish.KindReal:
			Expect(v.Kind()).To(Equal(parquet.Double), c.Name)
			cells[i] = publish.FormatReal(v.Double(), c.Decimals)
		case c.Kind == publish.KindInt:
			Expect(v.Kind()).To(Equal(parquet.Int64), c.Name)
			cells[i] = strconv.FormatInt(v.Int64(), 10)
		default:
			Expect(v.Kind()).To(Equal(parquet.ByteArray), c.Name)
			cells[i] = string(v.ByteArray())
		}
	}
	return cells
}

// parquetTable renders one entry and returns its rows as CSV cells.
func parquetTable(e archive.Entry, spec publish.FileSpec) [][]string {
	GinkgoHelper()
	var rows [][]string
	for _, row := range readParquetRows(openParquet(render(e))) {
		rows = append(rows, parquetCells(spec, row))
	}
	return rows
}

// csvTwinRows concatenates, header dropped, the CSV entries that carry
// spec's table: the one file of a whole-state table, or the per-station
// / per-cell shards in path order — the row set the Parquet file must
// equal cell for cell.
func csvTwinRows(entries []archive.Entry, spec publish.FileSpec) [][]string {
	GinkgoHelper()
	var rows [][]string
	found := 0
	for _, e := range entries {
		if e.Path != spec.Name+".csv" && !strings.HasPrefix(e.Path, spec.Name+"/") {
			continue
		}
		found++
		recs := records(render(e))
		Expect(recs[0]).To(Equal(spec.Header()), e.Path)
		rows = append(rows, recs[1:]...)
	}
	Expect(found).To(BeNumerically(">", 0), spec.Name)
	return rows
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func columnIndex(spec publish.FileSpec, name string) int {
	GinkgoHelper()
	i := slices.Index(spec.Header(), name)
	Expect(i).To(BeNumerically(">=", 0), name)
	return i
}

const pinnedCreatedBy = "conagua-etl version 0.1(build parquet-go-v0.32.0)"

var _ = Describe("ParquetEntries", func() {
	var (
		ctx        context.Context
		db         *sql.DB
		ids        map[string]int64
		entries    []archive.Entry
		csvEntries []archive.Entry
	)

	BeforeEach(func() {
		ctx = context.Background()
		db = openTempDB()
		ids = seedTwoStates(db)
		seedPower(db, ids)
		var err error
		entries, err = publish.ParquetEntries(ctx, db, yucatan)
		Expect(err).NotTo(HaveOccurred())

		conagua, err := publish.ConaguaEntries(ctx, db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())
		nasaPower, err := publish.PowerEntries(ctx, db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())
		combined, err := publish.CombinedEntries(ctx, db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())
		csvEntries = slices.Concat(conagua, nasaPower, combined)
	})

	It("lists the ten per-table Parquet files, Path-sorted, disjoint from the CSV paths", func() {
		var paths []string
		for _, e := range entries {
			paths = append(paths, e.Path)
		}
		Expect(paths).To(Equal(parquetPaths()))
		for _, e := range csvEntries {
			Expect(paths).NotTo(ContainElement(e.Path))
		}
	})

	It("holds every file to its CSV twin cell for cell, in the CSV's row order, NULL as the empty cell", func() {
		for _, path := range parquetPaths() {
			spec := parquetSpecs[path]
			got := parquetTable(entryByPath(entries, path), spec)
			Expect(got).NotTo(BeEmpty(), path)
			Expect(got).To(Equal(csvTwinRows(csvEntries, spec)), path)
		}
	})

	It("round-trips a null-heavy wide row: a pre-1981 combined row with its 31 POWER nulls", func() {
		spec := publish.CombinedDaily.FileSpec
		rows := readParquetRows(openParquet(render(entryByPath(entries, "combined/combined_daily.parquet"))))
		var found bool
		for _, row := range rows {
			cells := parquetCells(spec, row)
			if cells[0] != "3101" || cells[1] != "1975-06-15" {
				continue
			}
			found = true
			Expect(cells).To(Equal(append([]string{"3101", "1975-06-15", cellShared, "0.000", "36.20", "22.00", "45.10", "7.30"},
				make([]string, 31)...)))
			nulls := 0
			for _, v := range row {
				if v.IsNull() {
					nulls++
				}
			}
			Expect(nulls).To(Equal(31))
		}
		Expect(found).To(BeTrue())
	})

	It("declares each file's schema as its spec: export names in export order, keys required, the rest optional, types per Kind", func() {
		for _, path := range parquetPaths() {
			spec := parquetSpecs[path]
			f := openParquet(render(entryByPath(entries, path)))
			fields := f.Schema().Fields()
			Expect(fields).To(HaveLen(len(spec.Columns)), path)
			var names []string
			for i, field := range fields {
				c := spec.Columns[i]
				names = append(names, field.Name())
				Expect(field.Leaf()).To(BeTrue(), c.Name)
				Expect(field.Repeated()).To(BeFalse(), c.Name)
				Expect(field.Required()).To(Equal(c.Key), path+": "+c.Name)
				Expect(field.Optional()).To(Equal(!c.Key), path+": "+c.Name)
				switch c.Kind {
				case publish.KindInt:
					Expect(field.Type().Kind()).To(Equal(parquet.Int64), c.Name)
					Expect(field.Type().LogicalType()).NotTo(BeNil(), c.Name)
					Expect(field.Type().LogicalType().Value).To(Equal(&format.IntType{BitWidth: 64, IsSigned: true}), c.Name)
				case publish.KindReal:
					Expect(field.Type().Kind()).To(Equal(parquet.Double), c.Name)
					Expect(field.Type().LogicalType()).To(BeNil(), c.Name)
				default:
					Expect(field.Type().Kind()).To(Equal(parquet.ByteArray), c.Name)
					Expect(field.Type().LogicalType()).NotTo(BeNil(), c.Name)
					Expect(field.Type().LogicalType().Value).To(BeAssignableToTypeOf(&format.StringType{}), c.Name)
				}
			}
			Expect(names).To(Equal(spec.Header()), path)
			Expect(f.Schema().Columns()).To(Equal(publish.ParquetSchema(spec).Columns()), path)
		}
	})

	It("writes byte-identical files on two renders, Zstd throughout, the pinned created_by, no key-value metadata", func() {
		for _, path := range parquetPaths() {
			e := entryByPath(entries, path)
			first := render(e)
			Expect(sha256Hex(render(e))).To(Equal(sha256Hex(first)), path)
			md := openParquet(first).Metadata()
			Expect(md.CreatedBy).To(Equal(pinnedCreatedBy), path)
			Expect(md.KeyValueMetadata).To(BeEmpty(), path)
			Expect(md.RowGroups).NotTo(BeEmpty(), path)
			for _, rg := range md.RowGroups {
				for _, col := range rg.Columns {
					Expect(col.MetaData.Codec).To(Equal(format.Zstd), path+": "+strings.Join(col.MetaData.PathInSchema, "."))
				}
			}
		}
	})

	It("pins the library version in created_by to the module go.mod requires", func() {
		// go test runs a package's specs with the package directory as
		// the working directory; the test binary's build info carries no
		// dependency list, so the pin is held to go.mod itself.
		gomod, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
		Expect(err).NotTo(HaveOccurred())
		var version string
		for _, line := range strings.Split(string(gomod), "\n") {
			if fields := strings.Fields(line); len(fields) >= 2 && fields[0] == "github.com/parquet-go/parquet-go" {
				version = fields[1]
			}
		}
		Expect(version).NotTo(BeEmpty())
		Expect(pinnedCreatedBy).To(HaveSuffix("(build parquet-go-" + version + ")"))
		Expect(pinnedCreatedBy).To(HavePrefix("conagua-etl version " + publish.DatasetVersion + "("))
	})

	It("stores a REAL as the nearest double of its decimal rounded to the fixed export precision, positive zero for a value that rounds to zero", func() {
		mustExec(db, `UPDATE monthly_supplement SET solar_ghi_wm2 = 228.472222222222
		  WHERE cell_id = ? AND period = '1981-2010' AND month = 1`, cellShared)
		spec := publish.PowerMonthly
		solar := columnIndex(spec, "solar_ghi_wm2")
		var found bool
		for _, row := range readParquetRows(openParquet(render(entryByPath(entries, "nasa_power/monthly.parquet")))) {
			cells := parquetCells(spec, row)
			if cells[0] != cellShared || cells[1] != "1981-2010" || cells[2] != "1" {
				continue
			}
			found = true
			Expect(math.Float64bits(row[solar].Double())).To(Equal(math.Float64bits(228.47)))
			Expect(cells[solar]).To(Equal("228.47"))
		}
		Expect(found).To(BeTrue())

		// The observed values terminate at two decimals, so the hundredths
		// digit is load-bearing: the fixture's -0.04 keeps its own value
		// and a trace 0.01 mm stays apart from a dry day. Only a negative
		// below the two-decimal step rounds away, and it rounds to
		// positive zero.
		mustExec(db, `UPDATE daily_observations SET tmax = -0.004, precip = 0.01
		  WHERE station_id = ? AND date = '1999-12-31'`, ids["conv/31001"])
		spec = publish.DailyObservations
		tmax, tmin, precip := columnIndex(spec, "tmax_c"), columnIndex(spec, "tmin_c"), columnIndex(spec, "precip_mm")
		found = false
		for _, row := range readParquetRows(openParquet(render(entryByPath(entries, "conagua/daily_observations.parquet")))) {
			cells := parquetCells(spec, row)
			if cells[0] != "31001" || cells[1] != "1999-12-31" {
				continue
			}
			found = true
			Expect(math.Float64bits(row[tmin].Double())).To(Equal(math.Float64bits(-0.04)))
			Expect(cells[tmin]).To(Equal("-0.04"))
			Expect(math.Float64bits(row[precip].Double())).To(Equal(math.Float64bits(0.01)))
			Expect(row[precip].Double()).NotTo(Equal(0.0))
			Expect(cells[precip]).To(Equal("0.01"))
			Expect(row[tmax].Double()).To(Equal(0.0))
			Expect(math.Signbit(row[tmax].Double())).To(BeFalse())
			Expect(cells[tmax]).To(Equal("0.00"))
		}
		Expect(found).To(BeTrue())
	})

	It("streams a table longer than one batch into one row group, still equal to its CSV twin", func() {
		mustExec(db, `INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
		  WITH RECURSIVE d(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM d WHERE n < 2999)
		  SELECT ?, date('1900-01-01', n || ' days'), n * 0.1, NULL, 0.0, CASE WHEN n % 2 = 0 THEN n * 1.5 END FROM d`,
			ids["conv/31002"])
		spec := publish.DailyObservations
		data := render(entryByPath(entries, "conagua/daily_observations.parquet"))
		f := openParquet(data)
		Expect(f.NumRows()).To(Equal(int64(3000 + 4)))
		Expect(f.Metadata().RowGroups).To(HaveLen(1))
		var got [][]string
		for _, row := range readParquetRows(f) {
			got = append(got, parquetCells(spec, row))
		}
		Expect(got).To(Equal(csvTwinRows(csvEntries, spec)))
	})

	It("closes a row group at parquetRowGroupRows, so the buffer is bounded whatever the table's length", func() {
		// 262,145 rows for the station with no daily rows: one full row
		// group of 262,144 plus a second holding the remainder and the
		// fixture's four rows that follow in station order.
		mustExec(db, `INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
		  WITH RECURSIVE d(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM d WHERE n < 262144)
		  SELECT ?, date('1900-01-01', n || ' days'), n * 0.1, NULL, 0.0, CASE WHEN n % 2 = 0 THEN n * 1.5 END FROM d`,
			ids["conv/31003"])
		spec := publish.DailyObservations
		f := openParquet(render(entryByPath(entries, "conagua/daily_observations.parquet")))
		Expect(f.NumRows()).To(Equal(int64(262145 + 4)))
		groups := f.Metadata().RowGroups
		Expect(groups).To(HaveLen(2))
		Expect(groups[0].NumRows).To(Equal(int64(262144)))
		Expect(groups[1].NumRows).To(Equal(int64(5)))
		var got [][]string
		for _, row := range readParquetRows(f) {
			got = append(got, parquetCells(spec, row))
		}
		Expect(got).To(Equal(csvTwinRows(csvEntries, spec)))
	})

	It("writes a valid, readable file with the schema and zero rows for an empty table", func() {
		empty := publish.State{Code: "NLE", Slug: "nle", Name: "Nuevo León"}
		emptyEntries, err := publish.ParquetEntries(ctx, db, empty)
		Expect(err).NotTo(HaveOccurred())
		for _, path := range parquetPaths() {
			spec := parquetSpecs[path]
			f := openParquet(render(entryByPath(emptyEntries, path)))
			Expect(f.NumRows()).To(BeZero(), path)
			Expect(readParquetRows(f)).To(BeEmpty(), path)
			var names []string
			for _, field := range f.Schema().Fields() {
				names = append(names, field.Name())
			}
			Expect(names).To(Equal(spec.Header()), path)
			Expect(f.Metadata().CreatedBy).To(Equal(pinnedCreatedBy), path)
		}
	})

	It("refuses a non-finite REAL by unit, row, and column rather than write it", func() {
		mustExec(db, `UPDATE daily_supplement SET ps_kpa = 9e999 WHERE cell_id = ? AND date = '2020-01-01'`, cellShared)
		err := entryByPath(entries, "nasa_power/daily.parquet").Write(io.Discard)
		Expect(err).To(MatchError("cell " + cellShared + ": row 2: column ps_kpa: non-finite value +Inf"))
		err = entryByPath(entries, "combined/combined_daily.parquet").Write(io.Discard)
		Expect(err).To(MatchError("station 31001: row 2: column ps_kpa: non-finite value +Inf"))
	})

	It("numbers the refused row within its unit, as the CSV shard does, not across the file", func() {
		// 3101 sorts after 31001 and 31002, which contribute three rows
		// before it; its first row is row 1 in both formats.
		mustExec(db, `UPDATE daily_observations SET tmax = 9e999 WHERE station_id = ?`, ids["conv/3101"])
		err := entryByPath(entries, "conagua/daily_observations.parquet").Write(io.Discard)
		Expect(err).To(MatchError("station 3101: row 1: column tmax_c: non-finite value +Inf"))
		err = entryByPath(csvEntries, "conagua/daily_observations/yuc/daily-3101.csv").Write(io.Discard)
		Expect(err).To(MatchError("conagua/daily_observations: row 1: column tmax_c: non-finite value +Inf"))
	})

	It("refuses a NULL in a key column: the leaf is required", func() {
		e := publish.ParquetTableEntry(ctx, db, "conagua/daily_observations.parquet", publish.DailyObservations,
			`SELECT NULL, '2020-01-01', 1.0, 1.0, 1.0, 1.0`)
		Expect(e.Write(io.Discard)).To(MatchError("row 1: column station_id: NULL in a key column"))
	})

	It("honours cancellation at write time", func() {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancelable, err := publish.ParquetEntries(cancelCtx, db, yucatan)
		Expect(err).NotTo(HaveOccurred())
		cancel()
		err = entryByPath(cancelable, "conagua/stations.parquet").Write(io.Discard)
		Expect(errors.Is(err, context.Canceled)).To(BeTrue(), err)
	})
})
