package publish_test

// Specs for the values a Parquet twin carries, one column class at a
// time: every REAL decimal class of the fixed export precision (1, 2, 3,
// 4, and 6 decimals) is stored as the nearest double of its rounded
// decimal, bit for bit what the CSV cell parses to, a value that rounds
// to zero as positive zero whatever its sign — and, at the two decimals
// the observed values carry, a trace 0.01 mm rain day staying its own
// value rather than collapsing into the dry day beside it; INTEGER at
// both int64 extremes; TEXT verbatim — quotes, commas, newlines, tabs,
// non-ASCII — with the CSV twin parsing back to the same string; and an
// empty string kept apart from NULL.

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"slices"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/parquet-go/parquet-go"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// realCase is one REAL cell of one file: the key cells that locate its
// row, the column, and the CSV text the cell renders to — which is also
// the decimal the stored double must be the nearest double of.
type realCase struct {
	file   string
	key    []string
	column string
	want   string
}

// parquetRowByKey returns the row of file whose leading cells equal key,
// as parquet values and as CSV cells.
func parquetRowByKey(entries []archive.Entry, file string, key []string) (parquet.Row, []string) {
	GinkgoHelper()
	spec := parquetSpecs[file]
	for _, row := range readParquetRows(openParquet(render(entryByPath(entries, file)))) {
		cells := parquetCells(spec, row)
		if slices.Equal(cells[:len(key)], key) {
			return row, cells
		}
	}
	Fail(fmt.Sprintf("%s: no row keyed %v", file, key))
	return nil, nil
}

// csvRowByKey returns the CSV twin's row keyed by key, header dropped.
func csvRowByKey(entries []archive.Entry, spec publish.FileSpec, key []string) []string {
	GinkgoHelper()
	for _, row := range csvTwinRows(entries, spec) {
		if slices.Equal(row[:len(key)], key) {
			return row
		}
	}
	Fail(fmt.Sprintf("%s.csv: no row keyed %v", spec.Name, key))
	return nil
}

// bits is the IEEE-754 pattern of v — equality on it is exact-value
// identity, the sign of zero included.
func bits(v float64) uint64 { return math.Float64bits(v) }

var _ = Describe("Parquet twins' values by column class", func() {
	var (
		ctx        context.Context
		db         *sql.DB
		ids        map[string]int64
		entries    []archive.Entry
		csvEntries []archive.Entry
	)

	// A crafted value per class beyond the fixture's own: a trace rain
	// day and a negative that rounds to zero at two; a negative that
	// rounds to zero at one; a 3-decimal centroid just off the grid and
	// a negative that rounds to zero at three; a negative that rounds to
	// zero at four; one at six; the int64 extremes in an INTEGER column
	// of each kind of table; a station name with every CSV-hostile byte;
	// an empty municipality beside a NULL one.
	const hostileName = "Tizimín, \"El Cuyo\"\nsegunda línea\tñ — 🌧"

	BeforeEach(func() {
		ctx = context.Background()
		db = openTempDB()
		ids = seedTwoStates(db)
		seedPower(db, ids)
		// CONAGUA's "inappreciable" trace rain, the observation the
		// two-decimal contract exists for: one decimal would publish it
		// as the dry 0.0 the next day genuinely is. Its neighbour tmax
		// is the sub-precision negative at that same class.
		mustExec(db, `UPDATE daily_observations SET precip = 0.01, tmax = -0.004
		  WHERE station_id = ? AND date = '2020-01-01'`, ids["conv/31001"])
		// The same rounds-to-zero rule one class up, where the normals
		// proper terminate.
		mustExec(db, `UPDATE monthly_normals SET precip = -0.04
		  WHERE station_id = ? AND period = '1991-2020' AND month = 1`, ids["conv/31001"])
		mustExec(db, `UPDATE nasa_power_grid_cells SET lat = 21.5004, lon = -0.0004 WHERE cell_id = ?`, cellSingle)
		mustExec(db, `UPDATE stations SET wmo_completeness_bin_1971_2000 = -0.00004 WHERE id = ?`, ids["conv/3101"])
		mustExec(db, `UPDATE stations SET lon = -0.0000004, name = ?, first_year = ?, last_year = ? WHERE id = ?`,
			hostileName, int64(math.MaxInt64), int64(math.MinInt64), ids["conv/31002"])
		mustExec(db, `UPDATE stations SET municipality = '' WHERE id = ?`, ids["conv/31003"])
		mustExec(db, `UPDATE monthly_normals_extras SET tmax_monthly_extreme_year = ?, precip_years_with_data = ?
		  WHERE station_id = ? AND period = '1961-1990' AND month = 6`,
			int64(math.MaxInt64), int64(math.MinInt64), ids["conv/3101"])

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

	It("stores every REAL class as the nearest double of its rounded decimal, the CSV cell's exact value, positive zero when it rounds to zero", func() {
		cases := []realCase{
			// One decimal: the normals proper, the monthly extremes, rain
			// days, wind direction — where the source itself terminates.
			{"conagua/monthly_normals.parquet", []string{"31001", "1991-2020", "1"}, "evap_mm", "150.2"},
			{"conagua/monthly_normals.parquet", []string{"31001", "1981-2010", "12"}, "precip_mm", "24.5"},
			{"conagua/monthly_normals.parquet", []string{"31001", "1991-2020", "1"}, "precip_mm", "0.0"},
			{"conagua/monthly_normals_extras.parquet", []string{"3101", "1961-1990", "6"}, "rain_days", "12.2"},
			{"conagua/monthly_normals_extras.parquet", []string{"31001", "1981-2010", "1"}, "tmin_monthly_extreme_c", "8.2"},
			{"nasa_power/monthly.parquet", []string{cellShared, "1981-2010", "1"}, "wd10m_deg", "180.2"},
			{"nasa_power/monthly.parquet", []string{cellShared, "1981-2010", "1"}, "wd2m_deg", "300.3"},
			// Two decimals: the daily observations, whose hundredths the
			// export must keep — a trace 0.01 mm apart from a dry 0.00 —
			// the three daily extremes of the normals sheet, and the
			// POWER-31 block, through both scopes and the join.
			{"conagua/daily_observations.parquet", []string{"31001", "2020-01-01"}, "precip_mm", "0.01"},
			{"conagua/daily_observations.parquet", []string{"31001", "2020-01-02"}, "precip_mm", "0.00"},
			{"conagua/daily_observations.parquet", []string{"31001", "2020-01-01"}, "tmax_c", "0.00"},
			{"conagua/daily_observations.parquet", []string{"31001", "1999-12-31"}, "tmin_c", "-0.04"},
			{"conagua/monthly_normals_extras.parquet", []string{"31001", "1981-2010", "1"}, "tmax_daily_extreme_c", "42.00"},
			{"conagua/monthly_normals_extras.parquet", []string{"31001", "1981-2010", "1"}, "tmin_daily_extreme_c", "3.50"},
			{"conagua/monthly_normals_extras.parquet", []string{"31001", "1981-2010", "1"}, "precip_daily_extreme_mm", "98.70"},
			{"combined/combined_daily.parquet", []string{"31001", "1999-12-31"}, "tmin_c", "-0.04"},
			{"combined/combined_daily.parquet", []string{"31001", "2020-01-01"}, "precip_mm", "0.01"},
			{"nasa_power/daily.parquet", []string{cellShared, "2020-01-01"}, "t2m_min_c", "0.00"},
			{"nasa_power/daily.parquet", []string{cellShared, "2020-01-01"}, "solar_ghi_wm2", "228.47"},
			{"nasa_power/daily.parquet", []string{cellShared, "2020-01-01"}, "ts_min_c", "-3.35"},
			{"nasa_power/monthly.parquet", []string{cellShared, "1981-2010", "1"}, "t2m_c", "24.48"},
			{"combined/combined_daily.parquet", []string{"31001", "2020-01-01"}, "t2m_min_c", "0.00"},
			{"combined/combined_monthly.parquet", []string{"31001", "1981-2010", "1"}, "solar_ghi_wm2", "228.47"},
			// Three decimals: cell centroids and the great-circle distance.
			{"nasa_power/cells.parquet", []string{cellSingle}, "lat", "21.500"},
			{"nasa_power/cells.parquet", []string{cellSingle}, "lon", "0.000"},
			{"nasa_power/cells.parquet", []string{cellShared}, "lon", "-89.375"},
			{"nasa_power/station_cell_map.parquet", []string{"31001"}, "distance_km", "27.252"},
			{"nasa_power/station_cell_map.parquet", []string{"3101"}, "distance_km", "0.000"},
			{"combined/combined_daily.parquet", []string{"31001", "2020-01-01"}, "distance_km", "27.252"},
			{"combined/combined_monthly.parquet", []string{"3101", "1961-1990", "6"}, "distance_km", "0.000"},
			// Four decimals: the WMO completeness fractions.
			{"conagua/stations.parquet", []string{"31001"}, "wmo_completeness_bin_1961_1990", "0.9722"},
			{"conagua/stations.parquet", []string{"31001"}, "wmo_completeness_cont_1991_2020", "0.0009"},
			{"conagua/stations.parquet", []string{"31001"}, "wmo_completeness_cont_1961_1990", "0.9324"},
			{"conagua/stations.parquet", []string{"31001"}, "wmo_completeness_bin_1971_2000", "0.5000"},
			{"conagua/stations.parquet", []string{"3101"}, "wmo_completeness_bin_1971_2000", "0.0000"},
			// Six decimals: station coordinates.
			{"conagua/stations.parquet", []string{"31001"}, "lat", "21.850278"},
			{"conagua/stations.parquet", []string{"31001"}, "lon", "-89.375000"},
			{"conagua/stations.parquet", []string{"3101"}, "lat", "20.689100"},
			{"conagua/stations.parquet", []string{"31002"}, "lon", "0.000000"},
		}
		classes := map[int]bool{}
		for _, c := range cases {
			spec := parquetSpecs[c.file]
			i := columnIndex(spec, c.column)
			Expect(spec.Columns[i].Kind).To(Equal(publish.KindReal), c.column)
			classes[spec.Columns[i].Decimals] = true
			label := fmt.Sprintf("%s %v %s", c.file, c.key, c.column)

			row, cells := parquetRowByKey(entries, c.file, c.key)
			Expect(cells[i]).To(Equal(c.want), label)
			Expect(csvRowByKey(csvEntries, spec, c.key)[i]).To(Equal(c.want), label)

			want, err := strconv.ParseFloat(c.want, 64)
			Expect(err).NotTo(HaveOccurred())
			Expect(row[i].Kind()).To(Equal(parquet.Double), label)
			Expect(bits(row[i].Double())).To(Equal(bits(want)), label)
			if want == 0 {
				Expect(math.Signbit(row[i].Double())).To(BeFalse(), label)
			}
		}
		Expect(classes).To(Equal(map[int]bool{1: true, 2: true, 3: true, 4: true, 6: true}), "every decimal class exercised")
	})

	It("keeps a trace rain day its own value: 0.01 mm is a distinct double and a distinct cell from the dry day beside it", func() {
		// Why the column carries two decimals: CONAGUA publishes
		// "inappreciable" rain as 0.01 mm, and rounding the column to one
		// decimal would turn every such day into a dry 0.0 in the
		// exported files while the shipped SQLite keeps the true value.
		spec := publish.DailyObservations
		precip := columnIndex(spec, "precip_mm")
		Expect(spec.Columns[precip].Decimals).To(Equal(2))

		trace, traceCells := parquetRowByKey(entries, "conagua/daily_observations.parquet", []string{"31001", "2020-01-01"})
		dry, dryCells := parquetRowByKey(entries, "conagua/daily_observations.parquet", []string{"31001", "2020-01-02"})
		Expect(bits(trace[precip].Double())).To(Equal(bits(0.01)))
		Expect(bits(dry[precip].Double())).To(Equal(bits(0.0)))
		Expect(trace[precip].Double()).NotTo(Equal(dry[precip].Double()))
		Expect(traceCells[precip]).To(Equal("0.01"))
		Expect(dryCells[precip]).To(Equal("0.00"))
		// One decimal is what collapsed them; the pinned count does not.
		Expect(publish.FormatReal(trace[precip].Double(), 1)).To(Equal(publish.FormatReal(dry[precip].Double(), 1)))

		// The CSV twin and the joined combined file carry it too — the
		// trace day survives every export path, not just this one.
		Expect(csvRowByKey(csvEntries, spec, []string{"31001", "2020-01-01"})[precip]).To(Equal("0.01"))
		combined := publish.CombinedDaily.FileSpec
		i := columnIndex(combined, "precip_mm")
		row, cells := parquetRowByKey(entries, "combined/combined_daily.parquet", []string{"31001", "2020-01-01"})
		Expect(bits(row[i].Double())).To(Equal(bits(0.01)))
		Expect(cells[i]).To(Equal("0.01"))
		Expect(csvRowByKey(csvEntries, combined, []string{"31001", "2020-01-01"})[i]).To(Equal("0.01"))
	})

	It("stores an INTEGER at either int64 extreme exactly, the CSV twin's digits", func() {
		row, cells := parquetRowByKey(entries, "conagua/stations.parquet", []string{"31002"})
		first, last := columnIndex(publish.Stations, "first_year"), columnIndex(publish.Stations, "last_year")
		Expect(row[first].Kind()).To(Equal(parquet.Int64))
		Expect(row[first].Int64()).To(Equal(int64(math.MaxInt64)))
		Expect(row[last].Int64()).To(Equal(int64(math.MinInt64)))
		Expect(cells[first]).To(Equal("9223372036854775807"))
		Expect(cells[last]).To(Equal("-9223372036854775808"))
		csv := csvRowByKey(csvEntries, publish.Stations, []string{"31002"})
		Expect(csv[first]).To(Equal("9223372036854775807"))
		Expect(csv[last]).To(Equal("-9223372036854775808"))

		spec := publish.MonthlyNormalsExtras
		key := []string{"3101", "1961-1990", "6"}
		row, cells = parquetRowByKey(entries, "conagua/monthly_normals_extras.parquet", key)
		year, years := columnIndex(spec, "tmax_monthly_extreme_year"), columnIndex(spec, "precip_years_with_data")
		Expect(row[year].Int64()).To(Equal(int64(math.MaxInt64)))
		Expect(row[years].Int64()).To(Equal(int64(math.MinInt64)))
		Expect(cells[year]).To(Equal("9223372036854775807"))
		Expect(cells[years]).To(Equal("-9223372036854775808"))
		csv = csvRowByKey(csvEntries, spec, key)
		Expect(csv[year]).To(Equal("9223372036854775807"))
		Expect(csv[years]).To(Equal("-9223372036854775808"))
		// The integer key survives beside them.
		Expect(row[columnIndex(spec, "month")].Int64()).To(Equal(int64(6)))
	})

	It("stores TEXT verbatim — quotes, a comma, a newline, a tab, non-ASCII — and the CSV twin quotes it back to the same string", func() {
		name := columnIndex(publish.Stations, "name")
		row, cells := parquetRowByKey(entries, "conagua/stations.parquet", []string{"31002"})
		Expect(row[name].Kind()).To(Equal(parquet.ByteArray))
		Expect(string(row[name].ByteArray())).To(Equal(hostileName))
		Expect(cells[name]).To(Equal(hostileName))
		Expect(csvRowByKey(csvEntries, publish.Stations, []string{"31002"})[name]).To(Equal(hostileName))
		// RFC 4180 on the wire: the field quoted, the inner quotes doubled,
		// the newline and tab inside the quotes as they are.
		Expect(string(render(entryByPath(csvEntries, "conagua/stations.csv")))).
			To(ContainSubstring("31002,\"Tizimín, \"\"El Cuyo\"\"\nsegunda línea\tñ — 🌧\",YUC,"))

		// The fixture's own quoted, accented name, and the date and period
		// strings of an extras row, the same way.
		row, _ = parquetRowByKey(entries, "conagua/stations.parquet", []string{"31001"})
		Expect(string(row[name].ByteArray())).To(Equal(`Mérida, "La Plancha"`))
		spec := publish.MonthlyNormalsExtras
		row, _ = parquetRowByKey(entries, "conagua/monthly_normals_extras.parquet", []string{"31001", "1981-2010", "1"})
		Expect(string(row[columnIndex(spec, "tmax_daily_extreme_date")].ByteArray())).To(Equal("1998-05-14"))
		Expect(string(row[columnIndex(spec, "period")].ByteArray())).To(Equal("1981-2010"))
	})

	It("keeps an empty string apart from NULL: a stored '' is a zero-length string, a NULL is null, and the CSV renders both empty", func() {
		// "An empty CSV field is unambiguously NULL" holds because no
		// nullable TEXT column ever stores ''; the Parquet twin mirrors
		// what is stored rather than assuming it, so the two stay
		// distinguishable there.
		municipality := columnIndex(publish.Stations, "municipality")
		row, cells := parquetRowByKey(entries, "conagua/stations.parquet", []string{"31003"})
		Expect(row[municipality].IsNull()).To(BeFalse())
		Expect(row[municipality].Kind()).To(Equal(parquet.ByteArray))
		Expect(row[municipality].ByteArray()).To(BeEmpty())
		Expect(cells[municipality]).To(Equal(""))
		Expect(csvRowByKey(csvEntries, publish.Stations, []string{"31003"})[municipality]).To(Equal(""))

		row, cells = parquetRowByKey(entries, "conagua/stations.parquet", []string{"31002"})
		Expect(row[municipality].IsNull()).To(BeTrue())
		Expect(cells[municipality]).To(Equal(""))
		Expect(csvRowByKey(csvEntries, publish.Stations, []string{"31002"})[municipality]).To(Equal(""))
	})
})
