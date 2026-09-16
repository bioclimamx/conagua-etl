package power_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/power"
)

// cannedPower is the fixture value each POWER parameter returns —
// every parameter in the registry, one distinct value per parameter so
// a cross-parameter mix-up flips a value. Radiation values are in
// MJ/m²/day (POWER's native unit) so the specs exercise the writer's
// unit conversion.
var cannedPower = map[string]float64{
	// Temperature
	"T2M": 22.0, "T2M_MAX": 28.0, "T2M_MIN": 15.0,
	"T2MWET": 19.0, "T2MDEW": 18.0,
	"TS": 24.0, "TS_MAX": 31.0, "TS_MIN": 16.0,
	// Humidity
	"RH2M": 70.0, "QV2M": 12.5,
	// Wind
	"WS2M": 2.0, "WS10M": 3.0, "WS50M": 4.5,
	"WD2M": 90.0, "WD10M": 270.0,
	// Solar — shortwave (MJ/m²/day; expect ×1e6/86400 → W/m²)
	"ALLSKY_SFC_SW_DWN": 21.0, "ALLSKY_SFC_SW_DIFF": 8.0,
	"ALLSKY_SFC_SW_DNI": 28.0, "CLRSKY_SFC_SW_DWN": 25.0,
	"ALLSKY_KT":          0.65,
	"ALLSKY_SFC_PAR_TOT": 13.0, "ALLSKY_SFC_UVA": 2.2, "ALLSKY_SFC_UVB": 0.10,
	// Solar — longwave
	"ALLSKY_SFC_LW_DWN": 35.0,
	// Sky / cloud / atmosphere
	"CLOUD_AMT": 42.0, "PS": 101.3,
	// Moisture and ET
	"PRECTOTCORR": 2.5, "EVLAND": 3.2,
	// Soil
	"GWETTOP": 0.4, "GWETROOT": 0.5, "GWETPROF": 0.6,
}

// requestLog records every fixture request keyed by the latitude query
// value, so specs can prove which cells were fetched (and how many
// attempts each took) without depending on cell iteration order.
type requestLog struct {
	mu   sync.Mutex
	hits []requestHit
}

type requestHit struct {
	Lat     string
	NParams int
}

func (l *requestLog) record(lat string, nParams int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.hits = append(l.hits, requestHit{Lat: lat, NParams: nParams})
}

func (l *requestLog) hitsFor(lat string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, h := range l.hits {
		if h.Lat == lat {
			n++
		}
	}
	return n
}

func (l *requestLog) latitudes() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, h := range l.hits {
		if !seen[h.Lat] {
			seen[h.Lat] = true
			out = append(out, h.Lat)
		}
	}
	return out
}

// latQuery renders a centroid latitude exactly as BuildURL puts it on
// the wire, so fixtures can key failure modes on a specific cell.
func latQuery(c power.Cell) string {
	return strconv.FormatFloat(c.Lat, 'f', -1, 64)
}

type monthlyServerOpts struct {
	failLat    string // every request at this latitude answers 500
	divergeLat string // sub-cap requests at this latitude get a different fill_value
	fillMonth  int    // calendar month served as all-fill for every parameter and year
}

// newMonthlyPowerServer serves canned POWER monthly JSON for any
// (lat, lon): each requested parameter carries its cannedPower value
// at every month of 1991–2020, plus the legacy "ANN" key the rollup
// must discard. The handler enforces POWER's monthly parameter cap so
// a FetchBatched regression (e.g. passing the wrong cap) surfaces as
// a 4xx, and can inject per-cell failure modes keyed on the latitude
// query value.
func newMonthlyPowerServer(o monthlyServerOpts) (*httptest.Server, *requestLog) {
	log := &requestLog{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		lat := q.Get("latitude")
		requested := strings.Split(q.Get("parameters"), ",")
		log.record(lat, len(requested))

		if len(requested) > power.MaxParametersPerRequestMonthly {
			http.Error(w, "too many params", http.StatusBadRequest)
			return
		}
		if o.failLat != "" && lat == o.failLat {
			http.Error(w, "synthetic upstream failure", http.StatusInternalServerError)
			return
		}
		fill := -999.0
		// The batched 31-parameter request splits 25 + 6: the second
		// sub-request is the only one under the cap, so keying on the
		// parameter count makes the fill divergence deterministic.
		if o.divergeLat != "" && lat == o.divergeLat && len(requested) < power.MaxParametersPerRequestMonthly {
			fill = -888.0
		}

		params := map[string]map[string]float64{}
		for _, p := range requested {
			v, ok := cannedPower[p]
			if !ok {
				continue
			}
			series := map[string]float64{"ANN": v}
			for y := 1991; y <= 2020; y++ {
				for m := 1; m <= 12; m++ {
					val := v
					if m == o.fillMonth {
						val = fill
					}
					series[fmt.Sprintf("%04d%02d", y, m)] = val
				}
			}
			params[p] = series
		}
		servePowerJSON(w, params, fill)
	}))
	DeferCleanup(srv.Close)
	return srv, log
}

type dailyServerOpts struct {
	fillDate string // ISO date served as all-fill for every parameter
}

// newDailyPowerServer is the monthly fixture's daily-endpoint sibling:
// every requested parameter gets its cannedPower value on every day of
// the requested window. POWER's daily keys are 8-digit YYYYMMDD
// strings — distinct from monthly's 6-digit YYYYMM — and the daily
// parameter cap is lower (20 vs 25), so the handler enforces it to
// pin the caller's cap choice.
func newDailyPowerServer(o dailyServerOpts) (*httptest.Server, *requestLog) {
	log := &requestLog{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		lat := q.Get("latitude")
		requested := strings.Split(q.Get("parameters"), ",")
		log.record(lat, len(requested))

		if len(requested) > power.MaxParametersPerRequestDaily {
			http.Error(w, "too many params", http.StatusBadRequest)
			return
		}
		startStr, endStr := q.Get("start"), q.Get("end")
		if len(startStr) != 8 || len(endStr) != 8 {
			http.Error(w, "want 8-digit start/end", http.StatusBadRequest)
			return
		}
		start, err1 := time.Parse("20060102", startStr)
		end, err2 := time.Parse("20060102", endStr)
		if err1 != nil || err2 != nil {
			http.Error(w, "bad start/end", http.StatusBadRequest)
			return
		}

		const fill = -999.0
		params := map[string]map[string]float64{}
		for _, p := range requested {
			v, ok := cannedPower[p]
			if !ok {
				continue
			}
			series := map[string]float64{}
			for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
				val := v
				if d.Format("2006-01-02") == o.fillDate {
					val = fill
				}
				series[d.Format("20060102")] = val
			}
			params[p] = series
		}
		servePowerJSON(w, params, fill)
	}))
	DeferCleanup(srv.Close)
	return srv, log
}

func f64(v float64) *float64 { return &v }

// seedStation writes one station through the ingest writer — the real
// producer of the rows the power orchestrator reads — so the
// stations → power seam is proven against the actual write path, not
// hand-rolled SQL.
func seedStation(db *sql.DB, source ingest.Source, ext, name string, lat, lon *float64) int64 {
	GinkgoHelper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	Expect(err).NotTo(HaveOccurred())
	id, err := ingest.UpsertStation(ctx, tx, ingest.StationUpsert{
		Source: source, ExternalID: ext, Name: name, Lat: lat, Lon: lon,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(tx.Commit()).To(Succeed())
	return id
}

// climatologicalMean folds `years` copies of v exactly the way the
// monthly rollup does — repeated float64 addition, then one division —
// so expectations stay float-exact against the ported arithmetic (a
// constant series makes the result independent of summation order).
func climatologicalMean(v float64, years int) float64 {
	sum := 0.0
	for i := 0; i < years; i++ {
		sum += v
	}
	return sum / float64(years)
}

// circularMean folds `years` copies of an angle v (degrees) with the
// vector mean the rollup applies to wind directions, mirroring the
// ported operations exactly.
func circularMean(v float64, years int) float64 {
	r := v * math.Pi / 180.0
	sumSin, sumCos := 0.0, 0.0
	for i := 0; i < years; i++ {
		sumSin += math.Sin(r)
		sumCos += math.Cos(r)
	}
	deg := math.Atan2(sumSin/float64(years), sumCos/float64(years)) * 180.0 / math.Pi
	if deg < 0 {
		deg += 360.0
	}
	return deg
}

// expectedMonthlyStored is the per-column stored value for a cell
// served the canned fixture over the full 30-year window: the
// climatological mean (circular for wind directions), times the
// registry factor where one applies — conversion happens once, at the
// writer.
func expectedMonthlyStored() []float64 {
	out := make([]float64, len(power.Registry))
	for i, p := range power.Registry {
		v := cannedPower[p.Name]
		var mean float64
		if p.Circular {
			mean = circularMean(v, 30)
		} else {
			mean = climatologicalMean(v, 30)
		}
		if p.Factor != 1.0 {
			mean *= p.Factor
		}
		out[i] = mean
	}
	return out
}

// expectedDailyStored is the per-column stored value in daily mode:
// POWER's value as-returned (daily values are already 24-h means),
// modulo the same writer-side unit conversion.
func expectedDailyStored() []float64 {
	out := make([]float64, len(power.Registry))
	for i, p := range power.Registry {
		v := cannedPower[p.Name]
		if p.Factor != 1.0 {
			v *= p.Factor
		}
		out[i] = v
	}
	return out
}

// supplementValues is the full value read-back of one supplement row:
// every value column in registry order, plus the run back-pointer.
type supplementValues struct {
	Values []sql.NullFloat64
	RunID  sql.NullInt64
}

func scanSupplement(db *sql.DB, query string, args ...any) supplementValues {
	GinkgoHelper()
	out := supplementValues{Values: make([]sql.NullFloat64, len(supplementValueColumns))}
	ptrs := make([]any, 0, len(supplementValueColumns)+1)
	for i := range out.Values {
		ptrs = append(ptrs, &out.Values[i])
	}
	ptrs = append(ptrs, &out.RunID)
	Expect(db.QueryRow(query, args...).Scan(ptrs...)).To(Succeed())
	return out
}

func selectMonthlySupplement(db *sql.DB, cellID, period string, month int) supplementValues {
	GinkgoHelper()
	q := "SELECT " + strings.Join(supplementValueColumns, ", ") + ", power_run_id" +
		" FROM monthly_supplement WHERE cell_id = ? AND period = ? AND month = ?"
	return scanSupplement(db, q, cellID, period, month)
}

func selectDailySupplement(db *sql.DB, cellID, date string) supplementValues {
	GinkgoHelper()
	q := "SELECT " + strings.Join(supplementValueColumns, ", ") + ", power_run_id" +
		" FROM daily_supplement WHERE cell_id = ? AND date = ?"
	return scanSupplement(db, q, cellID, date)
}

// expectStoredValues asserts each value column individually so a
// column-order regression names the exact column that drifted.
func expectStoredValues(got supplementValues, want []float64, runID int64) {
	GinkgoHelper()
	for i, col := range supplementValueColumns {
		Expect(got.Values[i].Valid).To(BeTrue(), "column %s must be non-NULL", col)
		Expect(got.Values[i].Float64).To(Equal(want[i]), "column %s", col)
	}
	Expect(got.RunID).To(Equal(validInt64(runID)))
}

type cellRow struct {
	ID  string
	Lat float64
	Lon float64
	Res string
}

func selectCells(db *sql.DB) []cellRow {
	GinkgoHelper()
	rows, err := db.Query(`SELECT cell_id, lat, lon, grid_resolution FROM nasa_power_grid_cells ORDER BY cell_id`)
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close() //nolint:errcheck // read-side close in a spec
	var out []cellRow
	for rows.Next() {
		var c cellRow
		Expect(rows.Scan(&c.ID, &c.Lat, &c.Lon, &c.Res)).To(Succeed())
		out = append(out, c)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

type assocRow struct {
	CellID     string
	DistanceKm float64
}

func selectAssociation(db *sql.DB, stationID int64) assocRow {
	GinkgoHelper()
	var a assocRow
	Expect(db.QueryRow(
		`SELECT cell_id, distance_km FROM station_power_cell WHERE station_id = ?`, stationID,
	).Scan(&a.CellID, &a.DistanceKm)).To(Succeed())
	return a
}

func selectInts(db *sql.DB, query string, args ...any) []int {
	GinkgoHelper()
	rows, err := db.Query(query, args...)
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close() //nolint:errcheck // read-side close in a spec
	var out []int
	for rows.Next() {
		var v int
		Expect(rows.Scan(&v)).To(Succeed())
		out = append(out, v)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

func selectStrings(db *sql.DB, query string, args ...any) []string {
	GinkgoHelper()
	rows, err := db.Query(query, args...)
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close() //nolint:errcheck // read-side close in a spec
	var out []string
	for rows.Next() {
		var v string
		Expect(rows.Scan(&v)).To(Succeed())
		out = append(out, v)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

func columnIndex(col string) int {
	for i, c := range supplementValueColumns {
		if c == col {
			return i
		}
	}
	Fail("unknown supplement column " + col)
	return -1
}

func cellResult(report *power.Report, cellID string) power.CellResult {
	GinkgoHelper()
	for _, r := range report.PerCell {
		if r.Cell.ID == cellID {
			return r
		}
	}
	Fail("no PerCell entry for " + cellID)
	return power.CellResult{}
}

var _ = Describe("Run", func() {
	ctx := context.Background()
	period := power.Period{StartYear: 1991, EndYear: 2020}

	Describe("monthly mode", func() {
		It("dedupes stations into cells, converts at the writer, and records the run manifest", func() {
			db := openTestDB()

			// Two stations sharing one MERRA-2 cell (Mérida region), one
			// station in a separate cell (Tacubaya). Two more stations
			// must stay out of the pull: no coordinates, and a
			// non-CONAGUA source.
			merida := seedStation(db, ingest.SourceConaguaConventional, "MR1", "Mérida", f64(20.98), f64(-89.65))
			sibling := seedStation(db, ingest.SourceConaguaConventional, "MR2", "Mérida sibling", f64(21.05), f64(-89.40))
			tacubaya := seedStation(db, ingest.SourceConaguaConventional, "TA1", "Tacubaya", f64(19.40), f64(-99.20))
			seedStation(db, ingest.SourceConaguaConventional, "NC1", "sin coordenadas", nil, nil)
			seedStation(db, ingest.SourceMeta, "META1", "meta station", f64(20.0), f64(-100.0))

			meridaCell := power.CellFor(20.98, -89.65)
			tacubayaCell := power.CellFor(19.40, -99.20)
			Expect(power.CellFor(21.05, -89.40).ID).To(Equal(meridaCell.ID),
				"the sibling must share Mérida's cell")

			srv, _ := newMonthlyPowerServer(monthlyServerOpts{})
			client := power.NewClient(srv.URL, nil)

			var events []power.ProgressEvent
			report, err := power.Run(ctx, db, power.Options{
				Client: client, Endpoint: srv.URL, Period: period, ETLGitSHA: "sha-monthly",
			}, func(ev power.ProgressEvent) { events = append(events, ev) })
			Expect(err).NotTo(HaveOccurred())

			Expect(report.Status).To(Equal("complete"))
			Expect(report.RunID).To(BeNumerically(">", 0))
			Expect(report.StationsCovered).To(Equal(3), "no-coordinate and non-CONAGUA stations stay out")
			Expect(report.UniqueCells).To(Equal(2))
			Expect(report.CellsAttempted).To(Equal(2))
			Expect(report.CellsSucceeded).To(Equal(2))
			Expect(report.CellsFailed).To(BeZero())
			Expect(report.SupplementRows).To(Equal(24), "2 cells × 12 months")

			// One synchronous progress event per cell.
			Expect(events).To(HaveLen(2))
			for i, ev := range events {
				Expect(ev.Index).To(Equal(i+1), "event %d", i)
				Expect(ev.Total).To(Equal(2))
				Expect(ev.OK).To(BeTrue())
				Expect(ev.Err).NotTo(HaveOccurred())
			}

			// Per-cell results carry the station fan-out.
			mer := cellResult(report, meridaCell.ID)
			Expect(mer.OK).To(BeTrue())
			Expect(mer.StationCount).To(Equal(2))
			Expect(mer.RowsWritten).To(Equal(12))
			Expect(mer.Err).To(BeEmpty())
			tac := cellResult(report, tacubayaCell.ID)
			Expect(tac.OK).To(BeTrue())
			Expect(tac.StationCount).To(Equal(1))
			Expect(tac.RowsWritten).To(Equal(12))

			// nasa_power_grid_cells round-trips both centroids.
			Expect(selectCells(db)).To(ConsistOf(
				cellRow{ID: meridaCell.ID, Lat: meridaCell.Lat, Lon: meridaCell.Lon, Res: power.Resolution},
				cellRow{ID: tacubayaCell.ID, Lat: tacubayaCell.Lat, Lon: tacubayaCell.Lon, Res: power.Resolution},
			))

			// station_power_cell: each station points at its cell with the
			// float-exact Haversine distance to the centroid.
			for _, tc := range []struct {
				id       int64
				lat, lon float64
				cell     power.Cell
			}{
				{merida, 20.98, -89.65, meridaCell},
				{sibling, 21.05, -89.40, meridaCell},
				{tacubaya, 19.40, -99.20, tacubayaCell},
			} {
				assoc := selectAssociation(db, tc.id)
				Expect(assoc.CellID).To(Equal(tc.cell.ID))
				Expect(assoc.DistanceKm).To(Equal(
					power.HaversineKm(tc.lat, tc.lon, tc.cell.Lat, tc.cell.Lon)))
			}

			// Every month of both cells round-trips all 31 value columns,
			// converted at the writer.
			want := expectedMonthlyStored()
			for m := 1; m <= 12; m++ {
				expectStoredValues(selectMonthlySupplement(db, meridaCell.ID, period.Label(), m), want, report.RunID)
			}
			expectStoredValues(selectMonthlySupplement(db, tacubayaCell.ID, period.Label(), 7), want, report.RunID)
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_supplement`)).To(Equal(24))

			// The solar conversion is float-exact against ConvertSolar —
			// the same runtime multiplication by the float64-rounded
			// 1e6/86400 factor the writer applies (a compile-time
			// constant-folded product differs in the last ulp).
			july := selectMonthlySupplement(db, meridaCell.ID, period.Label(), 7)
			Expect(july.Values[columnIndex("solar_ghi_wm2")].Float64).
				To(Equal(power.ConvertSolar(21.0)))

			// Observed tables stay untouched — provenance by table identity.
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_normals`)).To(BeZero())

			// power_runs is the reproducibility manifest.
			run := selectPowerRun(db, report.RunID)
			Expect(run.Status).To(Equal("complete"))
			Expect(run.Endpoint).To(Equal(srv.URL))
			Expect(run.Parameters).To(Equal(strings.Join(power.DefaultParameters(), ",")),
				"parameters must be comma-joined in registry order")
			Expect(run.Community).To(Equal(power.DefaultCommunity))
			Expect(run.StartYear).To(Equal(1991))
			Expect(run.EndYear).To(Equal(2020))
			Expect(run.GridResolution).To(Equal(power.Resolution))
			Expect(run.SolarConversion).To(Equal(power.SolarMJpm2dToWm2))
			Expect(run.TemporalMode).To(Equal("monthly"))
			Expect(run.StartDate.Valid).To(BeFalse(), "monthly runs leave the date span NULL")
			Expect(run.EndDate.Valid).To(BeFalse())
			Expect(run.Attempted).To(Equal(validInt64(2)))
			Expect(run.Succeeded).To(Equal(validInt64(2)))
			Expect(run.Failed).To(Equal(validInt64(0)))
			Expect(run.SupplementRows).To(Equal(validInt64(24)))
			Expect(run.ETLGitSHA).To(Equal(validStr("sha-monthly")))
			_, err = time.Parse(time.RFC3339, run.StartedAt)
			Expect(err).NotTo(HaveOccurred(), "started_at must be RFC3339")
			Expect(run.FinishedAt.Valid).To(BeTrue())

			var convs map[string]power.UnitConversion
			Expect(json.Unmarshal([]byte(run.UnitConversions.String), &convs)).To(Succeed())
			Expect(convs).To(Equal(power.Conversions),
				"unit_conversions JSON must round-trip the registry projection")
		})

		It("skips a month where every parameter is fill", func() {
			db := openTestDB()
			seedStation(db, ingest.SourceConaguaConventional, "MR1", "Mérida", f64(20.98), f64(-89.65))
			cell := power.CellFor(20.98, -89.65)

			srv, _ := newMonthlyPowerServer(monthlyServerOpts{fillMonth: 6})
			client := power.NewClient(srv.URL, nil)

			report, err := power.Run(ctx, db, power.Options{
				Client: client, Endpoint: srv.URL, Period: period,
				Parameters: []string{"T2M", "RH2M"},
			}, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.SupplementRows).To(Equal(11), "the all-fill month writes no row")

			Expect(selectInts(db,
				`SELECT month FROM monthly_supplement WHERE cell_id = ? ORDER BY month`, cell.ID,
			)).To(Equal([]int{1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12}))

			// A present month round-trips the requested parameters; every
			// unrequested column stays NULL — honest gaps, never invented.
			got := selectMonthlySupplement(db, cell.ID, period.Label(), 1)
			for i, col := range supplementValueColumns {
				switch col {
				case "t2m_c":
					Expect(got.Values[i]).To(Equal(validFloat64(climatologicalMean(cannedPower["T2M"], 30))), "column %s", col)
				case "rh2m_pct":
					Expect(got.Values[i]).To(Equal(validFloat64(climatologicalMean(cannedPower["RH2M"], 30))), "column %s", col)
				default:
					Expect(got.Values[i].Valid).To(BeFalse(), "unrequested column %s must be NULL", col)
				}
			}

			// The manifest projects the conversions to the requested subset.
			run := selectPowerRun(db, report.RunID)
			Expect(run.Parameters).To(Equal("T2M,RH2M"))
			var convs map[string]power.UnitConversion
			Expect(json.Unmarshal([]byte(run.UnitConversions.String), &convs)).To(Succeed())
			Expect(convs).To(Equal(map[string]power.UnitConversion{
				"T2M":  power.Conversions["T2M"],
				"RH2M": power.Conversions["RH2M"],
			}))
		})

		It("fails soft on a cell that 500s beyond retries", func() {
			db := openTestDB()
			seedStation(db, ingest.SourceConaguaConventional, "A", "a", f64(19.40), f64(-99.20))
			seedStation(db, ingest.SourceConaguaConventional, "B", "b", f64(21.00), f64(-89.40))
			failing := power.CellFor(19.40, -99.20)
			healthy := power.CellFor(21.00, -89.40)

			srv, log := newMonthlyPowerServer(monthlyServerOpts{failLat: latQuery(failing)})
			client := power.NewClient(srv.URL, nil)
			client.MaxAttempts = 2
			client.BaseBackoff = time.Millisecond

			report, err := power.Run(ctx, db, power.Options{
				Client: client, Endpoint: srv.URL, Period: period,
			}, nil)
			Expect(err).NotTo(HaveOccurred(), "a per-cell failure must not abort the run")

			Expect(report.Status).To(Equal("complete"))
			Expect(report.CellsAttempted).To(Equal(2))
			Expect(report.CellsSucceeded).To(Equal(1))
			Expect(report.CellsFailed).To(Equal(1))
			Expect(report.SupplementRows).To(Equal(12))
			Expect(log.hitsFor(latQuery(failing))).To(Equal(2),
				"the transient classifier must retry the 500 once before giving up")

			failed := cellResult(report, failing.ID)
			Expect(failed.OK).To(BeFalse())
			Expect(failed.Err).To(ContainSubstring("500"))
			Expect(failed.RowsWritten).To(BeZero())
			ok := cellResult(report, healthy.ID)
			Expect(ok.OK).To(BeTrue())
			Expect(ok.RowsWritten).To(Equal(12))

			// The healthy cell's rows landed in full; the failed cell
			// wrote nothing.
			expectStoredValues(selectMonthlySupplement(db, healthy.ID, period.Label(), 7),
				expectedMonthlyStored(), report.RunID)
			Expect(countRows(db,
				`SELECT COUNT(*) FROM monthly_supplement WHERE cell_id = ?`, failing.ID)).To(BeZero())

			run := selectPowerRun(db, report.RunID)
			Expect(run.Status).To(Equal("complete"))
			Expect(run.Succeeded).To(Equal(validInt64(1)))
			Expect(run.Failed).To(Equal(validInt64(1)))
			Expect(run.SupplementRows).To(Equal(validInt64(12)))
		})

		It("fails the cell on a divergent fill_value across sub-requests, writing no partial rows", func() {
			db := openTestDB()
			seedStation(db, ingest.SourceConaguaConventional, "A", "a", f64(19.40), f64(-99.20))
			seedStation(db, ingest.SourceConaguaConventional, "B", "b", f64(21.00), f64(-89.40))
			poisoned := power.CellFor(19.40, -99.20)
			healthy := power.CellFor(21.00, -89.40)

			srv, _ := newMonthlyPowerServer(monthlyServerOpts{divergeLat: latQuery(poisoned)})
			client := power.NewClient(srv.URL, nil)

			// The default 31-parameter set forces FetchBatched to split
			// (25 + 6) — the seam where the divergence is detectable.
			report, err := power.Run(ctx, db, power.Options{
				Client: client, Endpoint: srv.URL, Period: period,
			}, nil)
			Expect(err).NotTo(HaveOccurred(), "an integrity abort is per-cell, not run-fatal")

			Expect(report.Status).To(Equal("complete"))
			Expect(report.CellsSucceeded).To(Equal(1))
			Expect(report.CellsFailed).To(Equal(1))

			failed := cellResult(report, poisoned.ID)
			Expect(failed.OK).To(BeFalse())
			Expect(failed.Err).To(ContainSubstring("fill_value"))
			Expect(countRows(db,
				`SELECT COUNT(*) FROM monthly_supplement WHERE cell_id = ?`, poisoned.ID)).
				To(BeZero(), "an integrity-failed cell must write no partial rows")

			expectStoredValues(selectMonthlySupplement(db, healthy.ID, period.Label(), 7),
				expectedMonthlyStored(), report.RunID)
		})

		It("converges on re-run: identical values, re-pointed to the newest run, no duplicates", func() {
			db := openTestDB()
			seedStation(db, ingest.SourceConaguaConventional, "MR1", "Mérida", f64(20.98), f64(-89.65))
			cell := power.CellFor(20.98, -89.65)

			srv, _ := newMonthlyPowerServer(monthlyServerOpts{})
			client := power.NewClient(srv.URL, nil)
			opts := power.Options{Client: client, Endpoint: srv.URL, Period: period, ETLGitSHA: "sha"}

			want := expectedMonthlyStored()
			first, err := power.Run(ctx, db, opts, nil)
			Expect(err).NotTo(HaveOccurred())
			expectStoredValues(selectMonthlySupplement(db, cell.ID, period.Label(), 7), want, first.RunID)

			second, err := power.Run(ctx, db, opts, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(second.RunID).To(BeNumerically(">", first.RunID))

			// Same values, every month, now attributed to the second run.
			for m := 1; m <= 12; m++ {
				expectStoredValues(selectMonthlySupplement(db, cell.ID, period.Label(), m), want, second.RunID)
			}
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_supplement`)).To(Equal(12),
				"re-runs replace, never accumulate")
			Expect(countRows(db, `SELECT COUNT(*) FROM nasa_power_grid_cells`)).To(Equal(1))
			Expect(countRows(db, `SELECT COUNT(*) FROM station_power_cell`)).To(Equal(1))
			Expect(countRows(db, `SELECT COUNT(*) FROM power_runs`)).To(Equal(2))
		})

		It("stops the loop on cancellation without a failure storm and closes the run aborted", func() {
			db := openTestDB()
			seedStation(db, ingest.SourceConaguaConventional, "A", "a", f64(19.40), f64(-99.20))
			seedStation(db, ingest.SourceConaguaConventional, "B", "b", f64(21.00), f64(-89.40))

			srv, log := newMonthlyPowerServer(monthlyServerOpts{})
			client := power.NewClient(srv.URL, nil)

			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			var events []power.ProgressEvent
			report, err := power.Run(runCtx, db, power.Options{
				Client: client, Endpoint: srv.URL, Period: period,
			}, func(ev power.ProgressEvent) {
				events = append(events, ev)
				// Operator interrupt right after the first cell lands.
				cancel()
			})
			Expect(err).To(MatchError(context.Canceled))
			Expect(report).NotTo(BeNil(), "the report must survive an abort")

			// The loop stops between cells: exactly one cell attempted,
			// none of the remaining cells counted as failures.
			Expect(events).To(HaveLen(1), "no further cell may start after cancellation")
			first := events[0]
			Expect(first.OK).To(BeTrue())
			Expect(report.Status).To(Equal("aborted"))
			Expect(report.CellsAttempted).To(Equal(1))
			Expect(report.CellsSucceeded).To(Equal(1))
			Expect(report.CellsFailed).To(BeZero(), "a cancellation casualty is not a cell error")
			Expect(report.SupplementRows).To(Equal(12))
			Expect(log.latitudes()).To(HaveLen(1), "the fixture must see exactly one cell's traffic")

			// The completed cell's rows are durable; nothing else wrote.
			expectStoredValues(selectMonthlySupplement(db, first.CellID, period.Label(), 7),
				expectedMonthlyStored(), report.RunID)
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_supplement`)).To(Equal(12))

			// closeRun runs on a background context, so the run row can
			// never strand as 'running' — it records the counters at abort.
			run := selectPowerRun(db, report.RunID)
			Expect(run.Status).To(Equal("aborted"))
			Expect(run.FinishedAt.Valid).To(BeTrue())
			Expect(run.Attempted).To(Equal(validInt64(1)))
			Expect(run.Succeeded).To(Equal(validInt64(1)))
			Expect(run.Failed).To(Equal(validInt64(0)))
			Expect(run.SupplementRows).To(Equal(validInt64(12)))
		})
	})

	Describe("daily mode", func() {
		It("passes daily values through with conversion and records the date-span manifest", func() {
			db := openTestDB()
			merida := seedStation(db, ingest.SourceConaguaConventional, "MR1", "Mérida", f64(20.98), f64(-89.65))
			seedStation(db, ingest.SourceConaguaConventional, "TA1", "Tacubaya", f64(19.40), f64(-99.20))
			meridaCell := power.CellFor(20.98, -89.65)
			tacubayaCell := power.CellFor(19.40, -99.20)

			srv, _ := newDailyPowerServer(dailyServerOpts{})
			client := power.NewClient(srv.URL, nil)

			const startDate, endDate = "2020-01-01", "2020-01-31"
			report, err := power.Run(ctx, db, power.Options{
				Client: client, Endpoint: srv.URL,
				Temporal: power.TemporalDaily, StartDate: startDate, EndDate: endDate,
				ETLGitSHA: "sha-daily",
			}, nil)
			Expect(err).NotTo(HaveOccurred())

			Expect(report.Status).To(Equal("complete"))
			Expect(report.UniqueCells).To(Equal(2))
			Expect(report.CellsSucceeded).To(Equal(2))
			Expect(report.CellsFailed).To(BeZero())
			Expect(report.SupplementRows).To(Equal(62), "2 cells × 31 days")
			Expect(report.Manifest.Temporal).To(Equal(power.TemporalDaily))
			Expect(report.Manifest.StartDate).To(Equal(startDate))
			Expect(report.Manifest.EndDate).To(Equal(endDate))

			Expect(selectAssociation(db, merida).CellID).To(Equal(meridaCell.ID))
			Expect(countRows(db,
				`SELECT COUNT(*) FROM daily_supplement WHERE cell_id = ?`, meridaCell.ID)).To(Equal(31))

			// Every column round-trips keyed by (cell, date), values
			// as-returned modulo the writer's unit conversion.
			want := expectedDailyStored()
			expectStoredValues(selectDailySupplement(db, meridaCell.ID, "2020-01-15"), want, report.RunID)
			expectStoredValues(selectDailySupplement(db, tacubayaCell.ID, "2020-01-31"), want, report.RunID)

			jan15 := selectDailySupplement(db, meridaCell.ID, "2020-01-15")
			// Daily wind direction is POWER's own vector mean — stored
			// untouched, no re-averaging.
			Expect(jan15.Values[columnIndex("wd10m_deg")].Float64).To(Equal(cannedPower["WD10M"]))
			// The solar conversion is float-exact against ConvertSolar
			// (the writer's runtime multiplication, not a constant fold).
			Expect(jan15.Values[columnIndex("solar_ghi_wm2")].Float64).
				To(Equal(power.ConvertSolar(21.0)))

			// Daily mode must not touch the monthly table.
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_supplement`)).To(BeZero())

			// power_runs: the date span is authoritative; the year columns
			// derive from the dates so year-only readers still see the span.
			run := selectPowerRun(db, report.RunID)
			Expect(run.Status).To(Equal("complete"))
			Expect(run.TemporalMode).To(Equal("daily"))
			Expect(run.StartDate).To(Equal(validStr(startDate)))
			Expect(run.EndDate).To(Equal(validStr(endDate)))
			Expect(run.StartYear).To(Equal(2020))
			Expect(run.EndYear).To(Equal(2020))
			Expect(run.Endpoint).To(Equal(srv.URL))
			Expect(run.Parameters).To(Equal(strings.Join(power.DefaultParameters(), ",")))
			Expect(run.Community).To(Equal(power.DefaultCommunity))
			Expect(run.Attempted).To(Equal(validInt64(2)))
			Expect(run.Succeeded).To(Equal(validInt64(2)))
			Expect(run.Failed).To(Equal(validInt64(0)))
			Expect(run.SupplementRows).To(Equal(validInt64(62)))
			Expect(run.ETLGitSHA).To(Equal(validStr("sha-daily")))
		})

		It("skips a date where every parameter is fill", func() {
			db := openTestDB()
			seedStation(db, ingest.SourceConaguaConventional, "MR1", "Mérida", f64(20.98), f64(-89.65))
			cell := power.CellFor(20.98, -89.65)

			srv, _ := newDailyPowerServer(dailyServerOpts{fillDate: "2020-01-03"})
			client := power.NewClient(srv.URL, nil)

			report, err := power.Run(ctx, db, power.Options{
				Client: client, Endpoint: srv.URL,
				Temporal: power.TemporalDaily, StartDate: "2020-01-01", EndDate: "2020-01-05",
			}, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.SupplementRows).To(Equal(4), "the all-fill date writes no row")

			Expect(selectStrings(db,
				`SELECT date FROM daily_supplement WHERE cell_id = ? ORDER BY date`, cell.ID,
			)).To(Equal([]string{"2020-01-01", "2020-01-02", "2020-01-04", "2020-01-05"}))

			expectStoredValues(selectDailySupplement(db, cell.ID, "2020-01-02"),
				expectedDailyStored(), report.RunID)
		})
	})
})
