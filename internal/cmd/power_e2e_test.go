package cmd_test

// The power e2e story: the real compiled binary augments a pre-seeded
// SQLite DB against a local fixture NASA POWER — an httptest server
// speaking the point time-series JSON shape ({"properties":{"parameter":
// …}}, {"header":{"fill_value":-999}}) with scripted per-cell failures.
// The specs prove the cmd→power seam end to end: flag wiring, the
// 31-parameter batched fetch under the per-endpoint caps (25 monthly /
// 20 daily, split in registry order), grid snapping + cell dedup
// persisted to nasa_power_grid_cells / station_power_cell, unit
// conversion at the writer, both supplement write paths round-tripped
// column by column, the power_runs reproducibility manifest, transient
// retry vs per-cell fail-soft, and the exit-code contract (0 clean /
// 2 degraded / 1 run-level failure).
//
// Fixture arithmetic: five seeded stations — two snapping to the same
// AGS cell (the dedup case), one CDMX, one with NULL coordinates
// (excluded by the loader), one under source 'conagua_ema' (excluded by
// the source filter) — yield exactly two unique cells. Values are
// synthetic but exact: registry parameter i is served for month m as
// i+1 + m/100 in the single contributing year 1991, alongside an
// all-fill 1992 and 9999-valued YYYY13 annual keys the rollup must
// discard. The expectations reproduce the rollup and conversion
// arithmetic operation for operation, so every Equal below is
// float-exact — a column shuffle or a skipped conversion cannot pass.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// powerFill is the fixture's fill_value sentinel, matching POWER's
// production -999.
const powerFill = -999.0

// The two cells the seeded stations snap to.
var (
	powerCellAgs  = power.CellFor(21.85027778, -102.2908333)
	powerCellCdmx = power.CellFor(19.4036, -99.1962)

	agsWireKey  = powerWireKey(powerCellAgs)
	cdmxWireKey = powerWireKey(powerCellCdmx)
)

// powerWireKey renders a cell centroid exactly as the client puts it on
// the wire (strconv.FormatFloat 'f' -1), so keying fixture requests by
// "latitude,longitude" also pins the coordinate wire format.
func powerWireKey(c power.Cell) string {
	return strconv.FormatFloat(c.Lat, 'f', -1, 64) + "," + strconv.FormatFloat(c.Lon, 'f', -1, 64)
}

// powerSeed is one station row seeded into a power e2e DB.
type powerSeed struct {
	source     ingest.Source
	externalID string
	name       string
	lat, lon   *float64
}

func f64(v float64) *float64 { return &v }

// powerSeedStations is the station population of every power e2e DB.
// 1001 and 1097 snap to the same cell — the dedup case; 2002 (no
// coordinates) and E100 (EMA source) must be invisible to power.
var powerSeedStations = []powerSeed{
	{ingest.SourceConaguaConventional, "1001", "AGUASCALIENTES (OBS)", f64(21.85027778), f64(-102.2908333)},
	{ingest.SourceConaguaConventional, "1097", "AGUASCALIENTES II", f64(21.90555556), f64(-102.265)},
	{ingest.SourceConaguaConventional, "9048", "TACUBAYA CENTRAL", f64(19.4036), f64(-99.1962)},
	{ingest.SourceConaguaConventional, "2002", "SIN COORDENADAS", nil, nil},
	{ingest.SourceConaguaEMA, "E100", "EMA TACUBAYA", f64(19.4036), f64(-99.1962)},
}

// seedPowerDB creates a fresh schema DB holding the fixture stations,
// seeded through the ingest station writer, and returns its path.
func seedPowerDB() string {
	dir, err := os.MkdirTemp("", "power-e2e-*")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	DeferCleanup(func() error { return os.RemoveAll(dir) })

	dbPath := filepath.Join(dir, "bioclima.db")
	db, err := schema.Open(dbPath)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	tx, err := db.BeginTx(context.Background(), nil)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	for _, s := range powerSeedStations {
		_, err := ingest.UpsertStation(context.Background(), tx, ingest.StationUpsert{
			Source:     s.source,
			ExternalID: s.externalID,
			Name:       s.name,
			State:      "AGS",
			Status:     "operating",
			Lat:        s.lat,
			Lon:        s.lon,
		})
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), s.externalID)
	}
	ExpectWithOffset(1, tx.Commit()).To(Succeed())
	ExpectWithOffset(1, db.Close()).To(Succeed())
	return dbPath
}

// monthlyRaw is the POWER-native value the fixture serves for registry
// parameter i in calendar month m of the contributing year (1991).
func monthlyRaw(i, m int) float64 { return float64(i+1) + float64(m)/100.0 }

// dailyRaw is the POWER-native value for registry parameter i on
// day-of-month d.
func dailyRaw(i, d int) float64 { return float64(i+1) + float64(d)/1000.0 }

// The CDMX cell's scripted data gaps — the honest-NULL routes:
// GWETPROF is absent from its responses entirely, T2M December is
// all-fill in monthly mode, and RH2M 2020-01-05 is fill in daily mode.
const (
	gapDroppedParam  = "GWETPROF"
	gapFillParam     = "T2M"
	gapFillMonth     = 12
	gapDailyParam    = "RH2M"
	gapDailyWireDate = "20200105"
)

func servesParam(cellKey, param string) bool {
	return cellKey != cdmxWireKey || param != gapDroppedParam
}

func servesMonthly(cellKey, param string, m int) bool {
	if cellKey == cdmxWireKey && param == gapFillParam && m == gapFillMonth {
		return false
	}
	return servesParam(cellKey, param)
}

func servesDaily(cellKey, param, wireDate string) bool {
	if cellKey == cdmxWireKey && param == gapDailyParam && wireDate == gapDailyWireDate {
		return false
	}
	return servesParam(cellKey, param)
}

// powerParamIdx maps parameter name → registry index, so the fixture
// serves per-parameter series for whatever chunk the client requests.
var powerParamIdx = func() map[string]int {
	idx := make(map[string]int, len(power.Registry))
	for i, p := range power.Registry {
		idx[p.Name] = i
	}
	return idx
}()

// powerRequest is one recorded fixture hit: the parameter chunk in
// arrival order plus the start..end span as it appeared on the wire.
type powerRequest struct {
	params []string
	span   string
}

// powerFixture is the local stand-in for NASA POWER: it serves canned
// series for whatever parameters each request asks, records every
// request per cell, checks the wire contract (parameter caps, AG
// community, JSON format, span shape) into a violations list asserted
// after the run, and can script a standing or one-shot failure status
// per cell.
type powerFixture struct {
	*httptest.Server

	mu         sync.Mutex
	reqs       map[string][]powerRequest
	failAlways map[string]int
	failOnce   map[string]int
	violations []string
}

func startPowerFixture() *powerFixture {
	f := &powerFixture{
		reqs:       map[string][]powerRequest{},
		failAlways: map[string]int{},
		failOnce:   map[string]int{},
	}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	return f
}

// violate records a wire-contract breach; failing inside the handler
// would cascade into client retries and obscure the real defect.
func (f *powerFixture) violate(format string, args ...any) {
	f.violations = append(f.violations, fmt.Sprintf(format, args...))
}

// scriptFailAlways makes every request for cellKey fail with status.
// The server is already serving when a spec scripts a failure, so the
// write takes the fixture's lock the handler reads under.
func (f *powerFixture) scriptFailAlways(cellKey string, status int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAlways[cellKey] = status
}

// scriptFailOnce makes the first request for cellKey fail with status.
func (f *powerFixture) scriptFailOnce(cellKey string, status int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failOnce[cellKey] = status
}

func (f *powerFixture) serve(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	params := strings.Split(q.Get("parameters"), ",")
	cellKey := q.Get("latitude") + "," + q.Get("longitude")
	start, end := q.Get("start"), q.Get("end")

	f.mu.Lock()
	f.reqs[cellKey] = append(f.reqs[cellKey], powerRequest{params: params, span: start + ".." + end})
	firstHit := len(f.reqs[cellKey]) == 1

	monthly := len(start) == 4
	switch {
	case len(start) == 4 && len(end) == 4:
	case len(start) == 8 && len(end) == 8:
	default:
		f.violate("cell %s: unexpected span %s..%s", cellKey, start, end)
	}
	maxParams := power.MaxParametersPerRequestMonthly
	if !monthly {
		maxParams = power.MaxParametersPerRequestDaily
	}
	if len(params) > maxParams {
		f.violate("cell %s: %d parameters exceeds the cap %d", cellKey, len(params), maxParams)
	}
	if c := q.Get("community"); c != "AG" {
		f.violate("cell %s: community %q", cellKey, c)
	}
	if format := q.Get("format"); format != "JSON" {
		f.violate("cell %s: format %q", cellKey, format)
	}
	for _, p := range params {
		if _, known := powerParamIdx[p]; !known {
			f.violate("cell %s: unknown parameter %q", cellKey, p)
		}
	}

	status := f.failAlways[cellKey]
	if status == 0 && firstHit {
		status = f.failOnce[cellKey]
	}
	f.mu.Unlock()

	if status != 0 {
		http.Error(w, "scripted POWER failure", status)
		return
	}

	series := map[string]map[string]float64{}
	for _, p := range params {
		i, known := powerParamIdx[p]
		if !known || !servesParam(cellKey, p) {
			continue
		}
		if monthly {
			series[p] = monthlySeries(cellKey, p, i)
		} else if s := dailySeries(cellKey, p, i, start, end); s != nil {
			series[p] = s
		}
	}
	payload := map[string]any{
		"properties": map[string]any{"parameter": series},
		"header":     map[string]any{"fill_value": powerFill},
		"messages":   []string{},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

// monthlySeries serves twelve 1991 values, an all-fill 1992 (fill rows
// must not skew the climatological mean), and 9999-valued YYYY13
// annual-mean keys — leaking any of those into the rollup shifts every
// mean and fails the exact round-trips.
func monthlySeries(cellKey, param string, i int) map[string]float64 {
	s := make(map[string]float64, 26)
	for m := 1; m <= 12; m++ {
		v := powerFill
		if servesMonthly(cellKey, param, m) {
			v = monthlyRaw(i, m)
		}
		s[fmt.Sprintf("1991%02d", m)] = v
		s[fmt.Sprintf("1992%02d", m)] = powerFill
	}
	s["199113"] = 9999
	s["199213"] = 9999
	return s
}

// dailySeries serves one value per day of the requested wire window,
// inclusive. Returns nil on an unparseable span (already recorded as a
// violation).
func dailySeries(cellKey, param string, i int, start, end string) map[string]float64 {
	from, err1 := time.Parse("20060102", start)
	to, err2 := time.Parse("20060102", end)
	if err1 != nil || err2 != nil {
		return nil
	}
	s := map[string]float64{}
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		key := d.Format("20060102")
		v := powerFill
		if servesDaily(cellKey, param, key) {
			v = dailyRaw(i, d.Day())
		}
		s[key] = v
	}
	return s
}

func (f *powerFixture) reqSnapshot() map[string][]powerRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string][]powerRequest, len(f.reqs))
	for k, v := range f.reqs {
		out[k] = append([]powerRequest(nil), v...)
	}
	return out
}

func (f *powerFixture) hitCount(cellKey string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reqs[cellKey])
}

func (f *powerFixture) violationsSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.violations...)
}

// wantCleanRequests is the request sequence a clean fetch of one cell
// must produce: the 31-parameter registry list split at the endpoint
// cap in caller order — two sub-requests, never more.
func wantCleanRequests(span string, batchCap int) []powerRequest {
	full := power.DefaultParameters()
	return []powerRequest{
		{params: full[:batchCap], span: span},
		{params: full[batchCap:], span: span},
	}
}

// wantMonthlyRow is the full 31-column expectation for (cellKey, m):
// the fixture's raw values pushed through the rollup arithmetic (single
// contributing year; circular vector mean for wind direction) and the
// registry's unit conversion, operation for operation, so the
// comparison is float-exact.
func wantMonthlyRow(cellKey string, m int) []sql.NullFloat64 {
	out := make([]sql.NullFloat64, len(power.Registry))
	for i, p := range power.Registry {
		if !servesMonthly(cellKey, p.Name, m) {
			continue
		}
		v := monthlyRaw(i, m)
		if p.Circular {
			r := v * math.Pi / 180.0
			v = math.Atan2(math.Sin(r), math.Cos(r)) * 180.0 / math.Pi
			if v < 0 {
				v += 360.0
			}
		}
		if p.Factor != 1.0 {
			v = v * p.Factor
		}
		out[i] = sql.NullFloat64{Float64: v, Valid: true}
	}
	return out
}

// wantDailyRow is the daily analog: raw values with unit conversion
// only — POWER's daily values (including wind direction) are stored
// as-returned.
func wantDailyRow(cellKey, iso string) []sql.NullFloat64 {
	wireDate := strings.ReplaceAll(iso, "-", "")
	day, err := strconv.Atoi(iso[8:10])
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), iso)
	out := make([]sql.NullFloat64, len(power.Registry))
	for i, p := range power.Registry {
		if !servesDaily(cellKey, p.Name, wireDate) {
			continue
		}
		v := dailyRaw(i, day)
		if p.Factor != 1.0 {
			v = v * p.Factor
		}
		out[i] = sql.NullFloat64{Float64: v, Valid: true}
	}
	return out
}

// registryColumns is the SELECT list for both supplement tables,
// derived from the registry so the read-back covers every value column
// in registry order.
var registryColumns = func() string {
	cols := make([]string, len(power.Registry))
	for i, p := range power.Registry {
		cols[i] = p.Column
	}
	return strings.Join(cols, ", ")
}()

// readSupplementColumns scans all 31 registry value columns of the
// single row the query selects.
func readSupplementColumns(db *sql.DB, query string, args ...any) []sql.NullFloat64 {
	vals := make([]sql.NullFloat64, len(power.Registry))
	ptrs := make([]any, len(vals))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	ExpectWithOffset(1, db.QueryRow(query, args...).Scan(ptrs...)).To(Succeed())
	return vals
}

// expectMonthlyValues asserts every (cell, month) supplement row equals
// the fixture-derived expectation, column by column.
func expectMonthlyValues(db *sql.DB) {
	for _, cell := range []struct{ id, key string }{
		{powerCellCdmx.ID, cdmxWireKey},
		{powerCellAgs.ID, agsWireKey},
	} {
		for m := 1; m <= 12; m++ {
			got := readSupplementColumns(db,
				`SELECT `+registryColumns+` FROM monthly_supplement
 WHERE cell_id = ? AND period = '1991-2020' AND month = ?`, cell.id, m)
			ExpectWithOffset(1, got).To(Equal(wantMonthlyRow(cell.key, m)),
				"cell %s month %d", cell.id, m)
		}
	}
}

// expectGridAndCellLinks asserts the cell-dedup tables whole: exactly
// the two fixture cells, and exactly the three conventional stations
// with coordinates — the coordinate-less and EMA-source stations must
// be absent — each linked with the Haversine distance to its centroid.
func expectGridAndCellLinks(db *sql.DB) {
	type gridRow struct {
		lat, lon   float64
		resolution string
	}
	gotCells := map[string]gridRow{}
	rows, err := db.Query(`SELECT cell_id, lat, lon, grid_resolution FROM nasa_power_grid_cells`)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer rows.Close() //nolint:errcheck // spec teardown
	for rows.Next() {
		var id string
		var g gridRow
		ExpectWithOffset(1, rows.Scan(&id, &g.lat, &g.lon, &g.resolution)).To(Succeed())
		gotCells[id] = g
	}
	ExpectWithOffset(1, rows.Err()).NotTo(HaveOccurred())
	ExpectWithOffset(1, gotCells).To(Equal(map[string]gridRow{
		powerCellAgs.ID:  {lat: powerCellAgs.Lat, lon: powerCellAgs.Lon, resolution: powerCellAgs.Resolution},
		powerCellCdmx.ID: {lat: powerCellCdmx.Lat, lon: powerCellCdmx.Lon, resolution: powerCellCdmx.Resolution},
	}))

	type linkRow struct {
		cellID     string
		distanceKM float64
	}
	gotLinks := map[string]linkRow{}
	lrows, err := db.Query(`
SELECT s.external_id, spc.cell_id, spc.distance_km
  FROM station_power_cell spc JOIN stations s ON s.id = spc.station_id`)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer lrows.Close() //nolint:errcheck // spec teardown
	for lrows.Next() {
		var externalID string
		var l linkRow
		ExpectWithOffset(1, lrows.Scan(&externalID, &l.cellID, &l.distanceKM)).To(Succeed())
		gotLinks[externalID] = l
	}
	ExpectWithOffset(1, lrows.Err()).NotTo(HaveOccurred())

	wantLinks := map[string]linkRow{}
	for _, s := range powerSeedStations {
		if s.source != ingest.SourceConaguaConventional || s.lat == nil {
			continue
		}
		c := power.CellFor(*s.lat, *s.lon)
		wantLinks[s.externalID] = linkRow{
			cellID:     c.ID,
			distanceKM: power.HaversineKm(*s.lat, *s.lon, c.Lat, c.Lon),
		}
	}
	ExpectWithOffset(1, gotLinks).To(Equal(wantLinks))
}

// powerRunRow is one power_runs manifest row, whole.
type powerRunRow struct {
	startedAt, finishedAt, status                               string
	endpoint, parameters, community                             string
	startYear, endYear                                          int
	gridResolution                                              string
	solarConversion                                             float64
	unitConversions                                             sql.NullString
	temporalMode                                                string
	startDate, endDate                                          sql.NullString
	cellsAttempted, cellsSucceeded, cellsFailed, supplementRows sql.NullInt64
	etlGitSHA                                                   sql.NullString
}

func readPowerRun(db *sql.DB, id int64) powerRunRow {
	var r powerRunRow
	ExpectWithOffset(1, db.QueryRow(`
SELECT started_at, COALESCE(finished_at, ''), status,
       endpoint_url, parameters, community,
       period_start_year, period_end_year, grid_resolution, solar_conversion,
       unit_conversions, temporal_mode, period_start_date, period_end_date,
       cells_attempted, cells_succeeded, cells_failed, supplement_rows,
       etl_git_sha
  FROM power_runs WHERE id = ?`, id).
		Scan(&r.startedAt, &r.finishedAt, &r.status,
			&r.endpoint, &r.parameters, &r.community,
			&r.startYear, &r.endYear, &r.gridResolution, &r.solarConversion,
			&r.unitConversions, &r.temporalMode, &r.startDate, &r.endDate,
			&r.cellsAttempted, &r.cellsSucceeded, &r.cellsFailed, &r.supplementRows,
			&r.etlGitSHA)).To(Succeed())
	return r
}

// expectRunGitSHA keys off the binary's own --version output so the
// assertion stays honest in any build mode.
func expectRunGitSHA(v sql.NullString) {
	if binGitSHA != "" {
		ExpectWithOffset(2, v.String).To(Equal(binGitSHA))
	} else {
		// A VCS-less build stamps no SHA; NULL and '' are both honest.
		ExpectWithOffset(2, v.String).To(BeEmpty())
	}
}

// expectManifestCommon asserts the mode-independent manifest columns:
// the fixture endpoint, the registry parameter list verbatim (the
// string a reproducer pastes into a POWER URL), the AG community, the
// grid resolution, the exact solar factor as a literal, and the full
// per-parameter conversions JSON.
func expectManifestCommon(run powerRunRow, endpoint string) {
	ExpectWithOffset(1, run.endpoint).To(Equal(endpoint))
	ExpectWithOffset(1, run.parameters).To(Equal(strings.Join(power.DefaultParameters(), ",")))
	ExpectWithOffset(1, run.community).To(Equal("AG"))
	ExpectWithOffset(1, run.gridResolution).To(Equal("0.5x0.625"))
	// The reproducibility contract pinned as a literal, not a read-back
	// of the package constant.
	ExpectWithOffset(1, run.solarConversion).To(Equal(1e6 / 86400.0))
	ExpectWithOffset(1, run.unitConversions.Valid).To(BeTrue())
	var conv map[string]power.UnitConversion
	ExpectWithOffset(1, json.Unmarshal([]byte(run.unitConversions.String), &conv)).To(Succeed())
	ExpectWithOffset(1, conv).To(Equal(power.Conversions))
	for _, ts := range []string{run.startedAt, run.finishedAt} {
		_, err := time.Parse(time.RFC3339, ts)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), ts)
	}
	expectRunGitSHA(run.etlGitSHA)
}

// startPowerRun launches the binary with the canonical e2e flag set:
// the hidden --endpoint pointed at the fixture, pacing at the validated
// ceiling so the limiter stays real without slowing the suite.
func startPowerRun(dbPath, endpoint string, extra ...string) *gexec.Session {
	args := []string{
		"power",
		"--db", dbPath,
		"--endpoint", endpoint,
		"--rps", "4",
		"--max-rps", "5",
	}
	args = append(args, extra...)
	session, err := gexec.Start(exec.Command(binPath, args...), GinkgoWriter, GinkgoWriter)
	ExpectWithOffset(2, err).NotTo(HaveOccurred())
	return session
}

func runPowerToExit(dbPath, endpoint string, extra ...string) *gexec.Session {
	session := startPowerRun(dbPath, endpoint, extra...)
	// Generous: a persistently failing cell costs the full 2+4+8 s
	// backoff ladder before the run can complete.
	EventuallyWithOffset(1, session, 90*time.Second).Should(gexec.Exit())
	return session
}

// powerLineREFor matches one per-cell progress line on stderr for a
// plan of the given size — the ingest-style renderer the power CLI
// adopts — capturing the status marker and the cell ID.
func powerLineREFor(total int) *regexp.Regexp {
	return regexp.MustCompile(fmt.Sprintf(
		`(?m)^\[\d{2}:\d{2}:\d{2}\] \d+/%d (ok|FAIL)\s+(\S+)`, total))
}

var _ = Describe("conagua-etl power end-to-end", func() {
	Context("monthly against a fixture POWER", Ordered, func() {
		var (
			dbPath string
			srv    *powerFixture
		)

		BeforeAll(func() {
			dbPath = seedPowerDB()
			srv = startPowerFixture()
			DeferCleanup(srv.Close)
		})

		It("augments both cells: capped batches in registry order, converted values round-tripped", func() {
			session := runPowerToExit(dbPath, srv.URL)
			Expect(session.ExitCode()).To(Equal(0))

			// The wire contract held on every request…
			Expect(srv.violationsSnapshot()).To(BeEmpty())
			// …and each cell cost exactly two sub-requests: the
			// 31-parameter list split at the 25 cap in caller order over
			// the monthly year span. The map keys double as the centroid
			// coordinate wire-format assertion.
			want := wantCleanRequests("1991..2020", power.MaxParametersPerRequestMonthly)
			Expect(srv.reqSnapshot()).To(Equal(map[string][]powerRequest{
				cdmxWireKey: want,
				agsWireKey:  want,
			}))

			// One ok progress line per cell, in sorted-cell order.
			lines := powerLineREFor(2).FindAllStringSubmatch(string(session.Err.Contents()), -1)
			Expect(lines).To(HaveLen(2))
			Expect(lines[0][1]).To(Equal("ok"))
			Expect(lines[0][2]).To(Equal(powerCellCdmx.ID))
			Expect(lines[1][1]).To(Equal("ok"))
			Expect(lines[1][2]).To(Equal(powerCellAgs.ID))

			db := openIngestDB(dbPath)
			expectGridAndCellLinks(db)

			// 2 cells × 12 months, every row owned by run 1, and every
			// value column of every row read back exactly. The CDMX gaps
			// land as NULL (GWETPROF all year, T2M in December) — honest
			// gaps, never invented values.
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_supplement`)).To(Equal(24))
			Expect(countRows(db,
				`SELECT COUNT(*) FROM monthly_supplement WHERE power_run_id = 1 AND period = '1991-2020'`)).
				To(Equal(24))
			expectMonthlyValues(db)

			// The solar spot check: stored GHI is the served MJ/m²/day
			// value times exactly 1e6/86400, pinned as a literal.
			var ghi float64
			Expect(db.QueryRow(`
SELECT solar_ghi_wm2 FROM monthly_supplement
 WHERE cell_id = ? AND period = '1991-2020' AND month = 1`, powerCellAgs.ID).
				Scan(&ghi)).To(Succeed())
			Expect(ghi).To(Equal(monthlyRaw(powerParamIdx["ALLSKY_SFC_SW_DWN"], 1) * (1e6 / 86400.0)))

			// The power_runs manifest round-trips whole.
			run := readPowerRun(db, 1)
			expectManifestCommon(run, srv.URL)
			Expect(run.status).To(Equal("complete"))
			Expect(run.temporalMode).To(Equal("monthly"))
			Expect(run.startYear).To(Equal(1991))
			Expect(run.endYear).To(Equal(2020))
			Expect(run.startDate.Valid).To(BeFalse())
			Expect(run.endDate.Valid).To(BeFalse())
			Expect(run.cellsAttempted.Int64).To(Equal(int64(2)))
			Expect(run.cellsSucceeded.Int64).To(Equal(int64(2)))
			Expect(run.cellsFailed.Int64).To(Equal(int64(0)))
			Expect(run.supplementRows.Int64).To(Equal(int64(24)))
		})

		It("re-runs to convergence: identical values, rows reassigned to the new run", func() {
			rerun := runPowerToExit(dbPath, srv.URL)
			Expect(rerun.ExitCode()).To(Equal(0))

			// No skip machinery: the re-run re-fetches both cells whole.
			Expect(srv.hitCount(cdmxWireKey)).To(Equal(4))
			Expect(srv.hitCount(agsWireKey)).To(Equal(4))

			db := openIngestDB(dbPath)
			Expect(countRows(db, `SELECT COUNT(*) FROM power_runs WHERE status = 'complete'`)).To(Equal(2))
			// Converged, not accumulated: still 24 rows, every one now
			// owned by run 2, with float-identical values.
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_supplement`)).To(Equal(24))
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_supplement WHERE power_run_id = 2`)).To(Equal(24))
			expectMonthlyValues(db)
			Expect(readPowerRun(db, 2).supplementRows.Int64).To(Equal(int64(24)))
		})
	})

	Context("daily against a fixture POWER", Ordered, func() {
		const dailyStart, dailyEnd = "2020-01-01", "2020-01-10"

		var (
			dbPath string
			srv    *powerFixture
		)

		BeforeAll(func() {
			dbPath = seedPowerDB()
			srv = startPowerFixture()
			DeferCleanup(srv.Close)
		})

		It("passes the window through to daily_supplement, converted and round-tripped", func() {
			session := runPowerToExit(dbPath, srv.URL,
				"--temporal", "daily", "--start-date", dailyStart, "--end-date", dailyEnd)
			Expect(session.ExitCode()).To(Equal(0))

			Expect(srv.violationsSnapshot()).To(BeEmpty())
			// Daily splits at the 20 cap and renders the span in
			// POWER's YYYYMMDD wire form.
			want := wantCleanRequests("20200101..20200110", power.MaxParametersPerRequestDaily)
			Expect(srv.reqSnapshot()).To(Equal(map[string][]powerRequest{
				cdmxWireKey: want,
				agsWireKey:  want,
			}))

			db := openIngestDB(dbPath)
			expectGridAndCellLinks(db)

			// 2 cells × 10 days, all owned by run 1, every column of
			// every row exact — including the RH2M fill on 2020-01-05
			// and the dropped GWETPROF landing as NULL for CDMX.
			Expect(countRows(db, `SELECT COUNT(*) FROM daily_supplement`)).To(Equal(20))
			Expect(countRows(db, `SELECT COUNT(*) FROM daily_supplement WHERE power_run_id = 1`)).To(Equal(20))
			for _, cell := range []struct{ id, key string }{
				{powerCellCdmx.ID, cdmxWireKey},
				{powerCellAgs.ID, agsWireKey},
			} {
				for day := 1; day <= 10; day++ {
					iso := fmt.Sprintf("2020-01-%02d", day)
					got := readSupplementColumns(db,
						`SELECT `+registryColumns+` FROM daily_supplement WHERE cell_id = ? AND date = ?`,
						cell.id, iso)
					Expect(got).To(Equal(wantDailyRow(cell.key, iso)), "cell %s %s", cell.id, iso)
				}
			}

			// Daily radiation converts with the same literal factor.
			var ghi float64
			Expect(db.QueryRow(
				`SELECT solar_ghi_wm2 FROM daily_supplement WHERE cell_id = ? AND date = ?`,
				powerCellAgs.ID, dailyStart).Scan(&ghi)).To(Succeed())
			Expect(ghi).To(Equal(dailyRaw(powerParamIdx["ALLSKY_SFC_SW_DWN"], 1) * (1e6 / 86400.0)))

			// The daily manifest: date span authoritative, year columns
			// carrying the coarse span for year-only readers.
			run := readPowerRun(db, 1)
			expectManifestCommon(run, srv.URL)
			Expect(run.status).To(Equal("complete"))
			Expect(run.temporalMode).To(Equal("daily"))
			Expect(run.startDate.String).To(Equal(dailyStart))
			Expect(run.endDate.String).To(Equal(dailyEnd))
			Expect(run.startYear).To(Equal(2020))
			Expect(run.endYear).To(Equal(2020))
			Expect(run.cellsAttempted.Int64).To(Equal(int64(2)))
			Expect(run.cellsSucceeded.Int64).To(Equal(int64(2)))
			Expect(run.cellsFailed.Int64).To(Equal(int64(0)))
			Expect(run.supplementRows.Int64).To(Equal(int64(20)))
		})
	})

	Context("when one cell fails persistently", Ordered, func() {
		var (
			dbPath string
			srv    *powerFixture
		)

		BeforeAll(func() {
			dbPath = seedPowerDB()
			srv = startPowerFixture()
			srv.scriptFailAlways(cdmxWireKey, http.StatusInternalServerError)
			DeferCleanup(srv.Close)
		})

		It("completes fail-soft with exit 2 and a degraded summary naming the losses", func() {
			session := runPowerToExit(dbPath, srv.URL)
			Expect(session.ExitCode()).To(Equal(2))

			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring(
				"error: power run completed with 1/2 cells failed"))

			// The failed cell burned its whole retry budget — 4 attempts
			// on the first sub-request, the second never sent — while
			// the healthy cell fetched normally.
			Expect(srv.hitCount(cdmxWireKey)).To(Equal(4))
			Expect(srv.hitCount(agsWireKey)).To(Equal(2))

			// A FAIL line for the lost cell, ok for the other.
			lines := powerLineREFor(2).FindAllStringSubmatch(stderr, -1)
			Expect(lines).To(HaveLen(2))
			Expect(lines[0][1]).To(Equal("FAIL"))
			Expect(lines[0][2]).To(Equal(powerCellCdmx.ID))
			Expect(lines[1][1]).To(Equal("ok"))
			Expect(lines[1][2]).To(Equal(powerCellAgs.ID))

			// Fail-soft per unit: the healthy cell's 12 rows landed
			// whole and exact; the failed cell wrote nothing; the run
			// still closed 'complete' with honest counters.
			db := openIngestDB(dbPath)
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_supplement`)).To(Equal(12))
			Expect(countRows(db,
				`SELECT COUNT(*) FROM monthly_supplement WHERE cell_id = ?`, powerCellCdmx.ID)).
				To(BeZero())
			for m := 1; m <= 12; m++ {
				got := readSupplementColumns(db,
					`SELECT `+registryColumns+` FROM monthly_supplement
 WHERE cell_id = ? AND period = '1991-2020' AND month = ?`, powerCellAgs.ID, m)
				Expect(got).To(Equal(wantMonthlyRow(agsWireKey, m)), "month %d", m)
			}
			run := readPowerRun(db, 1)
			Expect(run.status).To(Equal("complete"))
			Expect(run.cellsAttempted.Int64).To(Equal(int64(2)))
			Expect(run.cellsSucceeded.Int64).To(Equal(int64(1)))
			Expect(run.cellsFailed.Int64).To(Equal(int64(1)))
			Expect(run.supplementRows.Int64).To(Equal(int64(12)))
		})
	})

	Context("when POWER rate-limits transiently", Ordered, func() {
		var (
			dbPath string
			srv    *powerFixture
		)

		BeforeAll(func() {
			dbPath = seedPowerDB()
			srv = startPowerFixture()
			srv.scriptFailOnce(cdmxWireKey, http.StatusTooManyRequests)
			DeferCleanup(srv.Close)
		})

		It("retries through the 429 and completes clean", func() {
			session := runPowerToExit(dbPath, srv.URL)
			Expect(session.ExitCode()).To(Equal(0))

			// The 429'd sub-request cost exactly one extra attempt; the
			// retry is invisible to the run outcome.
			Expect(srv.hitCount(cdmxWireKey)).To(Equal(3))
			Expect(srv.hitCount(agsWireKey)).To(Equal(2))
			Expect(string(session.Err.Contents())).NotTo(ContainSubstring("FAIL"))

			db := openIngestDB(dbPath)
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_supplement`)).To(Equal(24))
			expectMonthlyValues(db)
			run := readPowerRun(db, 1)
			Expect(run.status).To(Equal("complete"))
			Expect(run.cellsFailed.Int64).To(Equal(int64(0)))
			Expect(run.supplementRows.Int64).To(Equal(int64(24)))
		})
	})

	Context("with --max-cells", func() {
		It("caps the fetch to the first sorted cell but registers the whole grid", func() {
			dbPath := seedPowerDB()
			srv := startPowerFixture()
			DeferCleanup(srv.Close)

			session := runPowerToExit(dbPath, srv.URL, "--max-cells", "1")
			Expect(session.ExitCode()).To(Equal(0))

			// Only the first cell in sorted order was fetched.
			Expect(srv.violationsSnapshot()).To(BeEmpty())
			want := wantCleanRequests("1991..2020", power.MaxParametersPerRequestMonthly)
			Expect(srv.reqSnapshot()).To(Equal(map[string][]powerRequest{cdmxWireKey: want}))

			// Cell registration is uncapped — the dedup tables cover
			// every station — while the supplement holds only the
			// fetched cell.
			db := openIngestDB(dbPath)
			expectGridAndCellLinks(db)
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_supplement`)).To(Equal(12))
			Expect(countRows(db,
				`SELECT COUNT(*) FROM monthly_supplement WHERE cell_id = ?`, powerCellAgs.ID)).
				To(BeZero())
			run := readPowerRun(db, 1)
			Expect(run.status).To(Equal("complete"))
			Expect(run.cellsAttempted.Int64).To(Equal(int64(1)))
			Expect(run.cellsSucceeded.Int64).To(Equal(int64(1)))
			Expect(run.supplementRows.Int64).To(Equal(int64(12)))
		})
	})
})
