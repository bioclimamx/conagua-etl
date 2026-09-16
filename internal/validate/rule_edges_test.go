package validate_test

// The branch edges of the gate rules the characterization fixtures do
// not reach: bbox's remaining bounds — lon > 180, the closed ends of
// the possible range, the three envelope sides the London fixture leaves
// untouched, a single zero coordinate (only the (0, 0) pair is the
// sentinel), and a NULL beside a zero (the NULL branch precedes the
// sentinel) — each with the issue text pinned as a fixed string; and the
// anchors' quiet edges: a NULL wind direction, a reference to an aborted
// run, and file-level warnings that name no station.

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

var _ = Describe("bbox edges", func() {
	It("errors past lon 180, warns on the closed ends of the possible range, on each envelope side, and on a single zero coordinate", func() {
		db, _ := openTempDB()
		// lon > 180 — impossible.
		lonHigh := station(db, "lon-high", "east", f64(20.0), f64(180.5))
		// lat = 90 and lon = -180 are possible (closed range) — merely outside MX.
		pole := station(db, "pole", "corner", f64(90.0), f64(-180.0))
		// South of the envelope, west of it — valid coordinates, warned.
		south := station(db, "south", "Guatemala", f64(10.0), f64(-99.0))
		west := station(db, "west", "Pacific", f64(20.0), f64(-120.0))
		// A single zero is not the sentinel: (0, lon) and (lat, 0) are
		// outside the envelope, not impossible.
		latZero := station(db, "lat-zero", "equator", f64(0.0), f64(-99.0))
		lonZero := station(db, "lon-zero", "meridian", f64(20.0), f64(0.0))

		res := runRule(db, "bbox")
		Expect(res.Scanned).To(Equal(6))
		Expect(severities(res.Findings)).To(Equal([]string{
			validate.SeverityError, validate.SeverityWarn, validate.SeverityWarn,
			validate.SeverityWarn, validate.SeverityWarn, validate.SeverityWarn,
		}))
		Expect(issues(res.Findings)).To(Equal([]string{
			`station conagua_conventional/lon-high ("east") at impossible lat=20.0000 lon=180.5000 — error blocks publish`,
			`station conagua_conventional/pole ("corner") at lat=90.0000 lon=-180.0000 outside MX bbox [14.5,32.8]×[-118.5,-86.7] — likely typo`,
			`station conagua_conventional/south ("Guatemala") at lat=10.0000 lon=-99.0000 outside MX bbox [14.5,32.8]×[-118.5,-86.7] — likely typo`,
			`station conagua_conventional/west ("Pacific") at lat=20.0000 lon=-120.0000 outside MX bbox [14.5,32.8]×[-118.5,-86.7] — likely typo`,
			`station conagua_conventional/lat-zero ("equator") at lat=0.0000 lon=-99.0000 outside MX bbox [14.5,32.8]×[-118.5,-86.7] — likely typo`,
			`station conagua_conventional/lon-zero ("meridian") at lat=20.0000 lon=0.0000 outside MX bbox [14.5,32.8]×[-118.5,-86.7] — likely typo`,
		}))
		for i, want := range []int64{lonHigh, pole, south, west, latZero, lonZero} {
			Expect(res.Findings[i].StationID).To(HaveValue(Equal(want)))
			Expect(res.Findings[i].RuleID).To(Equal("bbox"))
		}
	})

	It("reports a NULL coordinate ahead of the (0, 0) sentinel and the range checks", func() {
		db, _ := openTempDB()
		station(db, "null-lat", "half", nil, f64(0.0))
		station(db, "null-lon", "other-half", f64(0.0), nil)
		station(db, "null-far", "far", nil, f64(999.0))
		res := runRule(db, "bbox")
		Expect(res.Scanned).To(Equal(3))
		Expect(severities(res.Findings)).To(Equal([]string{
			validate.SeverityWarn, validate.SeverityWarn, validate.SeverityWarn,
		}))
		Expect(issues(res.Findings)).To(Equal([]string{
			`station conagua_conventional/null-lat ("half") has NULL lat`,
			`station conagua_conventional/null-lon ("other-half") has NULL lon`,
			`station conagua_conventional/null-far ("far") has NULL lat`,
		}))
	})

	It("names the station's own source, so an EMA station reads as conagua_ema", func() {
		db, _ := openTempDB()
		id := upsertStation(db, ingest.StationUpsert{
			Source: ingest.SourceConaguaEMA, ExternalID: "77", Name: "EMA Cozumel", Lat: f64(0.0), Lon: f64(0.0),
		})
		res := runRule(db, "bbox")
		Expect(res.Scanned).To(Equal(1))
		Expect(res.Findings).To(HaveLen(1))
		Expect(res.Findings[0].Severity).To(Equal(validate.SeverityError))
		Expect(res.Findings[0].StationID).To(HaveValue(Equal(id)))
		Expect(res.Findings[0].Issue).To(Equal(
			`station conagua_ema/77 ("EMA Cozumel") at impossible lat=0.0000 lon=0.0000 — error blocks publish`))
	})
})

var _ = Describe("anchor edges", func() {
	It("wind-range: a NULL direction is a gap, not a value out of range", func() {
		db, _ := openTempDB()
		s := seedClean(db)
		insertDaily(db, cellA, "2020-01-02", s.dailyRun, map[string]any{"wd2m_deg": nil, "wd10m_deg": nil})
		insertMonthly(db, cellA, "1981-2010", 1, s.monthlyRun, map[string]any{"wd2m_deg": nil, "wd10m_deg": 359.999})
		res := runRule(db, "wind-range")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(6))
	})

	It("run-refs: a power_run_id naming an aborted or running run is a resolvable reference — status is the ledger rule's concern", func() {
		db, _ := openTempDB()
		aborted := insertPowerRun(db, "aborted", "daily", 2026, 2026)
		running := insertPowerRun(db, "running", "monthly", 1961, 1990)
		seedClean(db)
		insertDaily(db, cellA, "2020-01-02", aborted, nil)
		insertMonthly(db, cellA, "1961-1990", 1, running, nil)
		res := runRule(db, "run-refs")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(6))
		// The same rows do trip the ledger rule, by that rule's own text.
		flight := runRule(db, "runs-in-flight")
		Expect(flight.Scanned).To(Equal(1))
		Expect(issues(flight.Findings)).To(Equal([]string{
			"power_runs.id=" + itoa(running) + " started 2026-06-09T10:00:00Z, status='running' — a run in flight or stranded cannot be vouched for (finish it, or reconcile through validate)",
		}))
	})

	It("station-refs: file-level warnings with a NULL station_id are neither scanned nor dangling", func() {
		db, _ := openTempDB()
		seedClean(db)
		for range 3 {
			insertWarning(db, nil, "conagua-raw/2026-06-08/catalog.html")
		}
		res := runRule(db, "station-refs")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(8))
	})

	It("cell-refs: a link whose station dangles but whose cell resolves is station-refs' finding, not cell-refs'", func() {
		db, _ := openTempDB()
		seedClean(db)
		insertStationCell(db, 777, cellA)
		Expect(runRule(db, "cell-refs").Findings).To(BeEmpty())
		refs := runRule(db, "station-refs")
		Expect(issues(refs.Findings)).To(Equal([]string{
			"station_power_cell: 1 row with a station_id naming no stations row (1 distinct: 777)",
		}))
	})
})
