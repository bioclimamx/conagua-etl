package ingest_test

import (
	"context"
	"database/sql"
	"io"
	"log"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// The golden snapshot: real CONAGUA files for station 01001
// (Aguascalientes observatory) — the daily and 1991-2020 fixtures shared
// with the conagua parser suite, plus the 1981-2010 file copied verbatim
// from the national 2026-06-08 snapshot. The values
// asserted below are transcribed from the file bodies; they are the
// behavior spec.
const (
	goldenDate = "2026-06-08"
	// The catalog name deliberately differs from the file-header name
	// ("AGUASCALIENTES (OBS)") so the specs can prove header enrichment
	// wins over the seed (non-empty last writer).
	goldenCatalogName = "Aguascalientes Observatorio"
)

var (
	goldenDailyFixture = filepath.Join("..", "conagua", "testdata", "daily", "real_01001.txt")
	golden9120Fixture  = filepath.Join("..", "conagua", "testdata", "normals", "real_1991_2020_01001.txt")
	golden8110Fixture  = filepath.Join("testdata", "normals_1981_2010_01001.txt")

	goldenKinds = []conagua.Kind{
		conagua.KindDaily, conagua.KindNormals1981_2010, conagua.KindNormals1991_2020,
	}

	// AÑOS CON DATOS per fixture section (tmax and tmin carry identical
	// counts in both files), driving the expected WMO scores.
	years9120TmaxTmin = []int{30, 30, 29, 30, 28, 30, 29, 29, 30, 30, 30, 30}
	years9120Precip   = []int{30, 30, 29, 30, 29, 29, 29, 29, 30, 30, 30, 30}
	years8110TmaxTmin = []int{28, 29, 28, 29, 27, 29, 28, 28, 27, 27, 27, 27}
	years8110Precip   = []int{29, 30, 29, 30, 29, 29, 29, 29, 28, 28, 28, 28}
)

// scoreArrays reproduces the WMO arithmetic over per-month count arrays:
// binary = share of the 36 cells ≥ 24; continuous = mean of min(n,30)/30.
func scoreArrays(vars ...[]int) (bin, cont float64) {
	pass := 0
	sum := 0.0
	for _, v := range vars {
		for _, n := range v {
			if n >= 24 {
				pass++
			}
			if n > 30 {
				n = 30
			}
			sum += float64(n) / 30.0
		}
	}
	return float64(pass) / 36.0, sum / 36.0
}

type goldenEnv struct {
	metaRoot string
	sinkRoot string
	dbPath   string
	sink     *snapshot.LocalFS
}

func goldenAddr(kind conagua.Kind) snapshot.Address {
	return snapshot.Address{Date: goldenDate, Kind: kind, StationID: "01001"}
}

// newGoldenEnv stages the three golden bodies under a fresh sink root.
// The index is written separately so specs can shape (and re-shape) it.
func newGoldenEnv() *goldenEnv {
	GinkgoHelper()
	dir := GinkgoT().TempDir()
	e := &goldenEnv{
		metaRoot: filepath.Join(dir, "meta"),
		sinkRoot: filepath.Join(dir, "data"),
		dbPath:   filepath.Join(dir, "bioclima.db"),
	}
	e.sink = snapshot.NewLocalFS(e.sinkRoot)

	for kind, src := range map[conagua.Kind]string{
		conagua.KindDaily:            goldenDailyFixture,
		conagua.KindNormals1981_2010: golden8110Fixture,
		conagua.KindNormals1991_2020: golden9120Fixture,
	} {
		dst := e.sink.Path(goldenAddr(kind))
		body, err := os.ReadFile(src)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.MkdirAll(filepath.Dir(dst), 0o755)).To(Succeed())
		Expect(os.WriteFile(dst, body, 0o644)).To(Succeed())
	}
	return e
}

// goldenStation builds the 01001 manifest entry advertising the given
// kinds as fetched.
func goldenStation(kinds ...conagua.Kind) snapshot.StationProgress {
	files := make(map[conagua.Kind]snapshot.FileState, len(kinds))
	for _, k := range kinds {
		files[k] = snapshot.FileState{Outcome: snapshot.OutcomeFetched, HTTPCode: 200, Attempts: 1}
	}
	return snapshot.StationProgress{
		State: "ags", ID: "01001", Name: goldenCatalogName,
		Municipality: "AGUASCALIENTES", Status: "operating", Files: files,
	}
}

func (e *goldenEnv) writeIndex(stations ...snapshot.StationProgress) {
	GinkgoHelper()
	writeIndexFile(e.metaRoot, goldenDate, stations)
}

func (e *goldenEnv) options() ingest.Options {
	opts := runOpts(e.sink, e.metaRoot, goldenDate, e.dbPath)
	opts.ETLGitSHA = "spec-sha"
	return opts
}

var _ = Describe("Run over the golden snapshot", func() {
	ctx := context.Background()

	expectGoldenDailySpots := func(db *sql.DB, sid int64) {
		GinkgoHelper()
		Expect(selectDaily(db, sid, "1878-01-01")).To(Equal(dailyRow{
			Tmax: validFloat(20.2), Tmin: validFloat(9.8), Precip: validFloat(0), Evap: sql.NullFloat64{},
		}))
		Expect(selectDaily(db, sid, "1878-01-02")).To(Equal(dailyRow{
			Tmax: validFloat(20.2), Tmin: validFloat(19), Precip: validFloat(0), Evap: sql.NullFloat64{},
		}))
		// NULO across the board except precip — honest gaps stay NULL.
		Expect(selectDaily(db, sid, "1878-01-06")).To(Equal(dailyRow{
			Tmax: sql.NullFloat64{}, Tmin: sql.NullFloat64{}, Precip: validFloat(0), Evap: sql.NullFloat64{},
		}))
	}

	It("ingests station 01001 end to end and round-trips every table", func() {
		env := newGoldenEnv()
		env.writeIndex(goldenStation(goldenKinds...))

		report, err := ingest.Run(ctx, env.options())
		Expect(err).NotTo(HaveOccurred())

		// Report counters.
		Expect(report.StationsSeeded).To(Equal(1))
		Expect(report.StationsAttempted).To(Equal(1))
		Expect(report.StationsSucceeded).To(Equal(1))
		Expect(report.StationsFailed).To(BeZero())
		Expect(report.DailyRowsInserted).To(Equal(15), "the daily fixture carries 15 data rows")
		Expect(report.NormalsRowsInserted).To(Equal(24), "12 months × 2 periods")
		Expect(report.ExtrasRowsInserted).To(Equal(24))
		Expect(report.FilesOpened).To(Equal(3))
		Expect(report.FilesPullErrored).To(BeZero())
		Expect(report.FilesNotInCatalog).To(Equal(2), "the two normals periods CONAGUA doesn't publish here")
		Expect(report.StationsWithFirstLast).To(Equal(1))
		Expect(report.StationsWithWMO).To(Equal(1))
		Expect(report.Warnings).To(BeEmpty(), "the golden files parse clean")
		Expect(report.IngestRunID).NotTo(BeZero())

		db := openDB(env.dbPath)

		// Station row: header enrichment fills coordinates and its name
		// overrides the catalog seed; catalog-only fields survive.
		st := selectStation(db, "conagua_conventional", "1001")
		Expect(st.Name).To(Equal("AGUASCALIENTES (OBS)"), "header name wins over catalog seed")
		Expect(st.State).To(Equal(validStr("AGS")))
		Expect(st.Municipality).To(Equal(validStr("AGUASCALIENTES")))
		Expect(st.Status).To(Equal(validStr("operating")))
		Expect(st.Lat).To(Equal(validFloat(21.85027778)))
		Expect(st.Lon).To(Equal(validFloat(-102.2908333)))
		Expect(st.AltitudeM).To(Equal(validFloat(1890.8)))
		Expect(st.FirstYear).To(Equal(validInt(1878)), "MIN(date) of the daily series")
		Expect(st.LastYear).To(Equal(validInt(1878)), "MAX(date) of the daily series")

		sid := stationID(db, "conagua_conventional", "1001")

		// Daily spot values against known fixture lines.
		Expect(countRows(db, `SELECT COUNT(*) FROM daily_observations WHERE station_id = ?`, sid)).To(Equal(15))
		expectGoldenDailySpots(db, sid)

		// January NORMAL rows for both periods.
		Expect(selectNormals(db, sid, "1991-2020", 1)).To(Equal(normalsRow{
			Tmax: validFloat(23), Tmin: validFloat(4.9), Tmean: validFloat(13.9),
			Precip: validFloat(7.1), Evap: validFloat(98.5),
		}))
		Expect(selectNormals(db, sid, "1981-2010", 1)).To(Equal(normalsRow{
			Tmax: validFloat(22.7), Tmin: validFloat(4.4), Tmean: validFloat(13.5),
			Precip: validFloat(5.6), Evap: validFloat(110.3),
		}))

		// January extras, 1991-2020 — every column against the file.
		Expect(selectExtras(db, sid, "1991-2020", 1)).To(Equal(extrasRow{
			TmaxMonthlyExtreme:       validFloat(24.9),
			TmaxMonthlyExtremeYear:   validInt(2017),
			TmaxDailyExtreme:         validFloat(29.2),
			TmaxDailyExtremeDate:     validStr("2018-01-13"),
			TminMonthlyExtreme:       validFloat(2.5),
			TminMonthlyExtremeYear:   validInt(1999),
			TminDailyExtreme:         validFloat(-9),
			TminDailyExtremeDate:     validStr("2016-01-17"),
			PrecipMonthlyExtreme:     validFloat(42.6),
			PrecipMonthlyExtremeYear: validInt(2010),
			PrecipDailyExtreme:       validFloat(20.3),
			PrecipDailyExtremeDate:   validStr("2020-01-02"),
			TmaxYearsWithData:        validInt(30),
			TminYearsWithData:        validInt(30),
			TmeanYearsWithData:       validInt(30),
			PrecipYearsWithData:      validInt(30),
			EvapYearsWithData:        validInt(28),
			RainDays:                 validFloat(2.4),
			RainDaysYearsWithData:    validInt(30),
		}))

		// December extras, 1981-2010 — a different month and period so a
		// month-indexing or period-routing slip cannot hide.
		Expect(selectExtras(db, sid, "1981-2010", 12)).To(Equal(extrasRow{
			TmaxMonthlyExtreme:       validFloat(25.1),
			TmaxMonthlyExtremeYear:   validInt(2007),
			TmaxDailyExtreme:         validFloat(28.4),
			TmaxDailyExtremeDate:     validStr("1997-12-09"),
			TminMonthlyExtreme:       validFloat(1.7),
			TminMonthlyExtremeYear:   validInt(2010),
			TminDailyExtreme:         validFloat(-5),
			TminDailyExtremeDate:     validStr("1997-12-14"),
			PrecipMonthlyExtreme:     validFloat(15.2),
			PrecipMonthlyExtremeYear: validInt(1982),
			PrecipDailyExtreme:       validFloat(10.6),
			PrecipDailyExtremeDate:   validStr("2006-12-08"),
			TmaxYearsWithData:        validInt(27),
			TminYearsWithData:        validInt(27),
			TmeanYearsWithData:       validInt(28),
			PrecipYearsWithData:      validInt(28),
			EvapYearsWithData:        validInt(27),
			RainDays:                 validFloat(1.5),
			RainDaysYearsWithData:    validInt(28),
		}))

		// WMO completeness for the two covered periods; the other two
		// stay NULL.
		bin91, cont91 := scoreArrays(years9120TmaxTmin, years9120TmaxTmin, years9120Precip)
		bin81, cont81 := scoreArrays(years8110TmaxTmin, years8110TmaxTmin, years8110Precip)
		w := selectWMO(db, sid)
		Expect(w.Bin61).To(Equal(sql.NullFloat64{}))
		Expect(w.Cont61).To(Equal(sql.NullFloat64{}))
		Expect(w.Bin71).To(Equal(sql.NullFloat64{}))
		Expect(w.Cont71).To(Equal(sql.NullFloat64{}))
		Expect(w.Bin81.Valid).To(BeTrue())
		Expect(w.Bin81.Float64).To(BeNumerically("~", bin81, 1e-9))
		Expect(w.Cont81.Float64).To(BeNumerically("~", cont81, 1e-9))
		Expect(w.Bin91.Valid).To(BeTrue())
		Expect(w.Bin91.Float64).To(BeNumerically("~", bin91, 1e-9))
		Expect(w.Cont91.Float64).To(BeNumerically("~", cont91, 1e-9))
		// Both fixtures pass the 80% rule in every cell.
		Expect(w.Bin81.Float64).To(BeNumerically("~", 1.0, 1e-9))
		Expect(w.Bin91.Float64).To(BeNumerically("~", 1.0, 1e-9))

		// The run manifest row.
		run := selectRun(db, report.IngestRunID)
		Expect(run.Status).To(Equal("complete"))
		Expect(run.SnapshotDate).To(Equal(goldenDate))
		Expect(run.SinkKind).To(Equal("local"))
		Expect(run.ETLGitSHA).To(Equal(validStr("spec-sha")))
		Expect(run.StartedAt).NotTo(BeEmpty())
		Expect(run.FinishedAt.Valid).To(BeTrue())
		Expect(run.Attempted).To(Equal(validInt(1)))
		Expect(run.Succeeded).To(Equal(validInt(1)))
		Expect(run.Failed).To(Equal(validInt(0)))
		Expect(run.DailyRows).To(Equal(validInt(15)))
		Expect(run.NormalsRows).To(Equal(validInt(24)))
		Expect(run.ExtrasRows).To(Equal(validInt(24)))
		Expect(run.Warnings).To(Equal(validInt(0)))
	})

	It("converges to identical DB state on a second run", func() {
		env := newGoldenEnv()
		env.writeIndex(goldenStation(goldenKinds...))

		report1, err := ingest.Run(ctx, env.options())
		Expect(err).NotTo(HaveOccurred())
		report2, err := ingest.Run(ctx, env.options())
		Expect(err).NotTo(HaveOccurred())

		db := openDB(env.dbPath)
		Expect(countRows(db, `SELECT COUNT(*) FROM stations`)).To(Equal(1))
		Expect(countRows(db, `SELECT COUNT(*) FROM daily_observations`)).To(Equal(15))
		Expect(countRows(db, `SELECT COUNT(*) FROM monthly_normals`)).To(Equal(24))
		Expect(countRows(db, `SELECT COUNT(*) FROM monthly_normals_extras`)).To(Equal(24))
		Expect(countRows(db, `SELECT COUNT(*) FROM parsing_warnings`)).To(BeZero(),
			"warnings must not accumulate across runs")

		sid := stationID(db, "conagua_conventional", "1001")
		expectGoldenDailySpots(db, sid)
		Expect(selectNormals(db, sid, "1991-2020", 1).Precip).To(Equal(validFloat(7.1)))

		// Each invocation leaves its own manifest row.
		Expect(report2.IngestRunID).To(BeNumerically(">", report1.IngestRunID))
		Expect(countRows(db, `SELECT COUNT(*) FROM ingest_runs`)).To(Equal(2))
		Expect(selectRun(db, report2.IngestRunID).Status).To(Equal("complete"))
	})

	It("restores mutated DB values to file truth on re-run (UPSERT overwrite)", func() {
		env := newGoldenEnv()
		env.writeIndex(goldenStation(goldenKinds...))
		_, err := ingest.Run(ctx, env.options())
		Expect(err).NotTo(HaveOccurred())

		db := openDB(env.dbPath)
		sid := stationID(db, "conagua_conventional", "1001")
		_, err = db.Exec(`UPDATE daily_observations SET tmax = 99.9
			WHERE station_id = ? AND date = '1878-01-01'`, sid)
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(`UPDATE monthly_normals SET precip = 123.4
			WHERE station_id = ? AND period = '1991-2020' AND month = 1`, sid)
		Expect(err).NotTo(HaveOccurred())

		_, err = ingest.Run(ctx, env.options())
		Expect(err).NotTo(HaveOccurred())

		Expect(selectDaily(db, sid, "1878-01-01").Tmax).To(Equal(validFloat(20.2)))
		Expect(selectNormals(db, sid, "1991-2020", 1).Precip).To(Equal(validFloat(7.1)))
	})

	Describe("--kind scoping", func() {
		It("daily: converges daily only, leaves normals alone, and skips WMO scoring", func() {
			env := newGoldenEnv()
			env.writeIndex(goldenStation(goldenKinds...))
			_, err := ingest.Run(ctx, env.options())
			Expect(err).NotTo(HaveOccurred())

			db := openDB(env.dbPath)
			sid := stationID(db, "conagua_conventional", "1001")
			// Sentinels: a mutated daily value (must converge), a mutated
			// normals value (must survive), and mutated WMO columns (must
			// survive — daily runs skip scoring entirely).
			_, err = db.Exec(`UPDATE daily_observations SET tmax = 88.8
				WHERE station_id = ? AND date = '1878-01-01'`, sid)
			Expect(err).NotTo(HaveOccurred())
			_, err = db.Exec(`UPDATE monthly_normals SET precip = 777.7
				WHERE station_id = ? AND period = '1991-2020' AND month = 1`, sid)
			Expect(err).NotTo(HaveOccurred())
			_, err = db.Exec(`UPDATE stations SET wmo_completeness_bin_1991_2020 = 0.123,
				wmo_completeness_cont_1991_2020 = 0.456 WHERE id = ?`, sid)
			Expect(err).NotTo(HaveOccurred())

			opts := env.options()
			opts.Kind = "daily"
			report, err := ingest.Run(ctx, opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.FilesOpened).To(Equal(1), "only the daily file")
			Expect(report.NormalsRowsInserted).To(BeZero())
			Expect(report.ExtrasRowsInserted).To(BeZero())
			Expect(report.StationsWithWMO).To(BeZero(), "daily runs must not score")

			Expect(selectDaily(db, sid, "1878-01-01").Tmax).To(Equal(validFloat(20.2)))
			Expect(selectNormals(db, sid, "1991-2020", 1).Precip).To(Equal(validFloat(777.7)),
				"normals rows must be untouched")
			w := selectWMO(db, sid)
			Expect(w.Bin91).To(Equal(validFloat(0.123)), "WMO columns must keep prior values")
			Expect(w.Cont91).To(Equal(validFloat(0.456)))
		})

		It("normals: converges normals and rescores WMO, leaves daily alone", func() {
			env := newGoldenEnv()
			env.writeIndex(goldenStation(goldenKinds...))
			_, err := ingest.Run(ctx, env.options())
			Expect(err).NotTo(HaveOccurred())

			db := openDB(env.dbPath)
			sid := stationID(db, "conagua_conventional", "1001")
			_, err = db.Exec(`UPDATE daily_observations SET tmax = 88.8
				WHERE station_id = ? AND date = '1878-01-01'`, sid)
			Expect(err).NotTo(HaveOccurred())
			_, err = db.Exec(`UPDATE monthly_normals SET precip = 777.7
				WHERE station_id = ? AND period = '1991-2020' AND month = 1`, sid)
			Expect(err).NotTo(HaveOccurred())
			_, err = db.Exec(`UPDATE stations SET wmo_completeness_bin_1991_2020 = 0.123
				WHERE id = ?`, sid)
			Expect(err).NotTo(HaveOccurred())

			opts := env.options()
			opts.Kind = "normals"
			report, err := ingest.Run(ctx, opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.FilesOpened).To(Equal(2), "the two normals files")
			Expect(report.DailyRowsInserted).To(BeZero())

			Expect(selectDaily(db, sid, "1878-01-01").Tmax).To(Equal(validFloat(88.8)),
				"daily rows must be untouched")
			Expect(selectNormals(db, sid, "1991-2020", 1).Precip).To(Equal(validFloat(7.1)))
			w := selectWMO(db, sid)
			Expect(w.Bin91.Valid).To(BeTrue())
			Expect(w.Bin91.Float64).To(BeNumerically("~", 1.0, 1e-9),
				"normals runs rescore, replacing the sentinel")
		})
	})

	Describe("WMO re-scoring on a changed snapshot", func() {
		It("resets a period to NULL once its extras rows and index entry are both gone", func() {
			env := newGoldenEnv()
			env.writeIndex(goldenStation(goldenKinds...))
			_, err := ingest.Run(ctx, env.options())
			Expect(err).NotTo(HaveOccurred())

			db := openDB(env.dbPath)
			sid := stationID(db, "conagua_conventional", "1001")
			Expect(selectWMO(db, sid).Bin81.Valid).To(BeTrue(), "precondition: 1981-2010 scored")

			// The next snapshot lost the 1981-2010 file. Scoring reads DB
			// state, so the stale extras rows
			// must also be out of the picture for the reset to apply —
			// clear them the way a pruned rebuild would.
			_, err = db.Exec(`DELETE FROM monthly_normals_extras
				WHERE station_id = ? AND period = '1981-2010'`, sid)
			Expect(err).NotTo(HaveOccurred())
			env.writeIndex(goldenStation(conagua.KindDaily, conagua.KindNormals1991_2020))

			_, err = ingest.Run(ctx, env.options())
			Expect(err).NotTo(HaveOccurred())

			w := selectWMO(db, sid)
			Expect(w.Bin81).To(Equal(sql.NullFloat64{}), "absent period resets to NULL")
			Expect(w.Cont81).To(Equal(sql.NullFloat64{}))
			Expect(w.Bin91.Valid).To(BeTrue(), "the surviving period keeps its score")
			Expect(w.Bin91.Float64).To(BeNumerically("~", 1.0, 1e-9))
		})

		It("keeps scoring DB-resident extras even when the file left the index", func() {
			env := newGoldenEnv()
			env.writeIndex(goldenStation(goldenKinds...))
			_, err := ingest.Run(ctx, env.options())
			Expect(err).NotTo(HaveOccurred())

			// Index loses the 1981-2010 file but the extras rows written
			// by the first run stay in the DB — the score must reflect DB
			// state, not the last parse.
			env.writeIndex(goldenStation(conagua.KindDaily, conagua.KindNormals1991_2020))
			_, err = ingest.Run(ctx, env.options())
			Expect(err).NotTo(HaveOccurred())

			db := openDB(env.dbPath)
			sid := stationID(db, "conagua_conventional", "1001")
			w := selectWMO(db, sid)
			Expect(w.Bin81.Valid).To(BeTrue(), "DB-resident extras still score")
			Expect(w.Bin81.Float64).To(BeNumerically("~", 1.0, 1e-9))
		})
	})

	It("records a file-level parse failure as an error warning while the station still commits", func() {
		env := newGoldenEnv()
		env.writeIndex(goldenStation(goldenKinds...))

		// Plant the 1991-2020 body at the 1981-2010 sink key: the period
		// drift check must reject the file, warn at severity 'error',
		// and leave the station's other kinds fully ingested.
		body, err := os.ReadFile(golden9120Fixture)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(env.sink.Path(goldenAddr(conagua.KindNormals1981_2010)), body, 0o644)).To(Succeed())

		report, err := ingest.Run(ctx, env.options())
		Expect(err).NotTo(HaveOccurred())
		Expect(report.StationsSucceeded).To(Equal(1), "a bad file must not fail the station")
		Expect(report.NormalsRowsInserted).To(Equal(12), "only the healthy period lands")
		Expect(report.ExtrasRowsInserted).To(Equal(12))

		db := openDB(env.dbPath)
		sid := stationID(db, "conagua_conventional", "1001")
		Expect(selectWarnings(db)).To(Equal([]warningRow{{
			StationID:  validInt(sid),
			SourceFile: "conagua-raw/2026-06-08/normals_1981_2010/01001.txt",
			Line:       sql.NullInt64{},
			Severity:   "error",
			Issue:      `period mismatch: file says "1991-2020", expected "1981-2010"`,
		}}))

		// The healthy kinds committed alongside the failure.
		Expect(countRows(db, `SELECT COUNT(*) FROM daily_observations WHERE station_id = ?`, sid)).To(Equal(15))
		Expect(selectNormals(db, sid, "1991-2020", 1).Precip).To(Equal(validFloat(7.1)))
		Expect(countRows(db, `SELECT COUNT(*) FROM monthly_normals WHERE station_id = ? AND period = '1981-2010'`, sid)).
			To(BeZero())
	})

	It("derives first/last year from normals periods when the station has no daily rows", func() {
		env := newGoldenEnv()
		env.writeIndex(goldenStation(conagua.KindNormals1991_2020))

		_, err := ingest.Run(ctx, env.options())
		Expect(err).NotTo(HaveOccurred())

		db := openDB(env.dbPath)
		st := selectStation(db, "conagua_conventional", "1001")
		Expect(st.FirstYear).To(Equal(validInt(1991)), "period start is the fallback first_year")
		Expect(st.LastYear).To(Equal(validInt(2020)), "period end is the fallback last_year")
		Expect(countRows(db, `SELECT COUNT(*) FROM daily_observations`)).To(BeZero())
	})

	It("cancels between stations: station 1 committed, run row closed aborted", func() {
		env := newGoldenEnv()
		env.writeIndex(
			goldenStation(goldenKinds...),
			snapshot.StationProgress{
				State: "ags", ID: "01005", Name: "Cañada Honda", Status: "operating",
				Files: map[conagua.Kind]snapshot.FileState{},
			},
		)

		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		opts := env.options()
		opts.OnStationDone = func(o ingest.StationOutcome) {
			if o.Index == 1 {
				cancel()
			}
		}

		report, err := ingest.Run(cancelCtx, opts)
		Expect(err).To(MatchError(context.Canceled))
		Expect(report).NotTo(BeNil(), "a partial report comes back on every path")
		Expect(report.StationsSucceeded).To(Equal(1))

		db := openDB(env.dbPath)

		// Station 1's tx committed before the cancellation took effect.
		sid := stationID(db, "conagua_conventional", "1001")
		Expect(countRows(db, `SELECT COUNT(*) FROM daily_observations WHERE station_id = ?`, sid)).To(Equal(15))
		Expect(selectStation(db, "conagua_conventional", "1001").Lat).To(Equal(validFloat(21.85027778)))

		// Station 2 was seeded but never ingested.
		sid2 := stationID(db, "conagua_conventional", "1005")
		Expect(countRows(db, `SELECT COUNT(*) FROM daily_observations WHERE station_id = ?`, sid2)).To(BeZero())
		Expect(countRows(db, `SELECT COUNT(*) FROM monthly_normals WHERE station_id = ?`, sid2)).To(BeZero())

		// The run row closed 'aborted' (on a background context) with the
		// counters as they stood.
		run := selectRun(db, report.IngestRunID)
		Expect(run.Status).To(Equal("aborted"))
		Expect(run.FinishedAt.Valid).To(BeTrue())
		Expect(run.Attempted).To(Equal(validInt(2)))
		Expect(run.Succeeded).To(Equal(validInt(1)))
		Expect(run.Failed).To(Equal(validInt(0)))
		Expect(run.DailyRows).To(Equal(validInt(15)))
	})

	Describe("through a CachingSink", func() {
		quiet := log.New(io.Discard, "", 0)

		It("populates the cache read-through and reports misses then hits", func() {
			env := newGoldenEnv()
			env.writeIndex(goldenStation(goldenKinds...))
			cache := snapshot.NewLocalFS(filepath.Join(GinkgoT().TempDir(), "cache"))

			opts := env.options()
			opts.Sink = snapshot.NewCachingSink(env.sink, cache, quiet)
			report, err := ingest.Run(ctx, opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.CacheMisses).To(Equal(int64(3)), "cold cache: every body is a miss")
			Expect(report.CacheHits).To(BeZero())

			// The bodies landed in the cache verbatim.
			for _, kind := range goldenKinds {
				want, err := os.ReadFile(env.sink.Path(goldenAddr(kind)))
				Expect(err).NotTo(HaveOccurred())
				got, err := os.ReadFile(cache.Path(goldenAddr(kind)))
				Expect(err).NotTo(HaveOccurred(), "cached body for %s must exist", kind)
				Expect(got).To(Equal(want), "cached body for %s must be verbatim", kind)
			}

			// Second run through a fresh CachingSink over the same cache:
			// all hits, and the DB still converges to file truth.
			opts2 := env.options()
			opts2.Sink = snapshot.NewCachingSink(env.sink, cache, quiet)
			report2, err := ingest.Run(ctx, opts2)
			Expect(err).NotTo(HaveOccurred())
			Expect(report2.CacheHits).To(Equal(int64(3)))
			Expect(report2.CacheMisses).To(BeZero())

			db := openDB(env.dbPath)
			sid := stationID(db, "conagua_conventional", "1001")
			Expect(selectDaily(db, sid, "1878-01-01").Tmax).To(Equal(validFloat(20.2)))
		})
	})
})
