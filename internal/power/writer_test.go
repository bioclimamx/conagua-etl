package power_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// supplementValueColumns is the ordered list of the 31 value columns
// shared by monthly_supplement and daily_supplement. It is declared as
// a literal — not derived from the registry — so it is an independent
// anchor: if the registry order ever drifted, the lockstep spec below
// fails instead of both sides silently moving together.
var supplementValueColumns = []string{
	"t2m_c", "t2m_max_c", "t2m_min_c", "t2m_wet_c", "t2m_dew_c",
	"ts_c", "ts_max_c", "ts_min_c",
	"rh2m_pct", "qv2m_gkg",
	"ws2m_ms", "ws10m_ms", "ws50m_ms", "wd2m_deg", "wd10m_deg",
	"solar_ghi_wm2", "solar_dhi_wm2", "solar_dni_wm2", "solar_clrsky_wm2",
	"clearness_index", "par_wm2", "uva_wm2", "uvb_wm2",
	"lw_dwn_wm2",
	"cloud_amt_pct", "ps_kpa",
	"precip_mmpd", "evland_mmpd",
	"gwet_top", "gwet_root", "gwet_prof",
}

// supplementSentinels holds one distinct float per value column. The
// numbers don't have to be physically plausible — they're sentinels:
// a column-order swap flips two values and the assertion names the
// exact column that drifted.
var supplementSentinels = []float64{
	// Temperature (8)
	20.1, 28.2, 15.3, 17.4, 18.5, 22.6, 30.7, 14.8,
	// Humidity (2)
	71.5, 12.0,
	// Wind (5): WS×3, WD×2
	2.1, 3.4, 5.6, 90.0, 270.5,
	// Solar shortwave (8)
	245.6, 100.1, 320.2, 280.3, 0.55, 150.4, 25.5, 1.2,
	// Solar longwave (1)
	400.0,
	// Sky / cloud / atmosphere (2)
	42.0, 101.3,
	// Moisture and ET (2)
	2.5, 3.2,
	// Soil moisture (3)
	0.21, 0.42, 0.63,
}

// openTestDB opens a fresh SQLite DB in a spec-scoped TempDir through
// the one writer entry point and schedules its close with teardown.
func openTestDB() *sql.DB {
	GinkgoHelper()
	db, err := schema.Open(filepath.Join(GinkgoT().TempDir(), "bioclima.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = db.Close() })
	return db
}

func countRows(db *sql.DB, query string, args ...any) int {
	GinkgoHelper()
	var n int
	Expect(db.QueryRow(query, args...).Scan(&n)).To(Succeed())
	return n
}

func validInt64(v int64) sql.NullInt64       { return sql.NullInt64{Int64: v, Valid: true} }
func validStr(s string) sql.NullString       { return sql.NullString{String: s, Valid: true} }
func validFloat64(v float64) sql.NullFloat64 { return sql.NullFloat64{Float64: v, Valid: true} }

// powerRunRow is the full read-back of one power_runs row — every one
// of its 19 columns, so a manifest round-trip is a single Equal.
type powerRunRow struct {
	StartedAt       string
	FinishedAt      sql.NullString
	Status          string
	Endpoint        string
	Parameters      string
	Community       string
	StartYear       int
	EndYear         int
	GridResolution  string
	SolarConversion float64
	UnitConversions sql.NullString
	TemporalMode    string
	StartDate       sql.NullString
	EndDate         sql.NullString
	Attempted       sql.NullInt64
	Succeeded       sql.NullInt64
	Failed          sql.NullInt64
	SupplementRows  sql.NullInt64
	ETLGitSHA       sql.NullString
}

func selectPowerRun(db *sql.DB, id int64) powerRunRow {
	GinkgoHelper()
	var r powerRunRow
	err := db.QueryRow(`
SELECT started_at, finished_at, status,
       endpoint_url, parameters, community,
       period_start_year, period_end_year, grid_resolution, solar_conversion,
       unit_conversions, temporal_mode, period_start_date, period_end_date,
       cells_attempted, cells_succeeded, cells_failed,
       supplement_rows, etl_git_sha
  FROM power_runs WHERE id = ?`, id).
		Scan(&r.StartedAt, &r.FinishedAt, &r.Status,
			&r.Endpoint, &r.Parameters, &r.Community,
			&r.StartYear, &r.EndYear, &r.GridResolution, &r.SolarConversion,
			&r.UnitConversions, &r.TemporalMode, &r.StartDate, &r.EndDate,
			&r.Attempted, &r.Succeeded, &r.Failed,
			&r.SupplementRows, &r.ETLGitSHA)
	Expect(err).NotTo(HaveOccurred())
	return r
}

var _ = Describe("supplement write-path schema", func() {
	ctx := context.Background()

	It("declares the sentinel columns in registry order", func() {
		// The registry drives the writer's INSERT column list; the
		// sentinel fixture must track it or the round-trips below test
		// a stale shape.
		regCols := make([]string, len(power.Registry))
		for i, p := range power.Registry {
			regCols[i] = p.Column
		}
		Expect(regCols).To(Equal(supplementValueColumns))
		Expect(supplementSentinels).To(HaveLen(len(supplementValueColumns)))
	})

	Describe("the monthly_supplement chain", func() {
		// The chain (station → station_power_cell → nasa_power_grid_cells
		// → monthly_supplement) must insert and read back without
		// column-order drift: every one of the 31 value columns is
		// written with a distinct sentinel and read back individually.
		It("round-trips every column of every table in the chain", func() {
			db := openTestDB()
			tx, err := db.BeginTx(ctx, nil)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = tx.Rollback() })

			stationID, err := ingest.UpsertStation(ctx, tx, ingest.StationUpsert{
				Source: ingest.SourceConaguaConventional, ExternalID: "1001", Name: "test",
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = tx.ExecContext(ctx,
				`INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution)
				 VALUES (?, ?, ?, ?)`,
				"20.0N_100.0W", 20.0, -100.0, "0.5x0.625")
			Expect(err).NotTo(HaveOccurred())

			_, err = tx.ExecContext(ctx,
				`INSERT INTO station_power_cell (station_id, cell_id, distance_km)
				 VALUES (?, ?, ?)`, stationID, "20.0N_100.0W", 12.5)
			Expect(err).NotTo(HaveOccurred())

			placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(supplementValueColumns)), ", ")
			insert := "INSERT INTO monthly_supplement (cell_id, period, month, " +
				strings.Join(supplementValueColumns, ", ") + ") VALUES (?, ?, ?, " + placeholders + ")"
			args := []any{"20.0N_100.0W", "1991-2020", 7}
			for _, v := range supplementSentinels {
				args = append(args, v)
			}
			_, err = tx.ExecContext(ctx, insert, args...)
			Expect(err).NotTo(HaveOccurred())
			Expect(tx.Commit()).To(Succeed())

			var (
				cellID string
				clat   float64
				clon   float64
				gres   string
			)
			Expect(db.QueryRowContext(ctx,
				`SELECT cell_id, lat, lon, grid_resolution FROM nasa_power_grid_cells`,
			).Scan(&cellID, &clat, &clon, &gres)).To(Succeed())
			Expect(cellID).To(Equal("20.0N_100.0W"))
			Expect(clat).To(Equal(20.0))
			Expect(clon).To(Equal(-100.0))
			Expect(gres).To(Equal("0.5x0.625"))

			var (
				assocStation int64
				assocCell    string
				assocDist    float64
			)
			Expect(db.QueryRowContext(ctx,
				`SELECT station_id, cell_id, distance_km FROM station_power_cell`,
			).Scan(&assocStation, &assocCell, &assocDist)).To(Succeed())
			Expect(assocStation).To(Equal(stationID))
			Expect(assocCell).To(Equal("20.0N_100.0W"))
			Expect(assocDist).To(Equal(12.5))

			// SELECT every value column back in the order it was written
			// and assert each individually: any column-order regression in
			// a prepared INSERT surfaces as a mismatch on the column that
			// drifted, never as a silently passing count.
			selectSQL := "SELECT " + strings.Join(supplementValueColumns, ", ") + " FROM monthly_supplement"
			got := make([]float64, len(supplementValueColumns))
			ptrs := make([]any, len(supplementValueColumns))
			for i := range got {
				ptrs[i] = &got[i]
			}
			Expect(db.QueryRowContext(ctx, selectSQL).Scan(ptrs...)).To(Succeed())
			for i, col := range supplementValueColumns {
				Expect(got[i]).To(Equal(supplementSentinels[i]),
					"column %s round-trip — column order?", col)
			}
		})
	})

	Describe("the daily_supplement chain", func() {
		// Mirrors the monthly round-trip against daily_supplement: the
		// same 31 value columns, keyed by (cell_id, date) instead of
		// (cell_id, period, month).
		It("round-trips every value column keyed by (cell, date)", func() {
			db := openTestDB()
			tx, err := db.BeginTx(ctx, nil)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = tx.Rollback() })

			_, err = tx.ExecContext(ctx,
				`INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution)
				 VALUES (?, ?, ?, ?)`,
				"20.0N_100.0W", 20.0, -100.0, "0.5x0.625")
			Expect(err).NotTo(HaveOccurred())

			placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(supplementValueColumns)), ", ")
			insert := "INSERT INTO daily_supplement (cell_id, date, " +
				strings.Join(supplementValueColumns, ", ") + ") VALUES (?, ?, " + placeholders + ")"
			args := []any{"20.0N_100.0W", "2020-01-15"}
			for _, v := range supplementSentinels {
				args = append(args, v)
			}
			_, err = tx.ExecContext(ctx, insert, args...)
			Expect(err).NotTo(HaveOccurred())
			Expect(tx.Commit()).To(Succeed())

			selectSQL := "SELECT " + strings.Join(supplementValueColumns, ", ") +
				" FROM daily_supplement WHERE cell_id='20.0N_100.0W' AND date='2020-01-15'"
			got := make([]float64, len(supplementValueColumns))
			ptrs := make([]any, len(supplementValueColumns))
			for i := range got {
				ptrs[i] = &got[i]
			}
			Expect(db.QueryRowContext(ctx, selectSQL).Scan(ptrs...)).To(Succeed())
			for i, col := range supplementValueColumns {
				Expect(got[i]).To(Equal(supplementSentinels[i]),
					"daily column %s round-trip — column order?", col)
			}
		})
	})

	Describe("CHECK constraints", func() {
		// period and month CHECKs reject out-of-range values;
		// grid_resolution rejects unknown resolution tokens. These are
		// the schema's correctness guards in lieu of FK enforcement
		// (PRAGMA foreign_keys=OFF — see schema.Open).
		It("rejects out-of-range period, month, and grid resolution", func() {
			db := openTestDB()
			_, err := db.ExecContext(ctx,
				`INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution)
				 VALUES ('c', 0, 0, '0.5x0.625')`)
			Expect(err).NotTo(HaveOccurred())

			for name, stmt := range map[string]string{
				"bad period":          `INSERT INTO monthly_supplement (cell_id, period, month) VALUES ('c', '1951-1980', 1)`,
				"month=0":             `INSERT INTO monthly_supplement (cell_id, period, month) VALUES ('c', '1991-2020', 0)`,
				"month=13":            `INSERT INTO monthly_supplement (cell_id, period, month) VALUES ('c', '1991-2020', 13)`,
				"bad grid_resolution": `INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution) VALUES ('d', 0, 0, '1.0x1.0')`,
			} {
				_, err := db.ExecContext(ctx, stmt)
				Expect(err).To(HaveOccurred(), "case %s must violate a CHECK", name)
				Expect(err.Error()).To(SatisfyAny(
					ContainSubstring("constraint"), ContainSubstring("CHECK")),
					"case %s: error %q doesn't look like a CHECK violation", name, err)
			}
		})

		It("rejects an unknown power_runs status", func() {
			db := openTestDB()
			_, err := db.ExecContext(ctx, `
INSERT INTO power_runs (
    started_at, status, endpoint_url, parameters, community,
    period_start_year, period_end_year, grid_resolution, solar_conversion
) VALUES ('t', 'banana', 'u', 'p', 'c', 1991, 2020, '0.5x0.625', 1.0)`)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(SatisfyAny(
				ContainSubstring("constraint"), ContainSubstring("CHECK")))
		})
	})

	Describe("the power_runs manifest", func() {
		// Every column of the reproducibility manifest round-trips. A
		// column-order regression here would silently break any future
		// publish JSON whose `power_query` block is built from these
		// fields.
		It("round-trips all 19 columns and the supplement back-pointer", func() {
			db := openTestDB()

			const conversions = `{"ALLSKY_SFC_SW_DWN":{"power_unit":"MJ/m^2/day","stored_unit":"W/m^2","factor":11.574}}`
			res, err := db.ExecContext(ctx, `
INSERT INTO power_runs (
    started_at, finished_at, status,
    endpoint_url, parameters, community,
    period_start_year, period_end_year, grid_resolution, solar_conversion,
    unit_conversions, temporal_mode, period_start_date, period_end_date,
    cells_attempted, cells_succeeded, cells_failed,
    supplement_rows, etl_git_sha
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				"2026-04-25T12:00:00Z", "2026-04-25T12:40:00Z", "complete",
				"https://power.larc.nasa.gov/api/temporal/daily/point",
				"RH2M,T2MDEW,WS2M,WS10M,ALLSKY_SFC_SW_DWN,T2M_MAX,T2M_MIN",
				"AG", 1981, 2025, "0.5x0.625", 11.574,
				conversions, "daily", "1981-01-01", "2025-12-31",
				2400, 2398, 2, 28776, "abc123",
			)
			Expect(err).NotTo(HaveOccurred())
			runID, err := res.LastInsertId()
			Expect(err).NotTo(HaveOccurred())

			Expect(selectPowerRun(db, runID)).To(Equal(powerRunRow{
				StartedAt:       "2026-04-25T12:00:00Z",
				FinishedAt:      validStr("2026-04-25T12:40:00Z"),
				Status:          "complete",
				Endpoint:        "https://power.larc.nasa.gov/api/temporal/daily/point",
				Parameters:      "RH2M,T2MDEW,WS2M,WS10M,ALLSKY_SFC_SW_DWN,T2M_MAX,T2M_MIN",
				Community:       "AG",
				StartYear:       1981,
				EndYear:         2025,
				GridResolution:  "0.5x0.625",
				SolarConversion: 11.574,
				UnitConversions: validStr(conversions),
				TemporalMode:    "daily",
				StartDate:       validStr("1981-01-01"),
				EndDate:         validStr("2025-12-31"),
				Attempted:       validInt64(2400),
				Succeeded:       validInt64(2398),
				Failed:          validInt64(2),
				SupplementRows:  validInt64(28776),
				ETLGitSHA:       validStr("abc123"),
			}), "power_runs round-trip mismatched — column order?")

			// And exercise the supplement → power_runs link end-to-end.
			_, err = db.ExecContext(ctx,
				`INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution)
				 VALUES ('c', 0, 0, '0.5x0.625')`)
			Expect(err).NotTo(HaveOccurred())
			_, err = db.ExecContext(ctx, `
INSERT INTO monthly_supplement (cell_id, period, month, rh2m_pct, power_run_id)
VALUES ('c', '1991-2020', 1, 65.0, ?)`, runID)
			Expect(err).NotTo(HaveOccurred())

			var (
				linkedRun int64
				rh        float64
			)
			Expect(db.QueryRowContext(ctx,
				`SELECT power_run_id, rh2m_pct FROM monthly_supplement WHERE cell_id='c'`,
			).Scan(&linkedRun, &rh)).To(Succeed())
			Expect(linkedRun).To(Equal(runID))
			Expect(rh).To(Equal(65.0))
		})
	})

	Describe("idempotent schema reopen", func() {
		// schema.Open re-applies the DDL on every construction; a
		// reopen of a populated DB must keep existing rows — and their
		// values — intact.
		It("keeps rows and values across a close and reopen", func() {
			path := filepath.Join(GinkgoT().TempDir(), "bioclima.db")

			db, err := schema.Open(path)
			Expect(err).NotTo(HaveOccurred())
			_, err = db.ExecContext(ctx,
				`INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution)
				 VALUES ('keep', 1.0, 2.0, '0.5x0.625')`)
			Expect(err).NotTo(HaveOccurred())
			Expect(db.Close()).To(Succeed())

			db2, err := schema.Open(path)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = db2.Close() })

			var (
				cellID   string
				lat, lon float64
				res      string
			)
			Expect(db2.QueryRowContext(ctx,
				`SELECT cell_id, lat, lon, grid_resolution FROM nasa_power_grid_cells WHERE cell_id='keep'`,
			).Scan(&cellID, &lat, &lon, &res)).To(Succeed())
			Expect(cellID).To(Equal("keep"))
			Expect(lat).To(Equal(1.0))
			Expect(lon).To(Equal(2.0))
			Expect(res).To(Equal("0.5x0.625"))
		})
	})
})
