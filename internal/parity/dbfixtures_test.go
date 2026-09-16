package parity_test

// Synthetic ingest-DB fixtures for the DB comparator specs. Both sides
// are created through schema.Open — the real DDL, the real driver — and
// seeded through plain SQL with explicit surrogate ids (this package
// owns no writer), so every spec exercises the exact SELECT paths the
// production gate runs. The two sides always get *different* surrogate
// station ids (baseStationID vs newStationID): natural-key alignment,
// never id equality, is the core contract under test.

import (
	"context"
	"database/sql"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

const (
	seedSource = "conagua_conventional"
	seedExtID  = "1001"

	// Divergent by design: two ingests of the same snapshot assign
	// surrogate ids independently.
	baseStationID = 1
	newStationID  = 7
)

// dbKey builds the expected DBKey for the baseline station.
func dbKey(row string) parity.DBKey {
	return parity.DBKey{Source: seedSource, ExternalID: seedExtID, Row: row}
}

// openIngestDB creates a fresh database in temp space through the real
// writer opener.
func openIngestDB() *sql.DB {
	GinkgoHelper()
	db, err := schema.Open(filepath.Join(GinkgoT().TempDir(), "ingest.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(db.Close()).To(Succeed()) })
	return db
}

func execSQL(db *sql.DB, query string, args ...any) {
	GinkgoHelper()
	_, err := db.Exec(query, args...)
	Expect(err).NotTo(HaveOccurred())
}

// seedStation inserts the baseline station under the given surrogate id
// with every compared column populated distinctly — except municipality,
// deliberately NULL on both sides to pin NULL==NULL as identical.
func seedStation(db *sql.DB, stationID int64) {
	GinkgoHelper()
	execSQL(db, `INSERT INTO stations
		(id, source, external_id, name, state, municipality,
		 lat, lon, altitude_m, status, first_year, last_year,
		 wmo_completeness_bin_1961_1990, wmo_completeness_bin_1971_2000,
		 wmo_completeness_bin_1981_2010, wmo_completeness_bin_1991_2020,
		 wmo_completeness_cont_1961_1990, wmo_completeness_cont_1971_2000,
		 wmo_completeness_cont_1981_2010, wmo_completeness_cont_1991_2020)
		VALUES (?, ?, ?, 'AGUASCALIENTES (OBS)', 'AGS', NULL,
		        21.85027778, -102.2908333, 1890.8, 'operating', 1947, 2016,
		        0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8)`,
		stationID, seedSource, seedExtID)
}

// seedRun inserts one fully-populated ingest_runs row.
func seedRun(db *sql.DB, runID int64) {
	GinkgoHelper()
	execSQL(db, `INSERT INTO ingest_runs
		(id, started_at, finished_at, snapshot_date, sink_kind, etl_git_sha, status,
		 stations_attempted, stations_succeeded, stations_failed,
		 daily_rows, normals_rows, extras_rows, warnings_total)
		VALUES (?, '2026-07-18T10:00:00Z', '2026-07-18T10:05:00Z', '2026-06-08',
		        'local', 'abc1234', 'complete', 3, 2, 1, 100, 24, 24, 3)`, runID)
}

// seededRunCounters is the RunCounters value seedRun round-trips to.
func seededRunCounters(runID int64) *parity.RunCounters {
	valid := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	validInt := func(n int64) sql.NullInt64 { return sql.NullInt64{Int64: n, Valid: true} }
	return &parity.RunCounters{
		ID:                runID,
		StartedAt:         "2026-07-18T10:00:00Z",
		FinishedAt:        valid("2026-07-18T10:05:00Z"),
		SnapshotDate:      "2026-06-08",
		SinkKind:          "local",
		ETLGitSHA:         valid("abc1234"),
		Status:            "complete",
		StationsAttempted: validInt(3),
		StationsSucceeded: validInt(2),
		StationsFailed:    validInt(1),
		DailyRows:         validInt(100),
		NormalsRows:       validInt(24),
		ExtrasRows:        validInt(24),
		WarningsTotal:     validInt(3),
	}
}

// seedBaseline populates one side with the shared synthetic ingest:
// the station, two normals rows, one extras row (all 19 value columns),
// three daily rows (mixing values and NULLs), three warning rows (one
// tuple twice, plus a NULL-station tuple), and one run row.
func seedBaseline(db *sql.DB, stationID int64) {
	GinkgoHelper()
	seedStation(db, stationID)

	execSQL(db, `INSERT INTO monthly_normals (station_id, period, month, tmax, tmin, tmean, precip, evap)
		VALUES (?, '1961-1990', 1, 25.5, 10.1, 17.8, 14.2, 6.4)`, stationID)
	execSQL(db, `INSERT INTO monthly_normals (station_id, period, month, tmax, tmin, tmean, precip, evap)
		VALUES (?, '1991-2020', 12, NULL, 9.9, NULL, 0, 5.5)`, stationID)

	execSQL(db, `INSERT INTO monthly_normals_extras
		(station_id, period, month,
		 tmax_monthly_extreme, tmax_monthly_extreme_year,
		 tmax_daily_extreme, tmax_daily_extreme_date,
		 tmin_monthly_extreme, tmin_monthly_extreme_year,
		 tmin_daily_extreme, tmin_daily_extreme_date,
		 precip_monthly_extreme, precip_monthly_extreme_year,
		 precip_daily_extreme, precip_daily_extreme_date,
		 tmax_years_with_data, tmin_years_with_data, tmean_years_with_data,
		 precip_years_with_data, evap_years_with_data,
		 rain_days, rain_days_years_with_data)
		VALUES (?, '1961-1990', 1,
		        34.5, 1969, 36.0, '1969-01-21',
		        -2.0, 1971, -5.5, '1971-01-05',
		        88.1, 1992, 40.2, '1992-01-16',
		        28, 27, 26, 30, 25, 3.2, 29)`, stationID)

	execSQL(db, `INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
		VALUES (?, '1985-01-01', 20.2, 9.8, 0, NULL)`, stationID)
	execSQL(db, `INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
		VALUES (?, '1985-01-02', 21, 10.5, 1.5, 4.1)`, stationID)
	execSQL(db, `INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
		VALUES (?, '1985-01-03', NULL, NULL, NULL, NULL)`, stationID)

	for range 2 {
		execSQL(db, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
			VALUES (?, 'daily/1001.txt', 57, 'warn', 'unparseable TMAX')`, stationID)
	}
	execSQL(db, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
		VALUES (NULL, NULL, NULL, 'error', 'orphan warning')`)

	seedRun(db, 1)
}

// seedPair builds the two baseline databases with divergent surrogate
// ids and returns (base, new).
func seedPair() (baseDB, newDB *sql.DB) {
	GinkgoHelper()
	baseDB = openIngestDB()
	newDB = openIngestDB()
	seedBaseline(baseDB, baseStationID)
	seedBaseline(newDB, newStationID)
	return baseDB, newDB
}

// mustCompare runs CompareDBs and asserts the infrastructure-level
// outcome before returning the comparison.
func mustCompare(ctx context.Context, baseDB, newDB *sql.DB) *parity.DBComparison {
	GinkgoHelper()
	cmp, err := parity.CompareDBs(ctx, baseDB, newDB)
	Expect(err).NotTo(HaveOccurred())
	return cmp
}
