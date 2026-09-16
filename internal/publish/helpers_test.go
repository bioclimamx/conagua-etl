package publish_test

import (
	"context"
	"database/sql"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

func f64(v float64) *float64 { return &v }

// openTempDB creates a fresh schema DB under the spec's temp dir through
// the one writer entry point and schedules its close.
func openTempDB() *sql.DB {
	GinkgoHelper()
	db, err := schema.Open(filepath.Join(GinkgoT().TempDir(), "publish.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = db.Close() })
	return db
}

func mustExec(db *sql.DB, query string, args ...any) {
	GinkgoHelper()
	_, err := db.ExecContext(context.Background(), query, args...)
	Expect(err).NotTo(HaveOccurred(), query)
}

// upsertStation seeds one stations row through the ingest writer (the
// only production path that creates stations) and returns its surrogate.
func upsertStation(db *sql.DB, s ingest.StationUpsert) int64 {
	GinkgoHelper()
	tx, err := db.BeginTx(context.Background(), nil)
	Expect(err).NotTo(HaveOccurred())
	id, err := ingest.UpsertStation(context.Background(), tx, s)
	Expect(err).NotTo(HaveOccurred())
	Expect(tx.Commit()).To(Succeed())
	return id
}

func insertNormals(db *sql.DB, stationID int64, period string, month int, values ...any) {
	GinkgoHelper()
	Expect(values).To(HaveLen(5))
	mustExec(db, `INSERT INTO monthly_normals
	  (station_id, period, month, tmax, tmin, tmean, precip, evap)
	  VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		append([]any{stationID, period, month}, values...)...)
}

func insertExtras(db *sql.DB, stationID int64, period string, month int, values ...any) {
	GinkgoHelper()
	Expect(values).To(HaveLen(19))
	mustExec(db, `INSERT INTO monthly_normals_extras
	  (station_id, period, month,
	   tmax_monthly_extreme, tmax_monthly_extreme_year, tmax_daily_extreme, tmax_daily_extreme_date,
	   tmin_monthly_extreme, tmin_monthly_extreme_year, tmin_daily_extreme, tmin_daily_extreme_date,
	   precip_monthly_extreme, precip_monthly_extreme_year, precip_daily_extreme, precip_daily_extreme_date,
	   tmax_years_with_data, tmin_years_with_data, tmean_years_with_data, precip_years_with_data,
	   evap_years_with_data, rain_days, rain_days_years_with_data)
	  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		append([]any{stationID, period, month}, values...)...)
}

func insertDaily(db *sql.DB, stationID int64, date string, values ...any) {
	GinkgoHelper()
	Expect(values).To(HaveLen(4))
	mustExec(db, `INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
	  VALUES (?, ?, ?, ?, ?, ?)`,
		append([]any{stationID, date}, values...)...)
}

// seedTwoStates populates a DB with three Yucatán stations (every
// stations column exercised, NULL variants included; ids chosen so
// bytewise and numeric order differ), one Aguascalientes station, and
// one EMA-source station in Yucatán that no CONAGUA-conventional export
// may pick up. Rows are inserted out of key order so the exports prove
// their sort. Returns the surrogate ids by "<source>/<external_id>".
func seedTwoStates(db *sql.DB) map[string]int64 {
	GinkgoHelper()
	conv := ingest.SourceConaguaConventional
	ids := map[string]int64{}

	ids["conv/31001"] = upsertStation(db, ingest.StationUpsert{
		Source: conv, ExternalID: "31001", Name: `Mérida, "La Plancha"`, State: "YUC",
		Municipality: "Mérida", Lat: f64(21.85027778), Lon: f64(-89.375), AltitudeM: f64(9),
		Status: "operating",
	})
	mustExec(db, `UPDATE stations SET first_year = 1951, last_year = 2026,
	  wmo_completeness_bin_1961_1990 = ?, wmo_completeness_bin_1971_2000 = 0.5,
	  wmo_completeness_bin_1981_2010 = NULL, wmo_completeness_bin_1991_2020 = 1,
	  wmo_completeness_cont_1961_1990 = 0.9324074074, wmo_completeness_cont_1971_2000 = 0,
	  wmo_completeness_cont_1981_2010 = NULL, wmo_completeness_cont_1991_2020 = ?
	  WHERE id = ?`, 35.0/36.0, 1.0/1080.0, ids["conv/31001"])

	ids["conv/31002"] = upsertStation(db, ingest.StationUpsert{
		Source: conv, ExternalID: "31002", Name: "Tizimín", State: "YUC",
	})

	ids["conv/3101"] = upsertStation(db, ingest.StationUpsert{
		Source: conv, ExternalID: "3101", Name: "Valladolid", State: "YUC",
		Municipality: "Valladolid", Lat: f64(20.6891), Lon: f64(-88.2011), AltitudeM: f64(25.5),
		Status: "suspended",
	})
	mustExec(db, `UPDATE stations SET first_year = 1961, last_year = 1995,
	  wmo_completeness_bin_1961_1990 = 0.25 WHERE id = ?`, ids["conv/3101"])

	ids["conv/1001"] = upsertStation(db, ingest.StationUpsert{
		Source: conv, ExternalID: "1001", Name: "Aguascalientes (OBS)", State: "AGS",
		Lat: f64(21.88), Lon: f64(-102.3), AltitudeM: f64(1878), Status: "operating",
	})

	ids["ema/31001"] = upsertStation(db, ingest.StationUpsert{
		Source: ingest.SourceConaguaEMA, ExternalID: "31001", Name: "EMA Mérida", State: "YUC",
	})

	// Normals — 31001 carries two periods (one month all-NULL), 3101 one
	// row of a third period; the AGS and EMA rows must never surface.
	insertNormals(db, ids["conv/31001"], "1991-2020", 12, nil, nil, nil, nil, nil)
	insertNormals(db, ids["conv/31001"], "1981-2010", 12, 30.1, 16.2, nil, 24.475, 110.3)
	insertNormals(db, ids["conv/31001"], "1981-2010", 1, 33.4, 17.9, 25.7, 28.3, 141.6)
	insertNormals(db, ids["conv/31001"], "1991-2020", 1, 33.0, 18.3, 25.8, 0.0, 150.25)
	insertNormals(db, ids["conv/3101"], "1961-1990", 6, 35.9, 22.1, 29.0, 152.7, 190.4)
	insertNormals(db, ids["conv/1001"], "1981-2010", 1, 29.9, 9.9, 19.9, 20.0, 200.0)
	insertNormals(db, ids["ema/31001"], "1981-2010", 1, 1.0, 1.0, 1.0, 1.0, 1.0)

	// Extras — every column populated once, a mixed-NULL row, an all-NULL
	// row inserted ahead of its month-1 sibling, and a third station.
	insertExtras(db, ids["conv/31001"], "1981-2010", 12,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	insertExtras(db, ids["conv/31001"], "1981-2010", 1,
		38.5, 1998, 42.0, "1998-05-14",
		8.25, 1985, 3.5, "1985-01-20",
		210.6, 2002, 98.7, "2002-09-23",
		28, 27, 27, 30, 25, 4.6, 29)
	insertExtras(db, ids["conv/31001"], "1991-2020", 1,
		39.0, nil, nil, nil,
		nil, nil, nil, nil,
		190.2, 2012, nil, nil,
		30, nil, nil, 30, nil, nil, nil)
	insertExtras(db, ids["conv/3101"], "1961-1990", 6,
		40.2, 1975, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil, nil, nil, 12.25, 30)
	insertExtras(db, ids["conv/1001"], "1981-2010", 1,
		1.0, 1, 1.0, "1981-01-01", 1.0, 1, 1.0, "1981-01-01", 1.0, 1, 1.0, "1981-01-01",
		1, 1, 1, 1, 1, 1.0, 1)

	// Daily — 31001 three dates inserted out of order with NULL cells and
	// a sub-precision negative; 31002 none; 3101 one; AGS and EMA rows
	// that must never surface.
	insertDaily(db, ids["conv/31001"], "2020-01-02", 31.0, 19.5, 0.0, 4.2)
	insertDaily(db, ids["conv/31001"], "2020-01-01", 30.5, nil, 12.4, nil)
	insertDaily(db, ids["conv/31001"], "1999-12-31", nil, -0.04, nil, nil)
	insertDaily(db, ids["conv/3101"], "1975-06-15", 36.2, 22.0, 45.1, 7.3)
	insertDaily(db, ids["conv/1001"], "2020-01-01", 20.0, 5.0, 0.0, 6.0)
	insertDaily(db, ids["ema/31001"], "2020-01-01", 1.0, 1.0, 1.0, 1.0)

	return ids
}
