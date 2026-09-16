package cmd_test

// The publish e2e story: the real compiled binary builds the per-state
// deposit — {state}-tabular.zip holding the four curated CSV scope
// folders (conagua/, nasa_power/, combined/, provenance/), the ten
// per-table Parquet twins, and the filtered {state}.db, and
// {state}-json.zip holding the per-station profile.json + daily.json
// pairs and provenance/ as JSON, plus manifest.json and CHECKSUMS — from
// a seeded SQLite DB into a fresh --out directory. The specs prove the
// cmd→publish→archive path end to end, exactly as an operator invokes
// it: flag wiring (--state, --only), the read-only DB posture (the
// file's bytes are unchanged), the per-unit and per-archive progress
// log, the stdout report, the exit-code contract (0 clean / 2 degraded
// / 1 run-level failure), and the byte-reproducibility claim on the
// real binary.
//
// Every exported value is round-tripped: each CSV inside the archive is
// compared whole — header row, every column of every row, PK order,
// NULL as the empty field — and each JSON document is decoded with its
// numbers as literal text and compared whole — every block, every
// field, every slot — against expectations written by hand from the
// seed and the per-file export spec (pinned decimals, nulls and sort,
// column names, the combined LEFT join, natural run labels, the derived
// annual slot and its null policy, the daily summary, archive paths),
// never against the publish package's own
// constants, so a drift in the package's spec tables cannot pass here.
// Each Parquet twin is decoded with the library and held, every column
// of every row at the column's pinned decimals, to the CSV entries of
// its table (one value across the flat formats); the {state}.db is
// extracted, opened read-only, and held table by table to the state's
// row set of the seeded source. manifest.json is decoded and compared field
// by field against the seeded provenance; CHECKSUMS is parsed and every
// digest recomputed from the bytes on disk.

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/parquet-go/parquet-go"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// publishSnapshotDate is the snapshot the seeded DB's one complete
// ingest run was built from; every archive entry is stamped with it at
// midnight UTC, and the report and manifest both name it.
const publishSnapshotDate = "2026-06-08"

// publishConversions is the stored unit_conversions text of the seeded
// power runs, in the compact registry-order shape power writes; the
// manifest must embed it and the provenance CSV must carry it verbatim.
const publishConversions = `{"T2M":{"power_unit":"C","stored_unit":"C","factor":1},` +
	`"ALLSKY_SFC_SW_DWN":{"power_unit":"MJ/m^2/day","stored_unit":"W/m^2","factor":11.574074074074074}}`

// The two seeded grid cells: one shared by two Yucatán stations, one
// only the Aguascalientes station references — it must never reach
// yuc-tabular.zip.
const (
	publishCellYUC = "n20.75_w89.375"
	publishCellAGS = "n21.75_w102.5"
)

func strp(s string) *string { return &s }
func i64p(n int64) *int64   { return &n }

// wantPower31 is the POWER-31 value block in its published order,
// written by hand. It is both the seed's INSERT column list and the tail
// of every POWER-bearing header, so an order slip in the package's
// registry-derived specs fails against it.
var wantPower31 = []string{
	"t2m_c", "t2m_max_c", "t2m_min_c", "t2m_wet_c", "t2m_dew_c", "ts_c", "ts_max_c", "ts_min_c",
	"rh2m_pct", "qv2m_gkg",
	"ws2m_ms", "ws10m_ms", "ws50m_ms", "wd2m_deg", "wd10m_deg",
	"solar_ghi_wm2", "solar_dhi_wm2", "solar_dni_wm2", "solar_clrsky_wm2", "clearness_index",
	"par_wm2", "uva_wm2", "uvb_wm2",
	"lw_dwn_wm2",
	"cloud_amt_pct", "ps_kpa",
	"precip_mmpd", "evland_mmpd",
	"gwet_top", "gwet_root", "gwet_prof",
}

// The seeded POWER-31 rows and their pinned renderings (two decimals, the
// two wind directions at one). The full row carries a distinct value
// per column — a column-order slip cannot pass — with the solar
// conversion tail, an exact half-to-even tie, a sub-precision negative,
// and integral values padded; the partial row is a per-variable gap
// inside an existing row, distinct from a missing row; the empty block
// is POWER-31 where the LEFT join matched nothing.
var (
	publishPowerFull = []any{
		24.475, 31.0, -0.004, 19.125, 15.5, 26.789, 40.0, -3.35,
		97.26, 12.3,
		2.0, 3.456, 5.0, 300.3, 180.25,
		228.472222222222, 80.555555555556, 150.0, 260.115740740741, 0.69,
		100.1, 10.0, 0.25,
		400.0,
		55.5, 101.3,
		3.7, 2.22,
		0.5, 0.72, 1.0,
	}
	publishPowerFullText = []string{
		"24.48", "31.00", "0.00", "19.12", "15.50", "26.79", "40.00", "-3.35",
		"97.26", "12.30",
		"2.00", "3.46", "5.00", "300.3", "180.2",
		"228.47", "80.56", "150.00", "260.12", "0.69",
		"100.10", "10.00", "0.25",
		"400.00",
		"55.50", "101.30",
		"3.70", "2.22",
		"0.50", "0.72", "1.00",
	}
	publishPowerPartial = []any{
		20.0, nil, nil, nil, nil, nil, nil, nil,
		nil, nil,
		nil, nil, nil, 5.06, nil,
		nil, nil, nil, nil, nil,
		nil, nil, nil,
		nil,
		nil, nil,
		0.0, nil,
		nil, nil, nil,
	}
	publishPowerPartialText = []string{
		"20.00", "", "", "", "", "", "", "",
		"", "",
		"", "", "", "5.1", "",
		"", "", "", "", "",
		"", "", "",
		"",
		"", "",
		"0.00", "",
		"", "", "",
	}
	publishPowerEmpty = make([]string, 31)
)

// publishPowerSeq is a POWER-31 row of base+0.5, base+1.5, …, base+30.5
// — distinct per row so a join that lands the wrong row is caught
// column by column, and exactly representable so the rendering is
// unambiguous; publishPowerSeqText is that rendering, the wind
// directions (positions 13 and 14) at one decimal. A base stays under
// 346 so the two wind directions stay inside the [0, 360] the gate
// holds them to.
func publishPowerSeq(base float64) []any {
	out := make([]any, 31)
	for i := range out {
		out[i] = base + float64(i) + 0.5
	}
	return out
}

func publishPowerSeqText(base float64) []string {
	out := make([]string, 31)
	for i := range out {
		v := base + float64(i) + 0.5
		if i == 13 || i == 14 {
			out[i] = fmt.Sprintf("%.1f", v)
		} else {
			out[i] = fmt.Sprintf("%.2f", v)
		}
	}
	return out
}

// withPower concatenates a row's leading cells with its POWER-31 cells.
func withPower(cells []string, power []string) []string {
	return append(slices.Clone(cells), power...)
}

// meridaNormals1971 is 31001's complete 1971-2000 period — (tmax, tmin,
// tmean, precip, evap) per month 1..12 — the one period of the seed
// with all twelve months, so the profile carries a derived annual for it:
// the unweighted means 34.9 / 20.6 / 27.7 for the temperatures and the
// sums 910.6 / 1910.0 for precip and evap (their means, 75.9 / 159.2,
// are what the annual must never be).
var meridaNormals1971 = [12][5]float64{
	{32.8, 17.5, 25.2, 30.1, 140.0},
	{34.2, 18.0, 26.1, 20.0, 150.0},
	{36.0, 19.5, 27.8, 15.5, 180.0},
	{37.5, 21.0, 29.3, 25.0, 200.0},
	{38.0, 22.5, 30.2, 80.0, 210.0},
	{36.2, 23.0, 29.6, 150.0, 170.0},
	{35.8, 22.6, 29.2, 120.0, 175.0},
	{35.9, 22.4, 29.1, 130.0, 170.0},
	{34.6, 22.3, 28.4, 180.0, 140.0},
	{33.0, 21.0, 27.0, 90.0, 130.0},
	{32.5, 19.2, 25.9, 40.0, 120.0},
	{32.0, 18.0, 25.0, 30.0, 125.0},
}

// meridaNormals1971Rows renders the complete period's twelve rows as
// the normals CSV carries them, one decimal, in month order.
func meridaNormals1971Rows() [][]string {
	rows := make([][]string, 0, 12)
	for m, v := range meridaNormals1971 {
		rows = append(rows, []string{"31001", "1971-2000", strconv.Itoa(m + 1),
			fmt.Sprintf("%.1f", v[0]), fmt.Sprintf("%.1f", v[1]), fmt.Sprintf("%.1f", v[2]),
			fmt.Sprintf("%.1f", v[3]), fmt.Sprintf("%.1f", v[4])})
	}
	return rows
}

// meridaCombined1971Rows is the same period through the combined_monthly
// LEFT join: the join context present, POWER-31 empty (POWER monthly
// was never pulled for 1971-2000).
func meridaCombined1971Rows() [][]string {
	rows := make([][]string, 0, 12)
	for _, r := range meridaNormals1971Rows() {
		rows = append(rows, withPower(append([]string{r[0], r[1], r[2], publishCellYUC, "33.100"}, r[3:]...), publishPowerEmpty))
	}
	return rows
}

// publishPowerRun is one seeded power_runs row; nil pointers seed NULL.
type publishPowerRun struct {
	startedAt, status, endpoint, parameters, temporalMode string
	finishedAt, startDate, endDate, conversions, gitSHA   *string
	startYear, endYear                                    int64
	counters                                              [4]int64
}

// seedPublishDB creates a fresh schema DB through the one writer entry
// point and returns its path. The population, chosen so every exported
// column of every folder is exercised and the sort and the combined join are
// proven:
//
//   - YUC: 31001 (every stations column populated, all 8 WMO
//     fractions, coordinates and altitude that round), 31002 (NULL
//     municipality and altitude, a sparse WMO row, no cell), 3101 (a
//     name that needs RFC-4180 quoting; ids chosen so bytewise and
//     numeric order differ), 31019 (catalog-only: every nullable column
//     NULL, no rows anywhere).
//   - AGS: 1001 (fully populated), 1002 (NULL municipality/altitude),
//     1010 (catalog-only) — the other state, absent from yuc-tabular.zip.
//   - Normals for 31001 across three periods and for 3101 in the fourth
//     — every period reaches combined_monthly — one row with every
//     column populated, one all-NULL, one mixed, and 1971-2000 complete
//     (all twelve months, so the profile's annual is derivable while
//     the two-month periods' annual stays null); plus one row for 1001.
//   - Daily rows for 31001 inserted out of date order, with NULL cells,
//     values whose second decimal is load-bearing (0.06 mm of rain is a
//     0.1 mm day only if the export throws the hundredths away), a
//     negative below the tenths, and a pre-1981 date; one row for the
//     cell-less 31002, and 1001's row carrying CONAGUA's 0.01 mm trace
//     ("inappreciable") rain — the value a one-decimal export published
//     as a dry day.
//   - Cells: 31001 and 3101 share one, and 1002 — the other state —
//     references it too, so its per-cell file ships in both state
//     archives and once nationally; 1001 has the other; 31002, 31019,
//     and 1010 have none.
//   - POWER monthly for the two periods POWER was pulled for only, with
//     a (1991-2020, 12) gap; POWER daily for a subset of the observed
//     post-1981 dates plus one date 31001 never observed.
//   - One complete ingest run for the snapshot, shadowed by a newer
//     aborted run that must not be chosen; four power runs, three
//     referenced by supplement rows and one aborted attempt no row
//     references — it shares a label with a referenced run and must
//     appear in no provenance file; one parsing warning, so every table
//     count is non-trivial.
func seedPublishDB() string {
	dir, err := os.MkdirTemp("", "publish-e2e-*")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	DeferCleanup(func() error { return os.RemoveAll(dir) })

	dbPath := filepath.Join(dir, "bioclima.db")
	db, err := schema.Open(dbPath)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	ctx := context.Background()

	exec := func(query string, args ...any) {
		_, err := db.ExecContext(ctx, query, args...)
		ExpectWithOffset(2, err).NotTo(HaveOccurred(), query)
	}
	upsert := func(s ingest.StationUpsert) int64 {
		s.Source = ingest.SourceConaguaConventional
		tx, err := db.BeginTx(ctx, nil)
		ExpectWithOffset(2, err).NotTo(HaveOccurred())
		id, err := ingest.UpsertStation(ctx, tx, s)
		ExpectWithOffset(2, err).NotTo(HaveOccurred(), s.ExternalID)
		ExpectWithOffset(2, tx.Commit()).To(Succeed())
		return id
	}
	normals := func(id int64, period string, month int, v ...any) {
		ExpectWithOffset(2, v).To(HaveLen(5))
		exec(`INSERT INTO monthly_normals (station_id, period, month, tmax, tmin, tmean, precip, evap)
		  VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, append([]any{id, period, month}, v...)...)
	}
	extras := func(id int64, period string, month int, v ...any) {
		ExpectWithOffset(2, v).To(HaveLen(19))
		exec(`INSERT INTO monthly_normals_extras (station_id, period, month,
		   tmax_monthly_extreme, tmax_monthly_extreme_year, tmax_daily_extreme, tmax_daily_extreme_date,
		   tmin_monthly_extreme, tmin_monthly_extreme_year, tmin_daily_extreme, tmin_daily_extreme_date,
		   precip_monthly_extreme, precip_monthly_extreme_year, precip_daily_extreme, precip_daily_extreme_date,
		   tmax_years_with_data, tmin_years_with_data, tmean_years_with_data, precip_years_with_data,
		   evap_years_with_data, rain_days, rain_days_years_with_data)
		  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			append([]any{id, period, month}, v...)...)
	}
	daily := func(id int64, date string, v ...any) {
		ExpectWithOffset(2, v).To(HaveLen(4))
		exec(`INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
		  VALUES (?, ?, ?, ?, ?, ?)`, append([]any{id, date}, v...)...)
	}
	powerRun := func(r publishPowerRun) int64 {
		var id int64
		ExpectWithOffset(2, db.QueryRowContext(ctx, `INSERT INTO power_runs (started_at, finished_at, status,
		   endpoint_url, parameters, community, period_start_year, period_end_year, grid_resolution,
		   solar_conversion, unit_conversions, temporal_mode, period_start_date, period_end_date,
		   cells_attempted, cells_succeeded, cells_failed, supplement_rows, etl_git_sha)
		  VALUES (?, ?, ?, ?, ?, 'AG', ?, ?, '0.5x0.625', 11.574074074074074, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`,
			r.startedAt, r.finishedAt, r.status, r.endpoint, r.parameters, r.startYear, r.endYear,
			r.conversions, r.temporalMode, r.startDate, r.endDate,
			r.counters[0], r.counters[1], r.counters[2], r.counters[3], r.gitSHA).Scan(&id)).To(Succeed())
		return id
	}
	supplementMonthly := func(cell, period string, month int, run int64, v []any) {
		ExpectWithOffset(2, v).To(HaveLen(31))
		exec(`INSERT INTO monthly_supplement (cell_id, period, month, `+strings.Join(wantPower31, ", ")+`, power_run_id)
		  VALUES (?, ?, ?, `+strings.Repeat("?, ", 31)+`?)`,
			append(append([]any{cell, period, month}, v...), run)...)
	}
	supplementDaily := func(cell, date string, run int64, v []any) {
		ExpectWithOffset(2, v).To(HaveLen(31))
		exec(`INSERT INTO daily_supplement (cell_id, date, `+strings.Join(wantPower31, ", ")+`, power_run_id)
		  VALUES (?, ?, `+strings.Repeat("?, ", 31)+`?)`,
			append(append([]any{cell, date}, v...), run)...)
	}

	// Stations, inserted out of key order.
	valladolid := upsert(ingest.StationUpsert{
		ExternalID: "3101", Name: "VALLADOLID, CENTRO", State: "YUC", Municipality: "Valladolid",
		Lat: f64(20.6891), Lon: f64(-88.2011), AltitudeM: f64(25.5), Status: "operating",
	})
	exec(`UPDATE stations SET first_year = 1961, last_year = 2026 WHERE id = ?`, valladolid)
	upsert(ingest.StationUpsert{ExternalID: "31019", Name: "PROGRESO", State: "YUC"})
	merida := upsert(ingest.StationUpsert{
		ExternalID: "31001", Name: "MERIDA (OBS)", State: "YUC", Municipality: "Mérida",
		Lat: f64(20.98027778), Lon: f64(-89.62138889), AltitudeM: f64(9.96), Status: "operating",
	})
	exec(`UPDATE stations SET first_year = 1951, last_year = 2026,
	   wmo_completeness_bin_1961_1990 = ?, wmo_completeness_bin_1971_2000 = 0.5,
	   wmo_completeness_bin_1981_2010 = 1, wmo_completeness_bin_1991_2020 = 0,
	   wmo_completeness_cont_1961_1990 = ?, wmo_completeness_cont_1971_2000 = 0.9324074074,
	   wmo_completeness_cont_1981_2010 = 0.25, wmo_completeness_cont_1991_2020 = ?
	   WHERE id = ?`, 35.0/36.0, 1.0/1080.0, 1079.0/1080.0, merida)
	tizimin := upsert(ingest.StationUpsert{
		ExternalID: "31002", Name: "TIZIMIN", State: "YUC",
		Lat: f64(21.14), Lon: f64(-88.15), Status: "suspended",
	})
	exec(`UPDATE stations SET first_year = 1961, last_year = 1995,
	   wmo_completeness_bin_1961_1990 = 0.25, wmo_completeness_cont_1961_1990 = 0.1 WHERE id = ?`, tizimin)

	ags := upsert(ingest.StationUpsert{
		ExternalID: "1001", Name: "AGUASCALIENTES (OBS)", State: "AGS", Municipality: "Aguascalientes",
		Lat: f64(21.85027778), Lon: f64(-102.2908333), AltitudeM: f64(1878.4), Status: "operating",
	})
	exec(`UPDATE stations SET first_year = 1948, last_year = 2026,
	   wmo_completeness_bin_1961_1990 = 1, wmo_completeness_bin_1971_2000 = 1,
	   wmo_completeness_bin_1981_2010 = 1, wmo_completeness_bin_1991_2020 = 1,
	   wmo_completeness_cont_1961_1990 = 0.5, wmo_completeness_cont_1971_2000 = 0.5,
	   wmo_completeness_cont_1981_2010 = 0.5, wmo_completeness_cont_1991_2020 = 0.5
	   WHERE id = ?`, ags)
	calvillo := upsert(ingest.StationUpsert{
		ExternalID: "1002", Name: "CALVILLO", State: "AGS",
		Lat: f64(21.85), Lon: f64(-102.72), Status: "suspended",
	})
	exec(`UPDATE stations SET first_year = 1950, last_year = 1990 WHERE id = ?`, calvillo)
	upsert(ingest.StationUpsert{ExternalID: "1010", Name: "SAN JOSE DE GRACIA", State: "AGS"})

	// Normals — out of key order; 24.46 / 16.26 / 30.14 / 150.26 round.
	normals(merida, "1991-2020", 12, nil, nil, nil, nil, nil)
	normals(merida, "1981-2010", 12, 30.14, 16.26, nil, 24.46, 110.3)
	normals(merida, "1981-2010", 1, 33.4, 17.9, 25.7, 28.3, 141.6)
	for m := 12; m >= 1; m-- {
		v := meridaNormals1971[m-1]
		normals(merida, "1971-2000", m, v[0], v[1], v[2], v[3], v[4])
	}
	normals(merida, "1991-2020", 1, 33.0, 18.3, 25.8, 0.0, 150.26)
	normals(valladolid, "1961-1990", 6, 35.9, 22.1, 29.0, 152.7, 190.4)
	normals(ags, "1981-2010", 1, 29.9, 9.9, 19.9, 20.0, 200.0)

	// Extras — the all-NULL row ahead of its month-1 sibling.
	extras(merida, "1981-2010", 12,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	extras(merida, "1981-2010", 1,
		38.5, 1998, 42.04, "1998-05-14",
		8.26, 1985, 3.5, "1985-01-20",
		210.6, 2002, 98.7, "2002-09-23",
		28, 27, 27, 30, 25, 4.6, 29)
	extras(merida, "1991-2020", 1,
		39.0, nil, nil, nil,
		nil, nil, nil, nil,
		190.2, 2012, nil, nil,
		30, nil, nil, 30, nil, 12.26, nil)
	extras(valladolid, "1961-1990", 6,
		40.2, 1975, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil, nil, nil, 3.0, 30)
	extras(ags, "1981-2010", 1,
		1.0, 1, 1.0, "1981-01-01", 1.0, 1, 1.0, "1981-01-01", 1.0, 1, 1.0, "1981-01-01",
		1, 1, 1, 1, 1, 1.0, 1)

	// Daily — out of date order.
	daily(merida, "2020-01-02", 31.0, 19.5, 0.0, 4.2)
	daily(merida, "2020-02-29", 18.94, -2.46, 0.06, 5.56)
	daily(merida, "2020-01-01", 30.5, nil, 12.46, nil)
	daily(merida, "1999-12-31", nil, -0.04, nil, nil)
	daily(merida, "1980-12-31", 28.0, 14.5, 0.0, 3.1)
	daily(tizimin, "1990-06-01", 35.5, 21.0, 0.0, 8.1)
	daily(valladolid, "1975-06-15", 36.2, 22.0, 45.1, 7.3)
	// CONAGUA's trace rain: 0.01 mm is "inappreciable" precipitation, a
	// wet day the source distinguishes from a dry one. It must reach the
	// artifacts as 0.01, never rounded to a dry 0.0.
	daily(ags, "2020-01-01", 20.0, 5.0, 0.01, 6.0)

	exec(`INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
	  VALUES (?, 'daily/31001.txt', 3, 'warn', 'x')`, merida)

	// Provenance.
	exec(`INSERT INTO ingest_runs (started_at, finished_at, snapshot_date, sink_kind, etl_git_sha, status,
	   stations_attempted, stations_succeeded, stations_failed, daily_rows, normals_rows, extras_rows, warnings_total)
	  VALUES ('2026-06-09T01:00:00Z', '2026-06-09T03:00:00Z', ?, 'local', 'f1r5t', 'complete', 7, 7, 0, 8, 7, 5, 1)`,
		publishSnapshotDate)
	exec(`INSERT INTO ingest_runs (started_at, snapshot_date, sink_kind, status)
	  VALUES ('2026-06-11T01:00:00Z', '2026-06-11', 'local', 'aborted')`)
	monthlyRun := powerRun(publishPowerRun{
		startedAt: "2026-07-01T00:00:00Z", finishedAt: strp("2026-07-01T06:00:00Z"), status: "complete",
		endpoint: "https://power.larc.nasa.gov/api/temporal/monthly/point", parameters: "T2M,ALLSKY_SFC_SW_DWN",
		startYear: 1981, endYear: 2010, temporalMode: "monthly", conversions: strp(publishConversions),
		counters: [4]int64{2, 2, 0, 3}, gitSHA: strp("p0w3r"),
	})
	// The aborted first attempt at 1991-2020: no supplement row references
	// it, so it is not provenance, even though its label is a referenced
	// run's.
	powerRun(publishPowerRun{
		startedAt: "2026-07-01T06:30:00Z", finishedAt: strp("2026-07-01T06:31:00Z"), status: "aborted",
		endpoint: "https://power.larc.nasa.gov/api/temporal/monthly/point", parameters: "T2M",
		startYear: 1991, endYear: 2020, temporalMode: "monthly", conversions: strp(`{}`),
		counters: [4]int64{0, 0, 0, 0}, gitSHA: strp("ab0rt"),
	})
	monthlyRun2 := powerRun(publishPowerRun{
		startedAt: "2026-07-01T07:00:00Z", finishedAt: strp("2026-07-01T09:00:00Z"), status: "complete",
		endpoint: "https://power.larc.nasa.gov/api/temporal/monthly/point", parameters: "T2M,ALLSKY_SFC_SW_DWN",
		startYear: 1991, endYear: 2020, temporalMode: "monthly", conversions: strp(publishConversions),
		counters: [4]int64{1, 1, 0, 1}, gitSHA: strp("p0w3r"),
	})
	dailyRun := powerRun(publishPowerRun{
		startedAt: "2026-07-02T00:00:00Z", finishedAt: strp("2026-07-03T12:00:00Z"), status: "complete",
		endpoint: "https://power.larc.nasa.gov/api/temporal/daily/point", parameters: "T2M,ALLSKY_SFC_SW_DWN",
		startYear: 1981, endYear: 2026, temporalMode: "daily",
		startDate: strp("1981-01-01"), endDate: strp("2026-06-08"), conversions: strp(publishConversions),
		counters: [4]int64{2, 2, 0, 4}, gitSHA: nil,
	})

	// Cells and the station → cell map.
	exec(`INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution) VALUES (?, 21.75, -102.5, '0.5x0.625')`,
		publishCellAGS)
	exec(`INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution) VALUES (?, 20.75, -89.375, '0.5x0.625')`,
		publishCellYUC)
	exec(`INSERT INTO station_power_cell (station_id, cell_id, distance_km) VALUES (?, ?, 33.1)`, merida, publishCellYUC)
	exec(`INSERT INTO station_power_cell (station_id, cell_id, distance_km) VALUES (?, ?, 27.2524061152165)`,
		valladolid, publishCellYUC)
	exec(`INSERT INTO station_power_cell (station_id, cell_id, distance_km) VALUES (?, ?, 12.3456)`, ags, publishCellAGS)
	exec(`INSERT INTO station_power_cell (station_id, cell_id, distance_km) VALUES (?, ?, 1337.5)`, calvillo, publishCellYUC)

	// POWER rows, out of key order.
	supplementMonthly(publishCellYUC, "1981-2010", 12, monthlyRun, publishPowerPartial)
	supplementMonthly(publishCellYUC, "1991-2020", 1, monthlyRun2, publishPowerSeq(200))
	supplementMonthly(publishCellYUC, "1981-2010", 1, monthlyRun, publishPowerFull)
	supplementMonthly(publishCellAGS, "1981-2010", 1, monthlyRun, publishPowerSeq(300))
	supplementDaily(publishCellYUC, "2020-02-29", dailyRun, publishPowerPartial)
	supplementDaily(publishCellYUC, "2020-01-03", dailyRun, publishPowerSeq(100))
	supplementDaily(publishCellYUC, "2020-01-01", dailyRun, publishPowerFull)
	supplementDaily(publishCellAGS, "2020-01-01", dailyRun, publishPowerSeq(300))

	ExpectWithOffset(1, db.Close()).To(Succeed())
	return dbPath
}

// The exact contents the seed must produce inside each archive, written
// from the per-file export spec: headers by name in export order,
// rows in PK order (TEXT keys bytewise: 31001 < 31002 < 3101 < 31019),
// numerics at their pinned decimals — the four daily_observations values
// and the three daily extremes of the extras at two, because CONAGUA
// publishes them at two and a one-decimal export would round a trace
// rain day into a dry one; the normals proper, the monthly extremes and
// rain_days at one — NULL as the empty field, the state code uppercase
// as stored, the join context and POWER-31 empty where the LEFT join
// matched nothing.
var (
	wantStationsHeader = []string{
		"station_id", "name", "state", "municipality", "lat", "lon", "altitude_m", "status",
		"first_year", "last_year",
		"wmo_completeness_bin_1961_1990", "wmo_completeness_bin_1971_2000",
		"wmo_completeness_bin_1981_2010", "wmo_completeness_bin_1991_2020",
		"wmo_completeness_cont_1961_1990", "wmo_completeness_cont_1971_2000",
		"wmo_completeness_cont_1981_2010", "wmo_completeness_cont_1991_2020",
	}
	wantNormalsHeader = []string{"station_id", "period", "month", "tmax_c", "tmin_c", "tmean_c", "precip_mm", "evap_mm"}
	wantExtrasHeader  = []string{
		"station_id", "period", "month",
		"tmax_monthly_extreme_c", "tmax_monthly_extreme_year", "tmax_daily_extreme_c", "tmax_daily_extreme_date",
		"tmin_monthly_extreme_c", "tmin_monthly_extreme_year", "tmin_daily_extreme_c", "tmin_daily_extreme_date",
		"precip_monthly_extreme_mm", "precip_monthly_extreme_year", "precip_daily_extreme_mm", "precip_daily_extreme_date",
		"tmax_years_with_data", "tmin_years_with_data", "tmean_years_with_data", "precip_years_with_data",
		"evap_years_with_data", "rain_days", "rain_days_years_with_data",
	}
	wantDailyHeader = []string{"station_id", "date", "tmax_c", "tmin_c", "precip_mm", "evap_mm"}

	wantCellsHeader        = []string{"cell_id", "lat", "lon"}
	wantCellMapHeader      = []string{"station_id", "cell_id", "distance_km"}
	wantPowerMonthlyHeader = withPower([]string{"cell_id", "period", "month"}, wantPower31)
	wantPowerDailyHeader   = withPower([]string{"cell_id", "date"}, wantPower31)

	wantCombinedMonthlyHeader = withPower([]string{
		"station_id", "period", "month", "cell_id", "distance_km", "tmax_c", "tmin_c", "tmean_c", "precip_mm", "evap_mm",
	}, wantPower31)
	wantCombinedDailyHeader = withPower([]string{
		"station_id", "date", "cell_id", "distance_km", "tmax_c", "tmin_c", "precip_mm", "evap_mm",
	}, wantPower31)

	wantIngestRunsHeader = []string{
		"snapshot_date", "started_at", "finished_at", "sink_kind", "etl_git_sha", "status",
		"stations_attempted", "stations_succeeded", "stations_failed",
		"daily_rows", "normals_rows", "extras_rows", "warnings_total",
	}
	wantPowerRunsHeader = []string{
		"run_label", "started_at", "finished_at", "status", "endpoint_url", "parameters", "community",
		"period_start_year", "period_end_year", "grid_resolution", "unit_conversions", "temporal_mode",
		"period_start_date", "period_end_date",
		"cells_attempted", "cells_succeeded", "cells_failed", "supplement_rows", "etl_git_sha",
	}

	wantYucStations = [][]string{
		wantStationsHeader,
		{"31001", "MERIDA (OBS)", "YUC", "Mérida", "20.980278", "-89.621389", "10.0", "operating", "1951", "2026",
			"0.9722", "0.5000", "1.0000", "0.0000", "0.0009", "0.9324", "0.2500", "0.9991"},
		{"31002", "TIZIMIN", "YUC", "", "21.140000", "-88.150000", "", "suspended", "1961", "1995",
			"0.2500", "", "", "", "0.1000", "", "", ""},
		{"3101", "VALLADOLID, CENTRO", "YUC", "Valladolid", "20.689100", "-88.201100", "25.5", "operating", "1961", "2026",
			"", "", "", "", "", "", "", ""},
		{"31019", "PROGRESO", "YUC", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
	}
	wantYucNormals = slices.Concat([][]string{wantNormalsHeader}, meridaNormals1971Rows(), [][]string{
		{"31001", "1981-2010", "1", "33.4", "17.9", "25.7", "28.3", "141.6"},
		{"31001", "1981-2010", "12", "30.1", "16.3", "", "24.5", "110.3"},
		{"31001", "1991-2020", "1", "33.0", "18.3", "25.8", "0.0", "150.3"},
		{"31001", "1991-2020", "12", "", "", "", "", ""},
		{"3101", "1961-1990", "6", "35.9", "22.1", "29.0", "152.7", "190.4"},
	})
	wantYucExtras = [][]string{
		wantExtrasHeader,
		// The monthly extremes and rain_days terminate at one decimal;
		// the three daily extremes are single daily readings carried
		// into the normals sheet, so they carry two — the seeded 42.04
		// keeps its hundredths instead of collapsing to 42.0, and the
		// two stored at a tenth pad to 3.50 and 98.70.
		{"31001", "1981-2010", "1",
			"38.5", "1998", "42.04", "1998-05-14",
			"8.3", "1985", "3.50", "1985-01-20",
			"210.6", "2002", "98.70", "2002-09-23",
			"28", "27", "27", "30", "25", "4.6", "29"},
		{"31001", "1981-2010", "12",
			"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
		{"31001", "1991-2020", "1",
			"39.0", "", "", "",
			"", "", "", "",
			"190.2", "2012", "", "",
			"30", "", "", "30", "", "12.3", ""},
		{"3101", "1961-1990", "6",
			"40.2", "1975", "", "",
			"", "", "", "",
			"", "", "", "",
			"", "", "", "", "", "3.0", "30"},
	}
	// The observed values at two decimals: the seeded hundredths survive
	// (12.46 mm is not 12.5, 0.06 mm is not 0.1), a negative below the
	// tenths keeps its sign and its magnitude (-0.04, which one decimal
	// rendered as a flat 0.0), and a value stored with fewer decimals
	// pads rather than shortening.
	wantMeridaDaily = [][]string{
		wantDailyHeader,
		{"31001", "1980-12-31", "28.00", "14.50", "0.00", "3.10"},
		{"31001", "1999-12-31", "", "-0.04", "", ""},
		{"31001", "2020-01-01", "30.50", "", "12.46", ""},
		{"31001", "2020-01-02", "31.00", "19.50", "0.00", "4.20"},
		{"31001", "2020-02-29", "18.94", "-2.46", "0.06", "5.56"},
	}
	wantTiziminDaily = [][]string{
		wantDailyHeader,
		{"31002", "1990-06-01", "35.50", "21.00", "0.00", "8.10"},
	}
	wantValladolidDaily = [][]string{
		wantDailyHeader,
		{"3101", "1975-06-15", "36.20", "22.00", "45.10", "7.30"},
	}

	// nasa_power/: the one cell the state's stations reference (shared
	// by two of them, listed once), the map for the stations that have a
	// cell, and that cell's rows only.
	wantYucCells = [][]string{
		wantCellsHeader,
		{publishCellYUC, "20.750", "-89.375"},
	}
	wantYucCellMap = [][]string{
		wantCellMapHeader,
		{"31001", publishCellYUC, "33.100"},
		{"3101", publishCellYUC, "27.252"},
	}
	wantYucPowerMonthly = [][]string{
		wantPowerMonthlyHeader,
		withPower([]string{publishCellYUC, "1981-2010", "1"}, publishPowerFullText),
		withPower([]string{publishCellYUC, "1981-2010", "12"}, publishPowerPartialText),
		withPower([]string{publishCellYUC, "1991-2020", "1"}, publishPowerSeqText(200)),
	}
	wantYucPowerDaily = [][]string{
		wantPowerDailyHeader,
		withPower([]string{publishCellYUC, "2020-01-01"}, publishPowerFullText),
		withPower([]string{publishCellYUC, "2020-01-03"}, publishPowerSeqText(100)),
		withPower([]string{publishCellYUC, "2020-02-29"}, publishPowerPartialText),
	}

	// combined/: every normals row of the state's stations across all
	// four periods, POWER-31 only where the cell has the (period, month)
	// row; every observed date of each station, POWER-31 only where the
	// cell has the date — never the date 31001 did not observe; the
	// cell-less 31002 with an empty join context throughout.
	wantYucCombinedMonthly = slices.Concat([][]string{wantCombinedMonthlyHeader}, meridaCombined1971Rows(), [][]string{
		withPower([]string{"31001", "1981-2010", "1", publishCellYUC, "33.100", "33.4", "17.9", "25.7", "28.3", "141.6"}, publishPowerFullText),
		withPower([]string{"31001", "1981-2010", "12", publishCellYUC, "33.100", "30.1", "16.3", "", "24.5", "110.3"}, publishPowerPartialText),
		withPower([]string{"31001", "1991-2020", "1", publishCellYUC, "33.100", "33.0", "18.3", "25.8", "0.0", "150.3"}, publishPowerSeqText(200)),
		withPower([]string{"31001", "1991-2020", "12", publishCellYUC, "33.100", "", "", "", "", ""}, publishPowerEmpty),
		withPower([]string{"3101", "1961-1990", "6", publishCellYUC, "27.252", "35.9", "22.1", "29.0", "152.7", "190.4"}, publishPowerEmpty),
	})
	wantMeridaCombinedDaily = [][]string{
		wantCombinedDailyHeader,
		withPower([]string{"31001", "1980-12-31", publishCellYUC, "33.100", "28.00", "14.50", "0.00", "3.10"}, publishPowerEmpty),
		withPower([]string{"31001", "1999-12-31", publishCellYUC, "33.100", "", "-0.04", "", ""}, publishPowerEmpty),
		withPower([]string{"31001", "2020-01-01", publishCellYUC, "33.100", "30.50", "", "12.46", ""}, publishPowerFullText),
		withPower([]string{"31001", "2020-01-02", publishCellYUC, "33.100", "31.00", "19.50", "0.00", "4.20"}, publishPowerEmpty),
		withPower([]string{"31001", "2020-02-29", publishCellYUC, "33.100", "18.94", "-2.46", "0.06", "5.56"}, publishPowerPartialText),
	}
	wantTiziminCombinedDaily = [][]string{
		wantCombinedDailyHeader,
		withPower([]string{"31002", "1990-06-01", "", "", "35.50", "21.00", "0.00", "8.10"}, publishPowerEmpty),
	}
	wantValladolidCombinedDaily = [][]string{
		wantCombinedDailyHeader,
		withPower([]string{"3101", "1975-06-15", publishCellYUC, "27.252", "36.20", "22.00", "45.10", "7.30"}, publishPowerEmpty),
	}

	// provenance/: global — the same rows in every archive. The complete
	// ingest run only (the newer aborted run is not provenance); the
	// three referenced power runs by label, the aborted attempt absent;
	// every column, unit_conversions as the stored text.
	wantIngestRunsCSV = [][]string{
		wantIngestRunsHeader,
		{publishSnapshotDate, "2026-06-09T01:00:00Z", "2026-06-09T03:00:00Z", "local", "f1r5t", "complete",
			"7", "7", "0", "8", "7", "5", "1"},
	}
	wantPowerRunsCSV = [][]string{
		wantPowerRunsHeader,
		{"power-daily-1981-2026", "2026-07-02T00:00:00Z", "2026-07-03T12:00:00Z", "complete",
			"https://power.larc.nasa.gov/api/temporal/daily/point", "T2M,ALLSKY_SFC_SW_DWN", "AG",
			"1981", "2026", "0.5x0.625", publishConversions, "daily", "1981-01-01", "2026-06-08",
			"2", "2", "0", "4", ""},
		{"power-monthly-1981-2010", "2026-07-01T00:00:00Z", "2026-07-01T06:00:00Z", "complete",
			"https://power.larc.nasa.gov/api/temporal/monthly/point", "T2M,ALLSKY_SFC_SW_DWN", "AG",
			"1981", "2010", "0.5x0.625", publishConversions, "monthly", "", "",
			"2", "2", "0", "3", "p0w3r"},
		{"power-monthly-1991-2020", "2026-07-01T07:00:00Z", "2026-07-01T09:00:00Z", "complete",
			"https://power.larc.nasa.gov/api/temporal/monthly/point", "T2M,ALLSKY_SFC_SW_DWN", "AG",
			"1991", "2020", "0.5x0.625", publishConversions, "monthly", "", "",
			"1", "1", "0", "1", "p0w3r"},
	}

	wantAgsStations = [][]string{
		wantStationsHeader,
		{"1001", "AGUASCALIENTES (OBS)", "AGS", "Aguascalientes", "21.850278", "-102.290833", "1878.4", "operating",
			"1948", "2026", "1.0000", "1.0000", "1.0000", "1.0000", "0.5000", "0.5000", "0.5000", "0.5000"},
		{"1002", "CALVILLO", "AGS", "", "21.850000", "-102.720000", "", "suspended", "1950", "1990",
			"", "", "", "", "", "", "", ""},
		{"1010", "SAN JOSE DE GRACIA", "AGS", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
	}
	wantAgsNormals = [][]string{
		wantNormalsHeader,
		{"1001", "1981-2010", "1", "29.9", "9.9", "19.9", "20.0", "200.0"},
	}
	wantAgsExtras = [][]string{
		wantExtrasHeader,
		// One seeded value, 1.0, across every REAL of the row: the three
		// daily extremes pad to two decimals, the monthly extremes and
		// rain_days to one, so a column that took the wrong count is
		// visible here as a lone 1.0 among 1.00s or the reverse.
		{"1001", "1981-2010", "1",
			"1.0", "1", "1.00", "1981-01-01", "1.0", "1", "1.00", "1981-01-01", "1.0", "1", "1.00", "1981-01-01",
			"1", "1", "1", "1", "1", "1.0", "1"},
	}
	// The trace rain day: CONAGUA's 0.01 mm reaches the file as 0.01,
	// the wet day it is, and not as the dry 0.0 a one-decimal export
	// published (1,138,671 such days at 4,550 stations nationally).
	wantAgsDaily = [][]string{
		wantDailyHeader,
		{"1001", "2020-01-01", "20.00", "5.00", "0.01", "6.00"},
	}
	// The state's stations reference both cells — 1002 sits on the
	// Yucatán one — so both are listed, in cell_id order, with both
	// cells' monthly rows; the shared cell's per-cell daily file is the
	// same bytes as in yuc-tabular.zip.
	wantAgsCells = [][]string{
		wantCellsHeader,
		{publishCellYUC, "20.750", "-89.375"},
		{publishCellAGS, "21.750", "-102.500"},
	}
	wantAgsCellMap = [][]string{
		wantCellMapHeader,
		{"1001", publishCellAGS, "12.346"},
		{"1002", publishCellYUC, "1337.500"},
	}
	wantAgsPowerMonthly = slices.Concat(wantYucPowerMonthly, [][]string{
		withPower([]string{publishCellAGS, "1981-2010", "1"}, publishPowerSeqText(300)),
	})
	wantAgsPowerDaily = [][]string{
		wantPowerDailyHeader,
		withPower([]string{publishCellAGS, "2020-01-01"}, publishPowerSeqText(300)),
	}
	wantAgsCombinedMonthly = [][]string{
		wantCombinedMonthlyHeader,
		withPower([]string{"1001", "1981-2010", "1", publishCellAGS, "12.346", "29.9", "9.9", "19.9", "20.0", "200.0"}, publishPowerSeqText(300)),
	}
	wantAgsCombinedDaily = [][]string{
		wantCombinedDailyHeader,
		withPower([]string{"1001", "2020-01-01", publishCellAGS, "12.346", "20.00", "5.00", "0.01", "6.00"}, publishPowerSeqText(300)),
	}

	// The archives' entry lists: the four scope folders, bytewise-sorted,
	// one daily file per station under conagua/ and combined/ (the
	// catalog-only stations included, header-only), one per referenced
	// cell under nasa_power/; beside them the ten Parquet twins — a
	// whole-table twin right after its CSV ('.csv' < '.parquet'), a
	// sharded table's twin right before its shard folder ('.' < '/') —
	// and the {state}.db at the root, wherever the slug sorts it: after
	// provenance/ for yuc, before combined/ for ags.
	wantYucPaths = []string{
		"combined/combined_daily.parquet",
		"combined/combined_daily/yuc/daily-31001.csv",
		"combined/combined_daily/yuc/daily-31002.csv",
		"combined/combined_daily/yuc/daily-3101.csv",
		"combined/combined_daily/yuc/daily-31019.csv",
		"combined/combined_monthly.csv",
		"combined/combined_monthly.parquet",
		"conagua/daily_observations.parquet",
		"conagua/daily_observations/yuc/daily-31001.csv",
		"conagua/daily_observations/yuc/daily-31002.csv",
		"conagua/daily_observations/yuc/daily-3101.csv",
		"conagua/daily_observations/yuc/daily-31019.csv",
		"conagua/monthly_normals.csv",
		"conagua/monthly_normals.parquet",
		"conagua/monthly_normals_extras.csv",
		"conagua/monthly_normals_extras.parquet",
		"conagua/stations.csv",
		"conagua/stations.parquet",
		"nasa_power/cells.csv",
		"nasa_power/cells.parquet",
		"nasa_power/daily.parquet",
		"nasa_power/daily/daily-" + publishCellYUC + ".csv",
		"nasa_power/monthly.csv",
		"nasa_power/monthly.parquet",
		"nasa_power/station_cell_map.csv",
		"nasa_power/station_cell_map.parquet",
		"provenance/ingest_runs.csv",
		"provenance/power_runs.csv",
		"yuc.db",
	}
	wantAgsPaths = []string{
		"ags.db",
		"combined/combined_daily.parquet",
		"combined/combined_daily/ags/daily-1001.csv",
		"combined/combined_daily/ags/daily-1002.csv",
		"combined/combined_daily/ags/daily-1010.csv",
		"combined/combined_monthly.csv",
		"combined/combined_monthly.parquet",
		"conagua/daily_observations.parquet",
		"conagua/daily_observations/ags/daily-1001.csv",
		"conagua/daily_observations/ags/daily-1002.csv",
		"conagua/daily_observations/ags/daily-1010.csv",
		"conagua/monthly_normals.csv",
		"conagua/monthly_normals.parquet",
		"conagua/monthly_normals_extras.csv",
		"conagua/monthly_normals_extras.parquet",
		"conagua/stations.csv",
		"conagua/stations.parquet",
		"nasa_power/cells.csv",
		"nasa_power/cells.parquet",
		"nasa_power/daily.parquet",
		"nasa_power/daily/daily-" + publishCellYUC + ".csv",
		"nasa_power/daily/daily-" + publishCellAGS + ".csv",
		"nasa_power/monthly.csv",
		"nasa_power/monthly.parquet",
		"nasa_power/station_cell_map.csv",
		"nasa_power/station_cell_map.parquet",
		"provenance/ingest_runs.csv",
		"provenance/power_runs.csv",
	}

	// The ten logical tables a tabular archive carries as Parquet, by
	// name: each has the CSV twin(s) of the same name — one file, or
	// the shard folder — inside the archive.
	wantParquetTables = []string{
		"conagua/stations", "conagua/monthly_normals", "conagua/monthly_normals_extras", "conagua/daily_observations",
		"nasa_power/cells", "nasa_power/station_cell_map", "nasa_power/monthly", "nasa_power/daily",
		"combined/combined_monthly", "combined/combined_daily",
	}

	// The unit lines each archive logs: its per-station and per-cell
	// files and its {state}.db in write (= entry) order, numbered across
	// the archive; the Parquet twins are not units.
	wantYucUnits = []string{
		"combined/combined_daily/yuc/daily-31001.csv",
		"combined/combined_daily/yuc/daily-31002.csv",
		"combined/combined_daily/yuc/daily-3101.csv",
		"combined/combined_daily/yuc/daily-31019.csv",
		"conagua/daily_observations/yuc/daily-31001.csv",
		"conagua/daily_observations/yuc/daily-31002.csv",
		"conagua/daily_observations/yuc/daily-3101.csv",
		"conagua/daily_observations/yuc/daily-31019.csv",
		"nasa_power/daily/daily-" + publishCellYUC + ".csv",
		"yuc.db",
	}
	wantAgsUnits = []string{
		"ags.db",
		"combined/combined_daily/ags/daily-1001.csv",
		"combined/combined_daily/ags/daily-1002.csv",
		"combined/combined_daily/ags/daily-1010.csv",
		"conagua/daily_observations/ags/daily-1001.csv",
		"conagua/daily_observations/ags/daily-1002.csv",
		"conagua/daily_observations/ags/daily-1010.csv",
		"nasa_power/daily/daily-" + publishCellYUC + ".csv",
		"nasa_power/daily/daily-" + publishCellAGS + ".csv",
	}

	// The JSON archives' entry lists: per station its daily.json +
	// profile.json pair under combined/<slug>/<station_id>/, then
	// the provenance pair as JSON, bytewise-sorted; and their unit lines
	// — one per station, labelled by the profile.json that completes
	// its pair.
	wantYucJSONPaths = []string{
		"combined/yuc/31001/daily.json",
		"combined/yuc/31001/profile.json",
		"combined/yuc/31002/daily.json",
		"combined/yuc/31002/profile.json",
		"combined/yuc/3101/daily.json",
		"combined/yuc/3101/profile.json",
		"combined/yuc/31019/daily.json",
		"combined/yuc/31019/profile.json",
		"provenance/ingest_runs.json",
		"provenance/power_runs.json",
	}
	wantAgsJSONPaths = []string{
		"combined/ags/1001/daily.json",
		"combined/ags/1001/profile.json",
		"combined/ags/1002/daily.json",
		"combined/ags/1002/profile.json",
		"combined/ags/1010/daily.json",
		"combined/ags/1010/profile.json",
		"provenance/ingest_runs.json",
		"provenance/power_runs.json",
	}
	wantYucJSONUnits = []string{
		"combined/yuc/31001/profile.json",
		"combined/yuc/31002/profile.json",
		"combined/yuc/3101/profile.json",
		"combined/yuc/31019/profile.json",
	}
	wantAgsJSONUnits = []string{
		"combined/ags/1001/profile.json",
		"combined/ags/1002/profile.json",
		"combined/ags/1010/profile.json",
	}

	// The provenance the seed yields, as the manifest must carry it: the
	// complete run only (the newer aborted run is not provenance), every
	// column; the three referenced power runs under their natural labels, in
	// label order, the unreferenced aborted attempt absent.
	wantPublishIngestRuns = []publish.IngestRunRef{{
		SnapshotDate: publishSnapshotDate, StartedAt: "2026-06-09T01:00:00Z", FinishedAt: strp("2026-06-09T03:00:00Z"),
		SinkKind: "local", ETLGitSHA: strp("f1r5t"), Status: "complete",
		StationsAttempted: i64p(7), StationsSucceeded: i64p(7), StationsFailed: i64p(0),
		DailyRows: i64p(8), NormalsRows: i64p(7), ExtrasRows: i64p(5), WarningsTotal: i64p(1),
	}}
	wantPublishPowerRuns = []publish.PowerRunRef{
		{
			RunLabel: "power-daily-1981-2026", TemporalMode: "daily", PeriodStartYear: 1981, PeriodEndYear: 2026,
			PeriodStartDate: strp("1981-01-01"), PeriodEndDate: strp("2026-06-08"),
			StartedAt: "2026-07-02T00:00:00Z", FinishedAt: strp("2026-07-03T12:00:00Z"), Status: "complete",
			EndpointURL: "https://power.larc.nasa.gov/api/temporal/daily/point", Parameters: "T2M,ALLSKY_SFC_SW_DWN",
			Community: "AG", GridResolution: "0.5x0.625",
			CellsAttempted: i64p(2), CellsSucceeded: i64p(2), CellsFailed: i64p(0), SupplementRows: i64p(4),
			ETLGitSHA: nil,
		},
		{
			RunLabel: "power-monthly-1981-2010", TemporalMode: "monthly", PeriodStartYear: 1981, PeriodEndYear: 2010,
			PeriodStartDate: nil, PeriodEndDate: nil,
			StartedAt: "2026-07-01T00:00:00Z", FinishedAt: strp("2026-07-01T06:00:00Z"), Status: "complete",
			EndpointURL: "https://power.larc.nasa.gov/api/temporal/monthly/point", Parameters: "T2M,ALLSKY_SFC_SW_DWN",
			Community: "AG", GridResolution: "0.5x0.625",
			CellsAttempted: i64p(2), CellsSucceeded: i64p(2), CellsFailed: i64p(0), SupplementRows: i64p(3),
			ETLGitSHA: strp("p0w3r"),
		},
		{
			RunLabel: "power-monthly-1991-2020", TemporalMode: "monthly", PeriodStartYear: 1991, PeriodEndYear: 2020,
			PeriodStartDate: nil, PeriodEndDate: nil,
			StartedAt: "2026-07-01T07:00:00Z", FinishedAt: strp("2026-07-01T09:00:00Z"), Status: "complete",
			EndpointURL: "https://power.larc.nasa.gov/api/temporal/monthly/point", Parameters: "T2M,ALLSKY_SFC_SW_DWN",
			Community: "AG", GridResolution: "0.5x0.625",
			CellsAttempted: i64p(1), CellsSucceeded: i64p(1), CellsFailed: i64p(0), SupplementRows: i64p(1),
			ETLGitSHA: strp("p0w3r"),
		},
	}
	wantPublishCounts = publish.TableCounts{
		Stations: 7, MonthlyNormals: 18, MonthlyNormalsExtras: 5, DailyObservations: 8, ParsingWarnings: 1,
		IngestRuns: 2, PowerRuns: 4, NasaPowerGridCells: 2, StationPowerCell: 4, MonthlySupplement: 4, DailySupplement: 4,
	}
	wantPublishDataset = publish.Dataset{
		Title:        "BioclimaMX Stations: Mexican Climate Station Records (CONAGUA), Augmented with NASA POWER",
		Version:      "0.1",
		License:      "CC-BY-4.0",
		Creator:      "Pablo Trinidad",
		CreatorORCID: "0009-0007-4050-494X",
		// No DOI clause: none is baked into this build, and the DOI is
		// omitted rather than stood in for (a Zenodo file is immutable).
		SuggestedCitation: "Pablo Trinidad (2026). BioclimaMX Stations: Mexican Climate Station Records " +
			"(CONAGUA), Augmented with NASA POWER. Version 0.1. Zenodo.",
	}
)

// num is a JSON number by its literal text: the profile's numbers are
// asserted as the fixed-decimal literals the binary wrote, never as
// floats.
func num(literal string) json.Number { return json.Number(literal) }

// numOrNull maps a CSV cell to its JSON value: the empty field is null,
// anything else the same literal as a number.
func numOrNull(cell string) any {
	if cell == "" {
		return nil
	}
	return num(cell)
}

// series is a 13-slot month series: byMonth keyed 1..12 fills slots
// 0–11 (an absent month is null), annual is slot 12.
func series(annual any, byMonth map[int]any) []any {
	out := make([]any, 13)
	for m, v := range byMonth {
		out[m-1] = v
	}
	out[12] = annual
	return out
}

// monthOne is a series with only January set and no annual.
func monthOne(v any) []any { return series(nil, map[int]any{1: v}) }

// powerSeries maps POWER-31 to 13-slot series from two rendered rows —
// January and December — with the annual null (no seeded period is
// complete, so slot 12 is null throughout).
func powerSeries(january, december []string) map[string]any {
	out := make(map[string]any, len(wantPower31))
	for i, col := range wantPower31 {
		out[col] = series(nil, map[int]any{1: numOrNull(january[i]), 12: numOrNull(december[i])})
	}
	return out
}

// extrasSeries maps the 19 extras columns, in header order, to 13-slot
// series from their January values; December, seeded all-NULL for
// 1981-2010, and the annual (extras never derive one) are null.
func extrasSeries(january []any) map[string]any {
	cols := wantExtrasHeader[3:]
	ExpectWithOffset(1, january).To(HaveLen(len(cols)))
	out := make(map[string]any, len(cols))
	for i, col := range cols {
		out[col] = monthOne(january[i])
	}
	return out
}

func nullPeriods() map[string]any {
	return map[string]any{"1961-1990": nil, "1971-2000": nil, "1981-2010": nil, "1991-2020": nil}
}

// wantProfileMeta is every profile's meta block: the build's SHA, the
// snapshot, the runs by natural label (the same set as the provenance
// files), the license, the citation — and no generation stamp, which
// would break byte reproducibility.
func wantProfileMeta() map[string]any {
	return map[string]any{
		"schema_version": num(strconv.Itoa(schema.Version)),
		"etl_git_sha":    binGitSHA,
		"snapshot_date":  publishSnapshotDate,
		"runs": map[string]any{
			"ingest": []any{publishSnapshotDate},
			"power":  []any{"power-daily-1981-2026", "power-monthly-1981-2010", "power-monthly-1991-2020"},
		},
		"license":            "CC-BY-4.0",
		"suggested_citation": wantPublishDataset.SuggestedCitation,
	}
}

// wantMeridaProfile is 31001's profile.json, every block written by
// hand from the seed: the identity at pinned decimals; the eight WMO
// scores at four; the normals — 1971-2000 complete with its derived
// annual (means for the temperatures, sums for precip and evap), the two
// two-month periods with slot 12 null, the empty period null; the extras
// with slot 12 null; the cell; POWER monthly for the two pulled periods
// only, the two others null; the daily summary; the meta.
func wantMeridaProfile() map[string]any {
	normals1971 := map[string]any{}
	for k, col := range []string{"tmax_c", "tmin_c", "tmean_c", "precip_mm", "evap_mm"} {
		byMonth := map[int]any{}
		for m, v := range meridaNormals1971 {
			byMonth[m+1] = num(fmt.Sprintf("%.1f", v[k]))
		}
		normals1971[col] = series(num([]string{"34.9", "20.6", "27.7", "910.6", "1910.0"}[k]), byMonth)
	}
	return map[string]any{
		"station_id": "31001",
		"identity": map[string]any{
			"name": "MERIDA (OBS)", "state": "YUC", "state_name": "Yucatán", "municipality": "Mérida",
			"lat": num("20.980278"), "lon": num("-89.621389"), "altitude_m": num("10.0"), "status": "operating",
			"first_year": num("1951"), "last_year": num("2026"),
		},
		"wmo_completeness": map[string]any{
			"source": "bioclima_derived",
			"periods": map[string]any{
				"1961-1990": map[string]any{"bin": num("0.9722"), "cont": num("0.0009")},
				"1971-2000": map[string]any{"bin": num("0.5000"), "cont": num("0.9324")},
				"1981-2010": map[string]any{"bin": num("1.0000"), "cont": num("0.2500")},
				"1991-2020": map[string]any{"bin": num("0.0000"), "cont": num("0.9991")},
			},
		},
		"normals": map[string]any{
			"source": "conagua_published", "annual_slot": "bioclima_derived",
			"periods": map[string]any{
				"1961-1990": nil,
				"1971-2000": normals1971,
				"1981-2010": map[string]any{
					"tmax_c":    series(nil, map[int]any{1: num("33.4"), 12: num("30.1")}),
					"tmin_c":    series(nil, map[int]any{1: num("17.9"), 12: num("16.3")}),
					"tmean_c":   series(nil, map[int]any{1: num("25.7")}),
					"precip_mm": series(nil, map[int]any{1: num("28.3"), 12: num("24.5")}),
					"evap_mm":   series(nil, map[int]any{1: num("141.6"), 12: num("110.3")}),
				},
				"1991-2020": map[string]any{
					"tmax_c":    monthOne(num("33.0")),
					"tmin_c":    monthOne(num("18.3")),
					"tmean_c":   monthOne(num("25.8")),
					"precip_mm": monthOne(num("0.0")),
					"evap_mm":   monthOne(num("150.3")),
				},
			},
		},
		"extras": map[string]any{
			"source": "conagua_published",
			"periods": map[string]any{
				"1961-1990": nil,
				"1971-2000": nil,
				"1981-2010": extrasSeries([]any{
					num("38.5"), num("1998"), num("42.04"), "1998-05-14",
					num("8.3"), num("1985"), num("3.50"), "1985-01-20",
					num("210.6"), num("2002"), num("98.70"), "2002-09-23",
					num("28"), num("27"), num("27"), num("30"), num("25"), num("4.6"), num("29"),
				}),
				"1991-2020": extrasSeries([]any{
					num("39.0"), nil, nil, nil,
					nil, nil, nil, nil,
					num("190.2"), num("2012"), nil, nil,
					num("30"), nil, nil, num("30"), nil, num("12.3"), nil,
				}),
			},
		},
		"power_cell": map[string]any{
			"source": "bioclima_derived", "cell_id": publishCellYUC,
			"lat": num("20.750"), "lon": num("-89.375"), "distance_km": num("33.100"),
		},
		"power_monthly": map[string]any{
			"source": "nasa_power", "annual_slot": "bioclima_derived",
			"periods": map[string]any{
				"1961-1990": nil,
				"1971-2000": nil,
				"1981-2010": powerSeries(publishPowerFullText, publishPowerPartialText),
				"1991-2020": powerSeries(publishPowerSeqText(200), publishPowerEmpty),
			},
		},
		"daily_summary": map[string]any{
			"source": "bioclima_derived",
			"coverage": map[string]any{
				"observed": map[string]any{
					"first_date": "1980-12-31", "last_date": "2020-02-29", "days_with_obs": num("5"),
					"days_by_variable": map[string]any{
						"tmax_c": num("4"), "tmin_c": num("4"), "precip_mm": num("4"), "evap_mm": num("3"),
					},
				},
				"reanalysis": map[string]any{"first_date": "2020-01-01", "last_date": "2020-02-29", "days": num("3")},
			},
			// The records are daily readings, so they carry the daily
			// columns' two decimals: the wettest day is 12.46 mm, not a
			// rounded 12.5, and the coldest is -2.46 °C, not -2.5.
			"extremes": map[string]any{
				"source":                "conagua_observed",
				"record_tmax_c":         map[string]any{"value": num("31.00"), "date": "2020-01-02"},
				"record_tmin_c":         map[string]any{"value": num("-2.46"), "date": "2020-02-29"},
				"record_precip_mm_1day": map[string]any{"value": num("12.46"), "date": "2020-01-01"},
			},
			// Two single dry days; the NULL precip day and the wet days
			// break every run, and the earliest run keeps the tie.
			"dry_spell": map[string]any{
				"longest_dry_run_days": num("1"), "start_date": "1980-12-31", "end_date": "1980-12-31",
			},
		},
		"meta": wantProfileMeta(),
	}
}

// wantTiziminProfile is the cell-less 31002: the cell and the POWER
// blocks null as wholes, one WMO period, no normals, one observed day.
func wantTiziminProfile() map[string]any {
	return map[string]any{
		"station_id": "31002",
		"identity": map[string]any{
			"name": "TIZIMIN", "state": "YUC", "state_name": "Yucatán", "municipality": nil,
			"lat": num("21.140000"), "lon": num("-88.150000"), "altitude_m": nil, "status": "suspended",
			"first_year": num("1961"), "last_year": num("1995"),
		},
		"wmo_completeness": map[string]any{
			"source": "bioclima_derived",
			"periods": map[string]any{
				"1961-1990": map[string]any{"bin": num("0.2500"), "cont": num("0.1000")},
				"1971-2000": nil, "1981-2010": nil, "1991-2020": nil,
			},
		},
		"normals":       map[string]any{"source": "conagua_published", "annual_slot": "bioclima_derived", "periods": nullPeriods()},
		"extras":        map[string]any{"source": "conagua_published", "periods": nullPeriods()},
		"power_cell":    nil,
		"power_monthly": nil,
		"daily_summary": map[string]any{
			"source": "bioclima_derived",
			"coverage": map[string]any{
				"observed": map[string]any{
					"first_date": "1990-06-01", "last_date": "1990-06-01", "days_with_obs": num("1"),
					"days_by_variable": map[string]any{
						"tmax_c": num("1"), "tmin_c": num("1"), "precip_mm": num("1"), "evap_mm": num("1"),
					},
				},
				"reanalysis": nil,
			},
			"extremes": map[string]any{
				"source":                "conagua_observed",
				"record_tmax_c":         map[string]any{"value": num("35.50"), "date": "1990-06-01"},
				"record_tmin_c":         map[string]any{"value": num("21.00"), "date": "1990-06-01"},
				"record_precip_mm_1day": map[string]any{"value": num("0.00"), "date": "1990-06-01"},
			},
			"dry_spell": map[string]any{
				"longest_dry_run_days": num("1"), "start_date": "1990-06-01", "end_date": "1990-06-01",
			},
		},
		"meta": wantProfileMeta(),
	}
}

// wantProgresoProfile is the catalog-only 31019: every block present,
// every value an explicit null.
func wantProgresoProfile() map[string]any {
	return map[string]any{
		"station_id": "31019",
		"identity": map[string]any{
			"name": "PROGRESO", "state": "YUC", "state_name": "Yucatán", "municipality": nil,
			"lat": nil, "lon": nil, "altitude_m": nil, "status": nil, "first_year": nil, "last_year": nil,
		},
		"wmo_completeness": map[string]any{"source": "bioclima_derived", "periods": nullPeriods()},
		"normals":          map[string]any{"source": "conagua_published", "annual_slot": "bioclima_derived", "periods": nullPeriods()},
		"extras":           map[string]any{"source": "conagua_published", "periods": nullPeriods()},
		"power_cell":       nil,
		"power_monthly":    nil,
		"daily_summary": map[string]any{
			"source":   "bioclima_derived",
			"coverage": map[string]any{"observed": nil, "reanalysis": nil},
			"extremes": map[string]any{
				"source": "conagua_observed", "record_tmax_c": nil, "record_tmin_c": nil, "record_precip_mm_1day": nil,
			},
			"dry_spell": nil,
		},
		"meta": wantProfileMeta(),
	}
}

// decodeJSONDoc parses one JSON object with its numbers kept as their
// literal text, so a fixed-decimal rendering is what gets compared.
func decodeJSONDoc(text string) map[string]any {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var got map[string]any
	ExpectWithOffset(1, dec.Decode(&got)).To(Succeed())
	ExpectWithOffset(1, dec.More()).To(BeFalse(), "trailing content")
	return got
}

// jsonObj renders one compact {key:literal,…} object from CSV-shaped
// literals — "" is null, anything else the same literal as a number —
// in key order: the daily.json writer's row shape, written here from
// the CSV expectations so the two formats are held to one product.
func jsonObj(keys, literals []string) string {
	ExpectWithOffset(1, literals).To(HaveLen(len(keys)))
	parts := make([]string, len(keys))
	for i, k := range keys {
		v := literals[i]
		if v == "" {
			v = "null"
		}
		parts[i] = `"` + k + `":` + v
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// dailyRow is one daily.json row; a nil reanalysis is the whole-object
// null.
func dailyRow(date string, observed, reanalysis []string) string {
	re := "null"
	if reanalysis != nil {
		re = jsonObj(wantPower31, reanalysis)
	}
	return `{"date":"` + date + `","observed":` + jsonObj(wantDailyHeader[2:], observed) + `,"reanalysis":` + re + `}`
}

// dailyDoc is a daily.json document: the rows one per line inside a
// top-level array, a trailing LF; an empty series is "[\n]\n".
func dailyDoc(rows ...string) string {
	if len(rows) == 0 {
		return "[\n]\n"
	}
	return "[\n" + strings.Join(rows, ",\n") + "\n]\n"
}

// dailyDocFromCombined derives a station's daily.json from its
// combined_daily CSV expectation: the same dates, the observed block
// from the spine columns, the reanalysis block from POWER-31 — null as
// a whole where the CSV carries 31 empty fields, which in this seed
// (no all-NULL supplement row) is exactly "no POWER row for the date".
func dailyDocFromCombined(want [][]string) string {
	rows := make([]string, 0, len(want)-1)
	for _, r := range want[1:] {
		observed := r[4:8]
		reanalysis := r[8:]
		if slices.Equal(reanalysis, publishPowerEmpty) {
			reanalysis = nil
		}
		rows = append(rows, dailyRow(r[1], observed, reanalysis))
	}
	return dailyDoc(rows...)
}

// fieldText renders one decoded JSON field as the CSV carries it: a
// string verbatim, a number's literal, "" for null, an object compacted.
func fieldText(raw json.RawMessage) string {
	switch {
	case string(raw) == "null":
		return ""
	case raw[0] == '"':
		var str string
		ExpectWithOffset(1, json.Unmarshal(raw, &str)).To(Succeed())
		return str
	case raw[0] == '{':
		var compact bytes.Buffer
		ExpectWithOffset(1, json.Compact(&compact, raw)).To(Succeed())
		return compact.String()
	default:
		return string(raw)
	}
}

// expectJSONRowsEqualCSV holds a provenance JSON array to its CSV
// expectation cell for cell: one object per data row, exactly the
// header's keys, every value the CSV's text.
func expectJSONRowsEqualCSV(doc string, want [][]string) {
	var objects []map[string]json.RawMessage
	ExpectWithOffset(1, json.Unmarshal([]byte(doc), &objects)).To(Succeed())
	ExpectWithOffset(1, objects).To(HaveLen(len(want) - 1))
	header := want[0]
	for i, obj := range objects {
		ExpectWithOffset(1, obj).To(HaveLen(len(header)), "row %d", i+1)
		for j, key := range header {
			ExpectWithOffset(1, obj).To(HaveKey(key), "row %d", i+1)
			ExpectWithOffset(1, fieldText(obj[key])).To(Equal(want[i+1][j]), "row %d, %s", i+1, key)
		}
	}
}

// csvText renders rows as the exact bytes an RFC-4180 writer emits —
// LF line endings, quoting only where a field needs it — so an archive
// entry compares byte for byte against its expectation.
func csvText(rows [][]string) string {
	var sb strings.Builder
	w := csv.NewWriter(&sb)
	ExpectWithOffset(1, w.WriteAll(rows)).To(Succeed())
	return sb.String()
}

// zipEntry is one archive member as read back through archive/zip.
type zipEntry struct {
	Name     string
	Modified time.Time
	Content  string
}

func readZipEntries(path string) []zipEntry {
	zr, err := zip.OpenReader(path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer func() { _ = zr.Close() }()
	ExpectWithOffset(1, zr.Comment).To(BeEmpty())
	var out []zipEntry
	for _, f := range zr.File {
		rc, err := f.Open()
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		data, err := io.ReadAll(rc)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		ExpectWithOffset(1, rc.Close()).To(Succeed())
		out = append(out, zipEntry{Name: f.Name, Modified: f.Modified.UTC(), Content: string(data)})
	}
	return out
}

func entryNames(entries []zipEntry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	return names
}

func entryContent(entries []zipEntry, name string) string {
	for _, e := range entries {
		if e.Name == name {
			return e.Content
		}
	}
	Fail("no entry " + name)
	return ""
}

// expectEntryCSV compares one archive entry, byte for byte, against the
// RFC-4180 rendering of want.
func expectEntryCSV(entries []zipEntry, name string, want [][]string) {
	ExpectWithOffset(1, entryContent(entries, name)).To(Equal(csvText(want)), name)
}

// csvEntries is the subset of an archive's entries that are CSV files —
// the ones the text-shape checks (no BOM, LF, a trailing newline, the
// other state's values absent as substrings) apply to; the Parquet and
// SQLite entries are binary and are held to their content instead.
func csvEntries(entries []zipEntry) []zipEntry {
	var out []zipEntry
	for _, e := range entries {
		if strings.HasSuffix(e.Name, ".csv") {
			out = append(out, e)
		}
	}
	return out
}

// parquetDecimals is the pinned decimal count of every REAL column the
// Parquet twins carry, written by hand from the precision table. Two
// groups are keyed by file, because the same leaf name carries two
// counts: the grid cells' lat/lon (3) against the stations file's 6,
// and the four observed values (tmax_c, tmin_c, precip_mm, evap_mm) at
// the daily grain's 2 against the normals' 1 — the daily observations
// are published at two decimals, the normals at one.
func parquetDecimals(file, column string) int {
	if file == "nasa_power/cells.parquet" && (column == "lat" || column == "lon") {
		return 3
	}
	if file == "conagua/daily_observations.parquet" || file == "combined/combined_daily.parquet" {
		switch column {
		case "tmax_c", "tmin_c", "precip_mm", "evap_mm":
			return 2
		}
	}
	switch column {
	case "lat", "lon":
		return 6
	case "altitude_m", "tmax_c", "tmin_c", "tmean_c", "precip_mm", "evap_mm", "rain_days",
		"tmax_monthly_extreme_c", "tmin_monthly_extreme_c", "precip_monthly_extreme_mm",
		"wd2m_deg", "wd10m_deg":
		return 1
	case "tmax_daily_extreme_c", "tmin_daily_extreme_c", "precip_daily_extreme_mm":
		return 2
	case "distance_km":
		return 3
	}
	if strings.HasPrefix(column, "wmo_completeness_") {
		return 4
	}
	if slices.Contains(wantPower31, column) {
		return 2
	}
	Fail("no decimals for REAL column " + column + " in " + file)
	return 0
}

// fixedDecimal renders a double at decimals as the CSV cell does: the
// exact binary value correctly rounded, a value that rounds to zero as
// positive zero.
func fixedDecimal(v float64, decimals int) string {
	s := strconv.FormatFloat(v, 'f', decimals, 64)
	if strings.HasPrefix(s, "-") && strings.Trim(s[1:], "0.") == "" {
		return s[1:]
	}
	return s
}

// parquetCells decodes one Parquet entry with the library: its leaf
// names in schema order, and every row as CSV cells — a null as the
// empty cell, a double at the column's decimals, an int64 as digits,
// bytes verbatim — refusing any other physical type; the writer string
// is the pinned one, never a timestamp.
func parquetCells(name, data string) (header []string, rows [][]string) {
	f, err := parquet.OpenFile(strings.NewReader(data), int64(len(data)))
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), name)
	ExpectWithOffset(1, f.Metadata().CreatedBy).To(Equal("conagua-etl version 0.1(build parquet-go-v0.32.0)"), name)
	ExpectWithOffset(1, f.Metadata().KeyValueMetadata).To(BeEmpty(), name)
	for _, field := range f.Schema().Fields() {
		header = append(header, field.Name())
	}
	r := parquet.NewReader(f)
	defer func() { _ = r.Close() }()
	buf := make([]parquet.Row, 16)
	for {
		n, err := r.ReadRows(buf)
		for _, row := range buf[:n] {
			ExpectWithOffset(1, row).To(HaveLen(len(header)), name)
			cells := make([]string, len(header))
			for i, v := range row {
				ExpectWithOffset(1, v.Column()).To(Equal(i), name)
				switch {
				case v.IsNull():
					cells[i] = ""
				case v.Kind() == parquet.Double:
					cells[i] = fixedDecimal(v.Double(), parquetDecimals(name, header[i]))
				case v.Kind() == parquet.Int64:
					cells[i] = strconv.FormatInt(v.Int64(), 10)
				case v.Kind() == parquet.ByteArray:
					cells[i] = string(v.ByteArray())
				default:
					Fail(fmt.Sprintf("%s: column %s has physical type %v", name, header[i], v.Kind()))
				}
			}
			rows = append(rows, cells)
		}
		if errors.Is(err, io.EOF) {
			return header, rows
		}
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), name)
	}
}

// csvTwinRows parses the CSV entries that carry a Parquet twin's table
// — the one whole-table file, or the per-station / per-cell shards in
// entry order — and returns the header they all share and their rows
// concatenated, headers dropped.
func csvTwinRows(entries []zipEntry, table string) (header []string, rows [][]string) {
	for _, e := range entries {
		if e.Name != table+".csv" && !strings.HasPrefix(e.Name, table+"/") {
			continue
		}
		recs, err := csv.NewReader(strings.NewReader(e.Content)).ReadAll()
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), e.Name)
		if header == nil {
			header = recs[0]
		} else {
			ExpectWithOffset(1, recs[0]).To(Equal(header), e.Name)
		}
		rows = append(rows, recs[1:]...)
	}
	ExpectWithOffset(1, header).NotTo(BeNil(), table)
	return header, rows
}

// expectParquetTwinsEqualCSV holds each of an archive's ten Parquet
// twins to its CSV twin(s): the leaf names are the CSV header, and every
// column of every row, at the column's decimals, is the CSV cell — the
// three flat formats carry one value, in one row order.
func expectParquetTwinsEqualCSV(entries []zipEntry) {
	expectParquetEqualCSV(entries, entries)
}

// expectParquetEqualCSV holds the ten Parquet files among parquet to the
// CSV files of the same tables among csv — the whole-table files, or
// the per-station and per-cell shards in entry order, which for the
// seed is the national key order across both states as well as within
// one (every Aguascalientes key sorts before every Yucatán key).
func expectParquetEqualCSV(parquet, csv []zipEntry) {
	for _, table := range wantParquetTables {
		name := table + ".parquet"
		header, rows := parquetCells(name, entryContent(parquet, name))
		wantHeader, wantRows := csvTwinRows(csv, table)
		ExpectWithOffset(2, header).To(Equal(wantHeader), name)
		ExpectWithOffset(2, rows).To(Equal(wantRows), name)
	}
}

// openArchiveDB extracts an archive's SQLite entry to a fresh directory
// and opens it through the read-only opener, returning the handle and
// the directory — which must hold the one file: a rollback-journal
// database needs no side file to be read.
func openArchiveDB(entries []zipEntry, name string) (*sql.DB, string) {
	dir := GinkgoT().TempDir()
	path := filepath.Join(dir, name)
	ExpectWithOffset(1, os.WriteFile(path, []byte(entryContent(entries, name)), 0o600)).To(Succeed())
	db, err := schema.OpenReadOnly(path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	DeferCleanup(func() error { return db.Close() })
	return db, dir
}

// queryStrings runs a one-column query and returns its values as text.
func queryStrings(db *sql.DB, query string, args ...any) []string {
	rows, err := db.Query(query, args...)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), query)
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		ExpectWithOffset(1, rows.Scan(&s)).To(Succeed())
		out = append(out, s)
	}
	ExpectWithOffset(1, rows.Err()).NotTo(HaveOccurred())
	return out
}

func queryInt(db *sql.DB, query string, args ...any) int {
	var n int
	ExpectWithOffset(1, db.QueryRow(query, args...).Scan(&n)).To(Succeed(), query)
	return n
}

// tableRows reads every column of table's rows matching where, in a
// total order over all columns, as the driver's native values — so two
// databases compare column for column, NULLs and REAL bits included.
func tableRows(db *sql.DB, table, where string, args ...any) [][]any {
	n := queryInt(db, `SELECT COUNT(*) FROM pragma_table_info(?)`, table)
	ExpectWithOffset(1, n).To(BeNumerically(">", 0), table)
	order := make([]string, n)
	for i := range order {
		order[i] = strconv.Itoa(i + 1)
	}
	q := "SELECT * FROM " + table
	if where != "" {
		q += " WHERE " + where
	}
	rows, err := db.Query(q+" ORDER BY "+strings.Join(order, ", "), args...)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), q)
	defer func() { _ = rows.Close() }()
	var out [][]any
	for rows.Next() {
		row := make([]any, n)
		ptrs := make([]any, n)
		for i := range row {
			ptrs[i] = &row[i]
		}
		ExpectWithOffset(1, rows.Scan(ptrs...)).To(Succeed())
		out = append(out, row)
	}
	ExpectWithOffset(1, rows.Err()).NotTo(HaveOccurred())
	return out
}

// The row set of a state's database, table by table, as a WHERE
// clause over the seeded source: the state's conventional stations and
// what hangs off them; the referenced cell(s) and their rows; every run.
const (
	whereStateStations = `state = ? AND source = 'conagua_conventional'`
	whereStationRows   = `station_id IN (SELECT id FROM stations WHERE state = ? AND source = 'conagua_conventional')`
	whereCellRows      = `cell_id IN (SELECT m.cell_id FROM station_power_cell m JOIN stations s ON s.id = m.station_id
	                       WHERE s.state = ? AND s.source = 'conagua_conventional')`
)

// expectStateDB holds an extracted {state}.db to its specified shape and
// the state's row set of the source at srcPath: the stamp, rollback-journal
// mode, integrity, every FK resolvable in-file, all eleven tables, and
// every table's rows — every column — equal to the source's rows
// filtered that way; nothing of any other state.
func expectStateDB(entries []zipEntry, srcPath, slug, code string) {
	got, dir := openArchiveDB(entries, slug+".db")
	ExpectWithOffset(1, listDir(dir)).To(Equal([]string{slug + ".db"}))
	ExpectWithOffset(1, queryInt(got, "PRAGMA user_version")).To(Equal(schema.Version))
	ExpectWithOffset(1, queryStrings(got, "PRAGMA journal_mode")).To(Equal([]string{"delete"}))
	ExpectWithOffset(1, queryStrings(got, "PRAGMA integrity_check")).To(Equal([]string{"ok"}))
	ExpectWithOffset(1, queryInt(got, "SELECT COUNT(*) FROM pragma_foreign_key_check")).To(BeZero())
	ExpectWithOffset(1, queryStrings(got,
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)).To(Equal([]string{
		"daily_observations", "daily_supplement", "ingest_runs", "monthly_normals", "monthly_normals_extras",
		"monthly_supplement", "nasa_power_grid_cells", "parsing_warnings", "power_runs", "station_power_cell", "stations",
	}))

	src, err := schema.OpenReadOnly(srcPath)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer func() { _ = src.Close() }()
	for _, c := range []struct {
		table string
		where string
		args  []any
	}{
		{"stations", whereStateStations, []any{code}},
		{"monthly_normals", whereStationRows, []any{code}},
		{"monthly_normals_extras", whereStationRows, []any{code}},
		{"daily_observations", whereStationRows, []any{code}},
		{"parsing_warnings", whereStationRows, []any{code}},
		{"station_power_cell", whereStationRows, []any{code}},
		{"nasa_power_grid_cells", whereCellRows, []any{code}},
		{"monthly_supplement", whereCellRows, []any{code}},
		{"daily_supplement", whereCellRows, []any{code}},
		{"ingest_runs", "", nil},
		{"power_runs", "", nil},
	} {
		ExpectWithOffset(1, tableRows(got, c.table, "")).To(Equal(tableRows(src, c.table, c.where, c.args...)), c.table)
	}
	ExpectWithOffset(1, queryInt(got, `SELECT COUNT(*) FROM stations WHERE state <> ?`, code)).To(BeZero())
	// Both run tables are whole: the aborted runs ride along too.
	ExpectWithOffset(1, queryInt(got, `SELECT COUNT(*) FROM ingest_runs`)).To(Equal(2))
	ExpectWithOffset(1, queryInt(got, `SELECT COUNT(*) FROM ingest_runs WHERE status = 'aborted'`)).To(Equal(1))
	ExpectWithOffset(1, queryInt(got, `SELECT COUNT(*) FROM power_runs`)).To(Equal(4))
	ExpectWithOffset(1, queryInt(got, `SELECT COUNT(*) FROM power_runs WHERE status = 'aborted'`)).To(Equal(1))
}

// startPublish launches the verb on dbPath into out and returns the
// live session; the caller decides how it ends.
func startPublish(dbPath, out string, extra ...string) *gexec.Session {
	args := append([]string{"publish", "--db", dbPath, "--out", out}, extra...)
	session, err := gexec.Start(exec.Command(binPath, args...), GinkgoWriter, GinkgoWriter)
	ExpectWithOffset(2, err).NotTo(HaveOccurred())
	return session
}

func runPublishToExit(dbPath, out string, extra ...string) *gexec.Session {
	session := startPublish(dbPath, out, extra...)
	EventuallyWithOffset(1, session, 30*time.Second).Should(gexec.Exit())
	return session
}

func fileSHA(path string) string {
	hexsum, _, err := archive.SHA256File(path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return hexsum
}

func sizeOf(path string) int64 {
	st, err := os.Stat(path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return st.Size()
}

func readBytes(path string) []byte {
	data, err := os.ReadFile(path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return data
}

func listDir(dir string) []string {
	entries, err := os.ReadDir(dir)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// expectNoTempResidue walks dir and fails on any leftover of the
// atomic-write discipline: a dot-prefixed ".tmp-" sibling anywhere —
// the archive writer's temp file or the run's temp directory.
func expectNoTempResidue(dir string) {
	var residue []string
	ExpectWithOffset(1, filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p != dir && strings.Contains(d.Name(), ".tmp-") {
			residue = append(residue, p)
		}
		return nil
	})).To(Succeed())
	ExpectWithOffset(1, residue).To(BeEmpty())
}

// readChecksums parses CHECKSUMS and verifies every listed digest
// against a fresh sha256 of the file beside it, returning the entries
// in file order.
func readChecksums(dir string) []archive.Checksum {
	f, err := os.Open(filepath.Join(dir, "CHECKSUMS"))
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	sums, err := archive.ParseChecksums(f)
	ExpectWithOffset(1, f.Close()).To(Succeed())
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	for _, s := range sums {
		ExpectWithOffset(1, s.SHA256).To(Equal(fileSHA(filepath.Join(dir, s.Name))), s.Name)
	}
	return sums
}

func checksumNames(sums []archive.Checksum) []string {
	names := make([]string, len(sums))
	for i, s := range sums {
		names[i] = s.Name
	}
	return names
}

func readManifest(dir string) (publish.Manifest, string) {
	data := readBytes(filepath.Join(dir, "manifest.json"))
	var m publish.Manifest
	ExpectWithOffset(1, json.Unmarshal(data, &m)).To(Succeed())
	return m, string(data)
}

// expectPowerRuns compares the decoded power runs to their expectation:
// the embedded unit_conversions by compacted text (the manifest encoder
// re-indents the raw message, but key order and number literals are
// untouched, which is what proves it was never re-serialized), every
// other field exactly.
func expectPowerRuns(got, want []publish.PowerRunRef) {
	ExpectWithOffset(1, got).To(HaveLen(len(want)))
	for i := range want {
		var compact bytes.Buffer
		ExpectWithOffset(1, json.Compact(&compact, got[i].UnitConversions)).To(Succeed())
		ExpectWithOffset(1, compact.String()).To(Equal(publishConversions), want[i].RunLabel)
		g := got[i]
		g.UnitConversions = nil
		ExpectWithOffset(1, g).To(Equal(want[i]))
	}
}

// expectUnitLines asserts the captured unit lines are exactly units in
// order, numbered from 1 over total — the archive's unit count, which
// exceeds len(units) only when the archive failed before its last unit.
func expectUnitLines(lines [][]string, artifact string, units []string, total int) {
	ExpectWithOffset(1, lines).To(HaveLen(len(units)))
	for i, u := range units {
		ExpectWithOffset(1, lines[i][1:]).To(Equal([]string{
			strconv.Itoa(i + 1), strconv.Itoa(total), artifact, u,
		}))
	}
}

var (
	publishUnitLineRE     = regexp.MustCompile(`(?m)^\[\d{2}:\d{2}:\d{2}\] (\d+)/(\d+) ok   (\S+) (\S+)$`)
	publishArtifactLineRE = regexp.MustCompile(`(?m)^\[\d{2}:\d{2}:\d{2}\] (ok|FAIL)\s+(\S+) · entries=(\d+) bytes=(\d+) sha256=(\S+) · elapsed \S+(?: · (.*))?$`)
	generatedAtRE         = regexp.MustCompile(`"generated_at": "[^"]*"`)
)

var _ = Describe("conagua-etl publish end-to-end", func() {
	Context("against a seeded two-state DB", Ordered, func() {
		var (
			dbPath, dbSHA    string
			outRoot          string
			firstOut         string
			firstRunStarted  time.Time
			firstRunFinished time.Time
			yucEntries       []zipEntry
			yucJSON          []zipEntry
		)

		// The out dirs outlive one spec — later specs compare against the
		// first build's bytes — so they live under a context-lifetime
		// root rather than a per-spec TempDir.
		BeforeAll(func() {
			dbPath = seedPublishDB()
			dbSHA = fileSHA(dbPath)
			var err error
			outRoot, err = os.MkdirTemp("", "publish-e2e-out-*")
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() error { return os.RemoveAll(outRoot) })
		})

		It("builds one state with exit 0: both archives, an ok line per unit numbered across each, one per artifact, the summary, exactly four files", func() {
			firstOut = filepath.Join(outRoot, "publish")
			firstRunStarted = time.Now().UTC().Truncate(time.Second)
			session := runPublishToExit(dbPath, firstOut, "--state", "yuc")
			firstRunFinished = time.Now().UTC()
			Expect(session.ExitCode()).To(Equal(0))
			// Read-only posture: not one byte of the DB moved.
			Expect(fileSHA(dbPath)).To(Equal(dbSHA))

			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring("db=" + dbPath + " (read-only)  out=" + firstOut + "  root=./snapshots  states=yuc  only=all  doi=none\n"))
			Expect(stderr).NotTo(ContainSubstring("FAIL"))
			// The gate's lines come first, one per rule in execution order,
			// every one ok: the seed's two stations without coordinates are
			// bbox warnings, and no rule finds an error.
			expectGateLines(stderr, wantSeededGate)
			Expect(strings.Index(stderr, "] gate ok")).To(BeNumerically("<", strings.Index(stderr, "] 1/")))
			// The tabular archive's units, then the JSON archive's — one per
			// station, labelled by the profile.json that completes its pair.
			units := publishUnitLineRE.FindAllStringSubmatch(stderr, -1)
			Expect(units).To(HaveLen(len(wantYucUnits) + len(wantYucJSONUnits)))
			expectUnitLines(units[:len(wantYucUnits)], "yuc-tabular.zip", wantYucUnits, len(wantYucUnits))
			expectUnitLines(units[len(wantYucUnits):], "yuc-json.zip", wantYucJSONUnits, len(wantYucJSONUnits))
			artifacts := publishArtifactLineRE.FindAllStringSubmatch(stderr, -1)
			Expect(artifacts).To(HaveLen(2))
			for i, want := range []struct {
				name    string
				entries int
			}{{"yuc-tabular.zip", len(wantYucPaths)}, {"yuc-json.zip", len(wantYucJSONPaths)}} {
				Expect(artifacts[i][1:4]).To(Equal([]string{"ok", want.name, strconv.Itoa(want.entries)}))
				zipPath := filepath.Join(firstOut, want.name)
				Expect(artifacts[i][4]).To(Equal(strconv.FormatInt(sizeOf(zipPath), 10)))
				Expect(artifacts[i][5]).To(Equal(fileSHA(zipPath)[:12]))
			}

			stdout := string(session.Out.Contents())
			Expect(stdout).To(ContainSubstring("\npublish complete\n"))
			Expect(stdout).To(ContainSubstring("  snapshot date        : " + publishSnapshotDate + "\n"))
			Expect(stdout).To(ContainSubstring("  gate                 : passed · 9 rules · 2 warn · 0 error\n"))
			Expect(stdout).To(ContainSubstring("  states               : YUC\n"))
			Expect(stdout).To(ContainSubstring("  groups               : tabular,json\n"))
			Expect(stdout).To(ContainSubstring("  artifacts            : 2 ok / 0 failed / 2 attempted\n"))
			Expect(stdout).To(ContainSubstring("  top-level files      : 4\n"))
			Expect(stdout).To(ContainSubstring("  out dir              : " + firstOut + "\n"))
			Expect(stdout).To(MatchRegexp(`  elapsed              : \d+s\n`))
			Expect(stdout).NotTo(ContainSubstring("failed artifacts"))

			Expect(listDir(firstOut)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-json.zip", "yuc-tabular.zip"}))
			expectNoTempResidue(firstOut)
		})

		It("lists exactly the four scope folders' entries, the ten Parquet twins, and yuc.db in yuc-tabular.zip, sorted and snapshot-stamped, none of the other state", func() {
			yucEntries = readZipEntries(filepath.Join(firstOut, "yuc-tabular.zip"))
			names := entryNames(yucEntries)
			Expect(names).To(Equal(wantYucPaths))
			Expect(slices.IsSorted(names)).To(BeTrue())
			midnight := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
			for _, e := range yucEntries {
				Expect(e.Modified).To(Equal(midnight), e.Name)
			}
			for _, e := range csvEntries(yucEntries) {
				// UTF-8 without BOM, LF only, header row present.
				Expect(e.Content).NotTo(HavePrefix("\xef\xbb\xbf"), e.Name)
				Expect(e.Content).NotTo(ContainSubstring("\r"), e.Name)
				Expect(e.Content).To(HaveSuffix("\n"), e.Name)
				// The other state is absent from every file in this
				// archive: no AGS value, no row keyed by an AGS station,
				// and not the cell only AGS references.
				Expect(e.Content).NotTo(ContainSubstring("AGS"), e.Name)
				Expect(e.Content).NotTo(ContainSubstring(publishCellAGS), e.Name)
				for _, id := range []string{"1001", "1002", "1010"} {
					Expect(e.Content).NotTo(ContainSubstring("\n"+id+","), e.Name)
				}
			}
		})

		It("round-trips every column of every conagua/ CSV", func() {
			expectEntryCSV(yucEntries, "conagua/stations.csv", wantYucStations)
			expectEntryCSV(yucEntries, "conagua/monthly_normals.csv", wantYucNormals)
			expectEntryCSV(yucEntries, "conagua/monthly_normals_extras.csv", wantYucExtras)
			expectEntryCSV(yucEntries, "conagua/daily_observations/yuc/daily-31001.csv", wantMeridaDaily)
			expectEntryCSV(yucEntries, "conagua/daily_observations/yuc/daily-31002.csv", wantTiziminDaily)
			expectEntryCSV(yucEntries, "conagua/daily_observations/yuc/daily-3101.csv", wantValladolidDaily)
			// A station with no observations still gets its header-only
			// file, so the archive's station set matches stations.csv.
			expectEntryCSV(yucEntries, "conagua/daily_observations/yuc/daily-31019.csv", [][]string{wantDailyHeader})

			// The literal lines pin what the CSV writer above would
			// otherwise share with the binary: RFC-4180 quoting of the
			// one field that needs it, and an all-NULL tail as bare commas.
			stations := entryContent(yucEntries, "conagua/stations.csv")
			Expect(stations).To(HavePrefix(strings.Join(wantStationsHeader, ",") + "\n"))
			Expect(stations).To(ContainSubstring(
				"\n3101,\"VALLADOLID, CENTRO\",YUC,Valladolid,20.689100,-88.201100,25.5,operating,1961,2026,,,,,,,,\n"))
			Expect(stations).To(HaveSuffix("\n31019,PROGRESO,YUC" + strings.Repeat(",", 15) + "\n"))
		})

		It("round-trips every column of every nasa_power/ CSV: the referenced cell once, the map, monthly, the per-cell daily", func() {
			expectEntryCSV(yucEntries, "nasa_power/cells.csv", wantYucCells)
			expectEntryCSV(yucEntries, "nasa_power/station_cell_map.csv", wantYucCellMap)
			expectEntryCSV(yucEntries, "nasa_power/monthly.csv", wantYucPowerMonthly)
			expectEntryCSV(yucEntries, "nasa_power/daily/daily-"+publishCellYUC+".csv", wantYucPowerDaily)

			// An all-NULL POWER tail is a run of bare commas; a full one
			// carries the pinned decimals.
			daily := entryContent(yucEntries, "nasa_power/daily/daily-"+publishCellYUC+".csv")
			Expect(daily).To(ContainSubstring("\n" + publishCellYUC + ",2020-01-01," + strings.Join(publishPowerFullText, ",") + "\n"))
			Expect(daily).To(HaveSuffix("\n" + publishCellYUC + ",2020-02-29,20.00" + strings.Repeat(",", 13) + "5.1" +
				strings.Repeat(",", 13) + "0.00" + strings.Repeat(",", 4) + "\n"))
		})

		It("round-trips every column of every combined/ CSV: the combined LEFT join on all four periods and on observed dates", func() {
			expectEntryCSV(yucEntries, "combined/combined_monthly.csv", wantYucCombinedMonthly)
			expectEntryCSV(yucEntries, "combined/combined_daily/yuc/daily-31001.csv", wantMeridaCombinedDaily)
			expectEntryCSV(yucEntries, "combined/combined_daily/yuc/daily-3101.csv", wantValladolidCombinedDaily)
			// The cell-less station keeps its rows with an empty join
			// context and empty POWER-31.
			expectEntryCSV(yucEntries, "combined/combined_daily/yuc/daily-31002.csv", wantTiziminCombinedDaily)
			expectEntryCSV(yucEntries, "combined/combined_daily/yuc/daily-31019.csv", [][]string{wantCombinedDailyHeader})

			// The raw pre-1981 line: the observed spine present, the 31
			// POWER fields bare commas.
			merida := entryContent(yucEntries, "combined/combined_daily/yuc/daily-31001.csv")
			Expect(merida).To(ContainSubstring("\n31001,1980-12-31," + publishCellYUC + ",33.100,28.00,14.50,0.00,3.10" +
				strings.Repeat(",", 31) + "\n"))
			// The observed spine keeps the hundredths through the join:
			// 12.46 mm of rain, not the 12.5 a one-decimal export wrote.
			Expect(merida).To(ContainSubstring("\n31001,2020-01-01," + publishCellYUC + ",33.100,30.50,,12.46,," +
				strings.Join(publishPowerFullText, ",") + "\n"))
			Expect(merida).NotTo(ContainSubstring(",33.100,30.5,,12.5,,"))
		})

		It("round-trips every column of both provenance/ CSVs: the latest complete ingest run, the referenced power runs by label, the unreferenced run absent", func() {
			expectEntryCSV(yucEntries, "provenance/ingest_runs.csv", wantIngestRunsCSV)
			expectEntryCSV(yucEntries, "provenance/power_runs.csv", wantPowerRunsCSV)

			runs := entryContent(yucEntries, "provenance/power_runs.csv")
			Expect(runs).NotTo(ContainSubstring("aborted"))
			Expect(runs).NotTo(ContainSubstring("ab0rt"))
			// unit_conversions and the comma-joined parameters are the
			// stored text, RFC-4180 quoted.
			Expect(runs).To(ContainSubstring(`,"T2M,ALLSKY_SFC_SW_DWN",AG,1981,2010,0.5x0.625,"` +
				strings.ReplaceAll(publishConversions, `"`, `""`) + `",monthly,,,2,2,0,3,p0w3r` + "\n"))
		})

		It("round-trips every column of every row of the ten Parquet twins against the CSV entries of their tables, and pins the three requested by hand", func() {
			expectParquetTwinsEqualCSV(yucEntries)

			// The same three files held directly to the hand-written
			// expectations: one value across the flat formats, the
			// per-station tables whole in station order, the pre-1981 and
			// cell-less rows with their nulls.
			header, rows := parquetCells("conagua/stations.parquet", entryContent(yucEntries, "conagua/stations.parquet"))
			Expect(header).To(Equal(wantStationsHeader))
			Expect(rows).To(Equal(wantYucStations[1:]))
			header, rows = parquetCells("conagua/daily_observations.parquet", entryContent(yucEntries, "conagua/daily_observations.parquet"))
			Expect(header).To(Equal(wantDailyHeader))
			Expect(rows).To(Equal(slices.Concat(wantMeridaDaily[1:], wantTiziminDaily[1:], wantValladolidDaily[1:])))
			header, rows = parquetCells("combined/combined_daily.parquet", entryContent(yucEntries, "combined/combined_daily.parquet"))
			Expect(header).To(Equal(wantCombinedDailyHeader))
			Expect(rows).To(Equal(slices.Concat(wantMeridaCombinedDaily[1:], wantTiziminCombinedDaily[1:], wantValladolidCombinedDaily[1:])))
			for _, row := range rows {
				Expect(row).NotTo(ContainElement("AGS"))
				Expect(row).NotTo(ContainElement(publishCellAGS))
			}
		})

		It("ships yuc.db: the full schema, stamped, in rollback-journal mode, integrity ok, FKs resolvable, every table the per-state row set of the source — the state's stations and their rows, the referenced cell's POWER rows, every run, the state's warnings — and nothing of the other state", func() {
			expectStateDB(yucEntries, dbPath, "yuc", "YUC")
			got, _ := openArchiveDB(yucEntries, "yuc.db")

			// The station and cell sets are the CSVs' by natural key.
			stations, err := csv.NewReader(strings.NewReader(entryContent(yucEntries, "conagua/stations.csv"))).ReadAll()
			Expect(err).NotTo(HaveOccurred())
			var wantIDs []string
			for _, r := range stations[1:] {
				wantIDs = append(wantIDs, r[0])
			}
			Expect(queryStrings(got, `SELECT external_id FROM stations ORDER BY external_id`)).To(Equal(wantIDs))
			Expect(queryInt(got, `SELECT COUNT(*) FROM stations`)).To(Equal(len(wantYucStations) - 1))
			cells, err := csv.NewReader(strings.NewReader(entryContent(yucEntries, "nasa_power/cells.csv"))).ReadAll()
			Expect(err).NotTo(HaveOccurred())
			var gotCells [][]string
			rows, err := got.Query(`SELECT cell_id, lat, lon FROM nasa_power_grid_cells ORDER BY cell_id`)
			Expect(err).NotTo(HaveOccurred())
			for rows.Next() {
				var id string
				var lat, lon float64
				Expect(rows.Scan(&id, &lat, &lon)).To(Succeed())
				gotCells = append(gotCells, []string{id, fixedDecimal(lat, 3), fixedDecimal(lon, 3)})
			}
			Expect(rows.Close()).To(Succeed())
			Expect(gotCells).To(Equal(cells[1:]))
			Expect(gotCells).To(Equal(wantYucCells[1:]))
			Expect(queryInt(got, `SELECT COUNT(*) FROM nasa_power_grid_cells WHERE cell_id = ?`, publishCellAGS)).To(BeZero())

			// The seed's counts: the three stations' daily rows, the
			// cell's supplement rows, the one warning — for a YUC station.
			Expect(queryInt(got, `SELECT COUNT(*) FROM daily_observations`)).To(Equal(7))
			Expect(queryInt(got, `SELECT COUNT(*) FROM monthly_normals`)).To(Equal(len(wantYucNormals) - 1))
			Expect(queryInt(got, `SELECT COUNT(*) FROM monthly_normals_extras`)).To(Equal(len(wantYucExtras) - 1))
			Expect(queryInt(got, `SELECT COUNT(*) FROM station_power_cell`)).To(Equal(2))
			Expect(queryInt(got, `SELECT COUNT(*) FROM monthly_supplement`)).To(Equal(3))
			Expect(queryInt(got, `SELECT COUNT(*) FROM daily_supplement`)).To(Equal(3))
			Expect(queryInt(got, `SELECT COUNT(*) FROM parsing_warnings`)).To(Equal(1))
			Expect(queryInt(got, `SELECT COUNT(*) FROM parsing_warnings w JOIN stations s ON s.id = w.station_id WHERE s.external_id = '31001'`)).To(Equal(1))
			// The surrogate plumbing the flat files drop is here.
			Expect(queryInt(got, `SELECT COUNT(*) FROM daily_supplement WHERE power_run_id IS NULL`)).To(BeZero())
			Expect(fileSHA(dbPath)).To(Equal(dbSHA))
		})

		It("lists exactly the per-station pairs and the provenance pair in yuc-json.zip, sorted and snapshot-stamped, none of the other state, no generation stamp", func() {
			yucJSON = readZipEntries(filepath.Join(firstOut, "yuc-json.zip"))
			names := entryNames(yucJSON)
			Expect(names).To(Equal(wantYucJSONPaths))
			Expect(slices.IsSorted(names)).To(BeTrue())
			midnight := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
			for _, e := range yucJSON {
				Expect(e.Modified).To(Equal(midnight), e.Name)
				// UTF-8 without BOM, LF only, a trailing newline, valid JSON.
				Expect(e.Content).NotTo(HavePrefix("\xef\xbb\xbf"), e.Name)
				Expect(e.Content).NotTo(ContainSubstring("\r"), e.Name)
				Expect(e.Content).To(HaveSuffix("\n"), e.Name)
				Expect(json.Valid([]byte(e.Content))).To(BeTrue(), e.Name)
				// The other state is absent; nothing is stamped with the
				// wall clock; no personal identity anywhere.
				Expect(e.Content).NotTo(ContainSubstring("AGS"), e.Name)
				Expect(e.Content).NotTo(ContainSubstring(publishCellAGS), e.Name)
				Expect(e.Content).NotTo(ContainSubstring("generated"), e.Name)
				Expect(strings.ToLower(e.Content)).NotTo(ContainSubstring("orcid"), e.Name)
			}
		})

		It("round-trips every field of the fully seeded station's profile.json: the annual value on the complete period, null annuals on the partial ones, null annual slots in extras, the cell, POWER monthly, the station summary, the meta", func() {
			raw := entryContent(yucJSON, "combined/yuc/31001/profile.json")
			Expect(decodeJSONDoc(raw)).To(Equal(wantMeridaProfile()))

			// The literal renderings a decoder would otherwise hide:
			// two-space indent, block order, fixed-decimal numbers, no
			// HTML escaping, a trailing newline.
			Expect(raw).To(HavePrefix("{\n  \"station_id\": \"31001\",\n  \"identity\": {\n    \"name\": \"MERIDA (OBS)\",\n"))
			Expect(raw).To(ContainSubstring("\"lat\": 20.980278,\n"))
			Expect(raw).To(ContainSubstring("\"altitude_m\": 10.0,\n"))
			Expect(raw).To(ContainSubstring("\"distance_km\": 33.100\n"))
			Expect(raw).To(ContainSubstring("\"solar_ghi_wm2\": [\n          228.47,\n"))
			Expect(raw).NotTo(ContainSubstring("placeholder"))
			Expect(raw).NotTo(ContainSubstring("<"))
			Expect(raw).NotTo(ContainSubstring(`\u003c`))
			Expect(raw).NotTo(ContainSubstring("228.472"))
			Expect(raw).To(HaveSuffix("}\n"))
			// The annual is a sum for the totals, never their mean.
			Expect(raw).NotTo(ContainSubstring("75.9"))
			Expect(raw).NotTo(ContainSubstring("159.2"))
		})

		It("round-trips the cell-less and the catalog-only stations' profiles: every block present, the absent ones as explicit nulls", func() {
			Expect(decodeJSONDoc(entryContent(yucJSON, "combined/yuc/31002/profile.json"))).To(Equal(wantTiziminProfile()))
			Expect(decodeJSONDoc(entryContent(yucJSON, "combined/yuc/31019/profile.json"))).To(Equal(wantProgresoProfile()))
			raw := entryContent(yucJSON, "combined/yuc/31002/profile.json")
			Expect(raw).To(ContainSubstring("\"power_cell\": null,\n  \"power_monthly\": null,\n"))
		})

		It("round-trips every row of every daily.json as the combined_daily CSV's rows in the combined shape: reanalysis null before 1981 and where the cell lacks the date, the full block where it has it", func() {
			Expect(entryContent(yucJSON, "combined/yuc/31001/daily.json")).To(Equal(dailyDocFromCombined(wantMeridaCombinedDaily)))
			Expect(entryContent(yucJSON, "combined/yuc/31002/daily.json")).To(Equal(dailyDocFromCombined(wantTiziminCombinedDaily)))
			Expect(entryContent(yucJSON, "combined/yuc/3101/daily.json")).To(Equal(dailyDocFromCombined(wantValladolidCombinedDaily)))
			Expect(entryContent(yucJSON, "combined/yuc/31019/daily.json")).To(Equal("[\n]\n"))

			// The rows as written: the pre-1981 date with the whole-object
			// null, the 2020-01-01 date with the full block, a matched
			// partial row with its per-field nulls.
			merida := entryContent(yucJSON, "combined/yuc/31001/daily.json")
			Expect(merida).To(HavePrefix("[\n" + dailyRow("1980-12-31", []string{"28.00", "14.50", "0.00", "3.10"}, nil) + ",\n"))
			Expect(merida).To(ContainSubstring("\n" + dailyRow("2020-01-01", []string{"30.50", "", "12.46", ""}, publishPowerFullText) + ",\n"))
			Expect(merida).To(HaveSuffix("\n" + dailyRow("2020-02-29", []string{"18.94", "-2.46", "0.06", "5.56"}, publishPowerPartialText) + "\n]\n"))
			Expect(merida).To(ContainSubstring(`"t2m_max_c":null,`))
			// A value that rounds to zero is written as positive zero:
			// the POWER t2m_min_c of -0.004 is 0.00, not -0.00. An
			// observed value that is genuinely below the tenths keeps
			// both its sign and its magnitude — -0.04 °C, which the
			// one-decimal export flattened to 0.0.
			Expect(merida).To(ContainSubstring(`"t2m_min_c":0.00,`))
			Expect(merida).NotTo(ContainSubstring("-0.00"))
			Expect(merida).To(ContainSubstring(`{"date":"1999-12-31","observed":{"tmax_c":null,"tmin_c":-0.04,`))
		})

		It("writes provenance/*.json with the same runs, field for field, as the CSVs and the manifest", func() {
			var ingest []publish.IngestRunRef
			Expect(json.Unmarshal([]byte(entryContent(yucJSON, "provenance/ingest_runs.json")), &ingest)).To(Succeed())
			Expect(ingest).To(Equal(wantPublishIngestRuns))
			var power []publish.PowerRunRef
			Expect(json.Unmarshal([]byte(entryContent(yucJSON, "provenance/power_runs.json")), &power)).To(Succeed())
			expectPowerRuns(power, wantPublishPowerRuns)
			expectJSONRowsEqualCSV(entryContent(yucJSON, "provenance/ingest_runs.json"), wantIngestRunsCSV)
			expectJSONRowsEqualCSV(entryContent(yucJSON, "provenance/power_runs.json"), wantPowerRunsCSV)
			runs := entryContent(yucJSON, "provenance/power_runs.json")
			Expect(runs).NotTo(ContainSubstring("aborted"))
			Expect(runs).To(ContainSubstring("\"factor\": 11.574074074074074\n"))
		})

		It("writes a CHECKSUMS listing exactly the two archives and manifest.json, each digest matching the bytes on disk", func() {
			sums := readChecksums(firstOut)
			Expect(checksumNames(sums)).To(Equal([]string{"manifest.json", "yuc-json.zip", "yuc-tabular.zip"}))
			// sha256sum's text format, byte for byte.
			Expect(string(readBytes(filepath.Join(firstOut, "CHECKSUMS")))).To(Equal(
				sums[0].SHA256 + "  manifest.json\n" + sums[1].SHA256 + "  yuc-json.zip\n" + sums[2].SHA256 + "  yuc-tabular.zip\n"))
		})

		It("writes a manifest.json whose every field round-trips the seeded DB and the binary", func() {
			m, text := readManifest(firstOut)
			Expect(m.SchemaVersion).To(Equal(schema.Version))
			Expect(m.ETLGitSHA).To(Equal(binGitSHA))
			stamp, err := time.Parse(time.RFC3339, m.GeneratedAt)
			Expect(err).NotTo(HaveOccurred())
			Expect(m.GeneratedAt).To(HaveSuffix("Z"))
			Expect(stamp).To(BeTemporally(">=", firstRunStarted))
			Expect(stamp).To(BeTemporally("<=", firstRunFinished))
			Expect(m.SnapshotDate).To(Equal(publishSnapshotDate))
			Expect(m.Dataset).To(Equal(wantPublishDataset))
			Expect(m.States).To(Equal([]publish.ManifestState{
				{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-tabular.zip", "yuc-json.zip"}},
			}))
			Expect(m.Runs.Ingest).To(Equal(wantPublishIngestRuns))
			// The same three runs the provenance CSV carries; the
			// unreferenced aborted attempt is in neither.
			expectPowerRuns(m.Runs.Power, wantPublishPowerRuns)
			// The POWER-31 registry rides along so a reproducer can map
			// a run's parameter ids to exported columns and invert the
			// unit conversion; two rows pinned by hand, the runs' own
			// parameters resolve through it, and its column list is the
			// flat files' POWER-31 order.
			Expect(m.PowerParameters).To(HaveLen(31))
			Expect(m.PowerParameters[0]).To(Equal(publish.PowerParameter{
				ID: "T2M", Column: "t2m_c", PowerUnit: "C", StoredUnit: "C", Factor: 1,
			}))
			Expect(m.PowerParameters[15]).To(Equal(publish.PowerParameter{
				ID: "ALLSKY_SFC_SW_DWN", Column: "solar_ghi_wm2", PowerUnit: "MJ/m^2/day", StoredUnit: "W/m^2",
				Factor: 11.574074074074074,
			}))
			var columns []string
			for _, p := range m.PowerParameters {
				columns = append(columns, p.Column)
			}
			Expect(columns).To(Equal(wantPower31))
			for _, id := range strings.Split(wantPublishPowerRuns[0].Parameters, ",") {
				Expect(slices.ContainsFunc(m.PowerParameters, func(p publish.PowerParameter) bool {
					return p.ID == id
				})).To(BeTrue(), id)
			}
			Expect(m.Counts).To(Equal(wantPublishCounts))
			sums := readChecksums(firstOut)
			Expect(m.Files).To(Equal([]publish.ManifestFile{
				{Name: "yuc-json.zip", SHA256: sums[1].SHA256, Bytes: sizeOf(filepath.Join(firstOut, "yuc-json.zip"))},
				{Name: "yuc-tabular.zip", SHA256: sums[2].SHA256, Bytes: sizeOf(filepath.Join(firstOut, "yuc-tabular.zip"))},
			}))
			// The dataset creator's identity is the only one in the deposit's metadata.
			Expect(strings.ToLower(text)).To(ContainSubstring("\"creator\": \"pablo trinidad\""))
			Expect(strings.ToLower(text)).To(ContainSubstring("\"creator_orcid\": \"0009-0007-4050-494x\""))
		})

		It("builds byte-identical archive bytes on a second run; manifest.json differs only in generated_at (byte-reproducible, no generation stamp)", func() {
			out := filepath.Join(outRoot, "again")
			session := runPublishToExit(dbPath, out, "--state", "YUC")
			Expect(session.ExitCode()).To(Equal(0))
			Expect(fileSHA(dbPath)).To(Equal(dbSHA))

			for _, name := range []string{"yuc-tabular.zip", "yuc-json.zip"} {
				Expect(readBytes(filepath.Join(out, name))).To(Equal(readBytes(filepath.Join(firstOut, name))), name)
			}

			firstManifest := string(readBytes(filepath.Join(firstOut, "manifest.json")))
			secondManifest := string(readBytes(filepath.Join(out, "manifest.json")))
			Expect(generatedAtRE.FindString(secondManifest)).NotTo(BeEmpty())
			Expect(generatedAtRE.ReplaceAllLiteralString(secondManifest, "")).To(Equal(
				generatedAtRE.ReplaceAllLiteralString(firstManifest, "")))

			// CHECKSUMS carries manifest.json's digest, so it is byte-stable
			// exactly when manifest.json is: the archives' lines are
			// identical on every run, and the manifest's line tracks the
			// manifest's bytes.
			first, second := readChecksums(firstOut), readChecksums(out)
			Expect(checksumNames(second)).To(Equal(checksumNames(first)))
			Expect(second[1:]).To(Equal(first[1:]))
			Expect(second[0].SHA256 == first[0].SHA256).To(Equal(secondManifest == firstManifest))
		})

		It("builds only the JSON archive under --only json — the same bytes as beside the tabular one", func() {
			out := filepath.Join(outRoot, "onlyjson")
			session := runPublishToExit(dbPath, out, "--state", "yuc", "--only", "json")
			Expect(session.ExitCode()).To(Equal(0))
			Expect(fileSHA(dbPath)).To(Equal(dbSHA))

			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring("  states=yuc  only=json  doi=none\n"))
			expectUnitLines(publishUnitLineRE.FindAllStringSubmatch(stderr, -1), "yuc-json.zip", wantYucJSONUnits, len(wantYucJSONUnits))
			artifacts := publishArtifactLineRE.FindAllStringSubmatch(stderr, -1)
			Expect(artifacts).To(HaveLen(1))
			Expect(artifacts[0][1:4]).To(Equal([]string{"ok", "yuc-json.zip", strconv.Itoa(len(wantYucJSONPaths))}))
			stdout := string(session.Out.Contents())
			Expect(stdout).To(ContainSubstring("  groups               : json\n"))
			Expect(stdout).To(ContainSubstring("  artifacts            : 1 ok / 0 failed / 1 attempted\n"))
			Expect(stdout).To(ContainSubstring("  top-level files      : 3\n"))

			Expect(listDir(out)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-json.zip"}))
			Expect(checksumNames(readChecksums(out))).To(Equal([]string{"manifest.json", "yuc-json.zip"}))
			Expect(readBytes(filepath.Join(out, "yuc-json.zip"))).To(Equal(readBytes(filepath.Join(firstOut, "yuc-json.zip"))))
			m, _ := readManifest(out)
			Expect(m.States).To(Equal([]publish.ManifestState{
				{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-json.zip"}},
			}))
			Expect(m.Files).To(HaveLen(1))
			Expect(m.Files[0].Name).To(Equal("yuc-json.zip"))
		})

		It("builds only the tabular archive under --only tabular, any case", func() {
			out := filepath.Join(outRoot, "onlytabular")
			session := runPublishToExit(dbPath, out, "--state", "yuc", "--only", "Tabular")
			Expect(session.ExitCode()).To(Equal(0))

			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring("  states=yuc  only=Tabular  doi=none\n"))
			expectUnitLines(publishUnitLineRE.FindAllStringSubmatch(stderr, -1), "yuc-tabular.zip", wantYucUnits, len(wantYucUnits))
			Expect(publishArtifactLineRE.FindAllStringSubmatch(stderr, -1)).To(HaveLen(1))
			Expect(string(session.Out.Contents())).To(ContainSubstring("  groups               : tabular\n"))

			Expect(listDir(out)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-tabular.zip"}))
			Expect(readBytes(filepath.Join(out, "yuc-tabular.zip"))).To(Equal(readBytes(filepath.Join(firstOut, "yuc-tabular.zip"))))
			m, _ := readManifest(out)
			Expect(m.States).To(Equal([]publish.ManifestState{
				{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-tabular.zip"}},
			}))
		})

		It("exits 1 on an unknown group, naming the valid ones and writing nothing", func() {
			out := filepath.Join(outRoot, "badgroup")
			session := runPublishToExit(dbPath, out, "--state", "yuc", "--only", "tabular,xyz")
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(`error: unknown artifact group "xyz" (valid: tabular, json, national-csv, national-parquet, national-json, national-sqlite, raw, docs)`))
			Expect(string(session.Out.Contents())).To(ContainSubstring("\npublish aborted\n"))
			_, err := os.Stat(out)
			Expect(os.IsNotExist(err)).To(BeTrue())
			Expect(fileSHA(dbPath)).To(Equal(dbSHA))
		})

		It("re-runs into the same --out idempotently: exit 0, the same bytes, no temp residue", func() {
			out := filepath.Join(outRoot, "same")
			for range 2 {
				session := runPublishToExit(dbPath, out, "--state", "yuc")
				Expect(session.ExitCode()).To(Equal(0))
				Expect(listDir(out)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-json.zip", "yuc-tabular.zip"}))
				expectNoTempResidue(out)
			}
			for _, name := range []string{"yuc-tabular.zip", "yuc-json.zip"} {
				Expect(readBytes(filepath.Join(out, name))).To(Equal(readBytes(filepath.Join(firstOut, name))), name)
			}
			Expect(readChecksums(out)[1:]).To(Equal(readChecksums(firstOut)[1:]))
			Expect(fileSHA(dbPath)).To(Equal(dbSHA))
		})

		It("exits 1 on a subset build into an --out holding another state's archives, touching nothing", func() {
			out := filepath.Join(outRoot, "subset")
			Expect(runPublishToExit(dbPath, out, "--state", "yuc").ExitCode()).To(Equal(0))
			before := listDir(out)
			yucBytes := readBytes(filepath.Join(out, "yuc-tabular.zip"))

			session := runPublishToExit(dbPath, out, "--state", "ags")
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(
				"error: out dir " + out + " holds entries this run does not produce: yuc-json.zip, yuc-tabular.zip " +
					"(remove them or use a fresh --out)\n"))
			Expect(string(session.Err.Contents())).NotTo(ContainSubstring("ags-tabular.zip"))
			Expect(string(session.Out.Contents())).To(ContainSubstring("\npublish aborted\n"))
			Expect(listDir(out)).To(Equal(before))
			Expect(readBytes(filepath.Join(out, "yuc-tabular.zip"))).To(Equal(yucBytes))
			Expect(fileSHA(dbPath)).To(Equal(dbSHA))
		})

		It("creates a fresh nested --out directory", func() {
			out := filepath.Join(outRoot, "deep", "er", "publish")
			session := runPublishToExit(dbPath, out, "--state", "ags")
			Expect(session.ExitCode()).To(Equal(0))
			Expect(listDir(out)).To(Equal([]string{"CHECKSUMS", "ags-json.zip", "ags-tabular.zip", "manifest.json"}))
			Expect(checksumNames(readChecksums(out))).To(Equal([]string{"ags-json.zip", "ags-tabular.zip", "manifest.json"}))
		})

		It("exits 1 on an unknown state, naming the valid codes and writing nothing", func() {
			out := filepath.Join(outRoot, "bad")
			session := runPublishToExit(dbPath, out, "--state", "xx")
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(`error: unknown state "xx" (valid: AGS, YUC)`))
			Expect(string(session.Out.Contents())).To(ContainSubstring("\npublish aborted\n"))
			_, err := os.Stat(out)
			Expect(os.IsNotExist(err)).To(BeTrue())
			Expect(fileSHA(dbPath)).To(Equal(dbSHA))
		})
	})

	Context("a DB with no complete ingest run", func() {
		It("exits 1: there is no snapshot to publish", func() {
			dir := GinkgoT().TempDir()
			dbPath := filepath.Join(dir, "empty.db")
			db, err := schema.Open(dbPath)
			Expect(err).NotTo(HaveOccurred())
			_, err = db.Exec(`INSERT INTO ingest_runs (started_at, snapshot_date, sink_kind, status)
			  VALUES ('2026-06-11T01:00:00Z', '2026-06-11', 'local', 'aborted')`)
			Expect(err).NotTo(HaveOccurred())
			Expect(db.Close()).To(Succeed())

			out := filepath.Join(dir, "out")
			session := runPublishToExit(dbPath, out)
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring("error: no complete ingest run: nothing to publish\n"))
			Expect(string(session.Out.Contents())).To(ContainSubstring("\npublish aborted\n"))
			_, err = os.Stat(out)
			Expect(os.IsNotExist(err)).To(BeTrue())
		})
	})

	Context("a missing DB file", func() {
		It("exits 1 at open time rather than creating one", func() {
			dir := GinkgoT().TempDir()
			dbPath := filepath.Join(dir, "absent.db")
			session := runPublishToExit(dbPath, filepath.Join(dir, "out"))
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring("error: open " + dbPath + ": open read-only"))
			_, err := os.Stat(dbPath)
			Expect(os.IsNotExist(err)).To(BeTrue())
		})
	})
})
