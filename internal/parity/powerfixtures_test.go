package parity_test

// Synthetic power fixtures for the power parity and drift comparator
// specs. Everything is seeded through the real DDL (schema.Open) and
// plain SQL, with the faithful values computed by the same
// internal/power symbols the comparator checks against; the specs then
// perturb one stored field at a time to prove each finding category
// fires — a comparator that misses a tampered value would silently
// pass the gate.

import (
	"context"
	"database/sql"
	"encoding/json"
	"maps"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
	"github.com/bioclimamx/conagua-etl/internal/power"
)

// powerSeedStation is one fixture station. Coordinates are chosen so A
// and B snap into the same POWER cell while C lands in a second one.
type powerSeedStation struct {
	id  int64
	ext string
	lat float64
	lon float64
}

var powerSeedStations = []powerSeedStation{
	{id: 11, ext: "2001", lat: 19.40, lon: -99.10},
	{id: 12, ext: "2002", lat: 19.60, lon: -99.20},
	{id: 13, ext: "2003", lat: 25.10, lon: -100.90},
}

// powerCellShared is the cell A and B share; powerCellSolo is C's.
func powerCellShared() power.Cell { return power.CellFor(19.40, -99.10) }
func powerCellSolo() power.Cell   { return power.CellFor(25.10, -100.90) }

// seedPowerStationRow inserts a minimal station under the CONAGUA
// conventional source; lat/lon accept nil to seed a coordinate-less
// station outside the cell-math universe.
func seedPowerStationRow(db *sql.DB, id int64, ext string, lat, lon any) {
	GinkgoHelper()
	execSQL(db, `INSERT INTO stations (id, source, external_id, name, lat, lon)
		VALUES (?, ?, ?, 'POWER FIXTURE', ?, ?)`, id, seedSource, ext, lat, lon)
}

// seedFaithfulPowerMath seeds the three stations plus the grid cells
// and links the ported math derives for them.
func seedFaithfulPowerMath(db *sql.DB) {
	GinkgoHelper()
	seen := map[string]bool{}
	for _, s := range powerSeedStations {
		seedPowerStationRow(db, s.id, s.ext, s.lat, s.lon)
		c := power.CellFor(s.lat, s.lon)
		if !seen[c.ID] {
			seen[c.ID] = true
			execSQL(db, `INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution)
				VALUES (?, ?, ?, ?)`, c.ID, c.Lat, c.Lon, c.Resolution)
		}
		execSQL(db, `INSERT INTO station_power_cell (station_id, cell_id, distance_km)
			VALUES (?, ?, ?)`, s.id, c.ID, power.HaversineKm(s.lat, s.lon, c.Lat, c.Lon))
	}
}

// faithfulConversionsJSON marshals the registry projection the writer
// records for the given parameters.
func faithfulConversionsJSON(params []string) string {
	GinkgoHelper()
	m := make(map[string]power.UnitConversion, len(params))
	for _, p := range params {
		c, ok := power.Conversions[p]
		Expect(ok).To(BeTrue(), "parameter %s missing from power.Conversions", p)
		m[p] = c
	}
	j, err := json.Marshal(m)
	Expect(err).NotTo(HaveOccurred())
	return string(j)
}

// seedPowerRun inserts one power_runs row whose manifest columns are
// fully faithful to the pinned constants for its temporal mode.
func seedPowerRun(db *sql.DB, id int64, temporal power.TemporalMode, status string) {
	GinkgoHelper()
	params := power.DefaultParameters()
	endpoint := power.DefaultEndpointMonthly
	startYear, endYear := 1991, 2020
	var startDate, endDate any
	if temporal == power.TemporalDaily {
		endpoint = power.DefaultEndpointDaily
		startYear, endYear = 1981, 2026
		startDate, endDate = "1981-01-01", "2026-06-08"
	}
	execSQL(db, `INSERT INTO power_runs
		(id, started_at, finished_at, status, endpoint_url, parameters, community,
		 period_start_year, period_end_year, grid_resolution, solar_conversion,
		 unit_conversions, temporal_mode, period_start_date, period_end_date,
		 cells_attempted, cells_succeeded, cells_failed, supplement_rows, etl_git_sha)
		VALUES (?, '2026-06-08T10:00:00Z', '2026-06-08T11:00:00Z', ?, ?, ?, ?,
		        ?, ?, ?, ?, ?, ?, ?, ?, 2, 2, 0, 24, 'abc1234')`,
		id, status, endpoint, strings.Join(params, ","), power.DefaultCommunity,
		startYear, endYear, power.Resolution, power.SolarMJpm2dToWm2,
		faithfulConversionsJSON(params), string(temporal), startDate, endDate)
}

// seedFaithfulPowerDB builds a database whose cell math and manifests
// are fully faithful to the ported constants: two cells, three links,
// one complete run per temporal mode.
func seedFaithfulPowerDB() *sql.DB {
	GinkgoHelper()
	db := openIngestDB()
	seedFaithfulPowerMath(db)
	seedPowerRun(db, 1, power.TemporalMonthly, "complete")
	seedPowerRun(db, 2, power.TemporalDaily, "complete")
	return db
}

// mustComparePower runs ComparePower and asserts the infrastructure-
// level outcome before returning the comparison.
func mustComparePower(ctx context.Context, db *sql.DB) *parity.PowerComparison {
	GinkgoHelper()
	cmp, err := parity.ComparePower(ctx, db)
	Expect(err).NotTo(HaveOccurred())
	return cmp
}

// insertSupplementRow inserts one supplement row with only the given
// value columns set — the rest stay NULL, mirroring the sparse rows
// the power writer produces.
func insertSupplementRow(db *sql.DB, table string, keyCols []string, keyVals []any, vals map[string]float64) {
	GinkgoHelper()
	cols := append([]string{}, keyCols...)
	args := append([]any{}, keyVals...)
	for _, c := range slices.Sorted(maps.Keys(vals)) {
		cols = append(cols, c)
		args = append(args, vals[c])
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(cols)), ", ")
	execSQL(db, `INSERT INTO `+table+` (`+strings.Join(cols, ", ")+`) VALUES (`+placeholders+`)`, args...)
}

func insertMonthlySupplementRow(db *sql.DB, cellID, period string, month int, vals map[string]float64) {
	GinkgoHelper()
	insertSupplementRow(db, "monthly_supplement",
		[]string{"cell_id", "period", "month"}, []any{cellID, period, month}, vals)
}

func insertDailySupplementRow(db *sql.DB, cellID, date string, vals map[string]float64) {
	GinkgoHelper()
	insertSupplementRow(db, "daily_supplement",
		[]string{"cell_id", "date"}, []any{cellID, date}, vals)
}
