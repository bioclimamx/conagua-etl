package parity

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/power"
)

// ComparePower runs the offline power parity comparison against one
// ingest database: every station's cell
// snap and Haversine distance are recomputed with this repo's ported
// math and compared float-exactly against the stored
// nasa_power_grid_cells and station_power_cell rows, and every
// complete power_runs manifest is checked against the pinned
// endpoint/community constants, the parameter registry, and a
// BuildURL round-trip.
//
// The handle should come from schema.OpenReadOnly — the comparator
// never writes, and the read-only opener makes that a hard guarantee
// for the reference DB.
func ComparePower(ctx context.Context, db *sql.DB) (*PowerComparison, error) {
	cmp := &PowerComparison{
		Cells: PowerTable{Table: "nasa_power_grid_cells"},
		Links: PowerTable{Table: "station_power_cell"},
	}

	stations, err := loadPowerStations(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("load stations: %w", err)
	}
	cmp.StationsCompared = len(stations)

	if err := comparePowerCells(ctx, cmp, db, stations); err != nil {
		return nil, fmt.Errorf("nasa_power_grid_cells: %w", err)
	}
	if err := comparePowerLinks(ctx, cmp, db, stations); err != nil {
		return nil, fmt.Errorf("station_power_cell: %w", err)
	}
	if err := comparePowerRuns(ctx, cmp, db); err != nil {
		return nil, fmt.Errorf("power_runs: %w", err)
	}
	return cmp, nil
}

// powerStation is the station projection driving the cell math: the
// surrogate id (to address station_power_cell within the same DB —
// never compared or reported), the natural external_id for report
// keys, and the coordinates the math snaps.
type powerStation struct {
	id         int64
	externalID string
	lat        float64
	lon        float64
}

// powerStationKey renders a station's report key under the fixed
// source filter.
func powerStationKey(externalID string) DBKey {
	return DBKey{Source: string(ingest.SourceConaguaConventional), ExternalID: externalID}
}

// loadPowerStations mirrors the power orchestrator's station universe
// — CONAGUA conventional stations with coordinates — through the
// ingest-owned Source enum, never a raw SQL literal. Ordered by external_id so retained samples are
// deterministic across runs.
func loadPowerStations(ctx context.Context, db *sql.DB) ([]powerStation, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, external_id, lat, lon
		FROM stations
		WHERE source = ? AND lat IS NOT NULL AND lon IS NOT NULL
		ORDER BY external_id`, string(ingest.SourceConaguaConventional))
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []powerStation
	for rows.Next() {
		var s powerStation
		if err := rows.Scan(&s.id, &s.externalID, &s.lat, &s.lon); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return out, nil
}

// gridCellRow carries one stored nasa_power_grid_cells row's value
// columns; the cell_id keys the map it lives in.
type gridCellRow struct {
	lat        float64
	lon        float64
	resolution string
}

func loadGridCellRows(ctx context.Context, db *sql.DB) (map[string]gridCellRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT cell_id, lat, lon, grid_resolution FROM nasa_power_grid_cells`)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cells := make(map[string]gridCellRow)
	for rows.Next() {
		var id string
		var r gridCellRow
		if err := rows.Scan(&id, &r.lat, &r.lon, &r.resolution); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		cells[id] = r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return cells, nil
}

// derivedCells snaps every station and dedups to the expected cell
// set — the same math the power orchestrator runs (5,524 stations
// collapse to 609 cells nationally).
func derivedCells(stations []powerStation) map[string]power.Cell {
	cells := make(map[string]power.Cell, len(stations))
	for _, s := range stations {
		c := power.CellFor(s.lat, s.lon)
		cells[c.ID] = c
	}
	return cells
}

func comparePowerCells(ctx context.Context, cmp *PowerComparison, db *sql.DB, stations []powerStation) error {
	derived := derivedCells(stations)
	stored, err := loadGridCellRows(ctx, db)
	if err != nil {
		return err
	}

	for _, id := range sortedUnionKeys(derived, stored) {
		d, inDerived := derived[id]
		s, inStored := stored[id]
		key := DBKey{Row: id}
		switch {
		case !inStored:
			cmp.Cells.DerivedOnly.Add(key)
		case !inDerived:
			cmp.Cells.StoredOnly.Add(key)
		default:
			cmp.Cells.RowsCompared++
			c := powerDiffCollector{key: key}
			diffGridCellRow(&c, d, s)
			if len(c.diffs) == 0 {
				cmp.Cells.RowsIdentical++
			}
			for _, diff := range c.diffs {
				cmp.Cells.ValueDiffs.Add(diff)
			}
		}
	}
	return nil
}

// diffGridCellRow itemizes every nasa_power_grid_cells value column.
func diffGridCellRow(c *powerDiffCollector, derived power.Cell, stored gridCellRow) {
	c.float64Col("lat", derived.Lat, stored.lat)
	c.float64Col("lon", derived.Lon, stored.lon)
	c.strCol("grid_resolution", derived.Resolution, stored.resolution)
}

// linkRow carries one stored station_power_cell row's value columns,
// with the station resolved to its natural key for reporting — the
// surrogate station_id addresses rows only within this one DB.
type linkRow struct {
	key        DBKey
	cellID     string
	distanceKm float64
}

func loadLinkRows(ctx context.Context, db *sql.DB) (map[int64]linkRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT spc.station_id, s.source, s.external_id, spc.cell_id, spc.distance_km
		FROM station_power_cell spc LEFT JOIN stations s ON s.id = spc.station_id`)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	links := make(map[int64]linkRow)
	for rows.Next() {
		var stationID int64
		var source, externalID sql.NullString
		var r linkRow
		if err := rows.Scan(&stationID, &source, &externalID, &r.cellID, &r.distanceKm); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		r.key = DBKey{Source: source.String, ExternalID: externalID.String}
		if !externalID.Valid {
			// A station_id with no stations row cannot resolve to a
			// natural key; surface the dangling surrogate instead.
			r.key.ExternalID = "unresolved-station-id:" + strconv.FormatInt(stationID, 10)
		}
		links[stationID] = r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return links, nil
}

func comparePowerLinks(ctx context.Context, cmp *PowerComparison, db *sql.DB, stations []powerStation) error {
	stored, err := loadLinkRows(ctx, db)
	if err != nil {
		return err
	}

	seen := make(map[int64]struct{}, len(stations))
	for _, s := range stations {
		seen[s.id] = struct{}{}
		// Same operations in the same order as the orchestrator's
		// upsert: snap first, then the great-circle distance to the
		// snapped centroid.
		cell := power.CellFor(s.lat, s.lon)
		dist := power.HaversineKm(s.lat, s.lon, cell.Lat, cell.Lon)

		key := powerStationKey(s.externalID)
		st, ok := stored[s.id]
		if !ok {
			cmp.Links.DerivedOnly.Add(key)
			continue
		}
		cmp.Links.RowsCompared++
		c := powerDiffCollector{key: key}
		diffLinkRow(&c, cell.ID, dist, st)
		if len(c.diffs) == 0 {
			cmp.Links.RowsIdentical++
		}
		for _, diff := range c.diffs {
			cmp.Links.ValueDiffs.Add(diff)
		}
	}

	// Stored links outside the derived universe (a station of another
	// source, without coordinates, or missing entirely), in report-key
	// order for deterministic samples.
	var extras []linkRow
	for id, r := range stored {
		if _, ok := seen[id]; !ok {
			extras = append(extras, r)
		}
	}
	slices.SortFunc(extras, func(a, b linkRow) int {
		return strings.Compare(a.key.String(), b.key.String())
	})
	for _, r := range extras {
		cmp.Links.StoredOnly.Add(r.key)
	}
	return nil
}

// diffLinkRow itemizes every station_power_cell value column.
func diffLinkRow(c *powerDiffCollector, derivedCellID string, derivedDistanceKm float64, stored linkRow) {
	c.strCol("cell_id", derivedCellID, stored.cellID)
	c.float64Col("distance_km", derivedDistanceKm, stored.distanceKm)
}

// loadPowerRuns fetches every complete run's manifest columns in id
// order. Interrupted rows ('running'/'aborted') are skipped: only a
// complete run's manifest governed a full supplement write.
func loadPowerRuns(ctx context.Context, db *sql.DB) ([]PowerRunManifest, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, temporal_mode, endpoint_url, parameters, community,
		       period_start_year, period_end_year, period_start_date, period_end_date,
		       grid_resolution, solar_conversion, unit_conversions
		FROM power_runs WHERE status = 'complete' ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []PowerRunManifest
	for rows.Next() {
		var r PowerRunManifest
		if err := rows.Scan(&r.ID, &r.Temporal, &r.EndpointURL, &r.Parameters, &r.Community,
			&r.PeriodStartYear, &r.PeriodEndYear, &r.PeriodStartDate, &r.PeriodEndDate,
			&r.GridResolution, &r.SolarConversion, &r.UnitConversions); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return out, nil
}

func comparePowerRuns(ctx context.Context, cmp *PowerComparison, db *sql.DB) error {
	runs, err := loadPowerRuns(ctx, db)
	if err != nil {
		return err
	}
	cmp.Runs = runs

	probe, err := urlProbeCell(ctx, db)
	if err != nil {
		return err
	}
	for i := range runs {
		checkRunManifest(cmp, &runs[i], probe)
	}
	return nil
}

// urlProbeCell picks the centroid for the URL round-trip: the first
// stored grid cell in cell_id order, so the rebuilt URL is one a
// reproducer of this DB would actually paste, falling back to a fixed
// in-grid centroid for a database with no cells. Coordinates are
// per-cell, not manifest fields, so any centroid exercises BuildURL's
// coordinate formatting; both paths are deterministic.
func urlProbeCell(ctx context.Context, db *sql.DB) (power.Cell, error) {
	var id string
	var lat, lon float64
	err := db.QueryRowContext(ctx, `
		SELECT cell_id, lat, lon FROM nasa_power_grid_cells ORDER BY cell_id LIMIT 1`).
		Scan(&id, &lat, &lon)
	if errors.Is(err, sql.ErrNoRows) {
		return power.CellFor(19.43, -99.13), nil
	}
	if err != nil {
		return power.Cell{}, fmt.Errorf("probe cell: %w", err)
	}
	return power.Cell{ID: id, Lat: lat, Lon: lon, Resolution: power.Resolution}, nil
}

// checkRunManifest runs every manifest check for one complete run,
// recording each failure as a finding.
func checkRunManifest(cmp *PowerComparison, run *PowerRunManifest, probe power.Cell) {
	add := func(field, got, want string) {
		cmp.ManifestFindings.Add(ManifestFinding{RunID: run.ID, Field: field, Got: got, Want: want})
	}

	// The comma-joined registry order is the reproducibility contract;
	// string equality pins
	// identity and order at once.
	wantParams := strings.Join(power.DefaultParameters(), ",")
	if run.Parameters != wantParams {
		add("parameters", run.Parameters, wantParams)
	}

	daily := run.Temporal == string(power.TemporalDaily)
	wantEndpoint := power.DefaultEndpointMonthly
	if daily {
		wantEndpoint = power.DefaultEndpointDaily
	}
	if run.EndpointURL != wantEndpoint {
		add("endpoint_url", run.EndpointURL, wantEndpoint)
	}
	if run.Community != power.DefaultCommunity {
		add("community", run.Community, power.DefaultCommunity)
	}
	if run.GridResolution != power.Resolution {
		add("grid_resolution", run.GridResolution, power.Resolution)
	}
	if run.SolarConversion != power.SolarMJpm2dToWm2 {
		add("solar_conversion", renderFloat(run.SolarConversion), renderFloat(power.SolarMJpm2dToWm2))
	}

	params := strings.Split(run.Parameters, ",")
	checkUnitConversions(add, run, params)
	checkExampleURL(add, run, params, probe, daily)
}

// checkUnitConversions parses the run's unit_conversions JSON and
// compares it semantically — key set plus per-entry power_unit /
// stored_unit / exact factor — against the projection of the
// registry-derived Conversions map onto the run's own parameter list.
func checkUnitConversions(add func(field, got, want string), run *PowerRunManifest, params []string) {
	if !run.UnitConversions.Valid {
		add("unit_conversions", renderedNull, "per-parameter JSON manifest")
		return
	}
	var got map[string]power.UnitConversion
	if err := json.Unmarshal([]byte(run.UnitConversions.String), &got); err != nil {
		add("unit_conversions", fmt.Sprintf("unparseable JSON: %v", err), "per-parameter JSON manifest")
		return
	}

	requested := make(map[string]struct{}, len(params))
	want := make(map[string]power.UnitConversion, len(params))
	for _, p := range params {
		requested[p] = struct{}{}
		if c, ok := power.Conversions[p]; ok {
			want[p] = c
		}
	}

	for _, p := range sortedUnionKeys(got, want) {
		g, inGot := got[p]
		w, inWant := want[p]
		switch {
		case !inGot:
			add("unit_conversions."+p, "absent", renderConversion(w))
		case !inWant:
			reason := "absent (not in run parameters)"
			if _, ok := requested[p]; ok {
				reason = "absent (no registry entry)"
			}
			add("unit_conversions."+p, renderConversion(g), reason)
		case g != w:
			add("unit_conversions."+p, renderConversion(g), renderConversion(w))
		}
	}
}

// checkExampleURL rebuilds the POWER request URL from the run's
// manifest fields through BuildURL and parses it back, asserting every
// query parameter round-trips — the reproducibility contract: the URL
// a third party reconstructs from the manifest must be the one the
// client sends, parameter order preserved.
func checkExampleURL(add func(field, got, want string), run *PowerRunManifest, params []string, probe power.Cell, daily bool) {
	req := power.FetchRequest{
		Lat:        probe.Lat,
		Lon:        probe.Lon,
		Parameters: params,
		Community:  run.Community,
	}
	if daily {
		if !run.PeriodStartDate.Valid || !run.PeriodEndDate.Valid {
			add("url.span",
				fmt.Sprintf("period_start_date=%s period_end_date=%s",
					renderNullStr(run.PeriodStartDate), renderNullStr(run.PeriodEndDate)),
				"a daily manifest carries both dates")
			return
		}
		req.StartDate = run.PeriodStartDate.String
		req.EndDate = run.PeriodEndDate.String
	} else {
		req.StartYear = int(run.PeriodStartYear)
		req.EndYear = int(run.PeriodEndYear)
	}

	built, err := power.BuildURL(run.EndpointURL, req)
	if err != nil {
		add("url", fmt.Sprintf("BuildURL: %v", err), "a buildable URL")
		return
	}
	parsed, err := url.Parse(built)
	if err != nil {
		add("url", fmt.Sprintf("unparseable: %v", err), built)
		return
	}
	q := parsed.Query()
	parsed.RawQuery = ""

	check := func(field, got, want string) {
		if got != want {
			add(field, got, want)
		}
	}
	check("url.endpoint", parsed.String(), run.EndpointURL)
	check("url.parameters", q.Get("parameters"), run.Parameters)
	check("url.community", q.Get("community"), run.Community)
	check("url.latitude", q.Get("latitude"), strconv.FormatFloat(probe.Lat, 'f', -1, 64))
	check("url.longitude", q.Get("longitude"), strconv.FormatFloat(probe.Lon, 'f', -1, 64))
	check("url.format", q.Get("format"), "JSON")
	if daily {
		check("url.start", q.Get("start"), wireDate(run.PeriodStartDate.String))
		check("url.end", q.Get("end"), wireDate(run.PeriodEndDate.String))
	} else {
		check("url.start", q.Get("start"), strconv.FormatInt(run.PeriodStartYear, 10))
		check("url.end", q.Get("end"), strconv.FormatInt(run.PeriodEndYear, 10))
	}
}

// wireDate renders an ISO date in POWER's YYYYMMDD wire form, the
// shape BuildURL normalizes daily spans to.
func wireDate(iso string) string { return strings.ReplaceAll(iso, "-", "") }

// renderConversion renders one UnitConversion for a finding message,
// factor at full precision.
func renderConversion(c power.UnitConversion) string {
	return fmt.Sprintf("%s → %s ×%s", c.PowerUnit, c.StoredUnit, renderFloat(c.Factor))
}

// renderFloat renders a float64 at full round-trip precision, the
// report convention for exact-compare values.
func renderFloat(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

// powerDiffCollector accumulates column-level derived-vs-stored diffs
// for one key. Floats compare with ==, never a tolerance: the stored
// values came from the reference build's float64 math over the same
// inputs, so bit-identical reproduction is the parity contract.
type powerDiffCollector struct {
	key   DBKey
	diffs []PowerValueDiff
}

func (c *powerDiffCollector) record(column, derived, stored string) {
	c.diffs = append(c.diffs, PowerValueDiff{Key: c.key, Column: column, Derived: derived, Stored: stored})
}

func (c *powerDiffCollector) float64Col(column string, derived, stored float64) {
	if derived != stored {
		c.record(column, renderFloat(derived), renderFloat(stored))
	}
}

func (c *powerDiffCollector) strCol(column, derived, stored string) {
	if derived != stored {
		c.record(column, derived, stored)
	}
}
