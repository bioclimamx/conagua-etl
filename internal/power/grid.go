// Package power implements the NASA POWER puller and the supporting
// math: cell-grid arithmetic, time-series rollup to climatological
// monthly means, and the HTTP client.
//
// The MERRA-2-derived monthly grid POWER serves is 0.5° lat × 0.625°
// lon, and we *anchor* our cell centroids to that grid: lat centers
// at multiples of 0.5° from -90 to +90, lon centers at multiples of
// 0.625° from -180 to (just below) +180. Snapping a station's lat/lon
// to the nearest centroid is what dedupes the 5,524-station fetch
// into 609 unique cells over Mexico, and it's what makes a
// reproducer who fetches POWER themselves get the same numbers we
// got — they query with the centroid we publish, POWER returns the
// MERRA-2 cell that contains it, and as long as the centroid stays
// inside the same MERRA-2 cell across calls the numbers are stable.
package power

import (
	"fmt"
	"math"
)

// LatStep / LonStep are the MERRA-2 monthly grid spacing. POWER's
// other products (e.g. daily climatology) sit on the same lattice
// for the variables we use.
const (
	LatStep    = 0.5
	LonStep    = 0.625
	Resolution = "0.5x0.625" // matches schema.sql nasa_power_grid_cells.grid_resolution CHECK
)

// Cell is one POWER grid cell — a centroid + the deterministic ID we
// key off of in the DB and surface in published JSON for traceability.
type Cell struct {
	ID         string  // e.g. "19.5N_100.6250W" — see CellID for the format
	Lat        float64 // centroid, decimal degrees
	Lon        float64 // centroid, decimal degrees, west-negative
	Resolution string  // always Resolution for now; future-proofing for ERA5/etc.
}

// CellFor snaps a station's (lat, lon) to its enclosing POWER cell.
// math.Round is round-half-away-from-zero, so a point exactly on a
// cell boundary deterministically rounds outward; the choice doesn't
// matter as long as it's stable across runs.
func CellFor(lat, lon float64) Cell {
	cLat := math.Round(lat/LatStep) * LatStep
	cLon := math.Round(lon/LonStep) * LonStep
	// Normalize -0 to 0 so cell IDs at the equator / prime meridian
	// don't toggle hemispheres.
	if cLat == 0 {
		cLat = 0
	}
	if cLon == 0 {
		cLon = 0
	}
	return Cell{
		ID:         CellID(cLat, cLon),
		Lat:        cLat,
		Lon:        cLon,
		Resolution: Resolution,
	}
}

// CellID renders a centroid lat/lon as the readable, deterministic
// key we store in nasa_power_grid_cells.cell_id and reference from
// the supplement tables and station_power_cell.
//
// Format: "<|lat|>{N|S}_<|lon|>{E|W}" with 1-decimal lat (matches the
// 0.5° step) and 4-decimal lon (covers the 0.625° step exactly —
// `0.625` × n is a finite decimal at 4 places).
func CellID(lat, lon float64) string {
	latHemi := "N"
	if lat < 0 {
		latHemi = "S"
		lat = -lat
	}
	lonHemi := "E"
	if lon < 0 {
		lonHemi = "W"
		lon = -lon
	}
	return fmt.Sprintf("%.1f%s_%.4f%s", lat, latHemi, lon, lonHemi)
}

// HaversineKm returns the great-circle distance between two points
// in kilometers. Used to compute station_power_cell.distance_km, the
// "possible error" indicator surfaced next to POWER values.
//
// Earth radius 6371 km — a mean-sphere approximation; good to ~0.5%,
// which is well below the resolution of POWER's 50 km cells anyway.
func HaversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	const r = 6371.0
	rad := math.Pi / 180
	dLat := (lat2 - lat1) * rad
	dLon := (lon2 - lon1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*
			math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return r * c
}
