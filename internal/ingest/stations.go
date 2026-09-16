package ingest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// Source enumerates the values allowed by the stations.source CHECK
// constraint. Exported as typed constants so callers don't need to
// repeat the string literal (and so renames surface at compile time).
type Source string

// The six values of the stations.source CHECK enum. Only the CONAGUA
// conventional path writes today; the rest reserve the vocabulary for
// future sources.
const (
	SourceConaguaConventional Source = "conagua_conventional"
	SourceConaguaEMA          Source = "conagua_ema"
	SourceINIFAP              Source = "inifap"
	SourceNASAPower           Source = "nasa_power"
	SourceERA5                Source = "era5"
	SourceMeta                Source = "meta"
)

// StationUpsert is the wire format for UpsertStation. It is deliberately
// source-agnostic — every field maps to a column that any source could
// populate, so future NASA POWER / ERA5 / INIFAP seeders plug in without
// widening the schema.
//
// Pointer fields are for columns that are legitimately nullable at the
// source (lat/lon/altitude can be missing on older CONAGUA entries;
// future grid sources that don't have a "status" concept will pass an
// empty Status).
type StationUpsert struct {
	Source       Source
	ExternalID   string
	Name         string
	State        string
	Municipality string
	Lat          *float64
	Lon          *float64
	AltitudeM    *float64
	Status       string
}

// UpsertStation writes one station row keyed on (source, external_id).
// Returns the surrogate stations.id, which the caller threads through
// to monthly_normals/daily_observations inserts.
//
// The ON CONFLICT path updates only the fields the caller actually
// populated (COALESCE with excluded.*), so calling UpsertStation a
// second time with the seed (catalog-only) data does NOT overwrite
// later enrichment from file headers — idempotent reseeding is safe.
// Field semantics:
//
//   - Zero-valued strings ("") overwrite only if the stored value is
//     also empty or NULL. Non-empty incoming values always win (because
//     if we have a better name/state/etc., we want it).
//   - Nil pointers (*float64) never overwrite a stored non-null value.
//
// That asymmetry matches how seed vs enrichment flows: seed produces
// partial data, enrichment fills gaps; neither should clobber the
// other.
func UpsertStation(ctx context.Context, tx *sql.Tx, s StationUpsert) (int64, error) {
	if s.Source == "" {
		return 0, errors.New("UpsertStation: Source is required")
	}
	if s.ExternalID == "" {
		return 0, errors.New("UpsertStation: ExternalID is required")
	}
	if s.Name == "" {
		return 0, errors.New("UpsertStation: Name is required (stations.name NOT NULL)")
	}

	const q = `
INSERT INTO stations
    (source, external_id, name, state, municipality,
     lat, lon, altitude_m, status)
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (source, external_id) DO UPDATE SET
    name         = CASE WHEN excluded.name         != '' THEN excluded.name         ELSE stations.name         END,
    state        = CASE WHEN excluded.state        != '' THEN excluded.state        ELSE stations.state        END,
    municipality = CASE WHEN excluded.municipality != '' THEN excluded.municipality ELSE stations.municipality END,
    lat          = COALESCE(excluded.lat,        stations.lat),
    lon          = COALESCE(excluded.lon,        stations.lon),
    altitude_m   = COALESCE(excluded.altitude_m, stations.altitude_m),
    status       = CASE WHEN excluded.status       != '' THEN excluded.status       ELSE stations.status       END
RETURNING id;
`
	var id int64
	err := tx.QueryRowContext(ctx, q,
		string(s.Source), s.ExternalID, s.Name, nilIfEmpty(s.State), nilIfEmpty(s.Municipality),
		floatArg(s.Lat), floatArg(s.Lon), floatArg(s.AltitudeM), nilIfEmpty(s.Status),
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("upsert station (%s, %s): %w", s.Source, s.ExternalID, err)
	}
	return id, nil
}

// seedStations upserts one stations row per manifest entry inside the
// caller's tx, keyed by the short-form external_id ("1001", not the
// 5-digit URL form "01001"). CONAGUA's lowercase state slug is
// uppercased ("ags" → "AGS") so downstream consumers group by a
// canonical case. Lat/lon/altitude stay NULL at seed time — the
// catalog doesn't carry them; the daily/normals passes enrich from
// file headers.
//
// Returns a map of external_id → stations.id so the caller can thread
// the surrogate PK through to observation/normals inserts.
func seedStations(ctx context.Context, tx *sql.Tx, stations []snapshot.StationProgress, source Source) (map[string]int64, error) {
	ids := make(map[string]int64, len(stations))
	for _, sp := range stations {
		externalID := conagua.ShortID(sp.ID)
		id, err := UpsertStation(ctx, tx, StationUpsert{
			Source:       source,
			ExternalID:   externalID,
			Name:         sp.Name,
			State:        strings.ToUpper(string(sp.State)),
			Municipality: sp.Municipality,
			Status:       string(sp.Status),
		})
		if err != nil {
			return nil, fmt.Errorf("seed station %s: %w", sp.ID, err)
		}
		ids[externalID] = id
	}
	return ids, nil
}

// nilIfEmpty turns "" into a typed nil so SQLite stores NULL rather
// than an empty string. Matters for the state/municipality/status
// columns, which are semantically nullable — a query like
// "stations with no status" wants NULL matches, not empty-string matches.
func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// floatArg converts a *float64 to the driver's any argument form.
// database/sql accepts nil interface values as NULL, but typed nil
// pointers get wrapped as concrete types and confuse the placeholder
// binding; unwrap explicitly.
func floatArg(p *float64) any {
	if p == nil {
		return nil
	}
	return *p
}
