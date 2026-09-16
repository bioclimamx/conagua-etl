package validate

import (
	"context"
	"database/sql"
	"fmt"
)

// MX-territory bbox used by the bbox rule. Wide enough to cover Isla
// Guadalupe (~29°N, ~118.4°W) and the northern border at ~32.7°N.
// Stations outside trip a soft "likely typo" warning; impossible
// values (lat∉[-90,90], lon∉[-180,180], or both 0) trip an error.
const (
	mxLatMin = 14.5
	mxLatMax = 32.8
	mxLonMin = -118.5
	mxLonMax = -86.7
)

// ruleBBox flags stations whose lat/lon look implausible. Two
// severity tiers:
//
//   - Impossible coords (lat∉[-90,90], lon∉[-180,180], or both zero)
//     are 'error'. These are the only error-severity findings the
//     core rule set produces; they block publish because they make
//     every downstream interpolation meaningless.
//
//   - Outside the MX bbox (but otherwise valid) is 'warn'. Real
//     stations like Isla Guadalupe live near the bbox edge and we
//     want them surfaced for a sanity glance, not blocked.
//
// Stations with NULL lat or lon are reported as a warn — any future
// publish step needs coordinates to render anything.
func ruleBBox(ctx context.Context, db *sql.DB) (RuleResult, error) {
	rows, err := db.QueryContext(ctx, `
SELECT id, name, source, external_id, lat, lon
  FROM stations
 ORDER BY id`)
	if err != nil {
		return RuleResult{}, fmt.Errorf("scan stations: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	var out RuleResult
	for rows.Next() {
		var id int64
		var name, source, ext string
		var lat, lon sql.NullFloat64
		if err := rows.Scan(&id, &name, &source, &ext, &lat, &lon); err != nil {
			return RuleResult{}, fmt.Errorf("scan stations: %w", err)
		}
		out.Scanned++

		if !lat.Valid || !lon.Valid {
			stid := id
			out.Findings = append(out.Findings, Finding{
				StationID: &stid,
				RuleID:    "bbox",
				Severity:  SeverityWarn,
				Issue: fmt.Sprintf("station %s/%s (%q) has NULL %s",
					source, ext, name, missingCoordPart(lat, lon)),
			})
			continue
		}
		la, lo := lat.Float64, lon.Float64

		// Impossible: real coordinates can't sit outside Earth's range,
		// and (0, 0) is the canonical "default-zero coordinate" sentinel
		// you see in upstream data with a missing lat/lon.
		if la < -90 || la > 90 || lo < -180 || lo > 180 || (la == 0 && lo == 0) {
			stid := id
			out.Findings = append(out.Findings, Finding{
				StationID: &stid,
				RuleID:    "bbox",
				Severity:  SeverityError,
				Issue: fmt.Sprintf("station %s/%s (%q) at impossible lat=%.4f lon=%.4f — error blocks publish",
					source, ext, name, la, lo),
			})
			continue
		}

		// Outside the Mexican-territory envelope — almost always a typo
		// in CONAGUA's metadata (sign flipped, decimal nudged), but we
		// can't be sure, so warn.
		if la < mxLatMin || la > mxLatMax || lo < mxLonMin || lo > mxLonMax {
			stid := id
			out.Findings = append(out.Findings, Finding{
				StationID: &stid,
				RuleID:    "bbox",
				Severity:  SeverityWarn,
				Issue: fmt.Sprintf("station %s/%s (%q) at lat=%.4f lon=%.4f outside MX bbox [%.1f,%.1f]×[%.1f,%.1f] — likely typo",
					source, ext, name, la, lo, mxLatMin, mxLatMax, mxLonMin, mxLonMax),
			})
		}
	}
	if err := rows.Err(); err != nil {
		return RuleResult{}, fmt.Errorf("scan stations: %w", err)
	}
	return out, nil
}

// missingCoordPart names which of (lat, lon) is null, so the warning
// text reads naturally. If both are null, "lat" is reported (the
// rare case is fine; the warning is still actionable).
func missingCoordPart(lat, lon sql.NullFloat64) string {
	switch {
	case !lat.Valid && !lon.Valid:
		return "lat and lon"
	case !lat.Valid:
		return "lat"
	default:
		return "lon"
	}
}
