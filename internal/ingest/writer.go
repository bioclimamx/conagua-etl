package ingest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ingestWriter owns the prepared statements that the per-station tx loop
// reuses across every station in a full-snapshot run. Prepare once via
// newIngestWriter, rebind into each tx via tx.Stmt(w.X), Close at end of
// run. This amortizes compile cost across the ~5,400-station loop (5
// prepares total instead of ~27k) and centralizes the INSERT SQL.
type ingestWriter struct {
	insertDaily         *sql.Stmt
	insertMonthlyNormal *sql.Stmt
	insertNormalsExtras *sql.Stmt
	insertWarning       *sql.Stmt
}

// newIngestWriter prepares every statement the per-station tx loop
// needs. On partial failure it closes anything already prepared so
// callers never see a half-built writer.
func newIngestWriter(ctx context.Context, db *sql.DB) (*ingestWriter, error) {
	w := &ingestWriter{}

	steps := []struct {
		name string
		dst  **sql.Stmt
		sql  string
	}{
		{"daily", &w.insertDaily, `
INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (station_id, date) DO UPDATE SET
    tmax   = excluded.tmax,
    tmin   = excluded.tmin,
    precip = excluded.precip,
    evap   = excluded.evap;`},
		{"monthly_normal", &w.insertMonthlyNormal, `
INSERT INTO monthly_normals (station_id, period, month, tmax, tmin, tmean, precip, evap)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (station_id, period, month) DO UPDATE SET
    tmax      = excluded.tmax,
    tmin      = excluded.tmin,
    tmean     = excluded.tmean,
    precip    = excluded.precip,
    evap      = excluded.evap;`},
		{"normals_extras", &w.insertNormalsExtras, `
INSERT INTO monthly_normals_extras (
    station_id, period, month,
    tmax_monthly_extreme, tmax_monthly_extreme_year, tmax_daily_extreme, tmax_daily_extreme_date,
    tmin_monthly_extreme, tmin_monthly_extreme_year, tmin_daily_extreme, tmin_daily_extreme_date,
    precip_monthly_extreme, precip_monthly_extreme_year, precip_daily_extreme, precip_daily_extreme_date,
    tmax_years_with_data, tmin_years_with_data, tmean_years_with_data,
    precip_years_with_data, evap_years_with_data,
    rain_days, rain_days_years_with_data
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (station_id, period, month) DO UPDATE SET
    tmax_monthly_extreme        = excluded.tmax_monthly_extreme,
    tmax_monthly_extreme_year   = excluded.tmax_monthly_extreme_year,
    tmax_daily_extreme          = excluded.tmax_daily_extreme,
    tmax_daily_extreme_date     = excluded.tmax_daily_extreme_date,
    tmin_monthly_extreme        = excluded.tmin_monthly_extreme,
    tmin_monthly_extreme_year   = excluded.tmin_monthly_extreme_year,
    tmin_daily_extreme          = excluded.tmin_daily_extreme,
    tmin_daily_extreme_date     = excluded.tmin_daily_extreme_date,
    precip_monthly_extreme      = excluded.precip_monthly_extreme,
    precip_monthly_extreme_year = excluded.precip_monthly_extreme_year,
    precip_daily_extreme        = excluded.precip_daily_extreme,
    precip_daily_extreme_date   = excluded.precip_daily_extreme_date,
    tmax_years_with_data        = excluded.tmax_years_with_data,
    tmin_years_with_data        = excluded.tmin_years_with_data,
    tmean_years_with_data       = excluded.tmean_years_with_data,
    precip_years_with_data      = excluded.precip_years_with_data,
    evap_years_with_data        = excluded.evap_years_with_data,
    rain_days                   = excluded.rain_days,
    rain_days_years_with_data   = excluded.rain_days_years_with_data;`},
		{"warning", &w.insertWarning, `
INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
VALUES (?, ?, ?, ?, ?);`},
	}

	for _, s := range steps {
		stmt, err := db.PrepareContext(ctx, s.sql)
		if err != nil {
			_ = w.Close()
			return nil, fmt.Errorf("prepare %s: %w", s.name, err)
		}
		*s.dst = stmt
	}
	return w, nil
}

// Close releases every non-nil prepared statement. Safe to call more
// than once — each stmt is zeroed after close so subsequent calls
// no-op. Errors from later stmts do not mask earlier ones.
func (w *ingestWriter) Close() error {
	var firstErr error
	stmts := []**sql.Stmt{
		&w.insertDaily, &w.insertMonthlyNormal,
		&w.insertNormalsExtras, &w.insertWarning,
	}
	for _, p := range stmts {
		if *p == nil {
			continue
		}
		if err := (*p).Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		*p = nil
	}
	return firstErr
}

// WriteWarnings persists a batch of warnings through the writer's
// prepared statement, bound to the caller's tx. Line 0 and a nil
// StationID store as NULL; a severity outside {warn, error} is
// rejected before any row is written. Safe to call with an empty
// slice.
func (w *ingestWriter) WriteWarnings(ctx context.Context, tx *sql.Tx, ws []Warning) error {
	if len(ws) == 0 {
		return nil
	}
	if w.insertWarning == nil {
		return errors.New("ingestWriter.WriteWarnings: writer is closed")
	}
	stmt := tx.Stmt(w.insertWarning)
	for _, wn := range ws {
		if wn.Severity != SeverityWarn && wn.Severity != SeverityError {
			return fmt.Errorf("invalid severity %q (want %q or %q)", wn.Severity, SeverityWarn, SeverityError)
		}
		var line any
		if wn.Line > 0 {
			line = wn.Line
		}
		var stationID any
		if wn.StationID != nil {
			stationID = *wn.StationID
		}
		if _, err := stmt.ExecContext(ctx, stationID, wn.SourceFile, line, wn.Severity, wn.Issue); err != nil {
			return fmt.Errorf("insert warning (%s): %w", wn.SourceFile, err)
		}
	}
	return nil
}
