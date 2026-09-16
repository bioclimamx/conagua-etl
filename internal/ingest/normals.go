package ingest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// normalsResult is the per-file outcome of a normals ingest.
type normalsResult struct {
	rowsInserted   int
	extrasInserted int
	warnings       []Warning
	fileOpened     bool
	fileMissing    bool
}

// ingestNormals fetches and parses one normals file for (station, period),
// enriches the stations row from the file header, and inserts the 12
// monthly_normals rows plus the 12 monthly_normals_extras rows inside
// the caller's transaction.
//
// A missing file at the sink is not an error (stations may legitimately
// lack a given normals period). A period-mismatch between the sink
// Kind and the file's "NORMAL CLIMATOLÓGICA" banner, by contrast,
// IS surfaced as a warning so the snapshot can be investigated —
// but ingest continues with the other kinds for the same station.
func ingestNormals(ctx context.Context, sink snapshot.Sink, tx *sql.Tx, w *ingestWriter,
	sp snapshot.StationProgress, stationID int64, date string,
	kind conagua.Kind, period string,
) (normalsResult, error) {
	addr := snapshot.Address{Date: date, Kind: kind, StationID: sp.ID}
	sourceFile := keyForAddr(addr)
	stationIDPtr := &stationID

	// Warnings for this source_file are cleared by the orchestrator.

	rdr, err := sink.Get(ctx, addr)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Same ledger/sink divergence path as daily — record and skip.
			return normalsResult{
				fileMissing: true,
				warnings: []Warning{{
					StationID:  stationIDPtr,
					SourceFile: sourceFile,
					Severity:   SeverityWarn,
					Issue:      "pull ledger says fetched, but sink has no object at this key",
				}},
			}, nil
		}
		return normalsResult{}, fmt.Errorf("sink.Get %s: %w", sourceFile, err)
	}
	defer rdr.Close() //nolint:errcheck // read-side close; no recovery possible

	res := normalsResult{fileOpened: true}

	header, rows, extras, warns, err := conagua.ParseNormalsFile(rdr, period)
	for _, pw := range warns {
		res.warnings = append(res.warnings, warningFromParse(pw, sourceFile, stationIDPtr))
	}
	if err != nil {
		// Period mismatch / missing header / etc. — file-level fatal.
		// Record as 'error' so publish can gate on it; other kinds for
		// the same station continue.
		res.warnings = append(res.warnings, fileErrorWarning(sourceFile, stationIDPtr, err.Error()))
		return res, nil
	}

	if _, err := UpsertStation(ctx, tx, StationUpsert{
		Source:     SourceConaguaConventional,
		ExternalID: header.ExternalID,
		Name:       header.Name,
		Lat:        header.Lat,
		Lon:        header.Lon,
		AltitudeM:  header.AltitudeM,
	}); err != nil {
		return res, fmt.Errorf("enrich station from %s: %w", keyForAddr(addr), err)
	}

	// Rebind the run-scoped prepared statements into this tx once per
	// file. tx.Stmt auto-closes on tx commit/rollback.
	normalStmt := tx.Stmt(w.insertMonthlyNormal)
	extrasStmt := tx.Stmt(w.insertNormalsExtras)

	for _, row := range rows {
		// No RH here — provenance by table identity: monthly_normals is
		// the CONAGUA-observed table, and CONAGUA's conventional archive
		// publishes no observed humidity. POWER's reanalysis RH lives in
		// monthly_supplement, keyed by cell.
		_, err := normalStmt.ExecContext(ctx,
			stationID, period, row.Month,
			floatArg(row.Tmax), floatArg(row.Tmin), floatArg(row.Tmean),
			floatArg(row.Precip), floatArg(row.Evap),
		)
		if err != nil {
			return res, fmt.Errorf("insert normals %s/%s/m%d: %w", header.ExternalID, period, row.Month, err)
		}
		res.rowsInserted++
	}

	for _, e := range extras {
		_, err := extrasStmt.ExecContext(ctx,
			stationID, period, e.Month,
			floatArg(e.TmaxMonthlyExtreme), intArg(e.TmaxMonthlyExtremeYear),
			floatArg(e.TmaxDailyExtreme), stringArg(e.TmaxDailyExtremeDate),
			floatArg(e.TminMonthlyExtreme), intArg(e.TminMonthlyExtremeYear),
			floatArg(e.TminDailyExtreme), stringArg(e.TminDailyExtremeDate),
			floatArg(e.PrecipMonthlyExtreme), intArg(e.PrecipMonthlyExtremeYear),
			floatArg(e.PrecipDailyExtreme), stringArg(e.PrecipDailyExtremeDate),
			intArg(e.TmaxYearsWithData), intArg(e.TminYearsWithData), intArg(e.TmeanYearsWithData),
			intArg(e.PrecipYearsWithData), intArg(e.EvapYearsWithData),
			floatArg(e.RainDays), intArg(e.RainDaysYearsWithData),
		)
		if err != nil {
			return res, fmt.Errorf("insert extras %s/%s/m%d: %w", header.ExternalID, period, e.Month, err)
		}
		res.extrasInserted++
	}

	return res, nil
}

// intArg mirrors floatArg for *int values — unwrap typed nil pointers
// to an untyped nil so the driver binds NULL.
func intArg(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

// stringArg mirrors floatArg/intArg for *string values.
func stringArg(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}
