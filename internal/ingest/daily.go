package ingest

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// dailyResult is the per-file outcome of a daily ingest, shaped so the
// orchestrator can fold it into Report without threading raw counters
// through a function signature.
type dailyResult struct {
	rowsInserted int
	warnings     []Warning
	fileOpened   bool
	fileMissing  bool
}

// ingestDaily fetches and parses the daily file for one station,
// enriches the stations row from the file header (lat/lon/altitude),
// and inserts the daily observations inside the caller's transaction.
//
// A missing daily file at the sink is not an error — many stations
// have normals but no daily records. The caller sees fileMissing=true
// in the result and moves on.
func ingestDaily(ctx context.Context, sink snapshot.Sink, tx *sql.Tx, w *ingestWriter,
	sp snapshot.StationProgress, stationID int64, date string,
) (dailyResult, error) {
	addr := snapshot.Address{
		Date:      date,
		Kind:      conagua.KindDaily,
		StationID: sp.ID, // catalog form; the Sink wants 5-digit
	}
	sourceFile := keyForAddr(addr)
	stationIDPtr := &stationID

	// Note: warnings for this source_file have already been cleared by
	// the orchestrator's per-kind loop, so we don't need to do it here.

	rdr, err := sink.Get(ctx, addr)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Pull ledger said fetched, sink says missing — out-of-band
			// deletion or ledger/sink divergence. Surface as a warning
			// instead of silently counting as "not in catalog".
			return dailyResult{
				fileMissing: true,
				warnings: []Warning{{
					StationID:  stationIDPtr,
					SourceFile: sourceFile,
					Severity:   SeverityWarn,
					Issue:      "pull ledger says fetched, but sink has no object at this key",
				}},
			}, nil
		}
		return dailyResult{}, fmt.Errorf("sink.Get %s: %w", sourceFile, err)
	}
	defer rdr.Close() //nolint:errcheck // read-side close; no recovery possible

	res := dailyResult{fileOpened: true}
	br := bufio.NewReader(rdr)

	header, headerWarns, err := conagua.ParseHeader(br)
	for _, pw := range headerWarns {
		res.warnings = append(res.warnings, warningFromParse(pw, sourceFile, stationIDPtr))
	}
	if err != nil {
		// File-level failure: no header at all. Record as 'error' so
		// `publish` can gate on it, but don't bubble — the caller is
		// still ingesting other kinds for the same station.
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

	// Rebind the run-scoped prepared daily-insert statement into this tx.
	// tx.Stmt returns a *sql.Stmt that Close()s automatically on tx
	// commit/rollback, so we don't need a defer here.
	stmt := tx.Stmt(w.insertDaily)

	// parseErr carries the first insert failure out of the parse
	// callback — ParseDaily can't return it directly.
	var parseErr error
	err = conagua.ParseDaily(br, 0,
		func(row conagua.DailyRow) {
			if parseErr != nil {
				return
			}
			_, xerr := stmt.ExecContext(ctx, stationID, row.Date,
				floatArg(row.Tmax), floatArg(row.Tmin),
				floatArg(row.Precip), floatArg(row.Evap))
			if xerr != nil {
				parseErr = fmt.Errorf("insert daily %s/%s: %w", header.ExternalID, row.Date, xerr)
				return
			}
			res.rowsInserted++
		},
		func(pw conagua.Warning) {
			res.warnings = append(res.warnings, warningFromParse(pw, sourceFile, stationIDPtr))
		},
	)
	if parseErr != nil {
		return res, parseErr
	}
	if err != nil {
		return res, fmt.Errorf("parse daily %s: %w", keyForAddr(addr), err)
	}
	return res, nil
}

// keyForAddr mirrors the sink's own key-building rule so error
// messages self-identify across snapshots without forcing the caller to
// know sink internals.
func keyForAddr(a snapshot.Address) string {
	return fmt.Sprintf("conagua-raw/%s/%s/%s.txt", a.Date, string(a.Kind), a.StationID)
}
