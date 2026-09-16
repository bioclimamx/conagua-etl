package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// stationResult is the per-station outcome returned by ingestOneStation.
// Fields are filled monotonically as the per-station tx progresses; on
// err != nil the tx was rolled back and the row counters reflect work
// attempted but not persisted — the orchestrator must only fold a
// station into the top-level Report when err == nil.
type stationResult struct {
	dailyRows   int
	normalsRows int
	extrasRows  int

	filesOpened       int
	filesPullErrored  int
	filesNotInCatalog int

	// firstLastSet is true if UPDATE stations SET first_year/last_year
	// found a non-NULL value to store (either from daily MIN/MAX or the
	// normals-period fallback). wmoPeriodsSet is the count of periods
	// whose two wmo_completeness_* columns were updated to non-NULL
	// values for this station.
	firstLastSet  bool
	wmoPeriodsSet int

	warnings []Warning
	err      error
}

// ingestOneStation drives one station through the per-kind ingest,
// first/last year derivation, WMO scoring, and warnings persistence
// inside a single per-station tx. On any step's Go-level error the
// tx is rolled back and the error is returned in the stationResult;
// this is the failure path. In-band file-level parse errors (no
// header, malformed sections) are recorded as severity='error'
// warnings and DO NOT abort the station — they flow to
// parsing_warnings on commit.
//
// The orchestrator reads the result:
//   - err != nil → station rolled back, report it in PerStationFailures
//   - err == nil → station committed, fold counters into Report
func ingestOneStation(ctx context.Context, db *sql.DB, writer *ingestWriter,
	opts Options, stationID int64, sp snapshot.StationProgress,
) stationResult {
	var res stationResult

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		res.err = fmt.Errorf("begin tx: %w", err)
		return res
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	var warnings []Warning
	kinds := resolveKinds(opts.Kind)

	// Per-kind fetch + parse + insert.
	for _, k := range kinds {
		addr := snapshot.Address{
			Date:      opts.SnapshotDate,
			Kind:      k,
			StationID: sp.ID,
		}
		sourceFile := keyForAddr(addr)

		// Per-file idempotence: wipe any prior warnings for this exact
		// sink key so reingest never double-counts. Applies whether we
		// fetch, skip-on-error, or skip-on-catalog-miss.
		if err := clearWarningsForSourceFile(ctx, tx, sourceFile); err != nil {
			res.err = fmt.Errorf("clear warnings %s: %w", sourceFile, err)
			return res
		}

		fs, ok := sp.Files[k]
		switch {
		case !ok || fs.Outcome == snapshot.OutcomeNotFound:
			res.filesNotInCatalog++
			continue
		case fs.Outcome == snapshot.OutcomeError:
			sid := stationID
			warnings = append(warnings, Warning{
				StationID:  &sid,
				SourceFile: sourceFile,
				Severity:   SeverityWarn,
				Issue:      pullErrorIssue(fs),
			})
			res.filesPullErrored++
			continue
		case fs.Outcome != snapshot.OutcomeFetched && fs.Outcome != snapshot.OutcomeSkipped:
			sid := stationID
			warnings = append(warnings, Warning{
				StationID:  &sid,
				SourceFile: sourceFile,
				Severity:   SeverityWarn,
				Issue:      fmt.Sprintf("unexpected pull outcome %q in ledger", fs.Outcome),
			})
			res.filesNotInCatalog++
			continue
		}

		switch {
		case k == conagua.KindDaily:
			r, err := ingestDaily(ctx, opts.Sink, tx, writer, sp, stationID, opts.SnapshotDate)
			if err != nil {
				res.err = err
				return res
			}
			res.dailyRows += r.rowsInserted
			warnings = append(warnings, r.warnings...)
			if r.fileOpened {
				res.filesOpened++
			}
			if r.fileMissing {
				res.filesNotInCatalog++
			}
		case isIngestableNormalsKind(k):
			period, _ := conagua.PeriodForKind(k)
			r, err := ingestNormals(ctx, opts.Sink, tx, writer, sp, stationID, opts.SnapshotDate, k, period)
			if err != nil {
				res.err = err
				return res
			}
			res.normalsRows += r.rowsInserted
			res.extrasRows += r.extrasInserted
			warnings = append(warnings, r.warnings...)
			if r.fileOpened {
				res.filesOpened++
			}
			if r.fileMissing {
				res.filesNotInCatalog++
			}
		}
	}

	// first_year / last_year from daily MIN/MAX date with a
	// normals-period fallback. Doesn't error on an "empty station"
	// case — just leaves the columns NULL.
	didSet, err := updateFirstLastYear(ctx, tx, stationID)
	if err != nil {
		res.err = err
		return res
	}
	res.firstLastSet = didSet

	// WMO completeness scoring. Only meaningful if we touched normals.
	if includesNormals(opts.Kind) {
		byPeriod, err := LoadExtrasForScoring(ctx, tx, stationID)
		if err != nil {
			res.err = err
			return res
		}
		scores := map[string]WMOScore{}
		for period, cells := range byPeriod {
			if s, hadAny := ScoreWMO(cells); hadAny {
				scores[period] = s
			}
		}
		if err := StoreWMOCompleteness(ctx, tx, stationID, scores); err != nil {
			res.err = err
			return res
		}
		res.wmoPeriodsSet = len(scores)
	}

	// Persist warnings through the writer's prepared stmt.
	if err := writer.WriteWarnings(ctx, tx, warnings); err != nil {
		res.err = err
		return res
	}

	if err := tx.Commit(); err != nil {
		res.err = fmt.Errorf("commit station %s: %w", sp.ID, err)
		return res
	}
	committed = true

	// Only expose warnings on the success path. On failure the caller
	// already has err; the warnings list is what landed in the DB.
	res.warnings = warnings
	return res
}

// updateFirstLastYear sets stations.first_year / last_year from the
// daily series MIN/MAX date. If the station has no daily rows (common
// — many have only normals), falls back to the earliest/latest
// normals period endpoints we just wrote.
//
// Returns didSet=true iff at least one column ended up non-NULL, so
// the orchestrator can track how many stations got a date range.
func updateFirstLastYear(ctx context.Context, tx *sql.Tx, stationID int64) (bool, error) {
	var firstFromDaily, lastFromDaily sql.NullInt64
	err := tx.QueryRowContext(ctx, `
SELECT CAST(strftime('%Y', MIN(date)) AS INTEGER),
       CAST(strftime('%Y', MAX(date)) AS INTEGER)
  FROM daily_observations
 WHERE station_id = ?`, stationID).Scan(&firstFromDaily, &lastFromDaily)
	if err != nil {
		return false, fmt.Errorf("derive first/last from daily (station %d): %w", stationID, err)
	}

	first := firstFromDaily
	last := lastFromDaily

	if !first.Valid || !last.Valid {
		// Fall back to normals periods. The period string is like
		// "1991-2020" — first 4 chars is the start year, last 4 the end.
		var firstPeriod, lastPeriod sql.NullString
		err := tx.QueryRowContext(ctx, `
SELECT MIN(period), MAX(period)
  FROM monthly_normals
 WHERE station_id = ?`, stationID).Scan(&firstPeriod, &lastPeriod)
		if err != nil {
			return false, fmt.Errorf("derive first/last from normals (station %d): %w", stationID, err)
		}
		if !first.Valid && firstPeriod.Valid && len(firstPeriod.String) >= 9 {
			if y, convErr := strconv.Atoi(firstPeriod.String[:4]); convErr == nil {
				first = sql.NullInt64{Int64: int64(y), Valid: true}
			}
		}
		if !last.Valid && lastPeriod.Valid && len(lastPeriod.String) >= 9 {
			if y, convErr := strconv.Atoi(lastPeriod.String[5:]); convErr == nil {
				last = sql.NullInt64{Int64: int64(y), Valid: true}
			}
		}
	}

	// Bind NULL as untyped nil — sql.NullInt64{Valid:false} works with
	// modernc, but nil-any makes the intent unmistakable.
	var firstArg, lastArg any
	if first.Valid {
		firstArg = first.Int64
	}
	if last.Valid {
		lastArg = last.Int64
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE stations SET first_year = ?, last_year = ? WHERE id = ?`,
		firstArg, lastArg, stationID,
	); err != nil {
		return false, fmt.Errorf("update stations first/last (station %d): %w", stationID, err)
	}
	return first.Valid || last.Valid, nil
}
