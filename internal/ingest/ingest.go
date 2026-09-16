// Package ingest turns a pulled CONAGUA snapshot into rows in the
// SQLite database. It sits above the conagua parsers (which know
// nothing about SQL) and below the CLI (which wires it to a Sink and a
// DB path): Run drives the station seed, the per-station transactional
// load, first/last-year derivation, WMO completeness scoring, and
// warnings persistence.
package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// Options controls one ingest run. Zero values for the optional fields
// (Kind, ExternalID) select the broadest behavior.
type Options struct {
	// Sink points at the snapshot storage (local or R2). Required.
	Sink snapshot.Sink
	// SinkKind is the sink's storage backend name ("local" or "r2"),
	// recorded in ingest_runs for provenance. Falls back to "unknown"
	// if empty.
	SinkKind string
	// MetadataRoot is where _index.json lives on local disk. Metadata
	// is always on local disk per the architecture, even with --sink r2.
	MetadataRoot string
	// SnapshotDate is the YYYY-MM-DD prefix under conagua-raw/.
	SnapshotDate string
	// DBPath is the SQLite file to write into. A fresh file is created
	// if absent; the schema is applied idempotently.
	DBPath string
	// ETLGitSHA is the commit that built this binary, recorded in
	// ingest_runs for provenance. Empty stores NULL — downstream
	// consumers treat that as "unknown".
	ETLGitSHA string
	// ExternalID scopes ingest to one station. Empty means "every
	// station in the snapshot"; set to a short-form ID to restrict
	// to one.
	ExternalID string
	// StationsOnly seeds stations from _index.json and exits. Mutually
	// exclusive with Kind.
	StationsOnly bool
	// Kind restricts which data kinds are ingested. Valid values: "",
	// "daily", "normals". Empty means all.
	Kind string
	// OnStationDone fires once per station in the per-station loop,
	// after the tx has either committed or rolled back. The CLI uses
	// this to drive the live progress line; tests may leave it nil.
	OnStationDone func(StationOutcome)
}

// StationOutcome is the per-station event fed to OnStationDone. It
// carries enough state for a progress renderer to format a live line
// (index/total/name + row counts + elapsed) without having to peek into
// the Report struct.
type StationOutcome struct {
	Index       int // 1-based ordinal across Total
	Total       int
	ExternalID  string
	Name        string
	Elapsed     time.Duration
	DailyRows   int
	NormalsRows int
	ExtrasRows  int
	Warnings    int
	Err         error // non-nil ⇒ station was rolled back; tx state never landed
}

// StationFailure records one station that didn't commit. The
// orchestrator accumulates these in Report.PerStationFailures so the
// final summary can surface them without every caller having to
// subscribe to OnStationDone.
type StationFailure struct {
	ExternalID string
	Name       string
	Err        error
}

// Report totals the write-side work of a single Run invocation.
type Report struct {
	StationsSeeded      int
	DailyRowsInserted   int
	NormalsRowsInserted int
	ExtrasRowsInserted  int

	// Per-station loop outcome tallies. Attempted is the number of
	// stations the loop visited (either the whole manifest or the one
	// picked by ExternalID); Succeeded + Failed == Attempted in steady
	// state. Failed stations have their tx rolled back, so none of
	// their row counts flow into the per-row totals above.
	StationsAttempted  int
	StationsSucceeded  int
	StationsFailed     int
	PerStationFailures []StationFailure

	// StationsWithFirstLast is the count of stations where first_year
	// or last_year ended up non-NULL after this run. StationsWithWMO
	// is the same for at least one of the eight wmo_completeness_*
	// columns.
	StationsWithFirstLast int
	StationsWithWMO       int

	// IngestRunID is the PK of the `ingest_runs` row written for this
	// invocation. 0 when the run exited before the row was created
	// (e.g. --stations-only, or a seed-stage failure).
	IngestRunID int64

	// Cache stats from CachingSink (if wired). Zero for local-only runs.
	CacheHits   int64
	CacheMisses int64

	// Warnings holds every issue emitted during the run, already
	// enriched with source_file + station_id. Persisted into
	// parsing_warnings as part of each station's tx; also handed back
	// so the CLI can print a summary. Only committed stations
	// contribute: a failed station's warnings are dropped with its
	// rolled-back tx, and its error is in PerStationFailures.
	Warnings []Warning

	// FilesOpened counts kinds where we actually read+parsed the file.
	// FilesPullErrored is outcome='error' in the pull ledger — files
	// the pull couldn't retrieve (counts as a data gap in this
	// release, and each produces a parsing_warnings row). FilesNotInCatalog
	// is outcome='not_found' or entirely absent from the catalog —
	// CONAGUA simply doesn't publish that kind for this station; not
	// a gap we introduced, so no warning. It also absorbs ledger
	// entries with unexpected outcomes (e.g. a stale 'pending'), which
	// are surfaced as warnings and then counted here.
	FilesOpened       int
	FilesPullErrored  int
	FilesNotInCatalog int
}

// Run executes one ingest over the snapshot at opts.MetadataRoot /
// opts.SnapshotDate, writing into opts.DBPath.
//
// Transaction structure:
//  1. Seed tx — upsert every station from _index.json. One tx, commit,
//     done. Fast (one row per station).
//  2. ingest_runs row — a tiny tx inserts a 'running' row for
//     provenance.
//  3. Per-station loop — each station gets its own tx spanning fetches,
//     inserts, first/last year, WMO scoring, and warnings. A failed
//     station rolls back that station's tx and the loop continues to
//     the next. This preserves the invariant "a station is either
//     fully ingested or not at all" while letting one bad file not
//     kill the run.
//  4. ingest_runs close — UPDATE the row with final totals.
//
// Returns a Report on every path (including loop failure) so the caller
// can print a partial summary.
func Run(ctx context.Context, opts Options) (*Report, error) {
	if err := validateOptions(opts); err != nil {
		return nil, err
	}

	db, err := schema.Open(opts.DBPath)
	if err != nil {
		return nil, fmt.Errorf("schema.Open: %w", err)
	}
	defer db.Close() //nolint:errcheck // teardown close; the run's outcome is already determined

	writer, err := newIngestWriter(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("ingest writer: %w", err)
	}
	defer writer.Close() //nolint:errcheck // teardown close; the run's outcome is already determined

	// Load the snapshot manifest; drives both the stations seed and the
	// per-station file fetches.
	indexPath := filepath.Join(opts.MetadataRoot, "conagua-raw", opts.SnapshotDate, "_index.json")
	if err := bootstrapIndexIfMissing(ctx, opts.Sink, opts.SnapshotDate, indexPath); err != nil {
		return nil, err
	}
	progress, err := loadIndex(indexPath)
	if err != nil {
		return nil, err
	}

	report := &Report{}

	// 1. Seed stations.
	ids, err := runSeedTx(ctx, db, progress, report)
	if err != nil {
		return nil, err
	}

	if opts.StationsOnly {
		// --stations-only is the verify-the-seed-path dev mode; no
		// ingest_runs row, no per-station loop. Report what we seeded
		// and exit.
		return report, nil
	}

	// 2. Pick targets: either the whole manifest or the one station
	// chosen by --external-id.
	targets, err := pickTargets(progress.Stations, opts.ExternalID)
	if err != nil {
		return nil, err
	}

	// 3. Open an ingest_runs row. On any subsequent error we close it
	// as 'aborted' so `validate` can tell apart clean vs. interrupted
	// runs.
	runID, err := openIngestRun(ctx, db, opts)
	if err != nil {
		return nil, err
	}
	report.IngestRunID = runID

	// 4. Per-station loop. Each iteration owns its own tx.
	loopErr := runPerStationLoop(ctx, db, writer, opts, ids, targets, report)

	// Capture cache stats before returning.
	if cs, ok := opts.Sink.(interface{ Stats() (int64, int64) }); ok {
		report.CacheHits, report.CacheMisses = cs.Stats()
	}

	// 5. Close the run row. Use context.Background so cancellation of
	// the outer context doesn't prevent us from writing the final
	// status — the DB update is quick and leaving the row at 'running'
	// forever is worse than a slightly-delayed shutdown.
	finalStatus := "complete"
	if loopErr != nil {
		finalStatus = "aborted"
	}
	if err := closeIngestRun(context.Background(), db, runID, finalStatus, report); err != nil {
		// Non-fatal: log and move on. A 'running' row can be
		// reconciled later.
		fmt.Fprintf(os.Stderr, "[%s] close ingest_runs row %d: %v\n",
			time.Now().Format("15:04:05"), runID, err)
	}

	if loopErr != nil {
		return report, loopErr
	}
	return report, nil
}

// runSeedTx upserts every station in the manifest in a single tx.
// The seed is unconditional and fast — we want it to happen once per
// run, outside the per-station tx loop, so a --kind daily run doesn't
// have to carry a full seed through every station's tx.
func runSeedTx(ctx context.Context, db *sql.DB, progress *snapshot.Progress, report *Report) (map[string]int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin seed tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ids, err := seedStations(ctx, tx, progress.Stations, SourceConaguaConventional)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit seed: %w", err)
	}
	report.StationsSeeded = len(progress.Stations)
	return ids, nil
}

// pickTargets returns the set of stations the per-station loop will
// iterate. Empty ExternalID means "everything in the manifest"; a
// non-empty one narrows to that single station.
func pickTargets(all []snapshot.StationProgress, externalID string) ([]snapshot.StationProgress, error) {
	if externalID == "" {
		return all, nil
	}
	target, ok := findTarget(all, externalID)
	if !ok {
		return nil, fmt.Errorf("external_id %q not in snapshot", externalID)
	}
	return []snapshot.StationProgress{target}, nil
}

// runPerStationLoop iterates the target stations, each in its own tx.
// A per-station failure is recorded in the Report (and surfaced by the
// caller's OnStationDone renderer) but does NOT abort the loop — one
// bad file shouldn't kill a 5,400-station run. A loop-level error (ctx
// cancellation, driver disconnect) IS fatal and returns.
func runPerStationLoop(ctx context.Context, db *sql.DB, writer *ingestWriter,
	opts Options, ids map[string]int64, targets []snapshot.StationProgress, report *Report,
) error {
	report.StationsAttempted = len(targets)
	for i, sp := range targets {
		// Respect context cancellation between stations. We let
		// an in-flight station finish first — the tx either
		// commits or is rolled back by its own defer.
		if err := ctx.Err(); err != nil {
			return err
		}

		externalID := conagua.ShortID(sp.ID)
		stationID, ok := ids[externalID]
		if !ok {
			// Shouldn't happen if the seed covered every manifest
			// entry; defensive for malformed input.
			return fmt.Errorf("station %q missing from seed map", externalID)
		}

		start := time.Now()
		res := ingestOneStation(ctx, db, writer, opts, stationID, sp)
		elapsed := time.Since(start)

		if res.err != nil {
			report.StationsFailed++
			report.PerStationFailures = append(report.PerStationFailures, StationFailure{
				ExternalID: externalID,
				Name:       sp.Name,
				Err:        res.err,
			})
		} else {
			report.StationsSucceeded++
			mergeStationResult(report, res)
		}

		if opts.OnStationDone != nil {
			opts.OnStationDone(StationOutcome{
				Index:       i + 1,
				Total:       len(targets),
				ExternalID:  externalID,
				Name:        sp.Name,
				Elapsed:     elapsed,
				DailyRows:   res.dailyRows,
				NormalsRows: res.normalsRows,
				ExtrasRows:  res.extrasRows,
				Warnings:    len(res.warnings),
				Err:         res.err,
			})
		}
	}
	return nil
}

// mergeStationResult rolls a successful per-station result into the
// top-level Report. Called only for committed stations; failed
// stations contribute a PerStationFailures entry but no row counts
// (since their tx was rolled back).
func mergeStationResult(r *Report, s stationResult) {
	r.DailyRowsInserted += s.dailyRows
	r.NormalsRowsInserted += s.normalsRows
	r.ExtrasRowsInserted += s.extrasRows
	r.FilesOpened += s.filesOpened
	r.FilesPullErrored += s.filesPullErrored
	r.FilesNotInCatalog += s.filesNotInCatalog
	r.Warnings = append(r.Warnings, s.warnings...)
	if s.firstLastSet {
		r.StationsWithFirstLast++
	}
	if s.wmoPeriodsSet > 0 {
		r.StationsWithWMO++
	}
}

// openIngestRun inserts a `status='running'` row into ingest_runs and
// returns its PK. Uses the outer DB connection (not a tx) — the row
// must be visible to downstream queries even if the per-station loop
// crashes before it can close the row.
func openIngestRun(ctx context.Context, db *sql.DB, opts Options) (int64, error) {
	sinkKind := opts.SinkKind
	if sinkKind == "" {
		sinkKind = "unknown"
	}
	var id int64
	err := db.QueryRowContext(ctx, `
INSERT INTO ingest_runs (started_at, snapshot_date, sink_kind, etl_git_sha, status)
VALUES (?, ?, ?, ?, 'running')
RETURNING id`,
		time.Now().UTC().Format(time.RFC3339),
		opts.SnapshotDate,
		sinkKind,
		nilIfEmpty(opts.ETLGitSHA),
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("open ingest_runs row: %w", err)
	}
	return id, nil
}

// closeIngestRun transitions the running row to its final status with
// counters filled from the report.
func closeIngestRun(ctx context.Context, db *sql.DB, runID int64, status string, report *Report) error {
	_, err := db.ExecContext(ctx, `
UPDATE ingest_runs SET
    finished_at        = ?,
    status             = ?,
    stations_attempted = ?,
    stations_succeeded = ?,
    stations_failed    = ?,
    daily_rows         = ?,
    normals_rows       = ?,
    extras_rows        = ?,
    warnings_total     = ?
 WHERE id = ?`,
		time.Now().UTC().Format(time.RFC3339),
		status,
		report.StationsAttempted,
		report.StationsSucceeded,
		report.StationsFailed,
		report.DailyRowsInserted,
		report.NormalsRowsInserted,
		report.ExtrasRowsInserted,
		len(report.Warnings),
		runID)
	if err != nil {
		return fmt.Errorf("close ingest_runs row %d: %w", runID, err)
	}
	return nil
}

// validateOptions rejects missing requirements and contradictions up
// front so later logic doesn't have to consider every combination.
func validateOptions(o Options) error {
	if o.Sink == nil {
		return errors.New("ingest: Sink is required")
	}
	if o.MetadataRoot == "" {
		return errors.New("ingest: MetadataRoot is required")
	}
	if o.SnapshotDate == "" {
		return errors.New("ingest: SnapshotDate is required")
	}
	if o.DBPath == "" {
		return errors.New("ingest: DBPath is required")
	}
	if o.StationsOnly && o.Kind != "" {
		return errors.New("ingest: --stations-only and --kind are mutually exclusive")
	}
	if o.Kind != "" && o.Kind != "daily" && o.Kind != "normals" {
		return fmt.Errorf("ingest: unknown --kind %q (want daily|normals)", o.Kind)
	}
	return nil
}

// bootstrapIndexIfMissing fetches _index.json from the sink and writes
// it to indexPath when the local copy is absent. Intended for the
// `ingest --sink r2` + fresh `--root` case: `pull` writes the index
// locally (and mirrors it to R2), but a consumer running ingest
// against a pristine cache dir has no local copy yet.
//
// The metadata-on-local-disk invariant is preserved: after this
// bootstrap, loadIndex reads local bytes like any other run.
//
// If the sink doesn't implement FetchIndex (LocalFS today), we leave
// the absence alone — loadIndex will surface a clear "no such file"
// error pointing at the path.
func bootstrapIndexIfMissing(ctx context.Context, sink snapshot.Sink, date, indexPath string) error {
	if _, err := os.Stat(indexPath); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat %s: %w", indexPath, err)
	}
	fetcher, ok := sink.(interface {
		FetchIndex(ctx context.Context, date string) ([]byte, error)
	})
	if !ok {
		return nil
	}
	body, err := fetcher.FetchIndex(ctx, date)
	if err != nil {
		return fmt.Errorf("bootstrap index from sink: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(indexPath), 0o755); err != nil {
		return fmt.Errorf("mkdir for %s: %w", indexPath, err)
	}
	// Atomic write (a snapshot.TempSuffix sibling + rename): a crash
	// mid-write must never leave a partial _index.json at the canonical
	// path.
	tmp, err := os.CreateTemp(filepath.Dir(indexPath), filepath.Base(indexPath)+snapshot.TempSuffix+"*")
	if err != nil {
		return fmt.Errorf("write %s: %w", indexPath, err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write %s: %w", indexPath, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write %s: %w", indexPath, err)
	}
	if err := os.Rename(tmpPath, indexPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write %s: %w", indexPath, err)
	}
	return nil
}

// loadIndex reads one snapshot's _index.json into a Progress document.
func loadIndex(path string) (*snapshot.Progress, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var p snapshot.Progress
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	return &p, nil
}

// findTarget returns the StationProgress matching the requested short-form
// external_id (or the literal catalog ID, since ShortID is idempotent on
// already-short IDs).
func findTarget(stations []snapshot.StationProgress, externalID string) (snapshot.StationProgress, bool) {
	want := conagua.ShortID(externalID)
	for _, sp := range stations {
		if conagua.ShortID(sp.ID) == want {
			return sp, true
		}
	}
	return snapshot.StationProgress{}, false
}

// resolveKinds expands the CLI --kind flag to the concrete Kinds to
// iterate. The normals set derives from conagua.NormalsKinds (the
// single owner of that vocabulary); fresh slices are returned so a
// caller can never mutate the shared backing array.
func resolveKinds(kind string) []conagua.Kind {
	switch kind {
	case "daily":
		return []conagua.Kind{conagua.KindDaily}
	case "normals":
		return append([]conagua.Kind(nil), conagua.NormalsKinds...)
	default:
		return append([]conagua.Kind{conagua.KindDaily}, conagua.NormalsKinds...)
	}
}

// includesNormals reports whether the configured --kind flag would
// touch any monthly_normals rows. Used to scope post-normals work
// (e.g. WMO completeness scoring) to runs that actually load normals.
func includesNormals(kind string) bool {
	return kind == "" || kind == "normals"
}

// isIngestableNormalsKind reports whether k is one of the four
// schema-accepted normals kinds (one per 30-year window). Derived from
// PeriodForKind: exactly the ingestable kinds map to a period string.
func isIngestableNormalsKind(k conagua.Kind) bool {
	_, ok := conagua.PeriodForKind(k)
	return ok
}

// pullErrorIssue formats a FileState into the 'issue' string we store
// in parsing_warnings for files the pull couldn't retrieve. Captures
// attempts + HTTP status (if any) + a truncated copy of last_error so
// an operator can tell at a glance whether the gap was a CONAGUA 500,
// a network timeout, or an R2-side failure — without having to cross-
// reference _index.json.
func pullErrorIssue(fs snapshot.FileState) string {
	last := fs.LastError
	// The cut is rune-based so a multi-byte character on the boundary
	// is never split into invalid UTF-8.
	const limit = 240
	if r := []rune(last); len(r) > limit {
		last = string(r[:limit]) + "…"
	}
	attempts := fs.Attempts
	if attempts == 0 {
		attempts = 1
	}
	if fs.HTTPCode != 0 {
		return fmt.Sprintf("pull errored after %d attempt(s), http=%d: %s",
			attempts, fs.HTTPCode, last)
	}
	return fmt.Sprintf("pull errored after %d attempt(s): %s", attempts, last)
}
