package cmd

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/bioclimamx/conagua-etl/internal/envfile"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

var (
	ingestSinkFlag     string
	ingestSnapshotFlag string
	ingestDBFlag       string
	ingestExternalID   string
	ingestStationsOnly bool
	ingestKindFlag     string
)

var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "Parse a snapshot into SQLite",
	Long: "Ingests a CONAGUA snapshot into the SQLite database.\n\n" +
		"Reads _index.json from the local metadata root under --root (always\n" +
		"local, even with --sink r2), seeds the stations table, then walks\n" +
		"every station in the manifest — fetching its daily + normals files\n" +
		"via the sink, parsing, and inserting rows. Pass --external-id to\n" +
		"narrow to one station.\n\n" +
		"Each station is ingested in its own transaction, so a failure at one\n" +
		"station logs to stderr and the loop continues to the next. With\n" +
		"--sink r2 the fetched bytes are mirrored to --root as a read-through\n" +
		"cache; a subsequent run with --sink local against the same --root\n" +
		"works without hitting R2 again.\n\n" +
		"Exit codes: 0 clean completion; 2 when the run completed but one or\n" +
		"more stations failed (degraded — see the report); 1 on a run-level\n" +
		"failure.",
	RunE: runIngest,
}

func init() {
	f := ingestCmd.Flags()
	f.StringVar(&ingestSinkFlag, "sink", "local",
		"Where to read raw files from: 'local' (under --root) or 'r2' (Cloudflare R2, needs R2_* env vars).")
	f.StringVar(&ingestSnapshotFlag, "snapshot", "",
		"Snapshot date key YYYY-MM-DD (default: newest under <root>/conagua-raw).")
	f.StringVar(&ingestDBFlag, "db", "./bioclima.db",
		"SQLite file to write. Created (with schema applied) if absent.")
	f.StringVar(&ingestExternalID, "external-id", "",
		"Restrict to one station by external_id (short or catalog form). Empty = every station in the snapshot.")
	f.BoolVar(&ingestStationsOnly, "stations-only", false,
		"Seed stations from _index.json and exit (no daily/normals, no ingest_runs row).")
	f.StringVar(&ingestKindFlag, "kind", "",
		"Restrict to one kind: 'daily' or 'normals'. Default: both.")
}

func runIngest(cmd *cobra.Command, _ []string) error {
	// Best-effort: pick up R2_* (and anything else) from ./.env. Real env
	// always wins — Load doesn't overwrite.
	if err := envfile.Load(".env"); err != nil {
		fprintf(cmd.ErrOrStderr(), "warning: .env load: %v\n", err)
	}

	sink, sinkLabel, err := buildSink(ingestSinkFlag, snapshotRootFlag)
	if err != nil {
		return err
	}
	// Under r2, wrap with a LocalFS read-through cache rooted at --root:
	// repeat runs (and a later --sink local run against the same --root)
	// avoid re-fetching from R2. Cache-write failures are non-fatal.
	if ingestSinkFlag == "r2" {
		sink = snapshot.NewCachingSink(sink, snapshot.NewLocalFS(snapshotRootFlag), nil)
		sinkLabel += "  (cached under " + snapshotRootFlag + ")"
	}

	// Graceful cancellation is the only runtime intervention: SIGINT or
	// SIGTERM cancels ctx, the loop stops at the next between-stations
	// check (the in-flight station's tx commits or rolls back whole), and
	// ingest.Run still closes the ingest_runs row as 'aborted'.
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stderr := cmd.ErrOrStderr()
	stdout := cmd.OutOrStdout()

	fprintf(stderr, "sink: %s\n", sinkLabel)

	date := ingestSnapshotFlag
	if date == "" {
		date, err = newestSnapshotDate(snapshotRootFlag)
		if err != nil {
			return err
		}
		fprintf(stderr, "snapshot: %s (newest under %s)\n", date, snapshotRootFlag)
	} else {
		fprintf(stderr, "snapshot: %s\n", date)
	}

	renderer := newIngestLog(stderr)

	report, runErr := ingest.Run(ctx, ingest.Options{
		Sink:          sink,
		SinkKind:      ingestSinkFlag,
		MetadataRoot:  snapshotRootFlag,
		SnapshotDate:  date,
		DBPath:        ingestDBFlag,
		ETLGitSHA:     buildGitSHA(),
		ExternalID:    ingestExternalID,
		StationsOnly:  ingestStationsOnly,
		Kind:          ingestKindFlag,
		OnStationDone: renderer.stationDone,
	})
	renderer.finish()

	// The report prints on every path that produced one — a run-level
	// failure still deserves its partial summary.
	if report != nil {
		printIngestReport(stdout, report)
	}
	// runErr is a run-level failure (ctx cancellation, driver loss, bad
	// options) and maps to exit 1; per-station fail-soft casualties live
	// in the report and map to the degraded exit 2 instead.
	if runErr != nil {
		return runErr
	}
	if report.StationsFailed > 0 {
		return DegradedError{Summary: fmt.Sprintf(
			"ingest degraded: %d of %d stations failed (see report)",
			report.StationsFailed, report.StationsAttempted)}
	}
	return nil
}

// newestSnapshotDate picks the lexicographically-largest YYYY-MM-DD
// directory under <root>/conagua-raw/ so the operator doesn't have to
// spell out --snapshot for the common case. A missing conagua-raw dir
// reports as "no snapshots" per the ListSnapshotDates contract.
func newestSnapshotDate(root string) (string, error) {
	dir := filepath.Join(root, "conagua-raw")
	dates, err := snapshot.ListSnapshotDates(snapshot.NewLocalFS(root))
	if err != nil {
		return "", fmt.Errorf("read %s: %w", dir, err)
	}
	if len(dates) == 0 {
		return "", fmt.Errorf("no snapshots under %s", dir)
	}
	return dates[0], nil
}

// printIngestReport produces the compact end-of-run summary on stdout.
// Warnings are truncated so the common case doesn't flood the terminal;
// per-station failures always print in full — a 5,400-station run with
// 3 failures wants to surface those 3 names.
func printIngestReport(w io.Writer, r *ingest.Report) {
	fprintln(w)
	fprintln(w, "ingest complete")
	if r.IngestRunID != 0 {
		fprintf(w, "  ingest_runs.id        : %d\n", r.IngestRunID)
	}
	fprintf(w, "  stations seeded       : %d\n", r.StationsSeeded)
	if r.StationsAttempted > 0 {
		fprintf(w, "  stations ingested     : %d ok / %d failed / %d attempted\n",
			r.StationsSucceeded, r.StationsFailed, r.StationsAttempted)
		fprintf(w, "  stations w/ date range: %d\n", r.StationsWithFirstLast)
		fprintf(w, "  stations w/ wmo score : %d\n", r.StationsWithWMO)
	}
	fprintf(w, "  files opened          : %d\n", r.FilesOpened)
	fprintf(w, "  pull-errored          : %d  (data gaps we introduced; see parsing_warnings)\n", r.FilesPullErrored)
	fprintf(w, "  not in catalog        : %d  (CONAGUA didn't publish; not a gap)\n", r.FilesNotInCatalog)
	fprintf(w, "  daily rows inserted   : %d\n", r.DailyRowsInserted)
	fprintf(w, "  normals rows inserted : %d\n", r.NormalsRowsInserted)
	fprintf(w, "  extras rows inserted  : %d\n", r.ExtrasRowsInserted)
	if r.CacheHits > 0 || r.CacheMisses > 0 {
		fprintf(w, "  cache hits/misses     : %d / %d\n", r.CacheHits, r.CacheMisses)
	}
	fprintf(w, "  warnings              : %d\n", len(r.Warnings))

	if len(r.PerStationFailures) > 0 {
		fprintln(w, "  failed stations:")
		for _, f := range r.PerStationFailures {
			fprintf(w, "    %s (%s) — %v\n", f.ExternalID, f.Name, f.Err)
		}
	}

	const maxShown = 10
	shown := r.Warnings
	if len(shown) > maxShown {
		shown = shown[:maxShown]
	}
	for _, warn := range shown {
		prefix := fmt.Sprintf("[%s]", warn.Severity)
		if warn.Line > 0 {
			fprintf(w, "    %s %s:%d  %s\n", prefix, warn.SourceFile, warn.Line, warn.Issue)
		} else {
			fprintf(w, "    %s %s  %s\n", prefix, warn.SourceFile, warn.Issue)
		}
	}
	if len(r.Warnings) > maxShown {
		fprintf(w, "    … and %d more\n", len(r.Warnings)-maxShown)
	}
}
