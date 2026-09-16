package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/envfile"
	"github.com/bioclimamx/conagua-etl/internal/fetcher"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

var (
	pullStatesFlag      []string
	pullKindsFlag       []string
	pullMaxStationsFlag int
	pullSnapshotDate    string
	pullCatalogDelay    time.Duration
	pullRetryFailed     bool
	pullFlushInterval   time.Duration
	pullRPSFlag         float64
	pullMaxRPSFlag      float64
	pullJitterFlag      float64
	pullSinkFlag        string
	pullBaseURLFlag     string
)

var pullCmd = &cobra.Command{
	Use:   "pull",
	Short: "Download CONAGUA station files into the configured sink",
	Long: "Walks the catalog for each selected state, then downloads each\n" +
		"(station, kind) file through a jittered rate limiter and writes it\n" +
		"to the sink at\n" +
		"  <root>/conagua-raw/<snapshot-date>/<kind>/<station_id>.txt\n\n" +
		"A live _progress.json is flushed periodically so interrupting the\n" +
		"run (Ctrl-C) doesn't lose state — re-running with the same\n" +
		"parameters picks up where it left off.",
	RunE: runPull,
}

func init() {
	f := pullCmd.Flags()
	f.StringSliceVar(&pullStatesFlag, "state", nil,
		"Only fetch the listed state codes (default: all 32). Repeatable or comma-separated.")
	f.StringSliceVar(&pullKindsFlag, "kind", nil,
		"Only fetch the listed kinds. Default: all 7.")
	f.IntVar(&pullMaxStationsFlag, "max-stations", 0,
		"After catalog discovery, cap the total stations pulled (0 = no cap).")
	f.StringVar(&pullSnapshotDate, "snapshot-date", "",
		"Snapshot date key (default: today in YYYY-MM-DD).")
	f.DurationVar(&pullCatalogDelay, "catalog-delay", 2*time.Second,
		"Fixed sleep between state catalog page requests.")
	f.BoolVar(&pullRetryFailed, "retry-failed", false,
		"Re-attempt files marked as error in a prior _progress.json.")
	f.DurationVar(&pullFlushInterval, "flush-every", 5*time.Second,
		"How often to flush _progress.json to disk during a run.")

	defCfg := fetcher.DefaultRateConfig()
	f.Float64Var(&pullRPSFlag, "rps", defCfg.TargetRPS,
		"Target request rate, in requests per second. Primary throttle knob.")
	f.Float64Var(&pullMaxRPSFlag, "max-rps", defCfg.MaxRPS,
		fmt.Sprintf("Effective-rate ceiling in req/s. Capped at %v by the hard limit.", fetcher.HardMaxRPS))
	f.Float64Var(&pullJitterFlag, "jitter", defCfg.JitterFraction,
		"Jitter stddev as a fraction of the mean interval (0 = no jitter).")
	f.StringVar(&pullSinkFlag, "sink", "local",
		"Where to write raw files: 'local' (under --root) or 'r2' (Cloudflare R2, needs R2_* env vars).")
	f.StringVar(&pullBaseURLFlag, "base-url", conagua.BaseURL,
		"Override the CONAGUA base URL (hermetic testing only; must end in '/').")
	if err := f.MarkHidden("base-url"); err != nil {
		panic(err) // the flag is registered just above; failure is a programming error
	}
}

func runPull(cmd *cobra.Command, _ []string) error {
	// Best-effort: pick up R2_* (and anything else) from ./.env. Real env
	// always wins — Load doesn't overwrite.
	if err := envfile.Load(".env"); err != nil {
		fprintf(cmd.ErrOrStderr(), "warning: .env load: %v\n", err)
	}

	states, err := selectStates(pullStatesFlag)
	if err != nil {
		return err
	}
	kinds, err := selectKinds(pullKindsFlag)
	if err != nil {
		return err
	}
	date := pullSnapshotDate
	if date == "" {
		date = time.Now().Format("2006-01-02")
	}
	// Relative hrefs resolve against the catalog page URL, so a base
	// without the trailing slash would silently shift the whole file tree.
	base := pullBaseURLFlag
	if !strings.HasSuffix(base, "/") {
		return fmt.Errorf("--base-url must end in \"/\" (got %q)", base)
	}
	// time.NewTicker panics on a non-positive interval, so the flusher
	// would crash mid-run — reject the flag before any work starts.
	if pullFlushInterval <= 0 {
		return errors.New("--flush-every must be positive")
	}

	rateCfg := fetcher.DefaultRateConfig()
	rateCfg.TargetRPS = pullRPSFlag
	rateCfg.MaxRPS = pullMaxRPSFlag
	rateCfg.JitterFraction = pullJitterFlag
	if err := rateCfg.Validate(); err != nil {
		return fmt.Errorf("rate config: %w", err)
	}

	// --- Sink -------------------------------------------------------------
	// Built before catalog discovery: fail on operator error before
	// spending polite catalog requests.
	// Raw files go to the user-selected sink; the live _progress.json ledger
	// always lives on local disk (atomic rename over R2 is painful and we
	// need frequent re-flushes).
	sink, sinkLabel, err := buildSink(pullSinkFlag, snapshotRootFlag)
	if err != nil {
		return err
	}

	// Graceful cancellation is the only runtime intervention: SIGINT or
	// SIGTERM cancels ctx, the fetcher winds down at the next wait/IO
	// point, and the shutdown path below flushes the ledger.
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stderr := cmd.ErrOrStderr()
	stdout := cmd.OutOrStdout()

	fprintf(stderr,
		"snapshot-date=%s  root=%s  retry-failed=%v\n"+
			"rate: target=%.2f/s  ceiling=%.2f/s  jitter=%.2f  mean-interval=%s\n"+
			"sink: %s\n",
		date, snapshotRootFlag, pullRetryFailed,
		rateCfg.TargetRPS, rateCfg.MaxRPS, rateCfg.JitterFraction,
		rateCfg.MeanInterval().Round(time.Millisecond),
		sinkLabel)

	client := conagua.NewClient()

	// --- Catalog discovery ------------------------------------------------
	// Fail-fast: a catalog page that won't fetch or parse aborts the run —
	// a partial national plan would silently under-pull entire states.
	var allStations []conagua.Station
	for i, st := range states {
		if i > 0 {
			if err := sleep(ctx, pullCatalogDelay); err != nil {
				return err
			}
		}
		fprintf(stderr, "[catalog %2d/%2d] %s (%s) … ",
			i+1, len(states), st, st.DisplayName())
		stations, err := client.FetchCatalog(ctx, st, base)
		if err != nil {
			fprintf(stderr, "FAILED (%v)\n", err)
			return err
		}
		fprintf(stderr, "%d stations\n", len(stations))
		allStations = append(allStations, stations...)
	}
	// StationsDiscovered records what the catalog walk found, before the
	// operator's --max-stations cap trims the pull.
	stationsDiscovered := len(allStations)
	if pullMaxStationsFlag > 0 && len(allStations) > pullMaxStationsFlag {
		allStations = allStations[:pullMaxStationsFlag]
	}

	progressFS := snapshot.NewLocalFS(snapshotRootFlag)
	progressPath := snapshot.ProgressPath(progressFS, date)

	retryPol := fetcher.DefaultRetryPolicy()
	// Static config structs; Marshal cannot fail on them.
	rateJSON, _ := json.Marshal(rateCfg)
	retryJSON, _ := json.Marshal(retryPol)

	// The seed is generated fresh per run and never printed; the ledger is
	// its audit trail — _progress.json / _index.json carry it so a run's
	// pacing stays reproducible.
	seed := fetcher.RandomSeed()

	ledger, err := snapshot.LoadOrInit(progressPath, snapshot.Progress{
		SnapshotDate: date,
		RNGSeed:      seed,
		RateConfig:   rateJSON,
		RetryPolicy:  retryJSON,
		ETLGitSHA:    buildGitSHA(),
	})
	if err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	ledger.SetCatalogSummary(snapshot.CatalogSummary{
		StatesDiscovered:   len(states),
		StationsDiscovered: stationsDiscovered,
	})
	for _, s := range allStations {
		ledger.UpsertStation(s)
	}

	// --- Task plan --------------------------------------------------------
	tasks := fetcher.BuildTasks(allStations, kinds)
	filtered := make([]fetcher.Task, 0, len(tasks))
	for _, t := range tasks {
		if ledger.ShouldSkip(t.Station.ID, t.Kind, pullRetryFailed) {
			continue
		}
		filtered = append(filtered, t)
	}
	skipped := len(tasks) - len(filtered)
	fprintln(stderr)

	if len(filtered) == 0 {
		finalizeSnapshot(ctx, stderr, ledger, progressFS, sink, date)
		fprintln(stderr, "nothing to fetch — snapshot already complete per ledger.")
		noteErroredFiles(stderr, snapshot.DeriveCounts(ledger.Snapshot()).FilesErrored)
		return nil
	}

	renderer := newPullLog(stdout, stderr)
	renderer.plan(len(filtered), len(allStations), skipped)

	limiter, err := fetcher.NewLimiter(rateCfg, seed)
	if err != nil {
		// rateCfg already validated, so this is unreachable in practice —
		// but the ledger exists now, and every exit path must flush it.
		if ferr := ledger.Flush(); ferr != nil {
			fprintf(stderr, "[final flush] %v\n", ferr)
		}
		return err
	}

	// --- Periodic flusher -------------------------------------------------
	// Runs on context.Background() so a Ctrl-C doesn't stop flushing while
	// the fetcher drains; stopped explicitly once Run returns.
	flushCtx, flushCancel := context.WithCancel(context.Background())
	var flushWG sync.WaitGroup
	flushWG.Go(func() {
		t := time.NewTicker(pullFlushInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if err := ledger.Flush(); err != nil {
					fprintf(stderr, "[flush] %v\n", err)
				}
			case <-flushCtx.Done():
				return
			}
		}
	})

	// --- Fetch ------------------------------------------------------------
	ft := &fetcher.Fetcher{
		Client:  client,
		Sink:    sink,
		Date:    date,
		Limiter: limiter,
		Retry:   retryPol,
		Hooks: fetcher.Hooks{
			// Synchronous by design: each result is in the ledger before
			// the next task starts, so an interrupt can lose at most the
			// not-yet-flushed tail — which the unconditional final Flush
			// below writes out.
			OnResult: func(res fetcher.Result) {
				renderer.result(res)
				ledger.RecordFile(resultToFileUpdate(res))
			},
			OnWait: renderer.wait,
		},
	}

	start := time.Now()
	runErr := ft.Run(ctx, filtered)

	flushCancel()
	flushWG.Wait()

	// Every exit path from here flushes, so the on-disk ledger always
	// reflects the last recorded result.
	completed := ctx.Err() == nil && runErr == nil
	if completed {
		finalizeSnapshot(ctx, stderr, ledger, progressFS, sink, date)
	} else if err := ledger.Flush(); err != nil {
		fprintf(stderr, "[final flush] %v\n", err)
	}

	fprintf(stderr,
		"\nDone in %v — fetched=%d skipped=%d not-found=%d errors=%d\n  ledger: %s\n",
		time.Since(start).Round(time.Millisecond),
		renderer.counts[fetcher.OutcomeFetched],
		renderer.counts[fetcher.OutcomeSkipped],
		renderer.counts[fetcher.OutcomeNotFound],
		renderer.counts[fetcher.OutcomeError],
		progressPath)
	if !completed {
		fprintln(stderr, "interrupted — re-run with the same parameters to resume")
	}
	noteErroredFiles(stderr, snapshot.DeriveCounts(ledger.Snapshot()).FilesErrored)
	return runErr
}

// finalizeSnapshot stamps the ledger complete, flushes it, and emits the
// terminal _index.json locally and to the sink. It is the single completion
// path — shared by the empty-plan and post-run exits so the two cannot
// drift. Emission failures are reported but not fatal: the flushed ledger
// still holds the full run record.
func finalizeSnapshot(
	ctx context.Context,
	stderr io.Writer,
	ledger *snapshot.Ledger,
	progressFS *snapshot.LocalFS,
	sink snapshot.Sink,
	date string,
) {
	ledger.MarkCompleted()
	if err := ledger.Flush(); err != nil {
		fprintf(stderr, "[final flush] %v\n", err)
	}
	indexPath := snapshot.IndexPath(progressFS, date)
	if err := snapshot.WriteIndex(indexPath, ledger.Snapshot()); err != nil {
		fprintf(stderr, "[write index] %v\n", err)
	} else {
		fprintf(stderr, "wrote %s\n", indexPath)
	}
	// Mirror _index.json to the Sink so an R2-only consumer has both
	// bytes AND manifest in one place. No-op for LocalFS.
	if err := snapshot.MirrorIndexToSink(ctx, sink, date, ledger.Snapshot()); err != nil {
		fprintf(stderr, "[mirror index] %v\n", err)
	}
}

// noteErroredFiles warns that the ledger holds files in outcome=error —
// terminal under plain resumption, so a bare re-run would strand them.
// Printed after every end-of-run summary (no-op when the count is zero) so
// the recipe the operator sees always covers the errored files.
func noteErroredFiles(stderr io.Writer, n int) {
	if n <= 0 {
		return
	}
	noun := "file"
	if n != 1 {
		noun = "files"
	}
	fprintf(stderr,
		"note: %d errored %s in the ledger — re-run with --retry-failed to re-attempt them\n",
		n, noun)
}

// resultToFileUpdate bridges fetcher.Result → snapshot.FileUpdate. The
// Outcome cast is safe: the two enums share string values by contract.
func resultToFileUpdate(r fetcher.Result) snapshot.FileUpdate {
	return snapshot.FileUpdate{
		StationID: r.Task.Station.ID,
		Kind:      r.Task.Kind,
		URL:       r.Task.URL,
		Outcome:   snapshot.FileOutcome(r.Outcome),
		HTTPCode:  r.Status,
		Bytes:     r.Bytes,
		SHA256:    r.SHA256,
		Attempts:  r.Attempts,
		Err:       r.Err,
		Elapsed:   r.Elapsed,
	}
}

// buildSink instantiates the requested sink and returns a human-readable
// description for the startup banner.
func buildSink(kind, localRoot string) (snapshot.Sink, string, error) {
	switch kind {
	case "local":
		return snapshot.NewLocalFS(localRoot),
			fmt.Sprintf("local fs  root=%s", localRoot),
			nil
	case "r2":
		cfg, err := snapshot.LoadR2Config()
		if err != nil {
			return nil, "", fmt.Errorf("--sink r2: %w (hint: set them in the environment or in ./.env)", err)
		}
		s, err := snapshot.NewR2Sink(cfg)
		if err != nil {
			return nil, "", err
		}
		return s,
			fmt.Sprintf("cloudflare r2  bucket=%s  endpoint=%s", cfg.Bucket, cfg.Endpoint()),
			nil
	default:
		return nil, "", fmt.Errorf("unknown --sink %q (expected 'local' or 'r2')", kind)
	}
}

// selectStates parses the --state flag. Empty → all 32.
func selectStates(flag []string) ([]conagua.StateCode, error) {
	if len(flag) == 0 {
		return conagua.AllStates, nil
	}
	out := make([]conagua.StateCode, 0, len(flag))
	for _, raw := range flag {
		c, err := conagua.ParseStateCode(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

// selectKinds parses the --kind flag. Empty → nil, which BuildTasks treats
// as "all kinds".
func selectKinds(flag []string) ([]conagua.Kind, error) {
	if len(flag) == 0 {
		return nil, nil
	}
	valid := make(map[conagua.Kind]bool, len(conagua.AllKinds))
	for _, k := range conagua.AllKinds {
		valid[k] = true
	}
	out := make([]conagua.Kind, 0, len(flag))
	for _, raw := range flag {
		k := conagua.Kind(raw)
		if !valid[k] {
			return nil, fmt.Errorf("unknown kind %q (expected one of %v)", raw, conagua.AllKinds)
		}
		out = append(out, k)
	}
	return out, nil
}

// sleep blocks for d, honouring ctx cancellation.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
