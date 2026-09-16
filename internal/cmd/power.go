package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/envfile"
	"github.com/bioclimamx/conagua-etl/internal/fetcher"
	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

var (
	powerDBFlag        string
	powerTemporalFlag  string
	powerStartYearFlag int
	powerEndYearFlag   int
	powerStartDateFlag string
	powerEndDateFlag   string
	powerMaxCellsFlag  int
	powerRPSFlag       float64
	powerMaxRPSFlag    float64
	powerJitterFlag    float64
	powerEndpointFlag  string
)

var powerCmd = &cobra.Command{
	Use:   "power",
	Short: "Augment the database with NASA POWER reanalysis",
	Long: "Augments the database with NASA POWER reanalysis (31 bioclimatic\n" +
		"variables CONAGUA does not observe).\n\n" +
		"Walks the stations table, snaps each station to its enclosing POWER\n" +
		"grid cell (0.5° lat × 0.625° lon, MERRA-2-anchored), fetches POWER\n" +
		"time series for each unique cell, and writes one of two supplement\n" +
		"tables depending on --temporal:\n\n" +
		"  --temporal monthly (default)\n" +
		"    Fetches the monthly endpoint, rolls each cell up to climatological\n" +
		"    monthly means over [--start-year, --end-year], and writes\n" +
		"    monthly_supplement keyed by (cell, period, month).\n\n" +
		"  --temporal daily\n" +
		"    Fetches the daily endpoint over [--start-date, --end-date], applies\n" +
		"    per-variable unit conversions, and writes daily_supplement keyed by\n" +
		"    (cell, date). POWER daily starts 1981-01-01. --end-date defaults to\n" +
		"    the most recent ingested snapshot date, falling back to today (UTC).\n\n" +
		"Provenance is encoded by table: monthly_supplement and daily_supplement\n" +
		"both hold POWER reanalysis values keyed by cell. Per-station consumers\n" +
		"resolve through station_power_cell at read time; monthly_normals and\n" +
		"daily_observations remain CONAGUA-observed only.\n\n" +
		"Exit codes: 0 clean completion; 2 when the run completed but one or\n" +
		"more cells failed (degraded — see the report); 1 on a run-level\n" +
		"failure.",
	RunE: runPower,
}

func init() {
	f := powerCmd.Flags()
	f.StringVar(&powerDBFlag, "db", "./bioclima.db",
		"SQLite file to write. Created (with schema applied) if absent.")
	f.StringVar(&powerTemporalFlag, "temporal", "monthly",
		"POWER endpoint mode: 'monthly' or 'daily'.")
	f.IntVar(&powerStartYearFlag, "start-year", 1991,
		"Monthly mode: first year of the climatology window.")
	f.IntVar(&powerEndYearFlag, "end-year", 2020,
		"Monthly mode: last year of the climatology window.")
	f.StringVar(&powerStartDateFlag, "start-date", "1981-01-01",
		"Daily mode: first date (YYYY-MM-DD). POWER daily starts 1981-01-01.")
	f.StringVar(&powerEndDateFlag, "end-date", "",
		"Daily mode: last date (YYYY-MM-DD). Default: newest ingested snapshot date, else today UTC.")
	f.IntVar(&powerMaxCellsFlag, "max-cells", 0,
		"Process at most N cells (0 = no cap).")
	f.Float64Var(&powerRPSFlag, "rps", 2.0,
		"Target request rate, in requests per second. Primary throttle knob.")
	f.Float64Var(&powerMaxRPSFlag, "max-rps", 4.0,
		fmt.Sprintf("Effective-rate ceiling in req/s. Capped at %v by the hard limit.", fetcher.HardMaxRPS))
	f.Float64Var(&powerJitterFlag, "jitter", 0.3,
		"Jitter stddev as a fraction of the mean interval (0 = no jitter).")
	f.StringVar(&powerEndpointFlag, "endpoint", "",
		"Override the POWER endpoint URL (hermetic testing only; default depends on --temporal).")
	if err := f.MarkHidden("endpoint"); err != nil {
		panic(err) // the flag is registered just above; failure is a programming error
	}
}

// powerDateLayout is the YYYY-MM-DD shape POWER's daily span and the
// ingest_runs.snapshot_date column share.
const powerDateLayout = "2006-01-02"

func runPower(cmd *cobra.Command, _ []string) error {
	// Best-effort: pick up anything set in ./.env. Real env always wins —
	// Load doesn't overwrite.
	if err := envfile.Load(".env"); err != nil {
		fprintf(cmd.ErrOrStderr(), "warning: .env load: %v\n", err)
	}

	temporal, err := parseTemporalMode(powerTemporalFlag)
	if err != nil {
		return err
	}
	if err := crossModeFlagError(cmd.Flags().Changed, temporal); err != nil {
		return err
	}

	if temporal == power.TemporalDaily {
		if _, err := time.Parse(powerDateLayout, powerStartDateFlag); err != nil {
			return fmt.Errorf("--start-date: %q is not a valid YYYY-MM-DD date", powerStartDateFlag)
		}
		if powerEndDateFlag != "" {
			if _, err := time.Parse(powerDateLayout, powerEndDateFlag); err != nil {
				return fmt.Errorf("--end-date: %q is not a valid YYYY-MM-DD date", powerEndDateFlag)
			}
		}
	} else {
		// Fail fast on a window the schema's period CHECK would reject
		// mid-run with an opaque driver error.
		label := power.Period{StartYear: powerStartYearFlag, EndYear: powerEndYearFlag}.Label()
		valid := validMonthlyPeriods()
		if !slices.Contains(valid, label) {
			return fmt.Errorf("--start-year/--end-year: %s is not a CONAGUA normals period (valid: %s)",
				label, strings.Join(valid, ", "))
		}
	}

	rateCfg := fetcher.RateConfig{
		TargetRPS:      powerRPSFlag,
		MaxRPS:         powerMaxRPSFlag,
		JitterFraction: powerJitterFlag,
		MaxInterval:    3 * time.Second,
		// Rests are CONAGUA mimicry (a human-shaped browsing cadence); to
		// POWER we present as an honest API consumer, so rest durations are
		// zero — jittered pacing plus backoff is the politeness there.
		RestEveryMin:    50,
		RestEveryMax:    200,
		RestDurationMin: 0,
		RestDurationMax: 0,
	}
	if err := rateCfg.Validate(); err != nil {
		return fmt.Errorf("rate config: %w", err)
	}

	// Graceful cancellation is the only runtime intervention: SIGINT or
	// SIGTERM cancels ctx, the cell loop stops at the next between-cells
	// check, and power.Run still closes the power_runs row as 'aborted'.
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stderr := cmd.ErrOrStderr()
	stdout := cmd.OutOrStdout()

	db, err := schema.Open(powerDBFlag)
	if err != nil {
		return fmt.Errorf("open %s: %w", powerDBFlag, err)
	}
	defer db.Close() //nolint:errcheck // teardown close; the run's outcome is already determined

	endpoint := powerEndpointFlag
	if endpoint == "" {
		if temporal == power.TemporalDaily {
			endpoint = power.DefaultEndpointDaily
		} else {
			endpoint = power.DefaultEndpointMonthly
		}
	}

	// Only the active mode's span fields are populated, so the manifest is
	// honest about which of period vs date-range is authoritative.
	opts := power.Options{
		Endpoint:  endpoint,
		Temporal:  temporal,
		MaxCells:  powerMaxCellsFlag,
		ETLGitSHA: buildGitSHA(),
	}
	var span string
	if temporal == power.TemporalDaily {
		endDate := powerEndDateFlag
		if endDate == "" {
			if endDate, err = resolveDailyEndDate(ctx, db); err != nil {
				return err
			}
		}
		opts.StartDate = powerStartDateFlag
		opts.EndDate = endDate
		span = fmt.Sprintf("dates=%s→%s", opts.StartDate, opts.EndDate)
	} else {
		opts.Period = power.Period{StartYear: powerStartYearFlag, EndYear: powerEndYearFlag}
		span = "period=" + opts.Period.Label()
	}

	limiter, err := fetcher.NewLimiter(rateCfg, fetcher.RandomSeed())
	if err != nil {
		return err
	}
	opts.Client = power.NewClient(endpoint, limiter)

	fprintf(stderr,
		"temporal=%s  %s  db=%s\n"+
			"endpoint: %s\n"+
			"rate: target=%.2f/s  ceiling=%.2f/s  jitter=%.2f  mean-interval=%s\n",
		temporal, span, powerDBFlag,
		endpoint,
		rateCfg.TargetRPS, rateCfg.MaxRPS, rateCfg.JitterFraction,
		rateCfg.MeanInterval().Round(time.Millisecond))

	renderer := newPowerLog(stderr)
	report, runErr := power.Run(ctx, db, opts, renderer.cellDone)

	// The report prints on every path that produced one — a run-level
	// failure still deserves its partial summary.
	if report != nil {
		printPowerReport(stdout, report)
	}
	// runErr is a run-level failure (ctx cancellation, driver loss, bad
	// options) and maps to exit 1; per-cell fail-soft casualties live in
	// the report and map to the degraded exit 2 instead.
	if runErr != nil {
		return runErr
	}
	if report.CellsFailed > 0 {
		return DegradedError{Summary: fmt.Sprintf(
			"power run completed with %d/%d cells failed",
			report.CellsFailed, report.CellsAttempted)}
	}
	return nil
}

// parseTemporalMode maps the --temporal flag onto the power.TemporalMode
// enum, rejecting anything but the two known modes.
func parseTemporalMode(s string) (power.TemporalMode, error) {
	switch s {
	case "", "monthly":
		return power.TemporalMonthly, nil
	case "daily":
		return power.TemporalDaily, nil
	default:
		return "", fmt.Errorf("--temporal: unknown value %q (want 'monthly' or 'daily')", s)
	}
}

// crossModeFlagError rejects a flag of the other temporal mode that was
// explicitly set — silently ignoring it would let an operator believe e.g.
// --start-date narrowed a monthly run. changed is
// cmd.Flags().Changed, injected so the check is testable without a cobra
// command.
func crossModeFlagError(changed func(string) bool, temporal power.TemporalMode) error {
	foreign, owner := []string{"start-date", "end-date"}, power.TemporalDaily
	if temporal == power.TemporalDaily {
		foreign, owner = []string{"start-year", "end-year"}, power.TemporalMonthly
	}
	var set []string
	for _, name := range foreign {
		if changed(name) {
			set = append(set, "--"+name)
		}
	}
	if len(set) > 0 {
		return fmt.Errorf("%s: only valid with --temporal %s", strings.Join(set, ", "), owner)
	}
	return nil
}

// validMonthlyPeriods derives the allowed monthly climatology windows from
// conagua's normals vocabulary — the single owner of the period set —
// never a hard-coded list.
func validMonthlyPeriods() []string {
	out := make([]string, 0, len(conagua.NormalsKinds))
	for _, k := range conagua.NormalsKinds {
		if p, ok := conagua.PeriodForKind(k); ok {
			out = append(out, p)
		}
	}
	return out
}

// resolveDailyEndDate picks the default for --end-date in daily mode: the
// most recent CONAGUA snapshot date the DB has ingested, falling back to
// today UTC only when no complete ingest_runs row exists yet. The snapshot
// date is the honest overlay boundary — POWER daily spans exactly the
// window the DB holds CONAGUA observations for — so any other query
// failure propagates rather than silently widening the span to today.
func resolveDailyEndDate(ctx context.Context, db *sql.DB) (string, error) {
	var snap string
	err := db.QueryRowContext(ctx,
		`SELECT snapshot_date FROM ingest_runs WHERE status='complete'
		  ORDER BY started_at DESC LIMIT 1`).Scan(&snap)
	switch {
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return "", fmt.Errorf("resolve --end-date default: %w", err)
	case err == nil && snap != "":
		return snap, nil
	}
	return time.Now().UTC().Format(powerDateLayout), nil
}

// printPowerReport produces the compact end-of-run summary on stdout. The
// per-cell failure detail already streamed to stderr as FAIL lines; the
// durable record lives in power_runs.
func printPowerReport(w io.Writer, r *power.Report) {
	fprintln(w)
	fprintln(w, "power run complete")
	fprintf(w, "  power_runs.id        : %d\n", r.RunID)
	fprintf(w, "  status               : %s\n", r.Status)
	temporal := string(r.Manifest.Temporal)
	if temporal == "" {
		temporal = "monthly"
	}
	fprintf(w, "  temporal mode        : %s\n", temporal)
	if r.Manifest.Temporal == power.TemporalDaily {
		fprintf(w, "  date range           : %s → %s\n", r.Manifest.StartDate, r.Manifest.EndDate)
	} else {
		fprintf(w, "  period               : %s\n", r.Manifest.Period.Label())
	}
	fprintf(w, "  stations covered     : %d\n", r.StationsCovered)
	fprintf(w, "  unique cells         : %d\n", r.UniqueCells)
	fprintf(w, "  cells attempted      : %d\n", r.CellsAttempted)
	fprintf(w, "  cells succeeded      : %d\n", r.CellsSucceeded)
	fprintf(w, "  cells failed         : %d\n", r.CellsFailed)
	fprintf(w, "  supplement rows      : %d\n", r.SupplementRows)
	if !r.StartedAt.IsZero() && !r.FinishedAt.IsZero() {
		fprintf(w, "  elapsed              : %s\n", r.FinishedAt.Sub(r.StartedAt).Round(time.Second))
	}
}
