package cmd

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/signal"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

var (
	validateDBFlag     string
	validatePeriodFlag string
	validateSkipFlag   []string
	validateReportFlag string
)

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Run the QC rules over the database; record findings and write the HTML report",
	Long: "Runs the QC rules over the ingested database plus its POWER supplement\n" +
		"and records every finding in parsing_warnings under\n" +
		"source_file = 'validate:<rule>'.\n\n" +
		"This verb MUTATES the database: it deletes its own prior findings (every\n" +
		"parsing_warnings row whose source_file starts with 'validate:'), runs\n" +
		"the rules, writes the new findings back, and reconciles stranded runs —\n" +
		"an ingest_runs or power_runs row still 'running' after 24 hours is\n" +
		"flipped to 'aborted' by the orphan-runs rule. Ingest's own warnings are\n" +
		"never touched. Re-running converges: the same database yields the same\n" +
		"findings, orphan-runs excepted — it reports a stranded run on the pass\n" +
		"that reconciles it.\n\n" +
		"--db is required and has no default, so a bare invocation never rewrites\n" +
		"a production database sitting in the working directory. To audit a\n" +
		"production database without touching it, validate a copy:\n\n" +
		"  sqlite3 \"file:bioclima.db?mode=ro\" \"VACUUM INTO 'copy.db'\"\n" +
		"  conagua-etl validate --db copy.db\n\n" +
		"The rule set, in execution order — the five QC rules, then the\n" +
		"integrity anchors publish's gate evaluates:\n\n" +
		"  orphan-runs             : reconcile stranded *_runs rows (warn per row)\n" +
		"  bbox                    : impossible (error) or out-of-MX (warn) coordinates\n" +
		"  wmo-month-completeness  : WMO §4.4.1 within-month completeness (warn)\n" +
		"  daily-sanity            : diurnal range and σ-z-score outliers (warn)\n" +
		"  cross-period            : same-month deltas across normals periods (warn)\n" +
		"  ingest-complete         : at least one complete ingest run (error)\n" +
		"  station-refs            : every station_id names a stations row (error)\n" +
		"  cell-refs               : every cell_id names a POWER grid cell (error)\n" +
		"  run-refs                : every supplement row names a power run (error)\n" +
		"  run-label-unique        : one power run per run label (error)\n" +
		"  fill-leak               : no POWER fill value stored (error)\n" +
		"  wind-range              : wind directions within [0, 360] (error)\n\n" +
		"Severity: 'error' is reserved for what makes the data unfit to publish —\n" +
		"impossible coordinates and a failed integrity anchor — and is what\n" +
		"publish's gate refuses on; everything else is 'warn', surfaced and never\n" +
		"blocking (CONAGUA's quirks are mirrored, not corrected). --period scopes\n" +
		"wmo-month-completeness and daily-sanity to one YYYY-YYYY window — a\n" +
		"CONAGUA normals period or any other, e.g. 2001-2020.\n\n" +
		"Progress on stderr: one line per rule as it starts and as it finishes\n" +
		"(scanned, warn, error, elapsed), with a heartbeat every 30 s while a\n" +
		"long rule runs. The summary table prints on stdout, and a self-contained\n" +
		"HTML report (no external assets) is written atomically to --report.\n\n" +
		"Exit codes: 0 when no error-severity finding exists (warnings never fail\n" +
		"the run); 1 when any error-severity finding exists — publish refuses\n" +
		"this DB — or on a run-level failure (an unknown --skip, a malformed\n" +
		"--period, a --db that is missing, does not exist or refuses to open, a\n" +
		"rule's own error, cancellation). The verb never creates a database.",
	RunE: runValidate,
}

func init() {
	f := validateCmd.Flags()
	// --db has no default: the verb writes the database it names, and a
	// default of ./bioclima.db would let a bare invocation from the repo
	// root rewrite the production dataset. The copy-first workflow is in
	// the Long help.
	f.StringVar(&validateDBFlag, "db", "",
		"SQLite file to validate; required, must exist. Opened for writing: this verb rewrites its own "+
			"'validate:' warnings and reconciles stranded runs. A WAL-mode database keeps its -shm/-wal "+
			"sidecars beside it, so the directory must be writable.")
	if err := validateCmd.MarkFlagRequired("db"); err != nil {
		panic(err) // the flag is registered just above; failure is a programming error
	}
	f.StringVar(&validatePeriodFlag, "period", validate.DefaultPeriod,
		"YYYY-YYYY window wmo-month-completeness and daily-sanity are scoped to.")
	f.StringSliceVar(&validateSkipFlag, "skip", nil,
		"Rule id to skip ("+strings.Join(validateRuleIDs(), ", ")+"), repeatable or comma-separated.")
	f.StringVar(&validateReportFlag, "report", "./validate-report.html",
		"Self-contained HTML report path (written atomically).")
}

// validateRuleIDs lists the verb's rule ids in execution order — the
// rule registry is the single owner of the set, never a hard-coded list.
// The period does not change the set.
func validateRuleIDs() []string {
	rules := validate.AllRules(validate.DefaultPeriod)
	ids := make([]string, 0, len(rules))
	for _, r := range rules {
		ids = append(ids, r.ID)
	}
	return ids
}

// resolveSkip validates the --skip ids against the rule registry and
// returns them as the set validate.Options carries. Whitespace and
// empty entries are tolerated; an unknown id is refused before the DB
// is opened, since silently skipping nothing would let an operator
// believe a rule was omitted.
func resolveSkip(ids []string) (map[string]bool, error) {
	valid := validateRuleIDs()
	skip := map[string]bool{}
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if !slices.Contains(valid, id) {
			return nil, fmt.Errorf("--skip: unknown rule %q (valid: %s)", id, strings.Join(valid, ", "))
		}
		skip[id] = true
	}
	return skip, nil
}

// periodShape is the YYYY-YYYY window --period takes: any window,
// since the two period-scoped rules read
// daily_observations by date range and any window holds data.
var periodShape = regexp.MustCompile(`^(\d{4})-(\d{4})$`)

// resolvePeriod checks --period's shape before the database is opened:
// four-digit years, and a window that does not end before it starts —
// an inverted window would scan nothing and report a clean DB.
// validate's own period parsing remains the arbiter within the rules.
func resolvePeriod(period string) error {
	m := periodShape.FindStringSubmatch(period)
	if m == nil {
		return fmt.Errorf("--period: %q is not a YYYY-YYYY window", period)
	}
	if m[1] > m[2] {
		return fmt.Errorf("--period: %s ends before it starts", period)
	}
	return nil
}

// checkDBExists refuses a --db that names no file. The writer opener
// would create one — an empty database validates as nothing to publish,
// and a typo'd path would manufacture a stray file beside the real one.
func checkDBExists(path string) error {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("--db: %s does not exist (validate never creates a database)", path)
		}
		return fmt.Errorf("--db: %w", err)
	}
	return nil
}

func runValidate(cmd *cobra.Command, _ []string) error {
	// Flag validation precedes the open: a refused flag must leave no
	// database behind (schema.Open creates a missing file).
	if err := resolvePeriod(validatePeriodFlag); err != nil {
		return err
	}
	skip, err := resolveSkip(validateSkipFlag)
	if err != nil {
		return err
	}
	if err := checkDBExists(validateDBFlag); err != nil {
		return err
	}

	// Graceful cancellation is the only runtime intervention: SIGINT or
	// SIGTERM cancels ctx, the rule in flight stops at its next IO point
	// and no further rule starts, nothing is written back (findings land
	// in one batch at the end), and the report of the rules that
	// completed still prints.
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stderr := cmd.ErrOrStderr()
	stdout := cmd.OutOrStdout()

	db, err := schema.Open(validateDBFlag)
	if err != nil {
		return fmt.Errorf("open %s: %w", validateDBFlag, err)
	}
	defer db.Close() //nolint:errcheck // teardown close; the run's outcome is already determined

	skipped := "none"
	if len(skip) > 0 {
		skipped = strings.Join(slices.Sorted(maps.Keys(skip)), ",")
	}
	fprintf(stderr, "db=%s (writer)  period=%s  skip=%s  report=%s\n",
		validateDBFlag, validatePeriodFlag, skipped, validateReportFlag)

	renderer := newValidateLog(stderr)
	report, runErr := validate.Run(ctx, db, validate.Options{
		Period:      validatePeriodFlag,
		Skip:        skip,
		OnRuleStart: renderer.ruleStart,
		OnRuleDone:  renderer.ruleDone,
	})
	renderer.finish()

	// The report prints on every path that produced one — a run-level
	// failure still deserves its partial summary.
	if report != nil {
		printValidateReport(stdout, report, runErr == nil)
	}
	// runErr is a run-level failure (a rule's own error, the write
	// failing, cancellation) and maps to exit 1 with no HTML report.
	if runErr != nil {
		return runErr
	}

	// The report is rendered in full and written atomically by
	// WriteHTMLReport, so a failure here leaves no partial file.
	if err := validate.WriteHTMLReport(validateReportFlag, report); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	fprintf(stdout, "\nHTML report: %s\n", validateReportFlag)

	// An error-severity finding is the signal publish refuses on; the
	// summary is already printed, so the error line carries the count.
	if report.ErrorsTotal > 0 {
		return fmt.Errorf("%d validation error(s) — publish refuses this DB", report.ErrorsTotal)
	}
	return nil
}

// printValidateReport produces the compact end-of-run summary on
// stdout: the totals, then one row per rule that ran (a skipped rule
// has none). A run a rule's error aborted prints the rules that
// completed under an "aborted" heading.
func printValidateReport(w io.Writer, r *validate.Report, complete bool) {
	fprintln(w)
	if complete {
		fprintln(w, "validate complete")
	} else {
		fprintln(w, "validate aborted")
	}
	fprintf(w, "  period            : %s\n", r.Period)
	fprintf(w, "  warnings          : %d\n", r.WarningsTotal)
	fprintf(w, "  errors            : %d\n", r.ErrorsTotal)
	if !r.StartedAt.IsZero() && !r.FinishedAt.IsZero() {
		fprintf(w, "  elapsed           : %s\n", r.FinishedAt.Sub(r.StartedAt).Round(time.Millisecond))
	}
	fprintln(w)
	fprintln(w, "  rule                     scanned   warn   error  elapsed")
	for _, rr := range r.PerRule {
		fprintf(w, "  %-24s %7d %6d %7d  %s\n",
			rr.ID, rr.Scanned, rr.Warnings, rr.Errors, rr.Elapsed.Round(time.Millisecond))
	}
}
