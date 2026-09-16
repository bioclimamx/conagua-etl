package cmd

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

var (
	publishDBFlag    string
	publishOutFlag   string
	publishStateFlag []string
	publishOnlyFlag  []string
	publishDOIFlag   string
)

var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "Build the Zenodo release artifacts from the database",
	Long: "Builds the citable Bioclima dataset — the Zenodo v0.1 deposit — from\n" +
		"the SQLite database into a deposit-ready directory.\n\n" +
		"Resolves the snapshot from the latest complete ingest run, then runs\n" +
		"the gate: the read-only integrity checks the deposit is conditioned on\n" +
		"— implausible station coordinates (bbox), any ingest or power run\n" +
		"still marked running, and the integrity anchors (every supplement row\n" +
		"tied to a power run, every cell and station reference resolvable, no\n" +
		"POWER fill value stored, wind directions within [0, 360], a complete\n" +
		"ingest run, one run per label) — one line per rule on stderr. An\n" +
		"error-severity finding refuses the run before anything is written,\n" +
		"every finding listed; a warn-severity finding never blocks and is\n" +
		"reported in QA-REPORT.md. The gate reads the database and writes\n" +
		"nothing to it. Then writes, per CONAGUA state (lowercase state code),\n" +
		"the archives of the two per-state artifact groups:\n\n" +
		"  tabular  {state}-tabular.zip — four curated CSV folders: conagua/\n" +
		"           (stations, monthly_normals, monthly_normals_extras, one\n" +
		"           daily_observations file per station), nasa_power/ (the\n" +
		"           cells the state's stations reference, the station to cell\n" +
		"           map, monthly, one daily file per cell), combined/ (the\n" +
		"           CONAGUA spine LEFT-joined to its cell's POWER row:\n" +
		"           combined_monthly and one combined_daily file per station),\n" +
		"           and provenance/ (ingest_runs and power_runs by natural\n" +
		"           label, identical in every archive); beside the CSVs, one\n" +
		"           Parquet file per logical table of the three data folders\n" +
		"           (ten — the per-station and per-cell tables whole, the same\n" +
		"           values as the CSVs, nulls native; provenance/ stays CSV);\n" +
		"           and at the archive root the filtered {state}.db — the\n" +
		"           full SQLite schema holding the state's stations, every\n" +
		"           row keyed to them, the cells they reference with those\n" +
		"           cells' POWER rows, every ingest and power run, and the\n" +
		"           stations' parsing warnings.\n" +
		"  json     {state}-json.zip — per station, under\n" +
		"           combined/<state>/<station_id>/: profile.json (the citable\n" +
		"           profile: identity, WMO completeness, the published normals\n" +
		"           and extras as 13-slot month series with a derived annual\n" +
		"           slot, the POWER cell and its monthly climatology, a daily\n" +
		"           summary, and the build's provenance) and daily.json (every\n" +
		"           observed date with its reanalysis, the same LEFT join as\n" +
		"           combined_daily); plus provenance/ as JSON.\n\n" +
		"Then, in a full run only (no --state), the deposit-wide groups, each\n" +
		"one archive, in this order:\n\n" +
		"  national-csv      national-csv.zip — every state's per-station and\n" +
		"                    per-cell CSV files copied raw from the state\n" +
		"                    tabular archives (a cell several states share\n" +
		"                    once), the seven whole-scope tables regenerated\n" +
		"                    over every station and cell, provenance/ once.\n" +
		"                    Needs every state's tabular archive: built with\n" +
		"                    tabular, and fails soft naming a state whose\n" +
		"                    archive failed.\n" +
		"  national-parquet  national-parquet.zip — the ten per-table Parquet\n" +
		"                    files regenerated over every station and cell.\n" +
		"  national-json     national-json.zip — every state's station pairs\n" +
		"                    copied raw from the state JSON archives,\n" +
		"                    provenance/ once. Needs every state's JSON\n" +
		"                    archive, as national-csv needs the tabular ones.\n" +
		"  national-sqlite   national-sqlite.zip — bioclima.db, the full\n" +
		"                    canonical database: every table and row, copied\n" +
		"                    compact and shipped in rollback-journal mode with\n" +
		"                    the schema version stamped.\n" +
		"  raw               conagua-raw-<snapshot_date>.zip — the complete\n" +
		"                    local snapshot directory of the shipped DB's\n" +
		"                    snapshot, <root>/conagua-raw/<date>/, verbatim:\n" +
		"                    every station file of every kind directory (the\n" +
		"                    unparsed monthly/ and extremes/ included),\n" +
		"                    _index.json, _progress.json. Anything else in the\n" +
		"                    directory — a stray file, a symlink, a crashed\n" +
		"                    pull's temp residue — fails the archive naming\n" +
		"                    it. Refused before anything is written when the\n" +
		"                    directory is missing, or a symlink, under --root.\n\n" +
		"Then the docs group — allowed in any run, since it describes the\n" +
		"database rather than the archive set — one file at a time, in this\n" +
		"order, each failing soft on its own:\n\n" +
		"  docs              README.md — the dataset, the artifact set (every\n" +
		"                    archive of a full run, the state code lookup),\n" +
		"                    how to read the archives, the conventions,\n" +
		"                    provenance, reproducibility, license and\n" +
		"                    citation, known limitations.\n" +
		"                    DATA-DICTIONARY.md, DATA-DICTIONARY.json — every\n" +
		"                    exported column of every file (type, unit,\n" +
		"                    decimals, source, the schema's own description),\n" +
		"                    the JSON profile and daily.json shapes, the\n" +
		"                    annual-slot rules, the documented constants and\n" +
		"                    dropped columns; generated from the schema.\n" +
		"                    LICENSE — the CC BY 4.0 legal code, verbatim.\n" +
		"                    NOTICE — the CONAGUA / SMN credit (Términos de\n" +
		"                    Libre Uso MX), the NASA POWER reference and\n" +
		"                    funding acknowledgement, the license scope, the\n" +
		"                    terms transparency note.\n" +
		"                    CITATION.cff — the citation file (CFF 1.2.0).\n" +
		"                    QA-REPORT.md — the lean QA report: the snapshot\n" +
		"                    and runs, per-table row counts, coverage, a null\n" +
		"                    / gap summary per value column, the gate's\n" +
		"                    results, each run's stored counters beside the\n" +
		"                    live counts, the parsing warnings by severity\n" +
		"                    and source.\n" +
		"                    zenodo-metadata.json — the build-only Zenodo\n" +
		"                    deposit metadata stub; no deposit is made.\n" +
		"                    Every number is computed from the database at\n" +
		"                    build time; no file carries a timestamp except\n" +
		"                    CITATION.cff's date-released and the stub's\n" +
		"                    publication_date, the build's UTC date.\n\n" +
		"Then manifest.json (the global provenance index: schema version, git\n" +
		"SHA, snapshot date, dataset identity, the states and the national\n" +
		"groups with their artifacts, the runs, per-table counts, per-file\n" +
		"checksums) and CHECKSUMS (sha256sum-compatible). Pass --state to\n" +
		"build a subset of states — the per-state groups by default, plus the\n" +
		"docs group when named; a national or raw group with --state is\n" +
		"refused — and --only a subset of groups.\n\n" +
		"The run is refused up front — before the database is opened and\n" +
		"before --out exists — when this binary was built from a modified\n" +
		"working tree: manifest.json, and every profile's meta, would stamp a\n" +
		"commit whose code is not the code that ran, and nothing in the\n" +
		"deposit could reveal it. Build from a committed tree to mint a\n" +
		"deposit. A binary carrying no VCS metadata at all is allowed — the\n" +
		"artifacts record an empty etl_git_sha, an honest gap rather than a\n" +
		"false claim — and says so on stderr.\n\n" +
		"The database is opened strictly read-only; publish never writes to\n" +
		"it. Archives are written atomically. CSV and JSON entries are\n" +
		"byte-reproducible: the same database and binary yield identical\n" +
		"bytes on every run. Parquet entries and the SQLite databases are\n" +
		"content-reproducible — the same values on every run — with their\n" +
		"writers kept deterministic (pinned settings, no timestamps).\n" +
		"The --out directory is the deposit: it may hold only this run's\n" +
		"files (a re-run into it converges; anything else there — another\n" +
		"state's or group's archive from a subset build, stray files —\n" +
		"refuses the run before anything is written), and holds at most " + strconv.Itoa(publish.MaxTopLevelFiles) + "\n" +
		"top-level files, Zenodo's per-record cap. Each archive and docs file\n" +
		"fails soft and independently — a failure logs to stderr, any prior\n" +
		"file at its name is removed, and the loop continues to the next\n" +
		"artifact; manifest.json and CHECKSUMS cover exactly what the\n" +
		"directory holds.\n\n" +
		"Exit codes: 0 clean completion; 2 when the run completed but one or\n" +
		"more artifacts failed (degraded — see the report); 1 on a run-level\n" +
		"failure (an unknown or out-of-scope group, no complete ingest run,\n" +
		"the gate refusing the database on an error-severity finding, a\n" +
		"missing snapshot directory for the raw group, an unknown state, a\n" +
		"foreign entry in --out, a provenance integrity signal, a binary\n" +
		"built from a modified working tree, a --doi that is not a bare DOI,\n" +
		"cancellation).",
	RunE: runPublish,
}

func init() {
	f := publishCmd.Flags()
	f.StringVar(&publishDBFlag, "db", "./bioclima.db",
		"SQLite file to read. Opened read-only; never modified — the gate included. A WAL-mode database's "+
			"-shm/-wal sidecars are maintained beside it even by a read-only open, so the directory must be "+
			"writable; the database file itself is never written by publish.")
	f.StringVar(&publishOutFlag, "out", "./build/publish",
		"Deposit-ready output directory (created if absent).")
	f.StringVar(&publishDOIFlag, "doi", "",
		"The deposit's DOI, bare (e.g. 10.5281/zenodo.1234567): the DOI reserved on the Zenodo draft the "+
			"deposit will be uploaded to. Written into the citation, CITATION.cff, manifest.json and every "+
			"profile. Empty = the deposit carries no DOI.")
	f.StringSliceVar(&publishStateFlag, "state", nil,
		"CONAGUA state code (any case), repeatable or comma-separated. Empty = every state in the database; "+
			"set = the per-state groups only.")
	f.StringSliceVar(&publishOnlyFlag, "only", nil,
		"Artifact group to build ("+groupList()+"), repeatable or comma-separated. Empty = every group of the run's scope.")
}

// groupList names the artifact groups the publish package builds, in
// build order, so the flag's help and ResolveOnly's valid set are one
// list.
func groupList() string {
	groups := publish.Groups()
	names := make([]string, 0, len(groups))
	for _, g := range groups {
		names = append(names, string(g))
	}
	return strings.Join(names, ", ")
}

// doiLabel renders --doi for the header line: the DOI, or "none".
func doiLabel(doi string) string {
	if doi == "" {
		return "none"
	}
	return doi
}

// selection renders a repeatable flag's values for the header line:
// the comma-joined list as typed, or "all" when the flag was not given.
func selection(values []string) string {
	if len(values) == 0 {
		return "all"
	}
	return strings.Join(values, ",")
}

// checkBuildProvenance decides whether this binary may mint a deposit,
// from the VCS provenance the toolchain stamped into it.
//
// A binary built from a modified working tree is refused: the SHA it
// would stamp into manifest.json and every profile's meta names a commit
// whose code is not the code that ran, and no consumer of the deposit
// could ever detect the lie. Refusing is the integrity line — polite
// resilience covers availability failures, never provenance ones.
//
// A binary with no VCS metadata at all is a different case and is let
// through: an empty etl_git_sha is an honest gap (the deposit's docs
// render it "(none)"), not a false claim, and refusing it would make the
// pipeline unrunnable from `go run` and from source builds outside a
// checkout. It is announced, since the mint it produces cannot be traced
// back to a commit.
func checkBuildProvenance(w io.Writer, revision string, modified bool) error {
	if modified {
		stamp := "an empty etl_git_sha"
		if revision != "" {
			stamp = "etl_git_sha " + revision
		}
		return fmt.Errorf("refusing to publish from a modified working tree: the deposit would stamp %s, "+
			"provenance that does not name the code that ran — commit or stash the changes, rebuild, and publish again", stamp)
	}
	if revision == "" {
		fprintf(w, "warning: this binary carries no VCS metadata; the deposit will record an empty "+
			"etl_git_sha and cannot be traced back to a commit\n")
	}
	return nil
}

func runPublish(cmd *cobra.Command, _ []string) error {
	// The provenance guard runs first, ahead of every open and every
	// mkdir: a refused run must leave the filesystem exactly as it found
	// it — no --out directory, and no -shm/-wal sidecars beside the DB.
	stderr := cmd.ErrOrStderr()
	revision, modified := buildVCS()
	if err := checkBuildProvenance(stderr, revision, modified); err != nil {
		return err
	}
	if err := publish.ValidateDOI(publishDOIFlag); err != nil {
		return fmt.Errorf("--doi: %w", err)
	}

	// Graceful cancellation is the only runtime intervention: SIGINT or
	// SIGTERM cancels ctx, the archive loop stops at the next
	// between-archives check (an in-flight archive is discarded whole —
	// nothing partial is ever left at a final path), and the report still
	// prints.
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stdout := cmd.OutOrStdout()

	db, err := schema.OpenReadOnly(publishDBFlag)
	if err != nil {
		return fmt.Errorf("open %s: %w", publishDBFlag, err)
	}
	defer db.Close() //nolint:errcheck // teardown close; the run's outcome is already determined

	fprintf(stderr, "db=%s (read-only)  out=%s  root=%s  states=%s  only=%s  doi=%s\n",
		publishDBFlag, publishOutFlag, snapshotRootFlag, selection(publishStateFlag), selection(publishOnlyFlag),
		doiLabel(publishDOIFlag))

	renderer := newPublishLog(stderr)
	report, runErr := publish.Run(ctx, db, publish.Options{
		OutDir:       publishOutFlag,
		SnapshotRoot: snapshotRootFlag,
		States:       publishStateFlag,
		Only:         publishOnlyFlag,
		// The revision the guard vouched for above — the same value
		// buildGitSHA returns, read once so the stamp and the check can
		// never disagree.
		ETLGitSHA: revision,
		DOI:       publishDOIFlag,
	}, renderer.event)

	// The report prints on every path that produced one — a run-level
	// failure still deserves its partial summary.
	if report != nil {
		printPublishReport(stdout, report, runErr == nil)
	}
	// runErr is a run-level failure (no snapshot, the gate's refusal, bad
	// state, cancellation, a provenance integrity signal) and maps to
	// exit 1 — a gate refusal's message carries every error finding, one
	// per line, through main's single error print; per-artifact fail-soft
	// casualties live in the report and map to exit 2 instead.
	if runErr != nil {
		return runErr
	}
	if report.Failed > 0 {
		return DegradedError{Summary: fmt.Sprintf(
			"publish degraded: %d of %d artifacts failed (see report)",
			report.Failed, len(report.Artifacts))}
	}
	return nil
}

// printPublishReport produces the compact end-of-run summary on stdout.
// Per-artifact failures already streamed to stderr as FAIL lines and
// are repeated here by name, so a degraded run's casualties are in the
// one place a caller captures. The gate line is present once the gate
// ran: passed, refused with its counts — the findings themselves are
// the error line's — or aborted, when a rule's own error stopped the
// evaluation and the counts are the completed rules' alone.
func printPublishReport(w io.Writer, r *publish.Report, complete bool) {
	fprintln(w)
	if complete {
		fprintln(w, "publish complete")
	} else {
		fprintln(w, "publish aborted")
	}
	fprintf(w, "  snapshot date        : %s\n", r.SnapshotDate)
	if r.Gate != nil {
		verdict := "passed"
		switch {
		case r.Gate.Errors > 0:
			verdict = "refused"
		case len(r.Gate.Rules) < len(validate.GateRules()):
			verdict = "aborted"
		}
		fprintf(w, "  gate                 : %s · %d rules · %d warn · %d error\n",
			verdict, len(r.Gate.Rules), r.Gate.Warnings, r.Gate.Errors)
	}
	fprintf(w, "  states               : %s\n", strings.Join(r.States, ","))
	fprintf(w, "  groups               : %s\n", strings.Join(r.Groups, ","))
	fprintf(w, "  artifacts            : %d ok / %d failed / %d attempted\n",
		len(r.Artifacts)-r.Failed, r.Failed, len(r.Artifacts))
	fprintf(w, "  top-level files      : %d\n", r.TopLevelFiles)
	fprintf(w, "  out dir              : %s\n", r.OutDir)
	if !r.StartedAt.IsZero() && !r.FinishedAt.IsZero() {
		fprintf(w, "  elapsed              : %s\n", r.FinishedAt.Sub(r.StartedAt).Round(time.Second))
	}
	if r.Failed > 0 {
		fprintln(w, "  failed artifacts:")
		for _, a := range r.Artifacts {
			if a.Err != "" {
				fprintf(w, "    %s: %s\n", a.Name, a.Err)
			}
		}
	}
}
