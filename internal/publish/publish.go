package publish

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// Options configures one publish run.
type Options struct {
	// OutDir is the deposit-ready directory the top-level files land in.
	OutDir string
	// SnapshotRoot is the local snapshot root (the --root flag): the raw
	// group ships <SnapshotRoot>/conagua-raw/<snapshot_date>/ whole. Read
	// only when the raw group is selected.
	SnapshotRoot string
	// States are the requested CONAGUA state codes, any case; empty
	// selects every state in the DB. A non-empty list scopes the run to
	// the per-state groups (ResolveOnly).
	States []string
	// Only are the requested artifact groups by name (the Group
	// constants, any case); empty builds every group of the run's scope.
	Only []string
	// ETLGitSHA is stamped into manifest.json and every profile's meta
	// block ("" when the build carried no VCS metadata).
	ETLGitSHA string
	// DOI is the deposit's DOI — the one reserved on the Zenodo draft the
	// deposit is uploaded to — or "" for none. It is rendered into the
	// suggested citation, CITATION.cff, manifest.json and every profile;
	// with "" each of them omits the DOI outright. Run refuses a value
	// ValidateDOI rejects before anything is read or written.
	DOI string
	// Now supplies the manifest's generated_at stamp and the report's
	// timestamps; nil means time.Now. Injected so specs can pin the one
	// non-reproducible field.
	Now func() time.Time
}

// ProgressEvent is one line of progress: a gate line (Rule set) as each
// rule of the validate gate completes, ahead of any write; a unit line
// (Unit set) as each unit of an archive is written — a per-station or
// per-cell daily file or the {state}.db of a tabular archive, a
// station's profile.json + daily.json pair of a JSON archive, a state
// archive consumed by a copy-built national archive, the bioclima.db of
// the national SQLite archive, a kind directory or root file of the raw
// snapshot — or an artifact line (Rule and Unit empty) when an archive
// or a docs-group file completes or fails. Unit is the label the unit is
// known by — an entry path for the per-state archives (the one name that
// tells the conagua/, combined/, and nasa_power/ files of the same id
// apart; a JSON pair is labelled by its profile.json, the entry whose
// write completes the pair), a state archive's file name for a national
// copy — and Index and Total are the unit's ordinal among the artifact's
// units in write order; Bytes, Entries, SHA256, and Elapsed describe a
// completed artifact; Err is set on a failed one — a fail-soft casualty
// the run continued past. File marks an artifact that is one top-level
// file rather than an archive — a docs-group file — which has no entry
// count to report. A gate line carries the rule's id, the rows it
// examined, its warn and error finding counts, and its Elapsed; Errors >
// 0 on any rule is the refusal that follows.
type ProgressEvent struct {
	Artifact string
	Unit     string
	Index    int
	Total    int
	Bytes    int64
	Entries  int
	SHA256   string
	Elapsed  time.Duration
	Err      error
	File     bool
	Rule     string
	Scanned  int
	Warnings int
	Errors   int
}

// ProgressFunc receives every ProgressEvent, in order. The CLI passes
// one to render stderr lines; tests record them.
type ProgressFunc func(ProgressEvent)

// ArtifactResult is one attempted top-level artifact — an archive, or a
// docs-group file, for which Entries is 0. Err is the failure message
// ("" on success); a failed artifact leaves nothing at its path — not a
// partial file, and not a prior run's file either.
type ArtifactResult struct {
	Name    string
	Bytes   int64
	Entries int
	SHA256  string
	Err     string
	Elapsed time.Duration
}

// Report is the record of one publish run — what the CLI summarizes.
// Gate is the validate gate's result, set once the gate ran: every rule,
// or the rules that completed before one's own error aborted the
// evaluation (nil when the run was refused before the gate ran);
// Errors > 0 in it is the refusal the run returned. States and Groups
// are the resolved selection in build order; Artifacts lists every
// artifact attempted, in build order: per state, each selected
// per-state group, then each selected national group and the raw
// snapshot, then the docs group's files. TopLevelFiles is the number of
// entries OutDir holds after the run, verified to be exactly the
// artifacts written plus manifest.json and CHECKSUMS and at most
// MaxTopLevelFiles — the count Zenodo's per-record cap is asserted
// against.
type Report struct {
	SnapshotDate  string
	OutDir        string
	Gate          *validate.GateReport
	States        []string
	Groups        []string
	Artifacts     []ArtifactResult
	TopLevelFiles int
	Failed        int
	StartedAt     time.Time
	FinishedAt    time.Time
}

// gateRefusalCap is how many error findings a GateError's message
// lists; the rest are counted. Every finding rides in Report.Gate.
const gateRefusalCap = 20

// GateError is the run-level refusal of a DB the validate gate found
// unfit to publish: every error-severity finding, in rule order, with
// the first gateRefusalCap spelled out. Nothing has been written when it
// is returned.
type GateError struct {
	Report *validate.GateReport
}

func (e *GateError) Error() string {
	findings := e.Report.ErrorFindings()
	var b strings.Builder
	fmt.Fprintf(&b, "gate refused: %d error finding%s", len(findings), plural(len(findings)))
	for i, f := range findings {
		if i == gateRefusalCap {
			fmt.Fprintf(&b, "\n  … and %d more", len(findings)-i)
			break
		}
		fmt.Fprintf(&b, "\n  %s — %s", f.RuleID, f.Issue)
	}
	return b.String()
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

const (
	manifestName       = "manifest.json"
	checksumsName      = "CHECKSUMS"
	snapshotDateLayout = "2006-01-02"

	// runTempPattern names the run-scoped temp directory Run creates
	// under OutDir for the entries that must build on disk before they
	// stream into an archive (the per-state and the national SQLite
	// databases). It lives in the deposit directory itself —
	// dot-prefixed and ".tmp-"-tagged like the archive writer's own temp
	// files, so a crashed run's residue is exactly what checkOutDir
	// refuses on the next run — and on the deposit's own filesystem, so
	// a database is never built on a small system temp volume.
	runTempPattern = ".publish.tmp-*"
)

// snapshotSQL resolves the snapshot the deposit is built from: the
// latest complete ingest run (latest by started_at, id breaking a tie).
const snapshotSQL = `
SELECT snapshot_date FROM ingest_runs WHERE status = 'complete'
 ORDER BY started_at DESC, id DESC LIMIT 1`

// Run builds the deposit into opts.OutDir: per selected state, one
// archive per selected per-state group in build order
// ({state}-tabular.zip, then {state}-json.zip); then, in a full run, the
// selected national archives and the raw snapshot in build order
// (national-csv.zip, national-parquet.zip, national-json.zip,
// national-sqlite.zip, conagua-raw-<date>.zip); then the docs group's
// files in write order (README.md, DATA-DICTIONARY.md,
// DATA-DICTIONARY.json, LICENSE, NOTICE, CITATION.cff, QA-REPORT.md,
// zenodo-metadata.json — over inputs loaded once for the group); then
// manifest.json, then CHECKSUMS. Everything
// that can refuse the whole build — an unknown or out-of-scope group,
// no complete ingest run, the validate gate finding the DB unfit (a
// GateError, every error finding named; the gate runs over db, reads
// only, and precedes every other check on the DB), a raw group whose
// snapshot directory is missing, an unknown state, an OutDir holding
// files this run does not produce, a power run_label collision — is
// checked before the first artifact is written; then the run's temp
// directory (runTempPattern) is created under OutDir for the entries
// that build on disk before they stream, and removed again before the
// out-dir post-check. Artifacts fail soft and independently — a failing
// archive or docs file is recorded, reported through progress, counted
// in Report.Failed, and any prior file at its path is removed while the
// loop continues to the next artifact; a copy-built national archive
// whose source state archive failed fails soft the same way, naming the
// state, without touching the archives that did build; only
// cancellation (checked before each artifact) or a lost driver
// connection aborts the run, in which case the error is returned with
// the Report populated and no manifest or CHECKSUMS is written. Run
// never prints; progress is the only live surface.
func Run(ctx context.Context, db *sql.DB, opts Options, progress ProgressFunc) (*Report, error) {
	if opts.OutDir == "" {
		return nil, errors.New("publish: OutDir is required")
	}
	if err := ValidateDOI(opts.DOI); err != nil {
		return nil, fmt.Errorf("publish: %w", err)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	if progress == nil {
		progress = func(ProgressEvent) {}
	}
	report := &Report{OutDir: opts.OutDir, States: []string{}, Groups: []string{}, StartedAt: now().UTC()}
	finish := func(err error) (*Report, error) {
		report.FinishedAt = now().UTC()
		return report, err
	}

	groups, err := ResolveOnly(opts.Only, len(opts.States) > 0)
	if err != nil {
		return finish(err)
	}
	report.Groups = groupNames(groups)

	snapshotDate, err := resolveSnapshot(ctx, db)
	if err != nil {
		return finish(err)
	}
	report.SnapshotDate = snapshotDate
	gate, err := validate.Gate(ctx, db, func(rr validate.RuleReport) {
		progress(ProgressEvent{Rule: rr.ID, Scanned: rr.Scanned, Warnings: rr.Warnings, Errors: rr.Errors, Elapsed: rr.Elapsed})
	})
	// The report carries the rules that completed even when a rule's own
	// error aborted the gate, so the summary of exactly that run names
	// what was evaluated.
	report.Gate = gate
	if err != nil {
		return finish(fmt.Errorf("gate: %w", err))
	}
	if gate.Errors > 0 {
		return finish(&GateError{Report: gate})
	}
	// time.Parse without a zone yields UTC: every entry is stamped with
	// the snapshot date at 00:00:00 UTC, so the archive's bytes depend on
	// the data, never on the wall clock.
	snapshot, err := time.Parse(snapshotDateLayout, snapshotDate)
	if err != nil {
		return finish(fmt.Errorf("ingest_runs.snapshot_date %q is not YYYY-MM-DD: %w", snapshotDate, err))
	}
	if slices.Contains(groups, GroupRaw) {
		if err := CheckRawSnapshot(opts.SnapshotRoot, snapshotDate); err != nil {
			return finish(err)
		}
	}

	all, err := LoadStates(ctx, db)
	if err != nil {
		return finish(err)
	}
	states, err := ResolveStates(all, opts.States)
	if err != nil {
		return finish(err)
	}
	report.States = codes(states)
	if err := checkOutDir(opts.OutDir, plannedNames(states, groups, snapshotDate)); err != nil {
		return finish(err)
	}
	runs, err := LoadRuns(ctx, db)
	if err != nil {
		return finish(err)
	}
	tmpDir, err := makeRunTempDir(opts.OutDir)
	if err != nil {
		return finish(err)
	}
	// The abort paths clean up through the defer; the success path
	// removes the directory explicitly below, ahead of the out-dir
	// post-check, so a failed removal is the run's error rather than an
	// unlisted entry in a deposit-ready directory.
	defer os.RemoveAll(tmpDir) //nolint:errcheck // abort-path cleanup; the success path checks the removal itself

	b := &build{
		ctx: ctx, db: db, opts: opts, snapshot: snapshot, snapshotDate: snapshotDate, runs: runs,
		meta: newProfileMeta(opts.ETLGitSHA, snapshot, runs, opts.DOI), tmpDir: tmpDir, progress: progress, report: report,
		built: map[Group][]StateArchive{}, failed: map[Group][]State{},
	}
	stateGroups, nationalGroups, docs := splitGroups(groups)
	b.total = len(states)*len(stateGroups) + len(nationalGroups)
	if docs {
		b.total += len(builtDocsFiles)
	}

	manifestStates := make([]ManifestState, 0, len(states))
	for _, st := range states {
		ms := ManifestState{Code: st.Code, Name: st.Name, Artifacts: []string{}}
		for _, g := range stateGroups {
			name := archiveName(st, g)
			ok, err := b.archive(name, func(onUnit UnitFunc) (*assembly, error) {
				return g.entries(ctx, db, st, runs, b.meta, tmpDir, onUnit)
			})
			if err != nil {
				return finish(err)
			}
			if ok {
				ms.Artifacts = append(ms.Artifacts, name)
				b.built[g] = append(b.built[g], StateArchive{State: st, Path: filepath.Join(opts.OutDir, name)})
			} else {
				b.failed[g] = append(b.failed[g], st)
			}
		}
		manifestStates = append(manifestStates, ms)
	}
	manifestNational := make([]ManifestNational, 0, len(nationalGroups))
	for _, g := range nationalGroups {
		name := nationalArchiveName(g, snapshotDate)
		mn := ManifestNational{Group: string(g), Artifacts: []string{}}
		ok, err := b.archive(name, func(onUnit UnitFunc) (*assembly, error) {
			return b.nationalEntries(g, name, onUnit)
		})
		if err != nil {
			return finish(err)
		}
		if ok {
			mn.Artifacts = append(mn.Artifacts, name)
		}
		manifestNational = append(manifestNational, mn)
	}
	if err := os.RemoveAll(tmpDir); err != nil {
		return finish(fmt.Errorf("remove temp dir %s: %w", tmpDir, err))
	}

	// The per-table counts serve the docs group and the manifest alike,
	// so they are read once, after the archives — the DB is read-only
	// and unchanging, so when they are read does not matter.
	counts, err := LoadCounts(ctx, db)
	if err != nil {
		return finish(err)
	}
	if docs {
		if err := b.writeDocs(counts, gate, all, now()); err != nil {
			return finish(err)
		}
	}

	generatedAt := now().UTC().Format(time.RFC3339)
	manifest := buildManifest(opts, snapshot, generatedAt, runs, counts, manifestStates, manifestNational, b.files)
	manifestRes, err := WriteManifest(filepath.Join(opts.OutDir, manifestName), manifest)
	if err != nil {
		return finish(err)
	}

	sums := make([]archive.Checksum, 0, len(b.files)+1)
	for _, f := range b.files {
		sums = append(sums, archive.Checksum{Name: f.Name, SHA256: f.SHA256, Bytes: f.Bytes})
	}
	sums = append(sums, archive.Checksum{Name: manifestName, SHA256: manifestRes.SHA256, Bytes: manifestRes.Bytes})
	if err := archive.WriteChecksums(filepath.Join(opts.OutDir, checksumsName), sums); err != nil {
		return finish(err)
	}
	report.TopLevelFiles, err = verifyOutDir(opts.OutDir, sums)
	if err != nil {
		return finish(err)
	}
	return finish(nil)
}

// build is the state of one Run the archive loops share: the inputs
// every archive is assembled from, the report and manifest lists the
// outcomes accumulate into, and — for the copy-built national archives
// — which state archives succeeded, in state order, and which failed.
type build struct {
	ctx          context.Context
	db           *sql.DB
	opts         Options
	snapshot     time.Time
	snapshotDate string
	runs         Runs
	meta         ProfileMeta
	tmpDir       string
	progress     ProgressFunc
	report       *Report
	total        int
	files        []ManifestFile
	built        map[Group][]StateArchive
	failed       map[Group][]State
}

// archive builds one archive and files its outcome (outcome).
// Cancellation is checked before the build, so an operator's interrupt
// lands between artifacts rather than inside the next one.
func (b *build) archive(name string, assemble assembler) (ok bool, err error) {
	if err := b.ctx.Err(); err != nil {
		return false, b.aborted(err)
	}
	res, buildErr := buildArchive(b.ctx, b.opts.OutDir, name, b.snapshot, b.progress, assemble)
	return b.outcome(res, buildErr)
}

// file writes one docs-group file — a top-level file that is not an
// archive — and files its outcome exactly as archive does, so the docs
// group is one more fail-soft unit of the deposit.
func (b *build) file(name string, write func(io.Writer) error) (ok bool, err error) {
	if err := b.ctx.Err(); err != nil {
		return false, b.aborted(err)
	}
	res, buildErr := buildFile(b.opts.OutDir, name, b.progress, write)
	return b.outcome(res, buildErr)
}

// outcome files one artifact's result: a loop-fatal error aborts the
// run (returned, the report populated); a fail-soft casualty is
// counted, its stale file removed, and the run continues (ok false); a
// success is listed for the manifest (ok true).
func (b *build) outcome(res ArtifactResult, buildErr error) (ok bool, err error) {
	b.report.Artifacts = append(b.report.Artifacts, res)
	switch {
	case buildErr != nil && isLoopFatal(b.ctx, buildErr):
		b.report.Failed++
		return false, b.aborted(buildErr)
	case buildErr != nil:
		b.report.Failed++
		return false, removeStale(filepath.Join(b.opts.OutDir, res.Name))
	}
	b.files = append(b.files, ManifestFile{Name: res.Name, SHA256: res.SHA256, Bytes: res.Bytes})
	return true, nil
}

// aborted is the run-level error of an interrupted loop.
func (b *build) aborted(err error) error {
	return fmt.Errorf("aborted after %d of %d artifacts: %w", len(b.report.Artifacts), b.total, err)
}

// nationalEntries assembles one deposit-wide archive. A copy-built
// archive reads the succeeded state archives of the group it copies
// from and needs every state's: a state whose archive failed fails the
// national archive here, naming the state, before anything is opened.
// The others regenerate from the DB or the snapshot directory and do not
// depend on the state archives.
func (b *build) nationalEntries(g Group, name string, onUnit UnitFunc) (*assembly, error) {
	if src, copies := g.copiesFrom(); copies {
		if err := b.checkSources(src, name); err != nil {
			return nil, err
		}
	}
	switch g {
	case GroupNationalCSV:
		n, err := NationalCSVEntries(b.ctx, b.db, b.built[GroupTabular], b.runs, onUnit)
		if err != nil {
			return nil, err
		}
		return &assembly{entries: n.Entries, units: n.Units, release: n.Close}, nil
	case GroupNationalJSON:
		n, err := NationalJSONEntries(b.ctx, b.db, b.built[GroupJSON], b.runs, onUnit)
		if err != nil {
			return nil, err
		}
		return &assembly{entries: n.Entries, units: n.Units, release: n.Close}, nil
	case GroupNationalParquet:
		entries, err := NationalParquetEntries(b.ctx, b.db)
		if err != nil {
			return nil, err
		}
		return generated(entries, 0), nil
	case GroupNationalSQLite:
		srcPath, err := sourcePath(b.ctx, b.db)
		if err != nil {
			return nil, err
		}
		u := unitList{onUnit: onUnit}
		e := NationalDBEntry(b.ctx, srcPath, b.tmpDir)
		u.add(e.Path, e.Write)
		return generated(u.entries, len(u.entries)), nil
	case GroupRaw:
		entries, err := RawSnapshotEntries(b.ctx, b.opts.SnapshotRoot, b.snapshotDate)
		if err != nil {
			return nil, err
		}
		units := rawUnits(entries, onUnit)
		return generated(entries, units), nil
	}
	return nil, fmt.Errorf("artifact group %q has no assembler", g)
}

// writeDocs writes the docs group: every file of builtDocsFiles, in
// write order, each its own fail-soft artifact through file, over inputs
// loaded once for the group (docsInputs). counts are the run's per-table
// counts, gate its validate gate result, states the DB's full state
// list, now the build time.
func (b *build) writeDocs(counts TableCounts, gate *validate.GateReport, states []State, now time.Time) error {
	d := loadDocsInputs(b.ctx, b.db, b.meta, counts, gate, states, now)
	if CheckRawSnapshot(b.opts.SnapshotRoot, b.snapshotDate) == nil {
		// The README describes the raw artifact whether or not this run
		// builds it, so the snapshot is read whenever it is present.
		d.in.RawHTMLPages, d.rawErr = RawHTMLPages(b.ctx, b.opts.SnapshotRoot, b.snapshotDate)
	}
	for _, name := range builtDocsFiles {
		if _, err := b.file(name, d.writer(name)); err != nil {
			return err
		}
	}
	return nil
}

// docsInputs is what the docs group's files are rendered from, loaded
// once per run ahead of the group's first file and shared by every file
// of it: the DocsInput the metadata renderers take — the build's meta,
// the conventional station count, the per-table counts, the QA report's
// coverage, the DB's full state list (so the docs describe the whole
// deposit whatever --state selected), and the build time, the one
// wall-clock input, stamped by CITATION.cff's date-released and
// zenodo-metadata.json's publication_date alone — the QA report the
// coverage was cut from, and the data
// dictionary, generated from the schema with no DB read. A load that
// fails is held rather than returned: every file that reads the failed
// input fails soft on its own, as an archive would, while the files
// that do not — the dictionary and LICENSE — still ship; a loop-fatal
// load error (cancellation, a lost driver) aborts the run through the
// first such file's outcome, exactly as it would from inside a write.
type docsInputs struct {
	meta    ProfileMeta
	in      DocsInput
	qa      *QAReport
	loadErr error
	// rawErr is a failed read of the local snapshot for the README's
	// raw-artifact note: it fails the README alone, the one file that
	// states what the scan found.
	rawErr  error
	dict    *Dictionary
	dictErr error
}

// loadDocsInputs reads the docs group's inputs: the station count and
// the QA report over db beside what the run already resolved, and the
// dictionary from the schema.
func loadDocsInputs(ctx context.Context, db *sql.DB, meta ProfileMeta, counts TableCounts,
	gate *validate.GateReport, states []State, now time.Time,
) *docsInputs {
	d := &docsInputs{meta: meta}
	d.dict, d.dictErr = BuildDictionary(meta)
	stations, err := LoadStationCount(ctx, db)
	if err != nil {
		d.loadErr = err
		return d
	}
	d.qa, err = LoadQA(ctx, db, meta.SnapshotDate, meta.Runs, counts, gate, states)
	if err != nil {
		d.loadErr = err
		return d
	}
	d.in = DocsInput{Meta: meta, Stations: stations, Counts: counts, Coverage: d.qa.Coverage, States: states, Now: now}
	return d
}

// writer returns the writer of one docs-group file, by the name
// builtDocsFiles lists it under. A file whose input failed to load
// returns that error from its writer, so the failure is filed against
// the file; a name the group does not know is an impossible state
// (builtDocsFiles is the dispatch list) and is filed the same way
// rather than left silent.
func (d *docsInputs) writer(name string) func(io.Writer) error {
	over := func(render func(io.Writer, DocsInput) error) func(io.Writer) error {
		return func(w io.Writer) error {
			if d.loadErr != nil {
				return d.loadErr
			}
			return render(w, d.in)
		}
	}
	dictionary := func(render func(io.Writer, *Dictionary) error) func(io.Writer) error {
		return func(w io.Writer) error {
			if d.dictErr != nil {
				return d.dictErr
			}
			return render(w, d.dict)
		}
	}
	switch name {
	case readmeName:
		readme := over(RenderREADME)
		return func(w io.Writer) error {
			if d.rawErr != nil {
				return d.rawErr
			}
			return readme(w)
		}
	case DictionaryMarkdownName:
		return dictionary(RenderDictionaryMarkdown)
	case DictionaryJSONName:
		return dictionary(RenderDictionaryJSON)
	case licenseName:
		return WriteLicense
	case noticeName:
		return over(RenderNotice)
	case citationName:
		return over(RenderCitation)
	case qaReportName:
		return func(w io.Writer) error {
			if d.loadErr != nil {
				return d.loadErr
			}
			return RenderQA(w, d.qa, d.meta)
		}
	case zenodoMetadataName:
		return over(RenderZenodoMetadata)
	}
	return func(io.Writer) error { return fmt.Errorf("docs file %q has no writer", name) }
}

// checkSources refuses a copy-built national archive whose source
// archives are not every state's: the ones that failed this run are
// named, state by state.
func (b *build) checkSources(src Group, name string) error {
	failed := b.failed[src]
	if len(failed) == 0 {
		return nil
	}
	parts := make([]string, len(failed))
	for i, st := range failed {
		parts[i] = fmt.Sprintf("state %s archive %s failed", st.Code, archiveName(st, src))
	}
	return fmt.Errorf("%s: %s needs every state's %s archive", strings.Join(parts, ", "), name, src)
}

// splitGroups separates the selected groups into the per-state
// archives, the deposit-wide archives (the national groups and the raw
// snapshot), each in build order, and whether the docs group — no
// archive at all — is selected.
func splitGroups(groups []Group) (perState, national []Group, docs bool) {
	for _, g := range groups {
		switch {
		case g == GroupDocs:
			docs = true
		case g.PerState():
			perState = append(perState, g)
		default:
			national = append(national, g)
		}
	}
	return perState, national, docs
}

// plannedNames lists every top-level file the run will produce: the
// per-state archives of the selected per-state groups, the selected
// national archives, the docs group's files when it is selected,
// manifest.json, and CHECKSUMS.
func plannedNames(states []State, groups []Group, snapshotDate string) []string {
	var names []string
	for _, g := range groups {
		switch {
		case g == GroupDocs:
			names = append(names, builtDocsFiles...)
		case !g.PerState():
			names = append(names, nationalArchiveName(g, snapshotDate))
		default:
			for _, st := range states {
				names = append(names, archiveName(st, g))
			}
		}
	}
	return append(names, manifestName, checksumsName)
}

// resolveSnapshot returns the snapshot_date of the latest complete
// ingest run: without one there is nothing to publish. The validate
// gate's ingest-complete anchor covers the same ground; this is the
// resolver, run first because the gate's own progress lines and the
// report name the snapshot.
func resolveSnapshot(ctx context.Context, db *sql.DB) (string, error) {
	var snapshotDate string
	err := db.QueryRowContext(ctx, snapshotSQL).Scan(&snapshotDate)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", errors.New("no complete ingest run: nothing to publish")
	case err != nil:
		return "", fmt.Errorf("resolve snapshot: %w", err)
	}
	return snapshotDate, nil
}

// checkOutDir refuses an OutDir that already holds an entry this run
// will not produce (expected lists what it will) — another state's or
// another group's archive from a subset build, a crashed run's temp
// residue, anything foreign. Such a file would end up in a
// deposit-ready directory unlisted by manifest.json and CHECKSUMS, and
// deleting it would be a destructive guess about the operator's intent;
// refusing before the first archive is written is the reversible
// choice. A missing OutDir is fine — the first write creates it.
func checkOutDir(dir string, expected []string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check out dir: %w", err)
	}
	var foreign []string
	for _, e := range entries {
		if !slices.Contains(expected, e.Name()) {
			foreign = append(foreign, e.Name())
		}
	}
	if len(foreign) > 0 {
		return fmt.Errorf("out dir %s holds entries this run does not produce: %s "+
			"(remove them or use a fresh --out)", dir, strings.Join(foreign, ", "))
	}
	return nil
}

// makeRunTempDir creates the run's temp directory under outDir
// (runTempPattern), creating outDir first: the pre-flight has passed by
// the time it is called, so the first thing the run puts in the deposit
// directory is its own temp space. Run removes it before the out-dir
// post-check.
func makeRunTempDir(outDir string) (string, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", fmt.Errorf("create out dir %s: %w", outDir, err)
	}
	dir, err := os.MkdirTemp(outDir, runTempPattern)
	if err != nil {
		return "", fmt.Errorf("create temp dir: %w", err)
	}
	return dir, nil
}

// removeStale deletes a prior run's archive at the path of an archive
// that failed soft this run, so the directory never carries an archive
// the new manifest does not vouch for; a path with nothing at it is the
// common case.
func removeStale(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale %s: %w", path, err)
	}
	return nil
}

// buildFile writes one docs-group file atomically and reports its
// outcome as buildArchive does for an archive: the ArtifactResult for
// the Report and the raw error, the artifact progress event fired on
// both outcomes and marked File. The write callback renders the file
// straight into the temp file, so a rendering that fails leaves nothing
// at the final path.
func buildFile(outDir, name string, progress ProgressFunc, write func(io.Writer) error) (ArtifactResult, error) {
	start := time.Now()
	res, err := archive.WriteFileAtomic(filepath.Join(outDir, name), write)
	elapsed := time.Since(start)
	if err != nil {
		progress(ProgressEvent{Artifact: name, File: true, Elapsed: elapsed, Err: err})
		return ArtifactResult{Name: name, Err: err.Error(), Elapsed: elapsed}, err
	}
	progress(ProgressEvent{Artifact: name, File: true, Bytes: res.Bytes, SHA256: res.SHA256, Elapsed: elapsed})
	return ArtifactResult{Name: name, Bytes: res.Bytes, SHA256: res.SHA256, Elapsed: elapsed}, nil
}

// verifyOutDir asserts the deposit-ready invariant after the last write
// — every entry in dir is one of the files CHECKSUMS lists, or CHECKSUMS
// itself, and there are at most MaxTopLevelFiles of them — and returns
// the entry count. checkOutDir and removeStale make an unlisted entry
// unreachable on this run's own paths, and the artifact set is 79
// files at its fullest, so a violation of either can only come from
// something else that touched the directory meanwhile; the check
// stands against that, and the count the cap is asserted against is
// what is on disk.
func verifyOutDir(dir string, sums []archive.Checksum) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("verify out dir: %w", err)
	}
	listed := make(map[string]bool, len(sums)+1)
	for _, s := range sums {
		listed[s.Name] = true
	}
	listed[checksumsName] = true
	var unlisted []string
	for _, e := range entries {
		if !listed[e.Name()] {
			unlisted = append(unlisted, e.Name())
		}
	}
	if len(unlisted) > 0 {
		return 0, fmt.Errorf("out dir %s holds entries CHECKSUMS does not list: %s",
			dir, strings.Join(unlisted, ", "))
	}
	if len(entries) > MaxTopLevelFiles {
		return 0, fmt.Errorf("out dir %s holds %d top-level files, over Zenodo's %d-file cap",
			dir, len(entries), MaxTopLevelFiles)
	}
	return len(entries), nil
}

// TabularEntries assembles one state's tabular archive — the four scope
// folders as CSV: conagua/, nasa_power/, combined/, and provenance/
// (rendered from runs, the row sets loaded once for the whole build);
// beside the CSVs, the ten per-table Parquet twins of the three data
// folders (provenance/ stays CSV-only); and at the archive root the
// filtered {state}.db, built under tmpDir — as the entry list
// archive.WriteZip takes: merged and Path-sorted once, since the writer
// takes the caller's order as the byte contract and refuses a duplicate
// path. The station list is read once and shared by the two per-station
// folders and the Parquet twins, the cell list once for nasa_power/ and
// its twins; every entry streams its rows at write time, so the
// single-connection DB is touched by one entry at a time. onUnit
// (optional) fires from each folder's unit entries as its per-station or
// per-cell daily files are written and once for the state database — the
// archive's slowest entry, every table copied, compacted, and finalized
// on disk before it streams, so it earns a progress line of its own,
// while the Parquet files, each one streaming query over rows the CSV
// twins already counted, do not — and units is how many of those there
// are: the Total the first unit event must already carry.
func TabularEntries(ctx context.Context, db *sql.DB, st State, runs Runs, tmpDir string, onUnit UnitFunc,
) (entries []archive.Entry, units int, err error) {
	stations, err := loadStateStations(ctx, db, st)
	if err != nil {
		return nil, 0, err
	}
	cells, err := loadStateCells(ctx, db, st)
	if err != nil {
		return nil, 0, err
	}
	conagua, conaguaUnits := conaguaEntries(ctx, db, st, stations, onUnit)
	nasaPower, powerUnits := powerEntries(ctx, db, st, cells, onUnit)
	combined, combinedUnits := combinedEntries(ctx, db, st, stations, onUnit)
	parquet := parquetEntries(ctx, db, st, stations, cells)
	stateDB := unitList{onUnit: onUnit}
	dbEntry := StateDBEntry(ctx, db, st, tmpDir)
	stateDB.add(dbEntry.Path, dbEntry.Write)
	entries = slices.Concat(conagua, nasaPower, combined, parquet, ProvenanceEntries(runs), stateDB.entries)
	slices.SortFunc(entries, func(a, b archive.Entry) int { return strings.Compare(a.Path, b.Path) })
	return entries, conaguaUnits + powerUnits + combinedUnits + len(stateDB.entries), nil
}

// assembly is one archive's entry list, ready to write: the entries in
// write order — generated, or copied raw from a source archive — the
// number of units among them, and release, the closing of whatever the
// entries hold open (the state archives a national copy reads from),
// run once the write has finished, on failure too; nil when the entries
// hold nothing open.
type assembly struct {
	entries []archive.MixedEntry
	units   int
	release func() error
}

// assembler produces an archive's assembly for the unit hook it is
// handed.
type assembler func(onUnit UnitFunc) (*assembly, error)

// generated is the assembly of generated entries only.
func generated(entries []archive.Entry, units int) *assembly {
	mixed := make([]archive.MixedEntry, len(entries))
	for i, e := range entries {
		mixed[i] = archive.Generated(e)
	}
	return &assembly{entries: mixed, units: units}
}

// buildArchive writes one archive — the assembly assemble returns, wired
// to the unit callback it is handed — and reports its outcome: the
// ArtifactResult for the Report and the raw error, which Run classifies
// as fail-soft or loop-fatal. The error is not prefixed with the
// archive name — the zip write already names its path, and the FAIL
// line and the Report carry the artifact. The artifact progress event
// fires here on both outcomes; unit events fire from inside the zip
// write as each unit lands, numbered across the whole archive in write
// order. A release that fails after a successful write is the archive's
// error: the file is not vouched for.
func buildArchive(ctx context.Context, outDir, name string, modified time.Time, progress ProgressFunc,
	assemble assembler,
) (ArtifactResult, error) {
	start := time.Now()
	res, err := func() (archive.Result, error) {
		// Units fire only from inside the zip write, after the entry list
		// is complete, so total is set before the first event reads it.
		var index, total int
		a, err := assemble(func(path string) {
			index++
			progress(ProgressEvent{Artifact: name, Unit: path, Index: index, Total: total})
		})
		if err != nil {
			return archive.Result{}, err
		}
		total = a.units
		res, err := archive.WriteZipMixed(ctx, filepath.Join(outDir, name), a.entries, archive.ZipOptions{Modified: modified})
		if a.release != nil {
			if releaseErr := a.release(); releaseErr != nil && err == nil {
				err = releaseErr
			}
		}
		return res, err
	}()
	elapsed := time.Since(start)
	if err != nil {
		progress(ProgressEvent{Artifact: name, Elapsed: elapsed, Err: err})
		return ArtifactResult{Name: name, Err: err.Error(), Elapsed: elapsed}, err
	}
	progress(ProgressEvent{Artifact: name, Bytes: res.Bytes, Entries: res.Entries, SHA256: res.SHA256, Elapsed: elapsed})
	return ArtifactResult{Name: name, Bytes: res.Bytes, Entries: res.Entries, SHA256: res.SHA256, Elapsed: elapsed}, nil
}

// isLoopFatal separates the two errors that must stop the run from the
// per-archive failures it continues past: the operator's cancellation
// (the context's own error, or one surfaced through a query it ended)
// and a lost driver connection, after which every remaining archive
// would fail the same way for a reason that is not the archive's.
func isLoopFatal(ctx context.Context, err error) bool {
	return ctx.Err() != nil ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone)
}
