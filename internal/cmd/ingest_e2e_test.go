package cmd_test

// The ingest e2e story: the real compiled binary ingests a snapshot that
// a real pull run produced from the fixture CONAGUA, into a fresh SQLite
// DB. The specs prove the cmd→ingest seam — flag wiring, sink and
// snapshot resolution, the per-station progress log, the report, the
// exit-code contract (0 clean / 2 degraded / 1 run-level failure) — and
// cross-check the printed report against what actually landed in the DB.
// Value-level correctness of the parsers and writers is owned by the
// internal/ingest specs and the parity gate; here the DB is read back to
// prove the binary wrote through the whole stack.
//
// A second context drives the same binary over a hand-built snapshot
// whose ledger carries a pull-errored kind and a fetched-but-missing
// body — the two data-gap warning paths a healthy pull never produces —
// and hosts the --external-id scoping specs.

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// ingestLineREFor matches one per-station progress line on stderr for a
// plan of the given size, capturing status, external_id, and the
// per-station daily/normals/extras/warn tallies. FAIL lines carry the
// station's error as an optional trailing segment.
func ingestLineREFor(total int) *regexp.Regexp {
	return regexp.MustCompile(fmt.Sprintf(
		`(?m)^\[\d{2}:\d{2}:\d{2}\] \d/%d (ok|FAIL)\s+(\d{4}) .*? · daily=(\d+) normals=(\d+) extras=(\d+) warn=(\d+) · elapsed \S+ · eta \S+(?: · .*)?$`,
		total))
}

// ingestLineRE is the pulled-snapshot contexts' five-station shape.
var ingestLineRE = ingestLineREFor(5)

// reportNumRE extracts a numeric report field from stdout by its label.
func reportNum(stdout, label string) int {
	re := regexp.MustCompile(`(?m)^  ` + regexp.QuoteMeta(label) + ` *: (\d+)`)
	m := re.FindStringSubmatch(stdout)
	ExpectWithOffset(1, m).NotTo(BeNil(), "report field %q not found", label)
	n, err := strconv.Atoi(m[1])
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return n
}

// dailyDateLineRE matches one data row in a raw CONAGUA daily file.
var dailyDateLineRE = regexp.MustCompile(`(?m)^\d{4}-\d{2}-\d{2}\t`)

// fixtureDailyRows counts the data rows the daily fixture for key carries
// — every date line becomes exactly one daily_observations row.
func fixtureDailyRows(key string) int {
	return len(dailyDateLineRE.FindAll(fixtureBody(servedFiles[key]), -1))
}

// pendingWarningFiles are the sink keys of the ledger's pending entries
// for ingest-relevant kinds: the pull e2e fixture excludes the three
// older normals kinds via --kind, so their hrefs stay pending and ingest
// must surface each as an "unexpected pull outcome" warning.
var pendingWarningFiles = []string{
	"conagua-raw/" + snapshotDate + "/normals_1961_1990/01001.txt",
	"conagua-raw/" + snapshotDate + "/normals_1961_1990/01003.txt",
	"conagua-raw/" + snapshotDate + "/normals_1971_2000/01001.txt",
	"conagua-raw/" + snapshotDate + "/normals_1971_2000/01004.txt",
	"conagua-raw/" + snapshotDate + "/normals_1981_2010/01001.txt",
	"conagua-raw/" + snapshotDate + "/normals_1981_2010/01004.txt",
}

// headerlessDailyFile is 1003's daily body: the fixture is deliberately
// headerless (it starts straight at the FECHA table), so ingest opens it,
// fails header parse, records an error-severity warning, and the station
// still commits — the file-level fail-soft path.
const headerlessDailyFile = "conagua-raw/" + snapshotDate + "/daily/01003.txt"

const headerlessIssueRE = `no station header found \(EOF at line \d+\)`

// wantWarningsTotal is every warning a full ingest of the fixture
// snapshot emits: the six pending hrefs plus 1003's headerless daily.
var wantWarningsTotal = len(pendingWarningFiles) + 1

func runIngestToExit(root string, extra ...string) *gexec.Session {
	args := []string{"ingest", "--root", root}
	args = append(args, extra...)
	session, err := gexec.Start(exec.Command(binPath, args...), GinkgoWriter, GinkgoWriter)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	EventuallyWithOffset(1, session, 60*time.Second).Should(gexec.Exit())
	return session
}

// openIngestDB opens the spec's DB read-only; closing is deferred to the
// spec's exit.
func openIngestDB(path string) *sql.DB {
	db, err := schema.OpenReadOnly(path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	DeferCleanup(func() error { return db.Close() })
	return db
}

func countRows(db *sql.DB, query string, args ...any) int {
	var n int
	ExpectWithOffset(1, db.QueryRow(query, args...).Scan(&n)).To(Succeed())
	return n
}

// --- Synthesized snapshot -------------------------------------------------
//
// A hand-built two-station snapshot whose bodies are the golden CONAGUA
// files the parser and ingest suites assert value-by-value, so the DB
// values the binary writes are directly comparable to those specs. The
// ledger is shaped to exercise both data-gap warning paths:
//   - 01001's normals_1961_1990 errored during pull (outcome=error with
//     attempts/http_code/last_error) → one pull-errored warn row;
//   - 01097's daily is marked fetched but deliberately has no body on
//     disk → one ledger/sink-divergence warn row.

const synthDate = "2026-01-01"

// synthFixtures maps each staged snapshot body (relative to the date
// dir) to the golden file it is copied from verbatim.
var synthFixtures = map[string]string{
	"daily/01001.txt":             "../conagua/testdata/daily/real_01001.txt",
	"normals_1981_2010/01001.txt": "../ingest/testdata/normals_1981_2010_01001.txt",
	"normals_1991_2020/01001.txt": "../conagua/testdata/normals/real_1991_2020_01001.txt",
	"normals_1991_2020/01097.txt": "testdata/Normales9120/ags/nor9120_01097.txt",
}

// The two warning rows the synthesized ledger must produce, keyed by the
// sink key each is anchored to.
const (
	synthPullErroredFile  = "conagua-raw/" + synthDate + "/normals_1961_1990/01001.txt"
	synthPullErroredIssue = "pull errored after 3 attempt(s), http=500: unexpected status 500"
	synthDivergedFile     = "conagua-raw/" + synthDate + "/daily/01097.txt"
	synthDivergedIssue    = "pull ledger says fetched, but sink has no object at this key"
)

// buildSynthRoot stages the golden bodies and marshals the hand-built
// _index.json under a fresh root. Ingest never writes under the root, so
// one build serves every spec in the context.
func buildSynthRoot() string {
	root, err := os.MkdirTemp("", "ingest-e2e-synth-*")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	DeferCleanup(func() error { return os.RemoveAll(root) })

	dateDir := filepath.Join(root, "conagua-raw", synthDate)
	for rel, src := range synthFixtures {
		body, readErr := os.ReadFile(filepath.FromSlash(src))
		ExpectWithOffset(1, readErr).NotTo(HaveOccurred())
		dst := filepath.Join(dateDir, filepath.FromSlash(rel))
		ExpectWithOffset(1, os.MkdirAll(filepath.Dir(dst), 0o755)).To(Succeed())
		ExpectWithOffset(1, os.WriteFile(dst, body, 0o644)).To(Succeed())
	}

	fetched := snapshot.FileState{Outcome: snapshot.OutcomeFetched, HTTPCode: 200, Attempts: 1}
	now := time.Now().UTC()
	p := snapshot.Progress{
		SnapshotDate:  synthDate,
		SchemaVersion: snapshot.ProgressSchemaVersion,
		StartedAt:     now,
		CompletedAt:   now,
		Catalog:       snapshot.CatalogSummary{StatesDiscovered: 1, StationsDiscovered: 2},
		Counts:        snapshot.Counts{FilesExpected: 6, FilesFetched: 5, FilesErrored: 1},
		Stations: []snapshot.StationProgress{
			{
				// The catalog name deliberately differs from the file
				// headers ("AGUASCALIENTES (OBS)") so the specs prove
				// header enrichment wins through the binary.
				State: conagua.Aguascalientes, ID: "01001",
				Name: "Aguascalientes Observatorio", Municipality: "Aguascalientes",
				Status: conagua.StatusOperating,
				Files: map[conagua.Kind]snapshot.FileState{
					conagua.KindDaily:            fetched,
					conagua.KindNormals1981_2010: fetched,
					conagua.KindNormals1991_2020: fetched,
					conagua.KindNormals1961_1990: {
						Outcome:   snapshot.OutcomeError,
						HTTPCode:  500,
						Attempts:  3,
						LastError: "unexpected status 500",
					},
				},
			},
			{
				State: conagua.Aguascalientes, ID: "01097",
				Name: "Aguascalientes Ii", Municipality: "Aguascalientes",
				Status: conagua.StatusOperating,
				Files: map[conagua.Kind]snapshot.FileState{
					// Marked fetched, but no body staged: the
					// ledger/sink divergence the run must surface.
					conagua.KindDaily:            fetched,
					conagua.KindNormals1991_2020: fetched,
				},
			},
		},
	}
	data, err := json.MarshalIndent(p, "", "  ")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	ExpectWithOffset(1, os.WriteFile(filepath.Join(dateDir, "_index.json"), data, 0o644)).To(Succeed())
	return root
}

// ingestWarningRow is a parsing_warnings row joined to its station's
// external_id, for whole-row comparisons.
type ingestWarningRow struct {
	externalID, sourceFile, severity, issue string
	noLine                                  bool
}

// readWarningRows returns every parsing_warnings row ordered by
// source_file.
func readWarningRows(db *sql.DB) []ingestWarningRow {
	rows, err := db.Query(`
SELECT s.external_id, w.source_file, w.severity, w.issue, w.line IS NULL
  FROM parsing_warnings w JOIN stations s ON s.id = w.station_id
 ORDER BY w.source_file`)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer rows.Close() //nolint:errcheck // spec teardown
	var got []ingestWarningRow
	for rows.Next() {
		var r ingestWarningRow
		ExpectWithOffset(1, rows.Scan(&r.externalID, &r.sourceFile, &r.severity, &r.issue, &r.noLine)).To(Succeed())
		got = append(got, r)
	}
	ExpectWithOffset(1, rows.Err()).NotTo(HaveOccurred())
	return got
}

// synthWantWarnings is the exact warning population a full run of the
// synthesized snapshot commits (ordered by source_file: daily/ < normals_).
var synthWantWarnings = []ingestWarningRow{
	{externalID: "1097", sourceFile: synthDivergedFile,
		severity: "warn", issue: synthDivergedIssue, noLine: true},
	{externalID: "1001", sourceFile: synthPullErroredFile,
		severity: "warn", issue: synthPullErroredIssue, noLine: true},
}

var _ = Describe("conagua-etl ingest end-to-end", func() {
	Context("against a locally pulled snapshot", Ordered, func() {
		var pristineRoot string

		BeforeAll(func() {
			var err error
			pristineRoot, err = os.MkdirTemp("", "ingest-e2e-pristine-*")
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() error { return os.RemoveAll(pristineRoot) })

			// Build the snapshot exactly as an operator would: a real pull
			// against the fixture CONAGUA. The server closes before any
			// ingest spec runs — ingest is an offline verb.
			srv := startFixtureServer()
			pull := runPullToExit(pristineRoot, srv)
			srv.Close()
			Expect(pull.ExitCode()).To(Equal(0))
			Expect(snapshotTree(pristineRoot)).To(Equal(completedTree))
		})

		It("ingests every station into a fresh DB, resolving the newest snapshot by default", func() {
			root := cloneTree(pristineRoot)
			dbPath := filepath.Join(GinkgoT().TempDir(), "bioclima.db")
			wantDaily := fixtureDailyRows("01001/daily")

			// No --snapshot: the single date dir under the root must resolve.
			session := runIngestToExit(root, "--db", dbPath)
			Expect(session.ExitCode()).To(Equal(0))

			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring("sink: local fs  root=" + root))
			Expect(stderr).To(ContainSubstring(
				"snapshot: " + snapshotDate + " (newest under " + root + ")"))

			// One progress line per station, all ok, with the per-station
			// daily and warning tallies the ledger dictates: the pending
			// normals hrefs cost 1001 three warnings and 1004 two; 1003
			// carries one pending plus its headerless-daily error, which
			// fails soft — the station still reads ok.
			type lineTally struct{ status, daily, warns string }
			got := map[string]lineTally{}
			for _, m := range ingestLineRE.FindAllStringSubmatch(stderr, -1) {
				got[m[2]] = lineTally{status: m[1], daily: m[3], warns: m[6]}
			}
			Expect(got).To(Equal(map[string]lineTally{
				"1001": {status: "ok", daily: strconv.Itoa(wantDaily), warns: "3"},
				"1003": {status: "ok", daily: "0", warns: "2"},
				"1004": {status: "ok", daily: "0", warns: "2"},
				"1025": {status: "ok", daily: "0", warns: "0"},
				"1097": {status: "ok", daily: "0", warns: "0"},
			}))

			stdout := string(session.Out.Contents())
			Expect(stdout).To(ContainSubstring("ingest complete"))
			Expect(reportNum(stdout, "ingest_runs.id")).To(Equal(1))
			Expect(reportNum(stdout, "stations seeded")).To(Equal(5))
			Expect(stdout).To(ContainSubstring(
				"stations ingested     : 5 ok / 0 failed / 5 attempted"))
			// 1001 from daily MIN/MAX; 1097 from the normals-period
			// fallback; 1003 (headerless daily), 1004, and 1025 end with
			// no data rows at all.
			Expect(reportNum(stdout, "stations w/ date range")).To(Equal(2))
			// Only 1001 and 1097 have normals extras to score.
			Expect(reportNum(stdout, "stations w/ wmo score")).To(Equal(2))
			Expect(reportNum(stdout, "files opened")).To(Equal(4))
			Expect(reportNum(stdout, "pull-errored")).To(Equal(0))
			Expect(reportNum(stdout, "not in catalog")).To(Equal(21))
			Expect(reportNum(stdout, "daily rows inserted")).To(Equal(wantDaily))
			Expect(reportNum(stdout, "warnings")).To(Equal(wantWarningsTotal))
			Expect(stdout).NotTo(ContainSubstring("failed stations"))
			for _, sourceFile := range pendingWarningFiles {
				Expect(stdout).To(ContainSubstring(
					"[warn] " + sourceFile + `  unexpected pull outcome "pending" in ledger`))
			}
			Expect(stdout).To(MatchRegexp(
				`\[error\] ` + regexp.QuoteMeta(headerlessDailyFile) + `  ` + headerlessIssueRE))
			normalsReported := reportNum(stdout, "normals rows inserted")
			extrasReported := reportNum(stdout, "extras rows inserted")
			Expect(normalsReported).To(BeNumerically(">", 0))
			Expect(extrasReported).To(BeNumerically(">", 0))

			// --- The DB agrees with the report ---------------------------
			db := openIngestDB(dbPath)
			Expect(countRows(db, `SELECT COUNT(*) FROM stations`)).To(Equal(5))
			Expect(countRows(db, `SELECT COUNT(*) FROM daily_observations`)).To(Equal(wantDaily))
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_normals`)).To(Equal(normalsReported))
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_normals_extras`)).To(Equal(extrasReported))

			// Station 1001 round-trips every column: seeded from the catalog
			// (state, municipality, status), enriched from the daily header
			// (name, lat, lon, altitude), first/last derived from its rows.
			var (
				source, name, state, municipality, status string
				lat, lon, altitudeM                       float64
				firstYear, lastYear                       int
			)
			Expect(db.QueryRow(`
SELECT source, name, state, municipality, status,
       lat, lon, altitude_m, first_year, last_year
  FROM stations WHERE external_id = '1001'`).
				Scan(&source, &name, &state, &municipality, &status,
					&lat, &lon, &altitudeM, &firstYear, &lastYear)).To(Succeed())
			Expect(source).To(Equal("conagua_conventional"))
			Expect(name).To(Equal("AGUASCALIENTES (OBS)"))
			Expect(state).To(Equal("AGS"))
			Expect(municipality).To(Equal("Aguascalientes"))
			Expect(status).To(Equal("operating"))
			Expect(lat).To(Equal(21.85027778))
			Expect(lon).To(Equal(-102.2908333))
			Expect(altitudeM).To(Equal(1890.8))
			Expect(firstYear).To(Equal(1878))
			Expect(lastYear).To(Equal(1878))
			// Its only normals period is 1991-2020: those two WMO columns
			// score, the other six stay NULL.
			Expect(countRows(db, `
SELECT COUNT(*) FROM stations
 WHERE external_id = '1001'
   AND wmo_completeness_bin_1991_2020  IS NOT NULL
   AND wmo_completeness_cont_1991_2020 IS NOT NULL
   AND wmo_completeness_bin_1961_1990  IS NULL
   AND wmo_completeness_cont_1961_1990 IS NULL
   AND wmo_completeness_bin_1971_2000  IS NULL
   AND wmo_completeness_cont_1971_2000 IS NULL
   AND wmo_completeness_bin_1981_2010  IS NULL
   AND wmo_completeness_cont_1981_2010 IS NULL`)).To(Equal(1))

			// One daily row round-trips its values, including the faithful
			// NULL for the NULO evaporation cell.
			var (
				tmax, tmin, precip float64
				evap               sql.NullFloat64
			)
			Expect(db.QueryRow(`
SELECT d.tmax, d.tmin, d.precip, d.evap
  FROM daily_observations d JOIN stations s ON s.id = d.station_id
 WHERE s.external_id = '1001' AND d.date = '1878-01-01'`).
				Scan(&tmax, &tmin, &precip, &evap)).To(Succeed())
			Expect(tmax).To(Equal(20.2))
			Expect(tmin).To(Equal(9.8))
			Expect(precip).To(Equal(0.0))
			Expect(evap.Valid).To(BeFalse())

			// The ingest_runs manifest row round-trips every column.
			var (
				startedAt, finishedAt, runSnapshot, sinkKind, runStatus string
				gitSHA                                                  sql.NullString
				attempted, succeeded, failed                            int
				dailyRows, normalsRows, extrasRows, warningsTotal       int
			)
			Expect(db.QueryRow(`
SELECT started_at, finished_at, snapshot_date, sink_kind, etl_git_sha, status,
       stations_attempted, stations_succeeded, stations_failed,
       daily_rows, normals_rows, extras_rows, warnings_total
  FROM ingest_runs WHERE id = 1`).
				Scan(&startedAt, &finishedAt, &runSnapshot, &sinkKind, &gitSHA, &runStatus,
					&attempted, &succeeded, &failed,
					&dailyRows, &normalsRows, &extrasRows, &warningsTotal)).To(Succeed())
			Expect(startedAt).NotTo(BeEmpty())
			Expect(finishedAt).NotTo(BeEmpty())
			Expect(runSnapshot).To(Equal(snapshotDate))
			Expect(sinkKind).To(Equal("local"))
			if binGitSHA != "" {
				Expect(gitSHA.String).To(Equal(binGitSHA))
			} else {
				Expect(gitSHA.Valid).To(BeFalse())
			}
			Expect(runStatus).To(Equal("complete"))
			Expect(attempted).To(Equal(5))
			Expect(succeeded).To(Equal(5))
			Expect(failed).To(Equal(0))
			Expect(dailyRows).To(Equal(wantDaily))
			Expect(normalsRows).To(Equal(normalsReported))
			Expect(extrasRows).To(Equal(extrasReported))
			Expect(warningsTotal).To(Equal(wantWarningsTotal))

			// Every warning landed, shaped exactly: no line anchor, a
			// station attached; the pending hrefs as warn severity and the
			// headerless daily as the one error-severity row.
			rows, err := db.Query(`
SELECT source_file, line IS NULL, severity, issue, station_id IS NOT NULL
  FROM parsing_warnings ORDER BY source_file`)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close() //nolint:errcheck // spec teardown
			gotPending := []string{}
			for rows.Next() {
				var (
					sourceFile, severity, issue string
					noLine, hasStation          bool
				)
				Expect(rows.Scan(&sourceFile, &noLine, &severity, &issue, &hasStation)).To(Succeed())
				Expect(noLine).To(BeTrue(), sourceFile)
				Expect(hasStation).To(BeTrue(), sourceFile)
				if sourceFile == headerlessDailyFile {
					Expect(severity).To(Equal("error"))
					Expect(issue).To(MatchRegexp("^" + headerlessIssueRE + "$"))
					continue
				}
				Expect(severity).To(Equal("warn"), sourceFile)
				Expect(issue).To(Equal(`unexpected pull outcome "pending" in ledger`), sourceFile)
				gotPending = append(gotPending, sourceFile)
			}
			Expect(rows.Err()).NotTo(HaveOccurred())
			Expect(gotPending).To(ConsistOf(pendingWarningFiles))
			Expect(countRows(db, `SELECT COUNT(*) FROM parsing_warnings`)).To(Equal(wantWarningsTotal))
		})

		It("re-runs idempotently: same counts, no accumulation, a second run row", func() {
			root := cloneTree(pristineRoot)
			dbPath := filepath.Join(GinkgoT().TempDir(), "bioclima.db")
			wantDaily := fixtureDailyRows("01001/daily")

			first := runIngestToExit(root, "--db", dbPath, "--snapshot", snapshotDate)
			Expect(first.ExitCode()).To(Equal(0))
			firstOut := string(first.Out.Contents())

			rerun := runIngestToExit(root, "--db", dbPath, "--snapshot", snapshotDate)
			Expect(rerun.ExitCode()).To(Equal(0))
			rerunOut := string(rerun.Out.Contents())
			// The explicit --snapshot path skips the newest-under-root note.
			Expect(string(rerun.Err.Contents())).To(ContainSubstring("snapshot: " + snapshotDate + "\n"))

			// The re-run reports identical work (bar the run id) …
			for _, label := range []string{
				"stations seeded", "files opened", "daily rows inserted",
				"normals rows inserted", "extras rows inserted", "warnings",
			} {
				Expect(reportNum(rerunOut, label)).To(Equal(reportNum(firstOut, label)), label)
			}
			Expect(reportNum(firstOut, "ingest_runs.id")).To(Equal(1))
			Expect(reportNum(rerunOut, "ingest_runs.id")).To(Equal(2))

			// … and the DB converged instead of accumulating: same station,
			// observation, and warning populations; one more run row.
			db := openIngestDB(dbPath)
			Expect(countRows(db, `SELECT COUNT(*) FROM stations`)).To(Equal(5))
			Expect(countRows(db, `SELECT COUNT(*) FROM daily_observations`)).To(Equal(wantDaily))
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_normals`)).
				To(Equal(reportNum(rerunOut, "normals rows inserted")))
			Expect(countRows(db, `SELECT COUNT(*) FROM parsing_warnings`)).
				To(Equal(reportNum(rerunOut, "warnings")))
			Expect(countRows(db, `SELECT COUNT(*) FROM ingest_runs WHERE status = 'complete'`)).To(Equal(2))
		})

		It("seeds and exits under --stations-only, writing no observations and no run row", func() {
			root := cloneTree(pristineRoot)
			dbPath := filepath.Join(GinkgoT().TempDir(), "bioclima.db")

			session := runIngestToExit(root, "--db", dbPath, "--snapshot", snapshotDate, "--stations-only")
			Expect(session.ExitCode()).To(Equal(0))

			stdout := string(session.Out.Contents())
			Expect(reportNum(stdout, "stations seeded")).To(Equal(5))
			Expect(stdout).NotTo(ContainSubstring("stations ingested"))
			Expect(stdout).NotTo(ContainSubstring("ingest_runs.id"))

			// The DB holds exactly the catalog-shaped seed: every column of
			// a never-enriched station reads back, coordinates and years NULL.
			db := openIngestDB(dbPath)
			Expect(countRows(db, `SELECT COUNT(*) FROM stations`)).To(Equal(5))
			Expect(countRows(db, `SELECT COUNT(*) FROM daily_observations`)).To(BeZero())
			Expect(countRows(db, `SELECT COUNT(*) FROM ingest_runs`)).To(BeZero())

			var (
				source, name, state, municipality, status string
				lat, lon, altitudeM                       sql.NullFloat64
				firstYear, lastYear                       sql.NullInt64
			)
			Expect(db.QueryRow(`
SELECT source, name, state, municipality, status,
       lat, lon, altitude_m, first_year, last_year
  FROM stations WHERE external_id = '1003'`).
				Scan(&source, &name, &state, &municipality, &status,
					&lat, &lon, &altitudeM, &firstYear, &lastYear)).To(Succeed())
			Expect(source).To(Equal("conagua_conventional"))
			Expect(name).To(Equal("Calvillo (Smn)"))
			Expect(state).To(Equal("AGS"))
			Expect(municipality).To(Equal("Calvillo"))
			Expect(status).To(Equal("suspended"))
			Expect(lat.Valid).To(BeFalse())
			Expect(lon.Valid).To(BeFalse())
			Expect(altitudeM.Valid).To(BeFalse())
			Expect(firstYear.Valid).To(BeFalse())
			Expect(lastYear.Valid).To(BeFalse())
		})

		It("fails soft per station and exits 2 (degraded) when one station cannot ingest", func() {
			if os.Geteuid() == 0 {
				Skip("permission-based failure injection is inert under root")
			}
			root := cloneTree(pristineRoot)
			dbPath := filepath.Join(GinkgoT().TempDir(), "bioclima.db")
			// An unreadable fetched body: sink.Get fails with a non-not-found
			// error, which is a Go-level station failure, not a parse warning.
			blocked := sinkPath(root, "01001/daily")
			Expect(os.Chmod(blocked, 0o000)).To(Succeed())
			DeferCleanup(func() error { return os.Chmod(blocked, 0o644) })

			session := runIngestToExit(root, "--db", dbPath, "--snapshot", snapshotDate)
			Expect(session.ExitCode()).To(Equal(2))

			stderr := string(session.Err.Contents())
			// The FAIL line is the run's only live failure surface, so it
			// must carry the error itself.
			Expect(stderr).To(MatchRegexp(
				`(?m)^\[\d{2}:\d{2}:\d{2}\] \d/5 FAIL 1001 .* · sink\.Get .*permission denied$`))
			Expect(stderr).To(ContainSubstring(
				"error: ingest degraded: 1 of 5 stations failed (see report)"))

			stdout := string(session.Out.Contents())
			Expect(stdout).To(ContainSubstring(
				"stations ingested     : 4 ok / 1 failed / 5 attempted"))
			Expect(stdout).To(ContainSubstring("failed stations:"))
			Expect(stdout).To(MatchRegexp(
				`(?m)^    1001 \(Aguascalientes \(Obs\)\) — sink\.Get .*permission denied`))

			// The failed station rolled back whole — no daily rows, no
			// enrichment — while the rest of the run committed (1097's
			// normals landed), and the run itself still closed 'complete'
			// (fail-soft, not aborted).
			db := openIngestDB(dbPath)
			Expect(countRows(db, `
SELECT COUNT(*) FROM daily_observations d
  JOIN stations s ON s.id = d.station_id
 WHERE s.external_id = '1001'`)).To(BeZero())
			Expect(countRows(db, `
SELECT COUNT(*) FROM monthly_normals n
  JOIN stations s ON s.id = n.station_id
 WHERE s.external_id = '1097'`)).To(BeNumerically(">", 0))
			var runStatus string
			var failed int
			Expect(db.QueryRow(
				`SELECT status, stations_failed FROM ingest_runs WHERE id = 1`).
				Scan(&runStatus, &failed)).To(Succeed())
			Expect(runStatus).To(Equal("complete"))
			Expect(failed).To(Equal(1))
		})
	})

	Context("against a synthesized snapshot with pull casualties", Ordered, func() {
		var root string

		BeforeAll(func() {
			root = buildSynthRoot()
		})

		It("ingests both stations, surfacing the pull error and the ledger divergence as warn rows", func() {
			dbPath := filepath.Join(GinkgoT().TempDir(), "bioclima.db")

			session := runIngestToExit(root, "--db", dbPath, "--snapshot", synthDate)
			// Both gaps are warn-severity data gaps, not station failures:
			// the run is clean, never degraded.
			Expect(session.ExitCode()).To(Equal(0))

			// One progress line per station, both ok, tallies exact: 01001
			// carries the pull-error warning, 01097 the divergence one.
			type lineTally struct{ status, daily, normals, extras, warns string }
			got := map[string]lineTally{}
			for _, m := range ingestLineREFor(2).FindAllStringSubmatch(string(session.Err.Contents()), -1) {
				got[m[2]] = lineTally{status: m[1], daily: m[3], normals: m[4], extras: m[5], warns: m[6]}
			}
			Expect(got).To(Equal(map[string]lineTally{
				"1001": {status: "ok", daily: "15", normals: "24", extras: "24", warns: "1"},
				"1097": {status: "ok", daily: "0", normals: "12", extras: "12", warns: "1"},
			}))

			stdout := string(session.Out.Contents())
			Expect(reportNum(stdout, "ingest_runs.id")).To(Equal(1))
			Expect(reportNum(stdout, "stations seeded")).To(Equal(2))
			Expect(stdout).To(ContainSubstring(
				"stations ingested     : 2 ok / 0 failed / 2 attempted"))
			Expect(reportNum(stdout, "stations w/ date range")).To(Equal(2))
			Expect(reportNum(stdout, "stations w/ wmo score")).To(Equal(2))
			Expect(reportNum(stdout, "files opened")).To(Equal(4))
			Expect(reportNum(stdout, "pull-errored")).To(Equal(1))
			// 01001's absent 1971-2000 + 01097's missing daily body and
			// three absent normals kinds.
			Expect(reportNum(stdout, "not in catalog")).To(Equal(5))
			Expect(reportNum(stdout, "daily rows inserted")).To(Equal(15))
			Expect(reportNum(stdout, "normals rows inserted")).To(Equal(36))
			Expect(reportNum(stdout, "extras rows inserted")).To(Equal(36))
			Expect(reportNum(stdout, "warnings")).To(Equal(2))
			Expect(stdout).NotTo(ContainSubstring("failed stations"))
			Expect(stdout).To(ContainSubstring(
				"[warn] " + synthPullErroredFile + "  " + synthPullErroredIssue))
			Expect(stdout).To(ContainSubstring(
				"[warn] " + synthDivergedFile + "  " + synthDivergedIssue))

			// --- The DB agrees, value by value ---------------------------
			db := openIngestDB(dbPath)

			// Both station rows round-trip every column: catalog seed
			// (state, municipality, status), header enrichment (name,
			// coordinates — 01097's from its normals header), and the
			// derived date range (01001 from daily MIN/MAX, 01097 from the
			// normals-period fallback).
			type stationRow struct {
				source, name, state, municipality, status string
				lat, lon, altitudeM                       float64
				firstYear, lastYear                       int
			}
			readStation := func(externalID string) stationRow {
				var s stationRow
				Expect(db.QueryRow(`
SELECT source, name, state, municipality, status,
       lat, lon, altitude_m, first_year, last_year
  FROM stations WHERE external_id = ?`, externalID).
					Scan(&s.source, &s.name, &s.state, &s.municipality, &s.status,
						&s.lat, &s.lon, &s.altitudeM, &s.firstYear, &s.lastYear)).To(Succeed())
				return s
			}
			Expect(readStation("1001")).To(Equal(stationRow{
				source: "conagua_conventional", name: "AGUASCALIENTES (OBS)",
				state: "AGS", municipality: "Aguascalientes", status: "operating",
				lat: 21.85027778, lon: -102.2908333, altitudeM: 1890.8,
				firstYear: 1878, lastYear: 1878,
			}))
			Expect(readStation("1097")).To(Equal(stationRow{
				source: "conagua_conventional", name: "AGUASCALIENTES II",
				state: "AGS", municipality: "Aguascalientes", status: "operating",
				lat: 21.90555556, lon: -102.265, altitudeM: 1945.5,
				firstYear: 1991, lastYear: 2020,
			}))

			// Two daily spot rows: real values, and the all-NULO line whose
			// honest gaps must stay NULL.
			var tmax, tmin, precip sql.NullFloat64
			var evap sql.NullFloat64
			readDaily := func(date string) {
				Expect(db.QueryRow(`
SELECT d.tmax, d.tmin, d.precip, d.evap
  FROM daily_observations d JOIN stations s ON s.id = d.station_id
 WHERE s.external_id = '1001' AND d.date = ?`, date).
					Scan(&tmax, &tmin, &precip, &evap)).To(Succeed())
			}
			readDaily("1878-01-01")
			Expect(tmax).To(Equal(sql.NullFloat64{Float64: 20.2, Valid: true}))
			Expect(tmin).To(Equal(sql.NullFloat64{Float64: 9.8, Valid: true}))
			Expect(precip).To(Equal(sql.NullFloat64{Float64: 0, Valid: true}))
			Expect(evap).To(Equal(sql.NullFloat64{}))
			readDaily("1878-01-06")
			Expect(tmax).To(Equal(sql.NullFloat64{}))
			Expect(tmin).To(Equal(sql.NullFloat64{}))
			Expect(precip).To(Equal(sql.NullFloat64{Float64: 0, Valid: true}))
			Expect(evap).To(Equal(sql.NullFloat64{}))

			// January NORMAL rows for all three (station, period) files.
			type normalsRow struct{ tmax, tmin, tmean, precip, evap float64 }
			readNormals := func(externalID, period string) normalsRow {
				var n normalsRow
				Expect(db.QueryRow(`
SELECT n.tmax, n.tmin, n.tmean, n.precip, n.evap
  FROM monthly_normals n JOIN stations s ON s.id = n.station_id
 WHERE s.external_id = ? AND n.period = ? AND n.month = 1`, externalID, period).
					Scan(&n.tmax, &n.tmin, &n.tmean, &n.precip, &n.evap)).To(Succeed())
				return n
			}
			Expect(readNormals("1001", "1991-2020")).To(Equal(
				normalsRow{tmax: 23, tmin: 4.9, tmean: 13.9, precip: 7.1, evap: 98.5}))
			Expect(readNormals("1001", "1981-2010")).To(Equal(
				normalsRow{tmax: 22.7, tmin: 4.4, tmean: 13.5, precip: 5.6, evap: 110.3}))
			Expect(readNormals("1097", "1991-2020")).To(Equal(
				normalsRow{tmax: 21.8, tmin: 2.8, tmean: 12.3, precip: 9.4, evap: 124.9}))

			// 01097's January extras row round-trips every column against
			// the file body (the ingest suite owns 01001's).
			var (
				tmaxME, tmaxDE, tminME, tminDE, precipME, precipDE, rainDays float64
				tmaxYear, tminYear, precipYear                               int
				tmaxDate, tminDate, precipDate                               string
				yTmax, yTmin, yTmean, yPrecip, yEvap, yRain                  int
			)
			Expect(db.QueryRow(`
SELECT e.tmax_monthly_extreme, e.tmax_monthly_extreme_year,
       e.tmax_daily_extreme, e.tmax_daily_extreme_date,
       e.tmin_monthly_extreme, e.tmin_monthly_extreme_year,
       e.tmin_daily_extreme, e.tmin_daily_extreme_date,
       e.precip_monthly_extreme, e.precip_monthly_extreme_year,
       e.precip_daily_extreme, e.precip_daily_extreme_date,
       e.tmax_years_with_data, e.tmin_years_with_data, e.tmean_years_with_data,
       e.precip_years_with_data, e.evap_years_with_data,
       e.rain_days, e.rain_days_years_with_data
  FROM monthly_normals_extras e JOIN stations s ON s.id = e.station_id
 WHERE s.external_id = '1097' AND e.period = '1991-2020' AND e.month = 1`).
				Scan(&tmaxME, &tmaxYear, &tmaxDE, &tmaxDate,
					&tminME, &tminYear, &tminDE, &tminDate,
					&precipME, &precipYear, &precipDE, &precipDate,
					&yTmax, &yTmin, &yTmean, &yPrecip, &yEvap,
					&rainDays, &yRain)).To(Succeed())
			Expect(tmaxME).To(Equal(26.8))
			Expect(tmaxYear).To(Equal(2019))
			Expect(tmaxDE).To(Equal(30.0))
			Expect(tmaxDate).To(Equal("2006-01-12"))
			Expect(tminME).To(Equal(-2.5))
			Expect(tminYear).To(Equal(2010))
			Expect(tminDE).To(Equal(-7.0))
			Expect(tminDate).To(Equal("2010-01-11"))
			Expect(precipME).To(Equal(46.2))
			Expect(precipYear).To(Equal(2010))
			Expect(precipDE).To(Equal(34.0))
			Expect(precipDate).To(Equal("2002-01-12"))
			Expect([]int{yTmax, yTmin, yTmean, yPrecip, yEvap}).To(Equal([]int{25, 25, 25, 25, 25}))
			Expect(rainDays).To(Equal(1.5))
			Expect(yRain).To(Equal(25))

			// WMO: 01097 scores exactly its one period — every cell has
			// ≥ 24 years (bin 1.0); cont is the mean of years/30 over the
			// 36 cells (per variable: 10 months at 25, 2 at 24 → 894/1080).
			var bin91, cont91 sql.NullFloat64
			Expect(db.QueryRow(`
SELECT wmo_completeness_bin_1991_2020, wmo_completeness_cont_1991_2020
  FROM stations WHERE external_id = '1097'`).Scan(&bin91, &cont91)).To(Succeed())
			Expect(bin91.Valid).To(BeTrue())
			Expect(bin91.Float64).To(BeNumerically("~", 1.0, 1e-9))
			Expect(cont91.Valid).To(BeTrue())
			Expect(cont91.Float64).To(BeNumerically("~", 894.0/1080.0, 1e-9))
			Expect(countRows(db, `
SELECT COUNT(*) FROM stations
 WHERE external_id = '1097'
   AND wmo_completeness_bin_1961_1990  IS NULL
   AND wmo_completeness_cont_1961_1990 IS NULL
   AND wmo_completeness_bin_1971_2000  IS NULL
   AND wmo_completeness_cont_1971_2000 IS NULL
   AND wmo_completeness_bin_1981_2010  IS NULL
   AND wmo_completeness_cont_1981_2010 IS NULL`)).To(Equal(1))
			// 01001 scores its two ingested periods; the pull-errored
			// 1961-1990 must NOT have produced a score.
			Expect(countRows(db, `
SELECT COUNT(*) FROM stations
 WHERE external_id = '1001'
   AND wmo_completeness_bin_1981_2010  IS NOT NULL
   AND wmo_completeness_cont_1981_2010 IS NOT NULL
   AND wmo_completeness_bin_1991_2020  IS NOT NULL
   AND wmo_completeness_cont_1991_2020 IS NOT NULL
   AND wmo_completeness_bin_1961_1990  IS NULL
   AND wmo_completeness_cont_1961_1990 IS NULL
   AND wmo_completeness_bin_1971_2000  IS NULL
   AND wmo_completeness_cont_1971_2000 IS NULL`)).To(Equal(1))

			// The run manifest row round-trips every column, its counters
			// matching the printed report.
			var (
				startedAt, finishedAt, runSnapshot, sinkKind, runStatus string
				gitSHA                                                  sql.NullString
				attempted, succeeded, failed                            int
				dailyRows, normalsRows, extrasRows, warningsTotal       int
			)
			Expect(db.QueryRow(`
SELECT started_at, finished_at, snapshot_date, sink_kind, etl_git_sha, status,
       stations_attempted, stations_succeeded, stations_failed,
       daily_rows, normals_rows, extras_rows, warnings_total
  FROM ingest_runs WHERE id = 1`).
				Scan(&startedAt, &finishedAt, &runSnapshot, &sinkKind, &gitSHA, &runStatus,
					&attempted, &succeeded, &failed,
					&dailyRows, &normalsRows, &extrasRows, &warningsTotal)).To(Succeed())
			Expect(startedAt).NotTo(BeEmpty())
			Expect(finishedAt).NotTo(BeEmpty())
			Expect(runSnapshot).To(Equal(synthDate))
			Expect(sinkKind).To(Equal("local"))
			if binGitSHA != "" {
				Expect(gitSHA.String).To(Equal(binGitSHA))
			} else {
				Expect(gitSHA.Valid).To(BeFalse())
			}
			Expect(runStatus).To(Equal("complete"))
			Expect(attempted).To(Equal(2))
			Expect(succeeded).To(Equal(2))
			Expect(failed).To(Equal(0))
			Expect(dailyRows).To(Equal(15))
			Expect(normalsRows).To(Equal(36))
			Expect(extrasRows).To(Equal(36))
			Expect(warningsTotal).To(Equal(2))

			// Exactly the two data-gap warnings, whole rows.
			Expect(readWarningRows(db)).To(Equal(synthWantWarnings))
			Expect(countRows(db, `SELECT COUNT(*) FROM parsing_warnings`)).To(Equal(2))
		})

		It("re-runs idempotently, clearing and rewriting the warn rows instead of accumulating", func() {
			dbPath := filepath.Join(GinkgoT().TempDir(), "bioclima.db")

			first := runIngestToExit(root, "--db", dbPath, "--snapshot", synthDate)
			Expect(first.ExitCode()).To(Equal(0))
			rerun := runIngestToExit(root, "--db", dbPath, "--snapshot", synthDate)
			Expect(rerun.ExitCode()).To(Equal(0))

			firstOut := string(first.Out.Contents())
			rerunOut := string(rerun.Out.Contents())
			for _, label := range []string{
				"stations seeded", "files opened", "pull-errored", "not in catalog",
				"daily rows inserted", "normals rows inserted", "extras rows inserted", "warnings",
			} {
				Expect(reportNum(rerunOut, label)).To(Equal(reportNum(firstOut, label)), label)
			}
			Expect(reportNum(firstOut, "ingest_runs.id")).To(Equal(1))
			Expect(reportNum(rerunOut, "ingest_runs.id")).To(Equal(2))

			db := openIngestDB(dbPath)
			Expect(countRows(db, `SELECT COUNT(*) FROM stations`)).To(Equal(2))
			Expect(countRows(db, `SELECT COUNT(*) FROM daily_observations`)).To(Equal(15))
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_normals`)).To(Equal(36))
			Expect(countRows(db, `SELECT COUNT(*) FROM monthly_normals_extras`)).To(Equal(36))
			// The warn rows converged to the same two, not four.
			Expect(readWarningRows(db)).To(Equal(synthWantWarnings))
			Expect(countRows(db, `SELECT COUNT(*) FROM ingest_runs WHERE status = 'complete'`)).To(Equal(2))
		})

		It("scopes to one station and kind under --external-id + --kind", func() {
			dbPath := filepath.Join(GinkgoT().TempDir(), "bioclima.db")

			// The catalog-form ID must be accepted (normalized to short form).
			session := runIngestToExit(root, "--db", dbPath, "--snapshot", synthDate,
				"--external-id", "01097", "--kind", "normals")
			Expect(session.ExitCode()).To(Equal(0))

			// The one progress line is 01097's, daily untouched by --kind
			// normals — which also skips its diverged daily ledger entry.
			lines := ingestLineREFor(1).FindAllStringSubmatch(string(session.Err.Contents()), -1)
			Expect(lines).To(HaveLen(1))
			Expect(lines[0][1:7]).To(Equal([]string{"ok", "1097", "0", "12", "12", "0"}))

			stdout := string(session.Out.Contents())
			Expect(reportNum(stdout, "ingest_runs.id")).To(Equal(1))
			// The seed is unconditional: the whole manifest seeds even
			// when the per-station loop is scoped to one target.
			Expect(reportNum(stdout, "stations seeded")).To(Equal(2))
			Expect(stdout).To(ContainSubstring(
				"stations ingested     : 1 ok / 0 failed / 1 attempted"))
			Expect(reportNum(stdout, "files opened")).To(Equal(1))
			Expect(reportNum(stdout, "pull-errored")).To(Equal(0))
			Expect(reportNum(stdout, "daily rows inserted")).To(Equal(0))
			Expect(reportNum(stdout, "normals rows inserted")).To(Equal(12))
			Expect(reportNum(stdout, "extras rows inserted")).To(Equal(12))
			Expect(reportNum(stdout, "warnings")).To(Equal(0))

			db := openIngestDB(dbPath)
			// Only 01097's normals landed: no daily rows at all, no rows
			// for 01001 despite its bodies sitting in the same snapshot,
			// and no warnings (01001's pull error is out of scope).
			Expect(countRows(db, `SELECT COUNT(*) FROM daily_observations`)).To(BeZero())
			Expect(countRows(db, `SELECT COUNT(*) FROM parsing_warnings`)).To(BeZero())
			Expect(countRows(db, `
SELECT COUNT(*) FROM monthly_normals n JOIN stations s ON s.id = n.station_id
 WHERE s.external_id <> '1097'`)).To(BeZero())
			Expect(countRows(db, `
SELECT COUNT(*) FROM monthly_normals n JOIN stations s ON s.id = n.station_id
 WHERE s.external_id = '1097' AND n.period = '1991-2020'`)).To(Equal(12))

			// 01097 was enriched from its normals header; 01001 stays a
			// bare catalog seed (never visited).
			var lat97, lat01 sql.NullFloat64
			Expect(db.QueryRow(
				`SELECT lat FROM stations WHERE external_id = '1097'`).Scan(&lat97)).To(Succeed())
			Expect(lat97).To(Equal(sql.NullFloat64{Float64: 21.90555556, Valid: true}))
			Expect(db.QueryRow(
				`SELECT lat FROM stations WHERE external_id = '1001'`).Scan(&lat01)).To(Succeed())
			Expect(lat01).To(Equal(sql.NullFloat64{}))

			var attempted int
			Expect(db.QueryRow(
				`SELECT stations_attempted FROM ingest_runs WHERE id = 1`).Scan(&attempted)).To(Succeed())
			Expect(attempted).To(Equal(1))
		})

		It("errors cleanly on an --external-id absent from the snapshot", func() {
			dbPath := filepath.Join(GinkgoT().TempDir(), "bioclima.db")
			session := runIngestToExit(root, "--db", dbPath, "--snapshot", synthDate,
				"--external-id", "9999")
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(
				`error: external_id "9999" not in snapshot`))
			// No report on a run-level failure before the loop opened.
			Expect(string(session.Out.Contents())).To(BeEmpty())
		})
	})

	Context("failure paths", func() {
		It("errors cleanly when the root holds no snapshots", func() {
			root := filepath.Join(GinkgoT().TempDir(), "nope")
			session := runIngestToExit(root)
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(
				"error: no snapshots under " + filepath.Join(root, "conagua-raw")))
		})

		It("rejects --stations-only combined with --kind", func() {
			session := runIngestToExit(GinkgoT().TempDir(),
				"--snapshot", snapshotDate, "--stations-only", "--kind", "daily")
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(
				"error: ingest: --stations-only and --kind are mutually exclusive"))
		})

		It("rejects an unknown --kind", func() {
			session := runIngestToExit(GinkgoT().TempDir(),
				"--snapshot", snapshotDate, "--kind", "bogus")
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(
				`error: ingest: unknown --kind "bogus" (want daily|normals)`))
		})

		It("rejects an unknown --sink", func() {
			session := runIngestToExit(GinkgoT().TempDir(),
				"--snapshot", snapshotDate, "--sink", "bogus")
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(
				`error: unknown --sink "bogus" (expected 'local' or 'r2')`))
		})
	})
})
