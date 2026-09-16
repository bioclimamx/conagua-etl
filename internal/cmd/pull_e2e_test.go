package cmd_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/fetcher"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// The pull e2e story: the real compiled binary runs against a local fixture
// server that serves the real cat_ags.html catalog (5 stations, 24 hrefs)
// plus a handful of real station-file bodies at the catalog-relative paths.
// Everything the catalog links that isn't served 404s naturally — mirroring
// CONAGUA, where most stations lack most kinds — and must land as
// not_found, never as an error.
//
// Plan arithmetic under --state ags --kind daily,monthly,extremes,
// normals_1991_2020: 18 tasks, of which 4 fetch, 14 404, 0 error; the 6
// hrefs of the excluded normals kinds stay pending in the ledger and are
// never requested. files_expected = 24 (every href the catalog advertises).

// snapshotDate pins the snapshot key so on-disk paths are deterministic.
const snapshotDate = "2026-01-15"

// catalogPath is the server-relative catalog page pull walks first.
const catalogPath = "catalogo/cat_ags.html"

// retryPath is scripted to return 500 on its first hit and 200 after,
// proving the transient-retry path end to end.
const retryPath = "Diarios/ags/dia01003.txt"

// servedFiles maps "<stationID>/<kind>" to the server-relative path of a
// real fixture body under testdata/. These four fetch successfully.
var servedFiles = map[string]string{
	"01001/daily":             "Diarios/ags/dia01001.txt",
	"01003/daily":             retryPath,
	"01001/normals_1991_2020": "Normales9120/ags/nor9120_01001.txt",
	"01097/normals_1991_2020": "Normales9120/ags/nor9120_01097.txt",
}

// notFoundFiles are planned hrefs with no body on the fixture server; the
// server 404s them and the ledger must record not_found (terminal).
var notFoundFiles = map[string]string{
	"01004/daily":             "Diarios/ags/dia01004.txt",
	"01025/daily":             "Diarios/ags/dia01025.txt",
	"01097/daily":             "Diarios/ags/dia01097.txt",
	"01001/monthly":           "Mensuales/ags/mes01001.txt",
	"01003/monthly":           "Mensuales/ags/mes01003.txt",
	"01004/monthly":           "Mensuales/ags/mes01004.txt",
	"01025/monthly":           "Mensuales/ags/mes01025.txt",
	"01097/monthly":           "Mensuales/ags/mes01097.txt",
	"01001/extremes":          "Med-Extr/ags/medex01001.txt",
	"01003/extremes":          "Med-Extr/ags/medex01003.txt",
	"01004/extremes":          "Med-Extr/ags/medex01004.txt",
	"01025/extremes":          "Med-Extr/ags/medex01025.txt",
	"01097/extremes":          "Med-Extr/ags/medex01097.txt",
	"01004/normals_1991_2020": "Normales9120/ags/nor9120_01004.txt",
}

// pendingFiles are catalog hrefs whose kinds the --kind filter excludes:
// they enter the ledger as pending and must never be requested.
var pendingFiles = map[string]string{
	"01001/normals_1961_1990": "Normales6190/ags/nor6190_01001.txt",
	"01003/normals_1961_1990": "Normales6190/ags/nor6190_01003.txt",
	"01001/normals_1971_2000": "Normales7100/ags/nor7100_01001.txt",
	"01004/normals_1971_2000": "Normales7100/ags/nor7100_01004.txt",
	"01001/normals_1981_2010": "Normales8110/ags/nor8110_01001.txt",
	"01004/normals_1981_2010": "Normales8110/ags/nor8110_01004.txt",
}

// plannedFiles is the task-plan size: len(servedFiles) + len(notFoundFiles).
const plannedFiles = 18

// expectedCounts is the terminal counts document for a completed pull of
// this fixture set.
var expectedCounts = snapshot.Counts{
	FilesExpected: 24,
	FilesFetched:  4,
	FilesNotFound: 14,
	FilesPending:  6,
}

// expectedStations is the station metadata as it appears in cat_ags.html,
// in the state-then-ID order the index must emit.
var expectedStations = []struct {
	id, name, municipality string
	status                 conagua.Status
}{
	{"01001", "Aguascalientes (Obs)", "Aguascalientes", conagua.StatusOperating},
	{"01003", "Calvillo (Smn)", "Calvillo", conagua.StatusSuspended},
	{"01004", "Cañada Honda", "Aguascalientes", conagua.StatusOperating},
	{"01025", "San Francisco De Los Romo (Smn)", "San Francisco De Los Romo", conagua.StatusSuspended},
	{"01097", "Aguascalientes Ii", "Aguascalientes", conagua.StatusOperating},
}

// Result-line shapes from the pull renderer, pinned to this plan's
// [idx/18] denominator.
var (
	okLineRE = regexp.MustCompile(fmt.Sprintf(
		`(?m)^\[\s*\d+/\s*%d\] ags/(\d{5}) (\S+)\s+OK\s+(\d+) B\s+\S+\s+sha256=([0-9a-f]{16})(?: \(attempts=(\d+)\))?\s*$`,
		plannedFiles))
	notFoundLineRE = regexp.MustCompile(fmt.Sprintf(
		`(?m)^\[\s*\d+/\s*%d\] ags/(\d{5}) (\S+)\s+404\s+\S+(?: \(attempts=\d+\))?\s*$`,
		plannedFiles))
	resultLineRE = regexp.MustCompile(fmt.Sprintf(`(?m)^\[\s*\d+/\s*%d\]`, plannedFiles))
)

// fixtureServer is a local stand-in for CONAGUA's SMN: it serves the files
// under testdata/ at their catalog-relative paths, 404s everything else,
// counts hits per path, and can script a one-shot 500 (transient) or a
// standing 403 (terminal, non-retryable) on selected paths.
type fixtureServer struct {
	*httptest.Server

	mu        sync.Mutex
	hits      map[string]int
	failFirst map[string]bool
	forbid    map[string]bool
}

func startFixtureServer(failFirst ...string) *fixtureServer {
	f := &fixtureServer{
		hits:      make(map[string]int),
		failFirst: make(map[string]bool),
		forbid:    make(map[string]bool),
	}
	for _, p := range failFirst {
		f.failFirst[p] = true
	}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	return f
}

// setForbidden scripts (or clears) a standing 403 for path — the fetcher
// classifies it as terminal, so the file lands in the ledger as error.
func (f *fixtureServer) setForbidden(path string, forbidden bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forbid[path] = forbidden
}

func (f *fixtureServer) serve(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")

	f.mu.Lock()
	f.hits[p]++
	fail := f.failFirst[p] && f.hits[p] == 1
	forbidden := f.forbid[p]
	f.mu.Unlock()

	if fail {
		http.Error(w, "simulated transient failure", http.StatusInternalServerError)
		return
	}
	if forbidden {
		http.Error(w, "simulated terminal failure", http.StatusForbidden)
		return
	}
	body, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(p)))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	_, _ = w.Write(body)
}

// baseURL is the --base-url value: the server root plus the trailing slash
// the flag requires.
func (f *fixtureServer) baseURL() string { return f.URL + "/" }

func (f *fixtureServer) hitCount(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits[path]
}

func (f *fixtureServer) hitSnapshot() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps.Clone(f.hits)
}

// startPull launches the binary with the canonical e2e flag set. The rate
// flags keep the run fast but real: the limiter's politeness floor still
// spaces requests at ≥ 200 ms (1/max-rps).
func startPull(root string, srv *fixtureServer, extra ...string) *gexec.Session {
	args := []string{
		"pull",
		"--root", root,
		"--state", "ags",
		"--kind", "daily,monthly,extremes,normals_1991_2020",
		"--snapshot-date", snapshotDate,
		"--base-url", srv.baseURL(),
		"--catalog-delay", "10ms",
		"--flush-every", "200ms",
		"--rps", "4",
		"--max-rps", "5",
	}
	args = append(args, extra...)
	session, err := gexec.Start(exec.Command(binPath, args...), GinkgoWriter, GinkgoWriter)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return session
}

func runPullToExit(root string, srv *fixtureServer, extra ...string) *gexec.Session {
	session := startPull(root, srv, extra...)
	EventuallyWithOffset(1, session, 30*time.Second).Should(gexec.Exit())
	return session
}

func fixtureBody(relPath string) []byte {
	body, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(relPath)))
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return body
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// sinkPath maps a "<stationID>/<kind>" key to its on-disk snapshot path.
func sinkPath(root, key string) string {
	station, kind, _ := strings.Cut(key, "/")
	return filepath.Join(root, "conagua-raw", snapshotDate, kind, station+".txt")
}

// snapshotTree lists every regular file under root as sorted slash paths
// relative to root — one Equal against it proves the fetched files, the
// absence of anything for 404 hrefs, and the absence of .tmp- residue.
func snapshotTree(root string) []string {
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	sort.Strings(files)
	return files
}

// completedTree is the exact on-disk shape of a completed pull of this
// fixture set.
var completedTree = []string{
	"conagua-raw/" + snapshotDate + "/_index.json",
	"conagua-raw/" + snapshotDate + "/_progress.json",
	"conagua-raw/" + snapshotDate + "/daily/01001.txt",
	"conagua-raw/" + snapshotDate + "/daily/01003.txt",
	"conagua-raw/" + snapshotDate + "/normals_1991_2020/01001.txt",
	"conagua-raw/" + snapshotDate + "/normals_1991_2020/01097.txt",
}

// loadProgressDoc decodes a _progress.json / _index.json both as the typed
// Progress (field round-trips) and as a raw key map (key-absence checks,
// which a typed decode can't express).
func loadProgressDoc(path string) (snapshot.Progress, map[string]json.RawMessage) {
	raw, err := os.ReadFile(path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	var p snapshot.Progress
	ExpectWithOffset(1, json.Unmarshal(raw, &p)).To(Succeed())
	var keys map[string]json.RawMessage
	ExpectWithOffset(1, json.Unmarshal(raw, &keys)).To(Succeed())
	return p, keys
}

// mustFileState returns the ledger entry for "<stationID>/<kind>".
func mustFileState(p snapshot.Progress, key string) snapshot.FileState {
	station, kind, _ := strings.Cut(key, "/")
	for _, st := range p.Stations {
		if st.ID == station {
			fst, ok := st.Files[conagua.Kind(kind)]
			ExpectWithOffset(1, ok).To(BeTrue(), "no ledger entry for %s", key)
			return fst
		}
	}
	Fail(fmt.Sprintf("no ledger station for %s", key), 1)
	return snapshot.FileState{}
}

// byteEqualFetchedFiles compares every fetched file on disk byte-for-byte
// against the fixture body the server served.
func byteEqualFetchedFiles(root string) {
	for key, rel := range servedFiles {
		got, err := os.ReadFile(sinkPath(root, key))
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		ExpectWithOffset(1, got).To(Equal(fixtureBody(rel)), "byte mismatch for %s", key)
	}
}

var _ = Describe("conagua-etl pull end-to-end", func() {
	Context("against a fixture CONAGUA", Ordered, func() {
		var (
			root         string
			srv          *fixtureServer
			progressPath string
			indexPath    string
			firstRun     *gexec.Session
		)

		BeforeAll(func() {
			var err error
			root, err = os.MkdirTemp("", "pull-e2e-*")
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() error { return os.RemoveAll(root) })

			srv = startFixtureServer(retryPath)
			DeferCleanup(srv.Close)

			progressPath = filepath.Join(root, "conagua-raw", snapshotDate, "_progress.json")
			indexPath = filepath.Join(root, "conagua-raw", snapshotDate, "_index.json")
		})

		It("fetches the plan, retries the transient 500, and 404s cleanly", func() {
			firstRun = runPullToExit(root, srv)
			Expect(firstRun.ExitCode()).To(Equal(0))

			stdout := string(firstRun.Out.Contents())
			stderr := string(firstRun.Err.Contents())

			Expect(stderr).To(ContainSubstring(
				"planned 18 files across 5 stations (0 already terminal in ledger)"))

			// Exactly one result line per planned file, and nothing else on
			// stdout; no ERR or SKIP dispositions.
			Expect(resultLineRE.FindAllString(stdout, -1)).To(HaveLen(plannedFiles))
			lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
			Expect(lines).To(HaveLen(plannedFiles))
			Expect(stdout).NotTo(ContainSubstring(" ERR "))
			Expect(stdout).NotTo(ContainSubstring("SKIP"))

			// Round-trip every OK line: station/kind, byte count, sha256
			// prefix, and the retry tag all match the fixture bodies.
			type okLine struct{ bytes, sha16, attempts string }
			gotOK := map[string]okLine{}
			for _, m := range okLineRE.FindAllStringSubmatch(stdout, -1) {
				gotOK[m[1]+"/"+m[2]] = okLine{bytes: m[3], sha16: m[4], attempts: m[5]}
			}
			wantOK := map[string]okLine{}
			for key, rel := range servedFiles {
				body := fixtureBody(rel)
				attempts := ""
				if rel == retryPath {
					attempts = "2"
				}
				wantOK[key] = okLine{
					bytes:    strconv.Itoa(len(body)),
					sha16:    sha256Hex(body)[:16],
					attempts: attempts,
				}
			}
			Expect(gotOK).To(Equal(wantOK))

			// Every unserved href surfaces as a 404 line, not an error.
			got404 := []string{}
			for _, m := range notFoundLineRE.FindAllStringSubmatch(stdout, -1) {
				got404 = append(got404, m[1]+"/"+m[2])
			}
			want404 := []string{}
			for key := range notFoundFiles {
				want404 = append(want404, key)
			}
			Expect(got404).To(ConsistOf(want404))

			Expect(stderr).To(MatchRegexp(
				`Done in \S+ — fetched=4 skipped=0 not-found=14 errors=0`))
			// A clean completion carries no --retry-failed note.
			Expect(stderr).NotTo(ContainSubstring("errored file"))

			// The seed is generated silently: no output line mentions it.
			Expect(stdout + stderr).NotTo(MatchRegexp(`(?i)seed`))
		})

		It("stores exactly the fetched bodies, atomically and byte-equal", func() {
			Expect(snapshotTree(root)).To(Equal(completedTree))
			byteEqualFetchedFiles(root)
		})

		It("records a faithful _index.json and _progress.json", func() {
			idx, idxKeys := loadProgressDoc(indexPath)

			Expect(idx.SchemaVersion).To(Equal(1))
			Expect(idx.SnapshotDate).To(Equal(snapshotDate))
			Expect(idx.StartedAt.IsZero()).To(BeFalse())
			Expect(idx.CompletedAt.IsZero()).To(BeFalse())
			// The index is terminal: the live-run field is stripped.
			Expect(idxKeys).NotTo(HaveKey("last_flush"))

			Expect(idx.Catalog).To(Equal(snapshot.CatalogSummary{
				StatesDiscovered:   1,
				StationsDiscovered: 5,
			}))
			Expect(idx.Counts).To(Equal(expectedCounts))

			// The pacing audit trail: nonzero seed, and the exact rate/retry
			// configuration the flags selected, round-tripped through JSON.
			Expect(idx.RNGSeed).NotTo(BeZero())
			var rc fetcher.RateConfig
			Expect(json.Unmarshal(idx.RateConfig, &rc)).To(Succeed())
			wantRC := fetcher.DefaultRateConfig()
			wantRC.TargetRPS = 4
			wantRC.MaxRPS = 5
			Expect(rc).To(Equal(wantRC))
			var rp fetcher.RetryPolicy
			Expect(json.Unmarshal(idx.RetryPolicy, &rp)).To(Succeed())
			Expect(rp).To(Equal(fetcher.DefaultRetryPolicy()))

			// Station identity round-trips from the catalog HTML, in
			// state-then-ID order.
			Expect(idx.Stations).To(HaveLen(len(expectedStations)))
			for i, want := range expectedStations {
				st := idx.Stations[i]
				Expect(st.State).To(Equal(conagua.Aguascalientes))
				Expect(st.ID).To(Equal(want.id))
				Expect(st.Name).To(Equal(want.name))
				Expect(st.Municipality).To(Equal(want.municipality))
				Expect(st.Status).To(Equal(want.status))
			}

			// Per-file round trips: outcome, URL, HTTP code, bytes, sha256,
			// attempts, and error text for all 24 hrefs.
			for key, rel := range servedFiles {
				body := fixtureBody(rel)
				fst := mustFileState(idx, key)
				Expect(fst.Outcome).To(Equal(snapshot.OutcomeFetched), key)
				Expect(fst.URL).To(Equal(srv.baseURL() + rel))
				Expect(fst.HTTPCode).To(Equal(http.StatusOK))
				Expect(fst.Bytes).To(Equal(int64(len(body))))
				Expect(fst.SHA256).To(Equal(sha256Hex(body)))
				wantAttempts := 1
				if rel == retryPath {
					wantAttempts = 2 // the scripted 500 cost one extra attempt
				}
				Expect(fst.Attempts).To(Equal(wantAttempts), key)
				Expect(fst.LastError).To(BeEmpty())
				Expect(fst.UpdatedAt.IsZero()).To(BeFalse())
			}
			for key, rel := range notFoundFiles {
				fst := mustFileState(idx, key)
				Expect(fst.Outcome).To(Equal(snapshot.OutcomeNotFound), key)
				Expect(fst.URL).To(Equal(srv.baseURL() + rel))
				Expect(fst.HTTPCode).To(Equal(http.StatusNotFound))
				Expect(fst.Bytes).To(BeZero())
				Expect(fst.SHA256).To(BeEmpty())
				Expect(fst.Attempts).To(Equal(1), key)
				Expect(fst.LastError).To(BeEmpty())
			}
			for key, rel := range pendingFiles {
				fst := mustFileState(idx, key)
				Expect(fst.Outcome).To(Equal(snapshot.OutcomePending), key)
				Expect(fst.URL).To(Equal(srv.baseURL() + rel))
				Expect(fst.HTTPCode).To(BeZero())
				Expect(fst.Attempts).To(BeZero())
				// Excluded kinds are never requested.
				Expect(srv.hitCount(rel)).To(BeZero(), rel)
			}

			// Wire-level fixture arithmetic: one hit per resolved file, two
			// for the 500-then-200 route, one catalog walk.
			Expect(srv.hitCount(catalogPath)).To(Equal(1))
			Expect(srv.hitCount(retryPath)).To(Equal(2))
			for _, rel := range servedFiles {
				if rel != retryPath {
					Expect(srv.hitCount(rel)).To(Equal(1), rel)
				}
			}
			for _, rel := range notFoundFiles {
				Expect(srv.hitCount(rel)).To(Equal(1), rel)
			}

			// etl_git_sha: assert against what this build actually embeds.
			if binGitSHA != "" {
				Expect(idx.ETLGitSHA).To(Equal(binGitSHA))
			} else {
				// omitempty: an empty SHA must simply be absent.
				Expect(idxKeys).NotTo(HaveKey("etl_git_sha"))
			}

			// The live ledger agrees with the index and keeps last_flush.
			prog, progKeys := loadProgressDoc(progressPath)
			Expect(progKeys).To(HaveKey("last_flush"))
			Expect(prog.SchemaVersion).To(Equal(1))
			Expect(prog.RNGSeed).To(Equal(idx.RNGSeed))
			Expect(prog.Counts).To(Equal(expectedCounts))

			// The persisted seed never appeared in the run's output. (Guarded
			// on length: a short decimal could collide with byte counts or
			// sha hex by chance; ≥ 16 digits cannot, and RandomSeed yields
			// < 16 digits with probability ~1e-4.)
			seed := strconv.FormatUint(idx.RNGSeed, 10)
			if len(seed) >= 16 {
				out := string(firstRun.Out.Contents()) + string(firstRun.Err.Contents())
				Expect(out).NotTo(ContainSubstring(seed))
			}
		})

		It("re-runs idempotently without re-requesting resolved files", func() {
			hitsBefore := srv.hitSnapshot()

			rerun := runPullToExit(root, srv)
			Expect(rerun.ExitCode()).To(Equal(0))
			Expect(string(rerun.Out.Contents())).To(BeEmpty())
			Expect(string(rerun.Err.Contents())).To(ContainSubstring(
				"nothing to fetch — snapshot already complete per ledger."))
			// The empty-plan path shares the completion finalizer, so it
			// re-emits the index and reports the path.
			Expect(string(rerun.Err.Contents())).To(ContainSubstring("wrote " + indexPath))
			Expect(string(rerun.Err.Contents())).NotTo(ContainSubstring("errored file"))

			// The catalog is re-walked (discovery is never cached) but every
			// resolved file — fetched, 404, and the once-500 route — is
			// terminal in the ledger and must not be requested again.
			hitsAfter := srv.hitSnapshot()
			Expect(hitsAfter[catalogPath]).To(Equal(hitsBefore[catalogPath] + 1))
			delete(hitsBefore, catalogPath)
			delete(hitsAfter, catalogPath)
			Expect(hitsAfter).To(Equal(hitsBefore))

			// The snapshot converged: same tree, same bytes, same counts.
			Expect(snapshotTree(root)).To(Equal(completedTree))
			byteEqualFetchedFiles(root)
			idx, _ := loadProgressDoc(indexPath)
			Expect(idx.Counts).To(Equal(expectedCounts))
			Expect(idx.RNGSeed).NotTo(BeZero())
		})
	})

	// The interrupt story is deterministic: the task in flight at SIGINT is
	// a cancellation casualty, not an error — it reaches no verdict, stays
	// pending in the ledger, and a plain re-run re-attempts it. The only
	// wire-level nondeterminism it leaves is whether its aborted attempt
	// reached the server, so the resume assertions pin final bytes/sha256
	// and total completion, not a mid-flight hit count.
	Context("when interrupted mid-run", Ordered, func() {
		var (
			root         string
			srv          *fixtureServer
			progressPath string
			indexPath    string

			firstOKKey string
			// terminalRels are the server paths already terminal (fetched or
			// not_found) in the interrupted ledger — resume must not touch them.
			terminalRels       []string
			hitsAfterInterrupt map[string]int
		)

		BeforeAll(func() {
			var err error
			root, err = os.MkdirTemp("", "pull-e2e-interrupt-*")
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() error { return os.RemoveAll(root) })

			// No scripted 500 here: the retry path is proven in the first
			// context, and a clean server keeps the resume deterministic.
			srv = startFixtureServer()
			DeferCleanup(srv.Close)

			progressPath = filepath.Join(root, "conagua-raw", snapshotDate, "_progress.json")
			indexPath = filepath.Join(root, "conagua-raw", snapshotDate, "_index.json")
		})

		It("flushes the ledger and prints the resume hint on SIGINT", func() {
			session := startPull(root, srv)
			// Wait for the first OK line — its result is recorded in the
			// ledger synchronously before the next task starts.
			Eventually(func() string {
				return string(session.Out.Contents())
			}, 15*time.Second, 10*time.Millisecond).Should(MatchRegexp(okLineRE.String()))
			session.Interrupt()
			Eventually(session, 15*time.Second).Should(gexec.Exit(1))

			stdout := string(session.Out.Contents())
			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring(
				"interrupted — re-run with the same parameters to resume"))
			Expect(stderr).To(ContainSubstring("error: context canceled"))
			// Cancellation produces no error verdicts, so the errored-files
			// note must not appear — the resume hint alone is the full recipe.
			Expect(stderr).NotTo(ContainSubstring("errored file"))

			// Interrupted, so no terminal index — only the flushed ledger.
			Expect(indexPath).NotTo(BeAnExistingFile())
			prog, _ := loadProgressDoc(progressPath)
			Expect(prog.SchemaVersion).To(Equal(1))
			Expect(prog.CompletedAt.IsZero()).To(BeTrue())
			Expect(prog.Counts.FilesPending).To(BeNumerically(">", 0))
			Expect(prog.Counts.FilesErrored).To(BeZero())

			// Every OK line the run printed is durable in the flushed ledger,
			// with the fixture's exact bytes and sha256.
			oks := okLineRE.FindAllStringSubmatch(stdout, -1)
			Expect(oks).NotTo(BeEmpty())
			for _, m := range oks {
				key := m[1] + "/" + m[2]
				rel, ok := servedFiles[key]
				Expect(ok).To(BeTrue(), "unexpected OK for %s", key)
				body := fixtureBody(rel)
				fst := mustFileState(prog, key)
				Expect(fst.Outcome).To(Equal(snapshot.OutcomeFetched), key)
				Expect(fst.Bytes).To(Equal(int64(len(body))))
				Expect(fst.SHA256).To(Equal(sha256Hex(body)))
			}
			firstOKKey = oks[0][1] + "/" + oks[0][2]

			// The in-flight casualty stays pending, never error: every ledger
			// entry is fetched, not_found, or pending. Record the terminal
			// paths for the resume spec's do-not-touch assertions.
			terminalRels = nil
			for _, st := range prog.Stations {
				for kind, fst := range st.Files {
					key := st.ID + "/" + string(kind)
					switch fst.Outcome {
					case snapshot.OutcomeFetched:
						terminalRels = append(terminalRels, servedFiles[key])
					case snapshot.OutcomeNotFound:
						terminalRels = append(terminalRels, notFoundFiles[key])
					case snapshot.OutcomePending:
						// The casualty and the not-yet-dispatched tail.
					default:
						Fail(fmt.Sprintf("unexpected outcome %q for %s", fst.Outcome, key))
					}
				}
			}
			hitsAfterInterrupt = srv.hitSnapshot()
		})

		It("resumes to a complete snapshot with a plain re-run", func() {
			resume := runPullToExit(root, srv)
			Expect(resume.ExitCode()).To(Equal(0))

			stderr := string(resume.Err.Contents())
			// The plan shrank by exactly the files the interrupted run
			// resolved; the pending casualty is back in the plan.
			m := regexp.MustCompile(`planned (\d+) files across 5 stations \((\d+) already terminal in ledger\)`).
				FindStringSubmatch(stderr)
			Expect(m).NotTo(BeNil())
			planned, _ := strconv.Atoi(m[1])
			already, _ := strconv.Atoi(m[2])
			Expect(planned + already).To(Equal(plannedFiles))
			Expect(already).To(BeNumerically(">", 0))
			Expect(stderr).To(MatchRegexp(`fetched=\d+ skipped=0 not-found=\d+ errors=0`))
			Expect(stderr).NotTo(ContainSubstring("errored file"))

			// The final snapshot is complete and identical to an
			// uninterrupted pull: all 18 planned files resolved.
			Expect(snapshotTree(root)).To(Equal(completedTree))
			byteEqualFetchedFiles(root)
			idx, _ := loadProgressDoc(indexPath)
			Expect(idx.CompletedAt.IsZero()).To(BeFalse())
			Expect(idx.Counts).To(Equal(expectedCounts))
			for key := range servedFiles {
				Expect(mustFileState(idx, key).Outcome).To(Equal(snapshot.OutcomeFetched), key)
			}
			for key := range notFoundFiles {
				Expect(mustFileState(idx, key).Outcome).To(Equal(snapshot.OutcomeNotFound), key)
			}

			// Nothing terminal in the interrupted ledger was re-requested —
			// in particular the first fetched file was hit exactly once
			// across both runs. The catalog walk is the only repeat.
			for _, rel := range terminalRels {
				Expect(srv.hitCount(rel)).To(Equal(hitsAfterInterrupt[rel]), "re-requested %s", rel)
			}
			Expect(srv.hitCount(servedFiles[firstOKKey])).To(Equal(1))
			Expect(srv.hitCount(catalogPath)).To(Equal(2))

			// Every file pending at the interrupt gained exactly the one
			// resumed request on top of whatever its aborted in-flight
			// attempt already cost — for the casualty that may total two
			// hits; its bytes/sha256 and terminal outcome are pinned above.
			terminal := make(map[string]bool, len(terminalRels))
			for _, rel := range terminalRels {
				terminal[rel] = true
			}
			for _, files := range []map[string]string{servedFiles, notFoundFiles} {
				for key, rel := range files {
					if terminal[rel] {
						continue
					}
					Expect(srv.hitCount(rel)).To(Equal(hitsAfterInterrupt[rel]+1),
						"resumed request count for %s (%s)", key, rel)
				}
			}
		})
	})

	// A permanently-403 path exercises the fail-soft error route: the run
	// still completes (one bad file never aborts the rest), but every
	// summary must carry the --retry-failed note until the file resolves.
	Context("when a file errors terminally", Ordered, func() {
		const (
			erroredKey  = "01001/daily"
			erroredRel  = "Diarios/ags/dia01001.txt"
			erroredNote = "note: 1 errored file in the ledger — re-run with --retry-failed to re-attempt them"
		)

		var (
			root         string
			srv          *fixtureServer
			progressPath string
			indexPath    string
		)

		// runDailyPull scopes the plan to --kind daily: 5 tasks, of which
		// 01003 fetches, three 404, and 01001 hits the scripted 403.
		runDailyPull := func(extra ...string) *gexec.Session {
			args := []string{
				"pull",
				"--root", root,
				"--state", "ags",
				"--kind", "daily",
				"--snapshot-date", snapshotDate,
				"--base-url", srv.baseURL(),
				"--catalog-delay", "10ms",
				"--flush-every", "200ms",
				"--rps", "4",
				"--max-rps", "5",
			}
			args = append(args, extra...)
			session, err := gexec.Start(exec.Command(binPath, args...), GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			Eventually(session, 30*time.Second).Should(gexec.Exit())
			return session
		}

		BeforeAll(func() {
			var err error
			root, err = os.MkdirTemp("", "pull-e2e-error-*")
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() error { return os.RemoveAll(root) })

			srv = startFixtureServer()
			srv.setForbidden(erroredRel, true)
			DeferCleanup(srv.Close)

			progressPath = filepath.Join(root, "conagua-raw", snapshotDate, "_progress.json")
			indexPath = filepath.Join(root, "conagua-raw", snapshotDate, "_index.json")
		})

		It("completes fail-soft and prints the --retry-failed note", func() {
			session := runDailyPull()
			Expect(session.ExitCode()).To(Equal(0))

			stdout := string(session.Out.Contents())
			stderr := string(session.Err.Contents())
			Expect(stdout).To(ContainSubstring("unexpected status 403"))
			Expect(stderr).To(MatchRegexp(
				`Done in \S+ — fetched=1 skipped=0 not-found=3 errors=1`))
			Expect(stderr).To(ContainSubstring(erroredNote))

			// The error is terminal on the first attempt (4xx is never
			// retried) and lands in the ledger with its cause.
			prog, _ := loadProgressDoc(progressPath)
			fst := mustFileState(prog, erroredKey)
			Expect(fst.Outcome).To(Equal(snapshot.OutcomeError))
			Expect(fst.HTTPCode).To(Equal(http.StatusForbidden))
			Expect(fst.Attempts).To(Equal(1))
			Expect(fst.LastError).To(ContainSubstring("unexpected status 403"))

			// By design: the run still counts as completed, so the
			// terminal index is emitted despite the errored file.
			idx, _ := loadProgressDoc(indexPath)
			Expect(idx.CompletedAt.IsZero()).To(BeFalse())
			Expect(idx.Counts.FilesErrored).To(Equal(1))
		})

		It("keeps the note on an empty-plan re-run without re-requesting the errored file", func() {
			hitsBefore := srv.hitCount(erroredRel)

			rerun := runDailyPull()
			Expect(rerun.ExitCode()).To(Equal(0))

			stderr := string(rerun.Err.Contents())
			Expect(stderr).To(ContainSubstring(
				"nothing to fetch — snapshot already complete per ledger."))
			Expect(stderr).To(ContainSubstring(erroredNote))
			// Plain resumption treats error as terminal: no new attempt.
			Expect(srv.hitCount(erroredRel)).To(Equal(hitsBefore))
		})

		It("re-attempts under --retry-failed and drops the note once resolved", func() {
			srv.setForbidden(erroredRel, false)

			retry := runDailyPull("--retry-failed")
			Expect(retry.ExitCode()).To(Equal(0))

			stderr := string(retry.Err.Contents())
			Expect(stderr).To(ContainSubstring(
				"planned 1 files across 5 stations (4 already terminal in ledger)"))
			Expect(stderr).To(MatchRegexp(
				`Done in \S+ — fetched=1 skipped=0 not-found=0 errors=0`))
			Expect(stderr).NotTo(ContainSubstring("errored file"))

			// The recovered file round-trips: ledger entry converges to
			// fetched with the error cleared, and the bytes hit the sink.
			body := fixtureBody(erroredRel)
			idx, _ := loadProgressDoc(indexPath)
			fst := mustFileState(idx, erroredKey)
			Expect(fst.Outcome).To(Equal(snapshot.OutcomeFetched))
			Expect(fst.HTTPCode).To(Equal(http.StatusOK))
			Expect(fst.Bytes).To(Equal(int64(len(body))))
			Expect(fst.SHA256).To(Equal(sha256Hex(body)))
			Expect(fst.Attempts).To(Equal(1))
			Expect(fst.LastError).To(BeEmpty())
			got, err := os.ReadFile(sinkPath(root, erroredKey))
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(body))
		})
	})

	// --max-stations caps the pull after catalog discovery: the index keeps
	// the pre-cap discovery count while the station list and task plan
	// shrink to the first N stations in catalog order. Under the canonical
	// kind subset, capping to {01001, 01003} plans 7 files (01001's four
	// selected hrefs + 01003's three), of which 3 fetch and 4 404; the
	// capped stations' 11 catalog hrefs stay in the ledger, the excluded
	// normals kinds as pending.
	Context("with --max-stations", func() {
		It("caps the station list and plan but records the pre-cap discovery count", func() {
			root, err := os.MkdirTemp("", "pull-e2e-cap-*")
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() error { return os.RemoveAll(root) })

			srv := startFixtureServer()
			DeferCleanup(srv.Close)

			session := runPullToExit(root, srv, "--max-stations", "2")
			Expect(session.ExitCode()).To(Equal(0))
			Expect(string(session.Err.Contents())).To(ContainSubstring(
				"planned 7 files across 2 stations (0 already terminal in ledger)"))

			// StationsDiscovered is the pre-cap catalog total; the station
			// list holds only the first two catalog stations, metadata intact.
			idx, _ := loadProgressDoc(
				filepath.Join(root, "conagua-raw", snapshotDate, "_index.json"))
			Expect(idx.Catalog).To(Equal(snapshot.CatalogSummary{
				StatesDiscovered:   1,
				StationsDiscovered: 5,
			}))
			Expect(idx.Stations).To(HaveLen(2))
			for i, want := range expectedStations[:2] {
				st := idx.Stations[i]
				Expect(st.State).To(Equal(conagua.Aguascalientes))
				Expect(st.ID).To(Equal(want.id))
				Expect(st.Name).To(Equal(want.name))
				Expect(st.Municipality).To(Equal(want.municipality))
				Expect(st.Status).To(Equal(want.status))
			}
			Expect(idx.Counts).To(Equal(snapshot.Counts{
				FilesExpected: 11,
				FilesFetched:  3,
				FilesNotFound: 4,
				FilesPending:  4,
			}))

			// Wire level: one catalog walk plus exactly the 7 planned hrefs —
			// nothing at all for the three capped-out stations.
			wantHits := map[string]int{catalogPath: 1}
			for _, key := range []string{
				"01001/daily", "01001/monthly", "01001/extremes", "01001/normals_1991_2020",
				"01003/daily", "01003/monthly", "01003/extremes",
			} {
				rel, ok := servedFiles[key]
				if !ok {
					rel = notFoundFiles[key]
				}
				wantHits[rel] = 1
			}
			Expect(srv.hitSnapshot()).To(Equal(wantHits))

			// On disk: only the capped stations' fetched bodies plus the
			// snapshot metadata.
			Expect(snapshotTree(root)).To(Equal([]string{
				"conagua-raw/" + snapshotDate + "/_index.json",
				"conagua-raw/" + snapshotDate + "/_progress.json",
				"conagua-raw/" + snapshotDate + "/daily/01001.txt",
				"conagua-raw/" + snapshotDate + "/daily/01003.txt",
				"conagua-raw/" + snapshotDate + "/normals_1991_2020/01001.txt",
			}))
		})
	})

	Context("flag validation", func() {
		It("rejects a non-positive --flush-every before touching the network", func() {
			session, err := gexec.Start(exec.Command(binPath,
				"pull", "--flush-every", "0s", "--base-url", "http://127.0.0.1:9/"),
				GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			Eventually(session, 15*time.Second).Should(gexec.Exit(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(
				"error: --flush-every must be positive"))
		})

		// The sink is validated before catalog discovery, so an operator
		// typo costs zero polite catalog requests: with a dead --base-url,
		// only failing before the walk can produce the sink error line.
		It("rejects an unknown --sink before touching the network", func() {
			session, err := gexec.Start(exec.Command(binPath,
				"pull", "--sink", "bogus", "--base-url", "http://127.0.0.1:9/"),
				GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			Eventually(session, 15*time.Second).Should(gexec.Exit(1))
			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring(
				`error: unknown --sink "bogus" (expected 'local' or 'r2')`))
			Expect(stderr).NotTo(ContainSubstring("[catalog"))
		})

		It("fails fast on --sink r2 when the R2 credentials are absent", func() {
			cmd := exec.Command(binPath,
				"pull", "--sink", "r2", "--base-url", "http://127.0.0.1:9/")
			// Hermetic: strip any ambient R2_* and run in an empty directory
			// so no ./.env can supply the credentials either.
			cmd.Dir = GinkgoT().TempDir()
			for _, kv := range os.Environ() {
				if !strings.HasPrefix(kv, "R2_") {
					cmd.Env = append(cmd.Env, kv)
				}
			}
			session, err := gexec.Start(cmd, GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			Eventually(session, 15*time.Second).Should(gexec.Exit(1))
			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring(
				"error: --sink r2: R2 env vars missing: " +
					"R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY " +
					"(hint: set them in the environment or in ./.env)"))
			Expect(stderr).NotTo(ContainSubstring("[catalog"))
		})
	})
})
