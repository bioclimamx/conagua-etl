package cmd_test

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// The mirror e2e story: with --sink local the source and destination
// coincide, so mirror degenerates into an in-place integrity audit of a
// locally-pulled snapshot. The specs build that snapshot the way an
// operator would — a real pull run against the fixture CONAGUA — then
// run the real binary's mirror verb against per-spec clones of it.

// mirroredKeys is servedFiles' key set in mirror's deterministic
// (state, station, kind) plan order.
var mirroredKeys = []string{
	"01001/daily",
	"01001/normals_1991_2020",
	"01003/daily",
	"01097/normals_1991_2020",
}

// mirrorPlanLine pins the plan summary for the four fetched files across
// their three distinct stations.
const mirrorPlanLine = "planned 4 fetched files across 3 stations"

// mirrorSkipRE matches one verified-skip result line, capturing station,
// kind, and byte count for the per-file round trip.
var mirrorSkipRE = regexp.MustCompile(
	`(?m)^\[\s*\d+/\s*4\] ags/(\d{5}) (\S+)\s+SKIP\s+(\d+) B  \(verified\)$`)

// expectedSkipLine renders the exact SKIP line mirror must emit for
// mirroredKeys[i] under --concurrency 1, byte count included.
func expectedSkipLine(i int, key string) string {
	station, kind, _ := strings.Cut(key, "/")
	body := fixtureBody(servedFiles[key])
	return fmt.Sprintf("[%5d/%5d] ags/%s %-17s SKIP %7d B  (verified)",
		i+1, len(mirroredKeys), station, kind, len(body))
}

// runMirrorToExit launches the binary's mirror verb against root in
// verify mode (--sink local) and waits for exit.
func runMirrorToExit(root string, extra ...string) *gexec.Session {
	args := []string{
		"mirror",
		"--root", root,
		"--snapshot-date", snapshotDate,
		"--sink", "local",
	}
	args = append(args, extra...)
	session, err := gexec.Start(exec.Command(binPath, args...), GinkgoWriter, GinkgoWriter)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	EventuallyWithOffset(1, session, 30*time.Second).Should(gexec.Exit())
	return session
}

// cloneTree copies a snapshot root into a fresh temp dir so each spec
// mutates its own copy; removal is deferred to the spec's exit.
func cloneTree(src string) string {
	dst, err := os.MkdirTemp("", "mirror-e2e-*")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	DeferCleanup(func() error { return os.RemoveAll(dst) })
	err = filepath.WalkDir(src, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, relErr := filepath.Rel(src, path)
		if relErr != nil {
			return relErr
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		return os.WriteFile(target, data, 0o644)
	})
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return dst
}

var _ = Describe("conagua-etl mirror end-to-end", func() {
	Context("in verify mode against a locally pulled snapshot", Ordered, func() {
		var pristineRoot string

		metaPath := func(root, name string) string {
			return filepath.Join(root, "conagua-raw", snapshotDate, name)
		}

		BeforeAll(func() {
			var err error
			pristineRoot, err = os.MkdirTemp("", "mirror-e2e-pristine-*")
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() error { return os.RemoveAll(pristineRoot) })

			// Build the snapshot exactly as an operator would: a real pull
			// against the fixture CONAGUA. The server closes before any
			// mirror spec runs — mirror is an offline verb and must not
			// need it.
			srv := startFixtureServer()
			pull := runPullToExit(pristineRoot, srv)
			srv.Close()
			Expect(pull.ExitCode()).To(Equal(0))
			Expect(snapshotTree(pristineRoot)).To(Equal(completedTree))
		})

		It("verifies every fetched body clean and modifies nothing", func() {
			root := cloneTree(pristineRoot)
			indexBefore, err := os.ReadFile(metaPath(root, "_index.json"))
			Expect(err).NotTo(HaveOccurred())
			progressBefore, err := os.ReadFile(metaPath(root, "_progress.json"))
			Expect(err).NotTo(HaveOccurred())

			// Default --concurrency exercises the real worker pool.
			session := runMirrorToExit(root)
			Expect(session.ExitCode()).To(Equal(0))

			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring("source: local fs  root=" + root))
			Expect(stderr).To(ContainSubstring("metadata: local _index.json"))
			Expect(stderr).To(ContainSubstring(mirrorPlanLine))
			Expect(stderr).To(MatchRegexp(`Done in \S+ — mirrored=0 skipped=4 errors=0`))
			Expect(stderr).NotTo(ContainSubstring("note:"))

			// Round-trip every SKIP line: pool completion order is
			// nondeterministic, so key by station/kind and compare the byte
			// count each line reports against the fixture body it verified.
			stdout := string(session.Out.Contents())
			Expect(strings.Split(strings.TrimRight(stdout, "\n"), "\n")).
				To(HaveLen(len(mirroredKeys)))
			gotBytes := map[string]string{}
			for _, m := range mirrorSkipRE.FindAllStringSubmatch(stdout, -1) {
				gotBytes[m[1]+"/"+m[2]] = m[3]
			}
			wantBytes := map[string]string{}
			for _, key := range mirroredKeys {
				wantBytes[key] = strconv.Itoa(len(fixtureBody(servedFiles[key])))
			}
			Expect(gotBytes).To(Equal(wantBytes))

			// Nothing on disk changed: same tree (so no .tmp-* residue),
			// same bytes, and the metadata is byte-identical — mirror never
			// rewrites it.
			Expect(snapshotTree(root)).To(Equal(completedTree))
			byteEqualFetchedFiles(root)
			indexAfter, err := os.ReadFile(metaPath(root, "_index.json"))
			Expect(err).NotTo(HaveOccurred())
			Expect(indexAfter).To(Equal(indexBefore))
			progressAfter, err := os.ReadFile(metaPath(root, "_progress.json"))
			Expect(err).NotTo(HaveOccurred())
			Expect(progressAfter).To(Equal(progressBefore))
		})

		It("emits a deterministic report at --concurrency 1 and converges on re-run", func() {
			root := cloneTree(pristineRoot)
			var want strings.Builder
			for i, key := range mirroredKeys {
				want.WriteString(expectedSkipLine(i, key) + "\n")
			}

			first := runMirrorToExit(root, "--concurrency", "1")
			Expect(first.ExitCode()).To(Equal(0))
			Expect(string(first.Out.Contents())).To(Equal(want.String()))

			// A re-run reproduces the identical report and state: the verb
			// is idempotent.
			rerun := runMirrorToExit(root, "--concurrency", "1")
			Expect(rerun.ExitCode()).To(Equal(0))
			Expect(string(rerun.Out.Contents())).To(Equal(want.String()))
			Expect(snapshotTree(root)).To(Equal(completedTree))
			byteEqualFetchedFiles(root)
		})

		It("reports a corrupted body, keeps it untouched, and still verifies the rest", func() {
			const key = "01001/daily"
			root := cloneTree(pristineRoot)
			original := fixtureBody(servedFiles[key])
			tampered := []byte("TAMPERED — not what the index recorded\n")
			Expect(os.WriteFile(sinkPath(root, key), tampered, 0o644)).To(Succeed())

			session := runMirrorToExit(root, "--concurrency", "1")
			Expect(session.ExitCode()).To(Equal(1))

			// The ERR line carries the full finding: both digests, both
			// sizes, and the kept-file disposition.
			stdout := string(session.Out.Contents())
			Expect(stdout).To(ContainSubstring("ags/01001 daily"))
			Expect(stdout).To(ContainSubstring("ERR"))
			Expect(stdout).To(ContainSubstring("local sha256=" + sha256Hex(tampered)[:12]))
			Expect(stdout).To(ContainSubstring("index sha256=" + sha256Hex(original)[:12]))
			Expect(stdout).To(ContainSubstring(
				fmt.Sprintf("(%d B local, %d B expected)", len(tampered), len(original))))
			Expect(stdout).To(ContainSubstring("kept, not overwritten"))
			// Fail-soft: the other three files still verified.
			Expect(mirrorSkipRE.FindAllString(stdout, -1)).To(HaveLen(3))

			stderr := string(session.Err.Contents())
			Expect(stderr).To(MatchRegexp(`Done in \S+ — mirrored=0 skipped=3 errors=1`))
			Expect(stderr).To(ContainSubstring(
				"note: 1 local file mismatched the index hash and was kept untouched — inspect, then delete to re-mirror"))
			Expect(stderr).To(ContainSubstring("error: 1 of 4 files failed"))

			// The corrupt file is evidence: byte-identical to what was
			// planted, never "repaired". Everything else is untouched.
			got, err := os.ReadFile(sinkPath(root, key))
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(tampered))
			Expect(snapshotTree(root)).To(Equal(completedTree))
			for _, other := range mirroredKeys {
				if other == key {
					continue
				}
				body, readErr := os.ReadFile(sinkPath(root, other))
				Expect(readErr).NotTo(HaveOccurred())
				Expect(body).To(Equal(fixtureBody(servedFiles[other])), other)
			}
		})

		It("reports a missing body per-file and fails soft", func() {
			const key = "01003/daily"
			root := cloneTree(pristineRoot)
			Expect(os.Remove(sinkPath(root, key))).To(Succeed())

			session := runMirrorToExit(root, "--concurrency", "1")
			Expect(session.ExitCode()).To(Equal(1))

			stdout := string(session.Out.Contents())
			Expect(stdout).To(ContainSubstring("ags/01003 daily"))
			Expect(stdout).To(ContainSubstring("marked fetched in the index but missing from the sink"))
			Expect(mirrorSkipRE.FindAllString(stdout, -1)).To(HaveLen(3))

			stderr := string(session.Err.Contents())
			Expect(stderr).To(MatchRegexp(`Done in \S+ — mirrored=0 skipped=3 errors=1`))
			// A missing file is not a mismatch: no kept-untouched note.
			Expect(stderr).NotTo(ContainSubstring("note:"))
			Expect(stderr).To(ContainSubstring("error: 1 of 4 files failed"))

			// Verify mode has no second copy to restore from: the file
			// stays absent and nothing else moved.
			wantTree := []string{}
			for _, p := range completedTree {
				if p != "conagua-raw/"+snapshotDate+"/daily/01003.txt" {
					wantTree = append(wantTree, p)
				}
			}
			Expect(snapshotTree(root)).To(Equal(wantTree))
		})

		It("falls back to the flushed _progress.json when no index exists", func() {
			root := cloneTree(pristineRoot)
			Expect(os.Remove(metaPath(root, "_index.json"))).To(Succeed())

			session := runMirrorToExit(root, "--concurrency", "1")
			Expect(session.ExitCode()).To(Equal(0))
			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring("metadata: local _progress.json"))
			Expect(stderr).To(MatchRegexp(`mirrored=0 skipped=4 errors=0`))
		})
	})

	Context("edge and error paths", func() {
		It("exits cleanly when the ledger records no fetched files", func() {
			root, err := os.MkdirTemp("", "mirror-e2e-nofetched-*")
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() error { return os.RemoveAll(root) })

			// A ledger whose every entry is bodiless (not_found / pending):
			// there is nothing to materialize or verify.
			p := snapshot.Progress{
				SnapshotDate:  snapshotDate,
				SchemaVersion: snapshot.ProgressSchemaVersion,
				Stations: []snapshot.StationProgress{{
					State: conagua.Aguascalientes,
					ID:    "01004",
					Files: map[conagua.Kind]snapshot.FileState{
						conagua.KindDaily:   {Outcome: snapshot.OutcomeNotFound},
						conagua.KindMonthly: {Outcome: snapshot.OutcomePending},
					},
				}},
			}
			data, err := json.MarshalIndent(p, "", "  ")
			Expect(err).NotTo(HaveOccurred())
			dir := filepath.Join(root, "conagua-raw", snapshotDate)
			Expect(os.MkdirAll(dir, 0o755)).To(Succeed())
			Expect(os.WriteFile(filepath.Join(dir, "_progress.json"), data, 0o644)).To(Succeed())

			session := runMirrorToExit(root)
			Expect(session.ExitCode()).To(Equal(0))
			Expect(string(session.Err.Contents())).To(ContainSubstring(
				"nothing to mirror — the snapshot metadata records no fetched files."))
			Expect(string(session.Out.Contents())).To(BeEmpty())
		})

		It("errors when no metadata exists and the local sink cannot serve an index", func() {
			root := GinkgoT().TempDir()
			session := runMirrorToExit(root)
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(fmt.Sprintf(
				"error: no snapshot metadata for %s under %s, and the selected sink cannot serve an index (--sink r2 can)",
				snapshotDate, root)))
		})

		It("requires --snapshot-date", func() {
			session, err := gexec.Start(exec.Command(binPath,
				"mirror", "--root", GinkgoT().TempDir()),
				GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			Eventually(session, 15*time.Second).Should(gexec.Exit(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(
				`error: required flag(s) "snapshot-date" not set`))
		})

		It("rejects an unknown --sink", func() {
			session, err := gexec.Start(exec.Command(binPath,
				"mirror", "--root", GinkgoT().TempDir(),
				"--snapshot-date", snapshotDate, "--sink", "bogus"),
				GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			Eventually(session, 15*time.Second).Should(gexec.Exit(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(
				`error: unknown --sink "bogus" (expected 'local' or 'r2')`))
		})
	})
})
