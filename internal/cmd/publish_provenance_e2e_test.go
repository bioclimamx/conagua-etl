package cmd_test

// The build-provenance guard on the real binary:
// publish mints the citable deposit, so it refuses to run at all when
// the binary was built from a modified working tree — manifest.json and
// every profile's meta would stamp a commit whose code is not the code
// that ran. The refusal lands before the database is opened and before
// --out is created, so a refused run leaves the filesystem untouched. A
// build carrying no VCS metadata is a different case: an empty
// etl_git_sha is an honest gap, so the run proceeds with the gap
// announced on stderr.
//
// The suite's own binary is built -buildvcs=false (see cmd_suite_test.go),
// so these specs also build a VCS-stamped one and assert against the
// provenance that binary actually carries: in any working tree with
// uncommitted changes — the normal state while `make check` runs — that
// is the refusal path.

import (
	"debug/buildinfo"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

const wantNoVCSWarning = "warning: this binary carries no VCS metadata; " +
	"the deposit will record an empty etl_git_sha and cannot be traced back to a commit\n"

// binaryVCS reads back the VCS provenance the toolchain stamped into the
// binary at path — the same pair the guard reads from its own build info
// at run time — so a spec asserts against what this build really claims
// instead of assuming the tree's state.
func binaryVCS(path string) (revision string, modified bool) {
	info, err := buildinfo.ReadFile(path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
	return revision, modified
}

// expectProvenanceGuard runs publish from bin against a database path
// that does not exist, and asserts the guard's verdict for the
// provenance that binary carries. The absent database is the ordering
// probe: "open …: open read-only" can only appear once the guard has let
// the run through, so its presence or absence proves which side of the
// guard the run died on. Every branch exits 1 and writes nothing.
func expectProvenanceGuard(bin string) {
	dir := GinkgoT().TempDir()
	dbPath := filepath.Join(dir, "absent.db")
	out := filepath.Join(dir, "deposit")

	session, err := gexec.Start(exec.Command(bin, "publish", "--db", dbPath, "--out", out), GinkgoWriter, GinkgoWriter)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	EventuallyWithOffset(1, session, 30*time.Second).Should(gexec.Exit(1))
	stdout, stderr := string(session.Out.Contents()), string(session.Err.Contents())

	revision, modified := binaryVCS(bin)
	switch {
	case modified:
		// A checkout with no commit yet stamps no revision; the message
		// says so rather than naming an empty one.
		stamp := "an empty etl_git_sha"
		if revision != "" {
			stamp = "etl_git_sha " + revision
		}
		// The whole of stderr: no header line, no gate line, no report —
		// the run stopped before it did anything at all.
		ExpectWithOffset(1, stderr).To(Equal("error: refusing to publish from a modified working tree: " +
			"the deposit would stamp " + stamp + ", provenance that does not name the code " +
			"that ran — commit or stash the changes, rebuild, and publish again\n"))
		ExpectWithOffset(1, stdout).To(BeEmpty())
	case revision == "":
		ExpectWithOffset(1, stderr).To(HavePrefix(wantNoVCSWarning))
		ExpectWithOffset(1, stderr).To(ContainSubstring("error: open " + dbPath + ": open read-only"))
	default:
		ExpectWithOffset(1, stderr).NotTo(ContainSubstring("modified working tree"))
		ExpectWithOffset(1, stderr).NotTo(ContainSubstring("no VCS metadata"))
		ExpectWithOffset(1, stderr).To(ContainSubstring("error: open " + dbPath + ": open read-only"))
	}

	_, statErr := os.Stat(out)
	ExpectWithOffset(1, os.IsNotExist(statErr)).To(BeTrue(), "the guard runs before --out is created")
	_, statErr = os.Stat(dbPath)
	ExpectWithOffset(1, os.IsNotExist(statErr)).To(BeTrue(), "no database, and no -shm/-wal sidecar, is created")
}

var _ = Describe("conagua-etl publish build provenance", func() {
	It("lets the suite's no-metadata binary mint, announcing the gap it will record", func() {
		expectProvenanceGuard(binPath)
	})

	It("refuses a VCS-stamped binary built from a modified tree, before the database is opened", func() {
		// -buildvcs=auto is the toolchain default: it stamps when the
		// source sits in a checkout and stays silent when it does not, so
		// the spec never fails for want of a repository.
		vcsBin, err := gexec.Build("github.com/bioclimamx/conagua-etl/cmd/conagua-etl", "-buildvcs=auto")
		Expect(err).NotTo(HaveOccurred())

		revision, _ := binaryVCS(vcsBin)
		if revision != "" {
			// The same revision the ledgers and manifest.json stamp is the
			// one --version reports.
			version, err := exec.Command(vcsBin, "--version").CombinedOutput()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(version)).To(ContainSubstring("(git " + revision + ")"))
		}
		expectProvenanceGuard(vcsBin)
	})

	It("documents the refusal, and the allowed no-metadata build, in --help", func() {
		session, err := gexec.Start(exec.Command(binPath, "publish", "--help"), GinkgoWriter, GinkgoWriter)
		Expect(err).NotTo(HaveOccurred())
		Eventually(session, 30*time.Second).Should(gexec.Exit(0))
		help := string(session.Out.Contents())
		Expect(help).To(ContainSubstring("The run is refused up front — before the database is opened and\n" +
			"before --out exists — when this binary was built from a modified\nworking tree"))
		Expect(help).To(ContainSubstring("A binary carrying no VCS metadata at all is allowed"))
		Expect(help).To(ContainSubstring("a provenance integrity signal, a binary\nbuilt from a modified working tree, a --doi that is not a bare DOI,\ncancellation)."))
	})
})
