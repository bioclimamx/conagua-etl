package cmd_test

import (
	"os/exec"
	"regexp"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

func TestCmd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cmd Suite")
}

// binPath is the real conagua-etl binary, compiled once per suite run. The
// e2e specs shell out to it, so the CLI surface is proven exactly as an
// operator invokes it — not through in-process cobra calls.
//
// It is built without VCS metadata on purpose. publish refuses a binary
// built from a modified working tree (checkBuildProvenance), and the tree
// is modified by definition whenever `make check` runs before a commit, so
// a VCS-stamped suite binary would make the publish specs pass or fail on
// the state of the developer's tree. -buildvcs=false is the ETL's
// supported no-metadata mode — every etl_git_sha assertion here already
// guards on binGitSHA being empty — and the guard's own specs build a
// second, VCS-stamped binary to exercise the stamped paths.
var binPath string

// binGitSHA is the vcs.revision the build stamped into the binary, learned
// from the binary's own --version output ("" when the build carried no VCS
// metadata, e.g. -buildvcs=off). The ledger etl_git_sha assertions key off
// this so they stay honest in any build mode.
var binGitSHA string

var versionGitRE = regexp.MustCompile(`\(git ([0-9a-f]+)\)`)

var _ = BeforeSuite(func() {
	var err error
	binPath, err = gexec.Build("github.com/bioclimamx/conagua-etl/cmd/conagua-etl", "-buildvcs=false")
	Expect(err).NotTo(HaveOccurred())

	out, err := exec.Command(binPath, "--version").CombinedOutput()
	Expect(err).NotTo(HaveOccurred())
	if m := versionGitRE.FindSubmatch(out); m != nil {
		binGitSHA = string(m[1])
	}
})

var _ = AfterSuite(func() {
	gexec.CleanupBuildArtifacts()
})
