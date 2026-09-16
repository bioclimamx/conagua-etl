package cmd_test

// The out-dir discipline across artifact groups on the real binary: a
// --only build into a directory holding the other group's archive exits
// 1 naming that archive and touches nothing, in both directions, and the
// full build into the same directory converges — the archive the subset
// build wrote is the same bytes beside its new sibling.

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("conagua-etl publish --only against an --out holding the other group", func() {
	var dbPath, dbSHA, root string

	BeforeEach(func() {
		dbPath = seedPublishDB()
		dbSHA = fileSHA(dbPath)
		var err error
		root, err = os.MkdirTemp("", "publish-e2e-only-*")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() error { return os.RemoveAll(root) })
	})

	// expectRefused runs the group-subset build into out and asserts the
	// exit-1 refusal naming foreign: no unit or artifact line, the aborted
	// report, the directory and the archive it holds untouched.
	expectRefused := func(out, only, foreign string, untouched map[string][]byte) {
		GinkgoHelper()
		before := listDir(out)
		session := runPublishToExit(dbPath, out, "--state", "yuc", "--only", only)
		Expect(session.ExitCode()).To(Equal(1))
		stderr := string(session.Err.Contents())
		Expect(stderr).To(ContainSubstring("  states=yuc  only=" + only + "  doi=none\n"))
		Expect(stderr).To(ContainSubstring("error: out dir " + out + " holds entries this run does not produce: " +
			foreign + " (remove them or use a fresh --out)\n"))
		Expect(publishUnitLineRE.FindAllStringSubmatch(stderr, -1)).To(BeEmpty())
		Expect(publishArtifactLineRE.FindAllStringSubmatch(stderr, -1)).To(BeEmpty())
		Expect(string(session.Out.Contents())).To(ContainSubstring("\npublish aborted\n"))
		Expect(listDir(out)).To(Equal(before))
		for name, data := range untouched {
			Expect(readBytes(filepath.Join(out, name))).To(Equal(data), name)
		}
		Expect(fileSHA(dbPath)).To(Equal(dbSHA))
	}

	It("exits 1 when a json-only build meets a tabular archive and when a tabular-only build meets a JSON one; the full build then converges on the same bytes", func() {
		tabularFirst := filepath.Join(root, "tabular-first")
		Expect(runPublishToExit(dbPath, tabularFirst, "--state", "yuc", "--only", "tabular").ExitCode()).To(Equal(0))
		Expect(listDir(tabularFirst)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-tabular.zip"}))
		tabularBytes := readBytes(filepath.Join(tabularFirst, "yuc-tabular.zip"))
		expectRefused(tabularFirst, "json", "yuc-tabular.zip", map[string][]byte{"yuc-tabular.zip": tabularBytes})

		jsonFirst := filepath.Join(root, "json-first")
		Expect(runPublishToExit(dbPath, jsonFirst, "--state", "yuc", "--only", "json").ExitCode()).To(Equal(0))
		Expect(listDir(jsonFirst)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-json.zip"}))
		jsonBytes := readBytes(filepath.Join(jsonFirst, "yuc-json.zip"))
		expectRefused(jsonFirst, "tabular", "yuc-json.zip", map[string][]byte{"yuc-json.zip": jsonBytes})

		// The full group set into the JSON-only directory is the converging
		// re-run: both archives listed, the JSON one the subset build's
		// bytes, the tabular one the tabular-only build's.
		session := runPublishToExit(dbPath, jsonFirst, "--state", "yuc")
		Expect(session.ExitCode()).To(Equal(0))
		stdout := string(session.Out.Contents())
		Expect(stdout).To(ContainSubstring("  groups               : tabular,json\n"))
		Expect(stdout).To(ContainSubstring("  artifacts            : 2 ok / 0 failed / 2 attempted\n"))
		Expect(stdout).To(ContainSubstring("  top-level files      : 4\n"))
		Expect(listDir(jsonFirst)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-json.zip", "yuc-tabular.zip"}))
		Expect(checksumNames(readChecksums(jsonFirst))).To(Equal([]string{"manifest.json", "yuc-json.zip", "yuc-tabular.zip"}))
		Expect(readBytes(filepath.Join(jsonFirst, "yuc-json.zip"))).To(Equal(jsonBytes))
		Expect(readBytes(filepath.Join(jsonFirst, "yuc-tabular.zip"))).To(Equal(tabularBytes))
		expectNoTempResidue(jsonFirst)
		Expect(fileSHA(dbPath)).To(Equal(dbSHA))
	})
})
