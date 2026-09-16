package cmd_test

// The --only selector on the real binary, over the deposit-wide groups:
// every national group and the raw snapshot exits 1 with --state, as
// typed, before anything is written — through one --only value or two —
// and repeated, reordered, mixed-case values resolve into build order,
// the report naming the groups once each.

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("conagua-etl publish --only over the deposit-wide groups", func() {
	var (
		dbPath string
		root   string
	)

	BeforeEach(func() {
		dbPath = seedPublishDB()
		root = filepath.Join(GinkgoT().TempDir(), "snapshots")
		seedPublishSnapshot(root)
	})

	DescribeTable("exits 1 on a deposit-wide group with --state, as typed, writing nothing",
		func(only []string) {
			out := filepath.Join(GinkgoT().TempDir(), "subset")
			args := []string{"--state", "yuc", "--root", root}
			for _, o := range only {
				args = append(args, "--only", o)
			}
			session := runPublishToExit(dbPath, out, args...)
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring(
				`error: artifact group "` + only[len(only)-1] + `" is built only in a full run: drop --state` + "\n"))
			Expect(publishArtifactLineRE.FindAllStringSubmatch(string(session.Err.Contents()), -1)).To(BeEmpty())
			Expect(string(session.Out.Contents())).To(ContainSubstring("\npublish aborted\n"))
			_, err := os.Stat(out)
			Expect(os.IsNotExist(err)).To(BeTrue())
		},
		Entry("national-parquet", []string{"national-parquet"}),
		Entry("National-JSON", []string{"National-JSON"}),
		Entry("NATIONAL-SQLITE", []string{"NATIONAL-SQLITE"}),
		Entry("raw", []string{"raw"}),
		Entry("raw after tabular, as two flags", []string{"tabular", "raw"}),
		Entry("national-csv after both per-state groups in one value", []string{"tabular,json", "national-csv"}),
	)

	It("resolves repeated, reordered, mixed-case --only values into build order, the report naming each group once", func() {
		out := filepath.Join(GinkgoT().TempDir(), "yuc")
		session := runPublishToExit(dbPath, out, "--state", "yuc", "--only", "json,TABULAR", "--only", "Json")
		Expect(session.ExitCode()).To(Equal(0))
		stderr := string(session.Err.Contents())
		Expect(stderr).To(ContainSubstring("  states=yuc  only=json,TABULAR,Json  doi=none\n"))
		artifacts := publishArtifactLineRE.FindAllStringSubmatch(stderr, -1)
		Expect(artifacts).To(HaveLen(2))
		Expect(artifacts[0][1:3]).To(Equal([]string{"ok", "yuc-tabular.zip"}))
		Expect(artifacts[1][1:3]).To(Equal([]string{"ok", "yuc-json.zip"}))
		stdout := string(session.Out.Contents())
		Expect(stdout).To(ContainSubstring("  groups               : tabular,json\n"))
		Expect(stdout).To(ContainSubstring("  artifacts            : 2 ok / 0 failed / 2 attempted\n"))
		Expect(listDir(out)).To(Equal([]string{"CHECKSUMS", "manifest.json", "yuc-json.zip", "yuc-tabular.zip"}))
		m, _ := readManifest(out)
		Expect(m.States).To(HaveLen(1))
		Expect(m.States[0].Artifacts).To(Equal([]string{"yuc-tabular.zip", "yuc-json.zip"}))
		Expect(m.National).To(BeEmpty())
	})
})
