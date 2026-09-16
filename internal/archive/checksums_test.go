package archive_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// fixtureSum is a syntactically valid, deterministic sha256 for a name.
func fixtureSum(name string) string {
	return sha256Hex([]byte(name))
}

var _ = Describe("WriteChecksums / ParseChecksums", func() {
	var (
		dir  string
		path string
	)

	BeforeEach(func() {
		dir = GinkgoT().TempDir()
		path = filepath.Join(dir, "CHECKSUMS")
	})

	It("round-trips every field the format carries, sorted by Name bytewise", func() {
		// Deliberately unsorted, with case chosen so that bytewise order
		// (uppercase before lowercase) differs from a case-folded sort.
		sums := []archive.Checksum{
			{Name: "b.zip", SHA256: fixtureSum("b.zip"), Bytes: 5},
			{Name: "ags-tabular.zip", SHA256: fixtureSum("ags-tabular.zip"), Bytes: 4},
			{Name: "README.md", SHA256: fixtureSum("README.md"), Bytes: 2},
			{Name: "a.zip", SHA256: fixtureSum("a.zip"), Bytes: 3},
			{Name: "B.zip", SHA256: fixtureSum("B.zip"), Bytes: 1},
		}
		wantOrder := []string{"B.zip", "README.md", "a.zip", "ags-tabular.zip", "b.zip"}
		original := append([]archive.Checksum(nil), sums...)

		Expect(archive.WriteChecksums(path, sums)).To(Succeed())
		Expect(sums).To(Equal(original), "the caller's slice must not be reordered")
		expectNoTempResidue(dir)

		f, err := os.Open(path)
		Expect(err).NotTo(HaveOccurred())
		defer f.Close() //nolint:errcheck // read-side close; no recovery possible
		got, err := archive.ParseChecksums(f)
		Expect(err).NotTo(HaveOccurred())

		Expect(got).To(HaveLen(len(sums)))
		for i, name := range wantOrder {
			Expect(got[i].Name).To(Equal(name), "position %d", i)
			Expect(got[i].SHA256).To(Equal(fixtureSum(name)))
			// Bytes is not part of the sha256sum line format, so it does
			// not survive the round trip; the parse reports zero.
			Expect(got[i].Bytes).To(BeZero())
		}
	})

	It("emits exactly sha256sum's text format: <hex>, two spaces, <name>, LF", func() {
		sums := []archive.Checksum{
			{Name: "yuc-tabular.zip", SHA256: fixtureSum("yuc-tabular.zip"), Bytes: 10},
			{Name: "manifest.json", SHA256: fixtureSum("manifest.json"), Bytes: 20},
		}
		Expect(archive.WriteChecksums(path, sums)).To(Succeed())

		got, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		want := fixtureSum("manifest.json") + "  manifest.json\n" +
			fixtureSum("yuc-tabular.zip") + "  yuc-tabular.zip\n"
		Expect(string(got)).To(Equal(want))
	})

	It("is verifiable by sha256sum -c against the real files", func() {
		if _, err := exec.LookPath("sha256sum"); err != nil {
			Skip("sha256sum not on PATH")
		}
		names := []string{"manifest.json", "yuc-tabular.zip", "README.md"}
		var sums []archive.Checksum
		for i, name := range names {
			p := filepath.Join(dir, name)
			Expect(os.WriteFile(p, []byte(fmt.Sprintf("payload %d\n", i)), 0o644)).To(Succeed())
			sum, n, err := archive.SHA256File(p)
			Expect(err).NotTo(HaveOccurred())
			sums = append(sums, archive.Checksum{Name: name, SHA256: sum, Bytes: n})
		}
		Expect(archive.WriteChecksums(path, sums)).To(Succeed())

		cmd := exec.Command("sha256sum", "-c", "--strict", "CHECKSUMS")
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), string(out))
		for _, name := range names {
			Expect(string(out)).To(ContainSubstring(name + ": OK"))
		}
	})

	It("writes and parses an empty list", func() {
		Expect(archive.WriteChecksums(path, nil)).To(Succeed())
		got, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(BeEmpty())

		sums, err := archive.ParseChecksums(strings.NewReader(""))
		Expect(err).NotTo(HaveOccurred())
		Expect(sums).To(BeEmpty())
	})

	DescribeTable("WriteChecksums rejects an invalid list and writes nothing",
		func(bad []archive.Checksum, reason string) {
			err := archive.WriteChecksums(path, bad)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(reason))
			expectAbsent(path)
		},
		Entry("empty name", []archive.Checksum{{Name: "", SHA256: fixtureSum("x")}}, "empty name"),
		Entry("line break in name", []archive.Checksum{{Name: "a\nb", SHA256: fixtureSum("x")}}, "line break"),
		Entry("short digest", []archive.Checksum{{Name: "a", SHA256: "abc"}}, "not 64 lowercase hex"),
		Entry("uppercase digest", []archive.Checksum{{Name: "a", SHA256: strings.ToUpper(fixtureSum("x"))}}, "not 64 lowercase hex"),
		Entry("non-hex digest", []archive.Checksum{{Name: "a", SHA256: strings.Repeat("g", 64)}}, "not 64 lowercase hex"),
		Entry("duplicate name", []archive.Checksum{{Name: "a", SHA256: fixtureSum("1")}, {Name: "a", SHA256: fixtureSum("2")}}, "duplicate name"),
	)

	DescribeTable("ParseChecksums rejects a malformed file, naming the line",
		func(text, reason string) {
			_, err := archive.ParseChecksums(strings.NewReader(text))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(reason))
		},
		Entry("single-space separator", fixtureSum("a")+" a.zip\n", "line 1: malformed"),
		Entry("missing name", fixtureSum("a")+"  \n", "line 1: malformed"),
		Entry("short digest", "abc  a.zip\n", "line 1: malformed"),
		Entry("uppercase digest", strings.ToUpper(fixtureSum("a"))+"  a.zip\n", "line 1: malformed"),
		Entry("blank line", fixtureSum("a")+"  a.zip\n\n", "line 2: malformed"),
		Entry("duplicate name", fixtureSum("a")+"  a.zip\n"+fixtureSum("b")+"  a.zip\n", "line 2: duplicate name"),
	)

	It("keeps a name that itself contains two spaces", func() {
		sums := []archive.Checksum{{Name: "odd  name.txt", SHA256: fixtureSum("odd")}}
		Expect(archive.WriteChecksums(path, sums)).To(Succeed())

		f, err := os.Open(path)
		Expect(err).NotTo(HaveOccurred())
		defer f.Close() //nolint:errcheck // read-side close; no recovery possible
		got, err := archive.ParseChecksums(f)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]archive.Checksum{{Name: "odd  name.txt", SHA256: fixtureSum("odd")}}))
	})
})
