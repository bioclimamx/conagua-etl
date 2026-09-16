package publish

// In-package specs for the out-dir post-check against Zenodo's cap:
// verifyOutDir counts what is on disk and refuses a deposit directory
// over MaxTopLevelFiles even when CHECKSUMS lists every file, and
// accepts one exactly at the cap.

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

var _ = ginkgo.Describe("verifyOutDir against the file cap", func() {
	// fill writes n listed files beside CHECKSUMS and returns the
	// checksums that vouch for them.
	fill := func(dir string, n int) []archive.Checksum {
		ginkgo.GinkgoHelper()
		gomega.Expect(os.MkdirAll(dir, 0o755)).To(gomega.Succeed())
		sums := make([]archive.Checksum, 0, n)
		for i := range n {
			name := fmt.Sprintf("file-%03d.zip", i)
			gomega.Expect(os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600)).To(gomega.Succeed())
			sums = append(sums, archive.Checksum{Name: name, SHA256: "0"})
		}
		gomega.Expect(os.WriteFile(filepath.Join(dir, checksumsName), []byte(""), 0o600)).To(gomega.Succeed())
		return sums
	}

	ginkgo.It("accepts a directory of exactly MaxTopLevelFiles entries and counts them", func() {
		dir := filepath.Join(ginkgo.GinkgoT().TempDir(), "out")
		sums := fill(dir, MaxTopLevelFiles-1)
		n, err := verifyOutDir(dir, sums)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(n).To(gomega.Equal(MaxTopLevelFiles))
	})

	ginkgo.It("refuses a directory of MaxTopLevelFiles + 1 entries, every one listed", func() {
		dir := filepath.Join(ginkgo.GinkgoT().TempDir(), "out")
		sums := fill(dir, MaxTopLevelFiles)
		n, err := verifyOutDir(dir, sums)
		gomega.Expect(err).To(gomega.MatchError("out dir " + dir + " holds 101 top-level files, over Zenodo's 100-file cap"))
		gomega.Expect(n).To(gomega.BeZero())
	})

	ginkgo.It("reports an unlisted entry ahead of the cap", func() {
		dir := filepath.Join(ginkgo.GinkgoT().TempDir(), "out")
		sums := fill(dir, MaxTopLevelFiles)
		_, err := verifyOutDir(dir, sums[1:])
		gomega.Expect(err).To(gomega.MatchError("out dir " + dir + " holds entries CHECKSUMS does not list: file-000.zip"))
	})
})
