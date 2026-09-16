package archive_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

var _ = Describe("WriteFileAtomic", func() {
	var dir string

	BeforeEach(func() {
		dir = GinkgoT().TempDir()
	})

	It("round-trips the content and describes the bytes on disk", func() {
		path := filepath.Join(dir, "manifest.json")
		content := "{\n  \"snapshot_date\": \"2026-06-08\"\n}\n"

		res, err := archive.WriteFileAtomic(path, func(w io.Writer) error {
			_, err := io.WriteString(w, content)
			return err
		})
		Expect(err).NotTo(HaveOccurred())

		got, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(got)).To(Equal(content))
		Expect(res.Bytes).To(Equal(int64(len(content))))
		Expect(res.SHA256).To(Equal(sha256Hex(got)))
		Expect(res.Entries).To(BeZero())
		expectNoTempResidue(dir)
	})

	It("creates the parent directory", func() {
		path := filepath.Join(dir, "a", "b", "CHECKSUMS")
		_, err := archive.WriteFileAtomic(path, func(w io.Writer) error {
			_, err := io.WriteString(w, "x")
			return err
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(path).To(BeAnExistingFile())
	})

	It("leaves nothing at path and no temp sibling when write fails", func() {
		path := filepath.Join(dir, "manifest.json")
		errBoom := errors.New("boom")

		_, err := archive.WriteFileAtomic(path, func(w io.Writer) error {
			_, _ = io.WriteString(w, "partial")
			return errBoom
		})
		Expect(err).To(MatchError(errBoom))
		Expect(err.Error()).To(HavePrefix("write file " + path))
		expectAbsent(path)
		expectNoTempResidue(dir)
	})

	It("preserves the previous file intact when a rewrite fails", func() {
		path := filepath.Join(dir, "manifest.json")
		_, err := archive.WriteFileAtomic(path, func(w io.Writer) error {
			_, err := io.WriteString(w, "original")
			return err
		})
		Expect(err).NotTo(HaveOccurred())

		_, err = archive.WriteFileAtomic(path, func(w io.Writer) error {
			_, _ = io.WriteString(w, "partial")
			return errors.New("boom")
		})
		Expect(err).To(HaveOccurred())

		got, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(got)).To(Equal("original"))
		expectNoTempResidue(dir)
	})

	It("rejects a nil write function", func() {
		path := filepath.Join(dir, "manifest.json")
		_, err := archive.WriteFileAtomic(path, nil)
		Expect(err).To(MatchError(ContainSubstring("nil write function")))
		expectAbsent(path)
	})
})

var _ = Describe("SHA256File", func() {
	var dir string

	BeforeEach(func() {
		dir = GinkgoT().TempDir()
	})

	It("matches crypto/sha256 over the bytes and reports the size", func() {
		path := filepath.Join(dir, "yuc-tabular.zip")
		content := []byte("PK\x03\x04 not really a zip, but bytes are bytes\n")
		Expect(os.WriteFile(path, content, 0o644)).To(Succeed())

		sum, n, err := archive.SHA256File(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(sum).To(Equal(sha256Hex(content)))
		Expect(n).To(Equal(int64(len(content))))
	})

	It("agrees with the Result of an atomic write", func() {
		path := filepath.Join(dir, "README.md")
		res, err := archive.WriteFileAtomic(path, func(w io.Writer) error {
			_, err := io.WriteString(w, "# Bioclima\n")
			return err
		})
		Expect(err).NotTo(HaveOccurred())

		sum, n, err := archive.SHA256File(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(sum).To(Equal(res.SHA256))
		Expect(n).To(Equal(res.Bytes))
	})

	It("fails with fs.ErrNotExist for a missing file", func() {
		_, _, err := archive.SHA256File(filepath.Join(dir, "missing"))
		Expect(err).To(MatchError(os.ErrNotExist))
	})
})
