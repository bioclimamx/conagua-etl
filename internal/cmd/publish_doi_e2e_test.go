package cmd_test

// The --doi flag on the real binary: a malformed DOI is refused before
// the database is opened or --out exists, and a bare DOI reaches the
// artifacts that carry the citation.

import (
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("conagua-etl publish --doi", Ordered, func() {
	var dbPath string

	BeforeAll(func() { dbPath = seedPublishDB() })

	DescribeTable("refuses a value that is not a bare DOI, before anything is written",
		func(doi, want string) {
			out := filepath.Join(GinkgoT().TempDir(), "deposit")
			session := runPublishToExit(dbPath, out, "--state", "yuc", "--only", "docs", "--doi", doi)
			Expect(session.ExitCode()).To(Equal(1))
			Expect(string(session.Err.Contents())).To(ContainSubstring("error: --doi: DOI \"" + doi + "\"" + want))
			Expect(out).NotTo(BeAnExistingFile())
		},
		Entry("a resolver URL", "https://doi.org/10.5281/zenodo.1234567", `: pass the bare DOI, "10.5281/zenodo.1234567"`),
		Entry("a record URL", "https://zenodo.org/records/1234567", " is not a DOI (expected the form 10.5281/zenodo.1234567)"),
	)

	It("writes a bare DOI into the manifest, CITATION.cff and the README citation, and names it in the header", func() {
		const doi = "10.5281/zenodo.1234567"
		out := filepath.Join(GinkgoT().TempDir(), "deposit")
		session := runPublishToExit(dbPath, out, "--state", "yuc", "--only", "docs", "--doi", doi)
		Expect(session.ExitCode()).To(Equal(0))
		Expect(string(session.Err.Contents())).To(ContainSubstring("  doi=" + doi + "\n"))

		m, _ := readManifest(out)
		Expect(m.Dataset.DOI).To(Equal(doi))
		Expect(m.Dataset.SuggestedCitation).To(HaveSuffix(" Zenodo. DOI: " + doi))
		Expect(string(readBytes(filepath.Join(out, "CITATION.cff")))).To(ContainSubstring("\ndoi: " + doi + "\n"))
		Expect(string(readBytes(filepath.Join(out, "README.md")))).To(ContainSubstring("\n> " + m.Dataset.SuggestedCitation + "\n"))
	})
})
