package cmd

// Direct specs for publish's build-provenance guard: the rule that
// decides, from the VCS metadata the toolchain stamped into the binary,
// whether this build may mint a citable deposit at all. A modified tree
// is refused (its SHA would name code that did not run); an absent
// revision is an honest gap and passes, announced; a clean stamped build
// passes in silence. buildGitSHA's contract — the bare revision, the
// value the ledgers and manifest.json record — is pinned here beside it.

import (
	"bytes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("checkBuildProvenance", func() {
	const revision = "bc6d506529f4864a2a9f32cd6aa203588c67af5e"

	var out *bytes.Buffer

	BeforeEach(func() { out = &bytes.Buffer{} })

	Context("a binary built from a modified working tree", func() {
		It("refuses, naming the SHA the deposit would have stamped and the way out", func() {
			err := checkBuildProvenance(out, revision, true)
			Expect(err).To(MatchError("refusing to publish from a modified working tree: " +
				"the deposit would stamp etl_git_sha " + revision + ", provenance that does not name the code " +
				"that ran — commit or stash the changes, rebuild, and publish again"))
			Expect(out.String()).To(BeEmpty())
		})

		It("is a plain refusal, not a degraded run: exit code 1", func() {
			Expect(ExitCode(checkBuildProvenance(out, revision, true))).To(Equal(1))
		})

		It("refuses a modified build whose revision the toolchain did not stamp", func() {
			err := checkBuildProvenance(out, "", true)
			Expect(err).To(MatchError("refusing to publish from a modified working tree: " +
				"the deposit would stamp an empty etl_git_sha, provenance that does not name the code " +
				"that ran — commit or stash the changes, rebuild, and publish again"))
			Expect(out.String()).To(BeEmpty())
		})
	})

	Context("a binary built from a clean checkout", func() {
		It("passes silently — nothing on stderr, nothing to report", func() {
			Expect(checkBuildProvenance(out, revision, false)).To(Succeed())
			Expect(out.String()).To(BeEmpty())
		})
	})

	Context("a binary carrying no VCS metadata", func() {
		It("passes — an empty etl_git_sha is an honest gap, not a false claim — and announces it", func() {
			Expect(checkBuildProvenance(out, "", false)).To(Succeed())
			Expect(out.String()).To(Equal("warning: this binary carries no VCS metadata; " +
				"the deposit will record an empty etl_git_sha and cannot be traced back to a commit\n"))
		})
	})
})

var _ = Describe("buildVCS", func() {
	It("feeds buildGitSHA the bare revision — the one value the ledgers and manifest.json record", func() {
		revision, _ := buildVCS()
		Expect(buildGitSHA()).To(Equal(revision))
		// The ledgers record a revision `git show` accepts: no "+dirty"
		// suffix, no decoration of any kind.
		Expect(revision).NotTo(ContainSubstring("dirty"))
		Expect(revision).To(Or(BeEmpty(), MatchRegexp(`^[0-9a-f]{40}$`)))
	})
})
