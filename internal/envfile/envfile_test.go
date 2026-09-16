package envfile

import (
	"bufio"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Spec keys are prefixed ENVFILE_SPEC_ so they can never collide with a
// real variable in the developer's or CI's environment.
var _ = Describe("Load", func() {
	Context("with a well-formed file", func() {
		// EMPTY= in the fixture body is pinned as set-to-empty-string.
		It("sets every parsed key to its exact value", func() {
			stashEnv("ENVFILE_SPEC_FOO", "ENVFILE_SPEC_BAZ",
				"ENVFILE_SPEC_EMPTY", "ENVFILE_SPEC_TRAILING")
			path := writeEnvFile("# comment\n" +
				"ENVFILE_SPEC_FOO=bar\n" +
				"  ENVFILE_SPEC_BAZ = \"qux\"  \n" +
				"ENVFILE_SPEC_EMPTY=\n" +
				"\n" +
				"ENVFILE_SPEC_TRAILING=yes\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_FOO", "bar")
			expectEnv("ENVFILE_SPEC_BAZ", "qux")
			expectEnv("ENVFILE_SPEC_EMPTY", "")
			expectEnv("ENVFILE_SPEC_TRAILING", "yes")
		})

		It("splits on the first '=' only, keeping later ones in the value", func() {
			stashEnv("ENVFILE_SPEC_DSN")
			path := writeEnvFile("ENVFILE_SPEC_DSN=user=pablo;pw=s=cret\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_DSN", "user=pablo;pw=s=cret")
		})
	})

	Context("comments and blank lines", func() {
		It("skips full-line comments, including indented ones", func() {
			stashEnv("ENVFILE_SPEC_HIDDEN", "ENVFILE_SPEC_INDENTED", "ENVFILE_SPEC_LIVE")
			path := writeEnvFile("# ENVFILE_SPEC_HIDDEN=nope\n" +
				"   # ENVFILE_SPEC_INDENTED=nope\n" +
				"ENVFILE_SPEC_LIVE=yes\n")

			Expect(Load(path)).To(Succeed())

			expectUnset("ENVFILE_SPEC_HIDDEN")
			expectUnset("ENVFILE_SPEC_INDENTED")
			expectEnv("ENVFILE_SPEC_LIVE", "yes")
		})

		It("does not treat '#' after a value as a comment (no inline comments)", func() {
			stashEnv("ENVFILE_SPEC_INLINE")
			path := writeEnvFile("ENVFILE_SPEC_INLINE=bar # not a comment\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_INLINE", "bar # not a comment")
		})

		It("skips blank and whitespace-only lines", func() {
			stashEnv("ENVFILE_SPEC_AFTER_BLANKS")
			path := writeEnvFile("\n   \n\t\nENVFILE_SPEC_AFTER_BLANKS=ok\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_AFTER_BLANKS", "ok")
		})
	})

	Context("whitespace semantics", func() {
		It("trims whitespace around the key and the value", func() {
			stashEnv("ENVFILE_SPEC_PADDED")
			path := writeEnvFile("   ENVFILE_SPEC_PADDED   =   spaced out   \n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_PADDED", "spaced out")
		})

		It("preserves interior whitespace in the value", func() {
			stashEnv("ENVFILE_SPEC_INNER")
			path := writeEnvFile("ENVFILE_SPEC_INNER=a b  c\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_INNER", "a b  c")
		})
	})

	Context("quoting semantics", func() {
		It("strips surrounding double quotes", func() {
			stashEnv("ENVFILE_SPEC_DQ")
			path := writeEnvFile("ENVFILE_SPEC_DQ=\"hello world\"\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_DQ", "hello world")
		})

		It("strips surrounding single quotes", func() {
			stashEnv("ENVFILE_SPEC_SQ")
			path := writeEnvFile("ENVFILE_SPEC_SQ='hello'\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_SQ", "hello")
		})

		It("preserves whitespace inside quotes (trim happens before unquoting)", func() {
			stashEnv("ENVFILE_SPEC_QPAD")
			path := writeEnvFile("ENVFILE_SPEC_QPAD=\"  padded  \"\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_QPAD", "  padded  ")
		})

		// Quotes are trimmed as a leading/trailing cutset, not matched as
		// pairs — these pin the resulting quirks exactly.
		It("strips quote runs and mismatched quotes from both ends, keeping interior quotes", func() {
			stashEnv("ENVFILE_SPEC_UNBALANCED", "ENVFILE_SPEC_MIXED",
				"ENVFILE_SPEC_DOUBLED", "ENVFILE_SPEC_INTERIOR")
			path := writeEnvFile("ENVFILE_SPEC_UNBALANCED=\"abc\n" +
				"ENVFILE_SPEC_MIXED='\"x\"'\n" +
				"ENVFILE_SPEC_DOUBLED=\"\"v\"\"\n" +
				"ENVFILE_SPEC_INTERIOR=a\"b\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_UNBALANCED", "abc")
			expectEnv("ENVFILE_SPEC_MIXED", "x")
			expectEnv("ENVFILE_SPEC_DOUBLED", "v")
			expectEnv("ENVFILE_SPEC_INTERIOR", "a\"b")
		})

		It("sets a bare quoted-empty value to the empty string", func() {
			stashEnv("ENVFILE_SPEC_QEMPTY")
			path := writeEnvFile("ENVFILE_SPEC_QEMPTY=\"\"\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_QEMPTY", "")
		})
	})

	Context("malformed lines", func() {
		It("silently skips lines without '=' and keeps loading", func() {
			stashEnv("ENVFILE_SPEC_AFTER_JUNK")
			path := writeEnvFile("this line has no equals sign\n" +
				"ENVFILE_SPEC_AFTER_JUNK=ok\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_AFTER_JUNK", "ok")
		})

		It("silently skips lines with an empty or whitespace-only key", func() {
			stashEnv("ENVFILE_SPEC_AFTER_EMPTYKEY")
			path := writeEnvFile("=orphan value\n" +
				"   =another orphan\n" +
				"ENVFILE_SPEC_AFTER_EMPTYKEY=ok\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_AFTER_EMPTYKEY", "ok")
		})
	})

	Context("precedence", func() {
		It("never overwrites an existing environment variable", func() {
			stashEnv("ENVFILE_SPEC_PRECEDENCE")
			Expect(os.Setenv("ENVFILE_SPEC_PRECEDENCE", "from-process")).To(Succeed())
			path := writeEnvFile("ENVFILE_SPEC_PRECEDENCE=from-file\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_PRECEDENCE", "from-process")
		})

		It("treats a set-but-empty variable as existing (LookupEnv, not Getenv)", func() {
			stashEnv("ENVFILE_SPEC_SETEMPTY")
			Expect(os.Setenv("ENVFILE_SPEC_SETEMPTY", "")).To(Succeed())
			path := writeEnvFile("ENVFILE_SPEC_SETEMPTY=from-file\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_SETEMPTY", "")
		})

		It("keeps the first occurrence when a key repeats within the file", func() {
			stashEnv("ENVFILE_SPEC_DUP")
			path := writeEnvFile("ENVFILE_SPEC_DUP=first\nENVFILE_SPEC_DUP=second\n")

			Expect(Load(path)).To(Succeed())

			expectEnv("ENVFILE_SPEC_DUP", "first")
		})
	})

	Context("missing file", func() {
		It("returns nil for a nonexistent file", func() {
			Expect(Load(filepath.Join(GinkgoT().TempDir(), "does-not-exist.env"))).To(Succeed())
		})

		It("returns nil when intermediate directories do not exist either", func() {
			Expect(Load(filepath.Join(GinkgoT().TempDir(), "no", "such", "dir", ".env"))).To(Succeed())
		})
	})

	Context("errors other than not-exist", func() {
		It("propagates open errors (permission denied)", func() {
			if os.Geteuid() == 0 {
				Skip("running as root: file modes are not enforced")
			}
			path := writeEnvFile("ENVFILE_SPEC_DENIED=x\n")
			Expect(os.Chmod(path, 0o000)).To(Succeed())

			Expect(Load(path)).To(MatchError(fs.ErrPermission))
		})

		It("propagates read errors (path is a directory)", func() {
			Expect(Load(GinkgoT().TempDir())).NotTo(Succeed())
		})

		It("propagates the scanner error for over-long lines, keeping earlier pairs", func() {
			stashEnv("ENVFILE_SPEC_BEFORE_LONG", "ENVFILE_SPEC_LONG")
			path := writeEnvFile("ENVFILE_SPEC_BEFORE_LONG=set\n" +
				"ENVFILE_SPEC_LONG=" + strings.Repeat("x", bufio.MaxScanTokenSize) + "\n")

			Expect(Load(path)).To(MatchError(bufio.ErrTooLong))

			// Pairs before the failing line were already applied — Load is
			// not transactional.
			expectEnv("ENVFILE_SPEC_BEFORE_LONG", "set")
			expectUnset("ENVFILE_SPEC_LONG")
		})

		It("propagates Setenv errors (NUL byte in a key)", func() {
			stashEnv("ENVFILE_SPEC_AFTER_NUL")
			path := writeEnvFile("BAD\x00KEY=v\n" +
				"ENVFILE_SPEC_AFTER_NUL=never\n")

			Expect(Load(path)).NotTo(Succeed())

			// The failing Setenv aborts the loop; later pairs are not applied.
			expectUnset("ENVFILE_SPEC_AFTER_NUL")
		})
	})
})
