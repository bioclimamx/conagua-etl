package envfile

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEnvfile(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Envfile Suite")
}

// writeEnvFile writes body to a fresh temp `.env` file and returns its path.
func writeEnvFile(body string) string {
	GinkgoHelper()
	path := filepath.Join(GinkgoT().TempDir(), ".env")
	Expect(os.WriteFile(path, []byte(body), 0o644)).To(Succeed())
	return path
}

// stashEnv unsets each key for a clean slate and registers a cleanup that
// restores its pre-spec state, so env mutations never leak between specs
// and the suite stays order-independent. Every spec stashes every key its
// fixture file could touch before calling Load.
func stashEnv(keys ...string) {
	GinkgoHelper()
	for _, key := range keys {
		if prev, wasSet := os.LookupEnv(key); wasSet {
			DeferCleanup(os.Setenv, key, prev)
		} else {
			DeferCleanup(os.Unsetenv, key)
		}
		Expect(os.Unsetenv(key)).To(Succeed())
	}
}

// expectEnv asserts key is set and round-trips exactly to want —
// distinguishing set-to-empty from unset via LookupEnv.
func expectEnv(key, want string) {
	GinkgoHelper()
	got, set := os.LookupEnv(key)
	Expect(set).To(BeTrue(), "expected %s to be set", key)
	Expect(got).To(Equal(want))
}

// expectUnset asserts key is absent from the environment entirely.
func expectUnset(key string) {
	GinkgoHelper()
	_, set := os.LookupEnv(key)
	Expect(set).To(BeFalse(), "expected %s to be unset", key)
}
