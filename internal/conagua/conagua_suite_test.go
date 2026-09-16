package conagua

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestConagua(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Conagua Suite")
}

// readFixture loads a file from testdata/, failing the spec on error.
func readFixture(parts ...string) []byte {
	GinkgoHelper()
	b, err := os.ReadFile(filepath.Join(append([]string{"testdata"}, parts...)...))
	Expect(err).NotTo(HaveOccurred())
	return b
}

// Pointer literal helpers for expected-value tables. Parsed values are
// produced by strconv from the same fixture tokens, so exact equality
// (not approximate float matching) is the correct assertion.
func fp(v float64) *float64 { return &v }
func ip(v int) *int         { return &v }
func sp(v string) *string   { return &v }
