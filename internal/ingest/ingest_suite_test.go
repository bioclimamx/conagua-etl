// Package ingest_test is external because Ginkgo's dot-imported API
// exports its own Report type, which would collide with ingest.Report
// in an internal test package. In-package specs that need unexported
// access import ginkgo by name instead (the parity package sets the
// precedent); both packages compile into the same test binary and run
// under this one RunSpecs.
package ingest_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestIngest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ingest Suite")
}
