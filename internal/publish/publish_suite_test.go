// Package publish_test is external so the Ginkgo dot-import cannot
// collide with the package's own exported names as later slices add
// them (the ingest suite sets the precedent). In-package specs that
// need unexported access import ginkgo by name; both compile into this
// one test binary and run under this RunSpecs.
package publish_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPublish(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Publish Suite")
}
