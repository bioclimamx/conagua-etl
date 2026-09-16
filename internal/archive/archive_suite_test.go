// Package archive_test is external so the specs exercise only the
// package's exported contract — the surface publish consumes.
package archive_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestArchive(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Archive Suite")
}
