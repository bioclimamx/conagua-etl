// Package validate_test is external so the Ginkgo dot-import cannot
// collide with the package's exported names and so the specs exercise
// only the surface publish codes against: GateRules and Gate.
package validate_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestValidate(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Validate Suite")
}
