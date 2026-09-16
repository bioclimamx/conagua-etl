// Package parity_test is external because Ginkgo's dot-imported API
// exports its own Report type, which would collide with parity.Report
// in an internal test package.
package parity_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestParity(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Parity Suite")
}
