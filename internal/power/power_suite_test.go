// Package power_test is external because Ginkgo's dot-imported API
// exports its own Report type, which would collide with power.Report in
// an internal test package (the ingest suite sets the precedent).
// Keeping the suite external also confines the specs to the package's
// exported contract — the surface the orchestrator and cmd layers
// consume.
package power_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPower(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Power Suite")
}
