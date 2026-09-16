package cmd

// Direct specs for the exit-code surface: ExitCode is the single map
// from the command tree's error to the process exit code, and
// DegradedError is the only error that earns the reserved code 2.

import (
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ExitCode", func() {
	It("maps nil to 0", func() {
		Expect(ExitCode(nil)).To(Equal(0))
	})

	It("maps a DegradedError to 2", func() {
		err := DegradedError{Summary: "ingest degraded: 3 of 5524 stations failed (see report)"}
		Expect(ExitCode(err)).To(Equal(2))
		Expect(err.Error()).To(Equal("ingest degraded: 3 of 5524 stations failed (see report)"))
	})

	It("maps a wrapped DegradedError to 2 — the chain, not the surface, decides", func() {
		wrapped := fmt.Errorf("outer context: %w", DegradedError{Summary: "degraded"})
		Expect(ExitCode(wrapped)).To(Equal(2))
	})

	It("maps any other error to 1", func() {
		Expect(ExitCode(errors.New("boom"))).To(Equal(1))
	})
})
