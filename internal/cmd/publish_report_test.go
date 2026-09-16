package cmd

// Direct specs for the end-of-run summary's gate line: the verdict is
// passed, refused, or — when a rule's own error stopped the evaluation
// — aborted, with the counts of the rules that completed; absent when
// the gate never ran.

import (
	"bytes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

var _ = Describe("printPublishReport's gate line", func() {
	// completed is a gate report of the first n rules, clean.
	completed := func(n int) *validate.GateReport {
		g := &validate.GateReport{}
		for _, r := range validate.GateRules()[:n] {
			g.Rules = append(g.Rules, validate.RuleReport{ID: r.ID, Name: r.Name})
		}
		return g
	}

	summarize := func(r *publish.Report, complete bool) string {
		var buf bytes.Buffer
		printPublishReport(&buf, r, complete)
		return buf.String()
	}

	It("reads passed with every rule, refused on an error, aborted when the rule set is incomplete", func() {
		all := len(validate.GateRules())
		Expect(summarize(&publish.Report{Gate: completed(all)}, true)).To(ContainSubstring(
			"  gate                 : passed · 9 rules · 0 warn · 0 error\n"))

		refused := completed(all)
		refused.Rules[0].Errors, refused.Errors, refused.Warnings = 2, 2, 1
		Expect(summarize(&publish.Report{Gate: refused}, false)).To(ContainSubstring(
			"  gate                 : refused · 9 rules · 1 warn · 2 error\n"))

		Expect(summarize(&publish.Report{Gate: completed(4)}, false)).To(ContainSubstring(
			"  gate                 : aborted · 4 rules · 0 warn · 0 error\n"))
	})

	It("omits the line when the gate never ran", func() {
		Expect(summarize(&publish.Report{}, false)).NotTo(ContainSubstring("  gate "))
	})
})
