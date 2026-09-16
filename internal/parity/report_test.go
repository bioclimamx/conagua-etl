package parity_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
)

var _ = Describe("Capped counters", func() {
	It("retains the first ExampleCap examples and keeps the total exact", func() {
		var c parity.Capped[int]
		for i := 0; i < parity.ExampleCap+3; i++ {
			c.Add(i)
		}
		Expect(c.Total).To(Equal(parity.ExampleCap + 3))
		want := make([]int, parity.ExampleCap)
		for i := range want {
			want[i] = i
		}
		Expect(c.Examples).To(Equal(want))
		Expect(c.Truncated()).To(BeTrue())
	})

	It("is not truncated while under the cap", func() {
		var c parity.Capped[string]
		c.Add("01001")
		Expect(c.Total).To(Equal(1))
		Expect(c.Examples).To(Equal([]string{"01001"}))
		Expect(c.Truncated()).To(BeFalse())
	})

	It("merges exact totals while capping the combined example list", func() {
		var a, b parity.Capped[int]
		for i := 0; i < 60; i++ {
			a.Add(i)
			b.Add(100 + i)
		}
		a.Merge(b)
		Expect(a.Total).To(Equal(120))
		want := make([]int, 0, parity.ExampleCap)
		for i := 0; i < 60; i++ {
			want = append(want, i)
		}
		for i := 0; i < parity.ExampleCap-60; i++ {
			want = append(want, 100+i)
		}
		Expect(a.Examples).To(Equal(want))
		Expect(a.Truncated()).To(BeTrue())
	})

	It("carries a donor's total beyond its retained examples through Merge", func() {
		var donor parity.Capped[string]
		for i := 0; i < parity.ExampleCap+50; i++ {
			donor.Add(fmt.Sprintf("s%03d", i))
		}
		var c parity.Capped[string]
		c.Merge(donor)
		Expect(c.Total).To(Equal(parity.ExampleCap + 50))
		Expect(c.Examples).To(Equal(donor.Examples))
		Expect(c.Truncated()).To(BeTrue())
	})
})

var _ = Describe("KindReport.UniverseIdentical", func() {
	It("is true when no station is one-sided", func() {
		Expect(parity.KindReport{StationsBoth: 3}.UniverseIdentical()).To(BeTrue())
	})

	It("is false when a station exists only in the base snapshot", func() {
		kr := parity.KindReport{BaseOnly: parity.Capped[string]{Total: 1, Examples: []string{"01001"}}}
		Expect(kr.UniverseIdentical()).To(BeFalse())
	})

	It("is false when a station exists only in the new snapshot", func() {
		kr := parity.KindReport{NewOnly: parity.Capped[string]{Total: 1, Examples: []string{"01001"}}}
		Expect(kr.UniverseIdentical()).To(BeFalse())
	})
})

var _ = Describe("Report.TotalParseErrors", func() {
	It("sums parse errors across every kind", func() {
		report := parity.Report{Kinds: []parity.KindReport{
			{ParseErrors: parity.Capped[parity.ParseError]{Total: 2}},
			{},
			{ParseErrors: parity.Capped[parity.ParseError]{Total: 3}},
		}}
		Expect(report.TotalParseErrors()).To(Equal(5))
	})
})
