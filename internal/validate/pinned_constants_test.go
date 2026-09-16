package validate

// The core QC rules' thresholds and literals, pinned by value from
// inside the package: the ones the rules bind as SQL parameters (the
// 25 °C diurnal range, 3σ, the 24 h orphan cutoff) never appear in the
// statement text pinned_sql_test.go pins, and the rest (the WMO floors,
// the ±3 °C cross-period tolerance, the MX envelope, the sample sizes)
// shape the finding texts and counts the parity harness compares. A
// change to any of these changes the finding set the rules produce, so
// it fails here first.
//
// ginkgo is imported qualified: its exported Report would collide with
// this package's under a dot-import.

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("pinned constants", func() {
	ginkgo.It("holds the orphan-runs cutoff at 24 h", func() {
		Expect(orphanThreshold).To(Equal(24 * time.Hour))
		Expect(runTables).To(Equal([]string{"ingest_runs", "power_runs"}))
	})

	ginkgo.It("holds the WMO §4.4.1 floors: 11 missing days, 5 consecutive", func() {
		Expect(wmoMissingThreshold).To(Equal(11))
		Expect(wmoConsecutiveThreshold).To(Equal(5))
	})

	ginkgo.It("holds daily-sanity's 25 °C diurnal range, 3σ outlier bound, and three inline samples", func() {
		Expect(diurnalRangeThreshold).To(Equal(25.0))
		Expect(zSigma).To(Equal(3.0))
		Expect(topPerGroup).To(Equal(3))
	})

	ginkgo.It("holds cross-period's ±3 °C tolerance", func() {
		Expect(crossPeriodMaxDeltaC).To(Equal(3.0))
	})

	ginkgo.It("holds bbox's MX envelope", func() {
		Expect([]float64{mxLatMin, mxLatMax, mxLonMin, mxLonMax}).To(Equal([]float64{14.5, 32.8, -118.5, -86.7}))
	})

	ginkgo.It("holds the report's 50-sample cap, the 'validate:' namespace, and the 1991-2020 default period", func() {
		Expect(sampleCap).To(Equal(50))
		Expect(sourcePrefix).To(Equal("validate:"))
		Expect(DefaultPeriod).To(Equal("1991-2020"))
		Expect(Finding{RuleID: "daily-sanity"}.SourceFile()).To(Equal("validate:daily-sanity"))
		Expect(Options{}.withDefaults().Period).To(Equal(DefaultPeriod))
		Expect(Options{Period: "1961-1990"}.withDefaults().Period).To(Equal("1961-1990"))
	})

	ginkgo.It("holds the two severity literals the parsing_warnings CHECK admits", func() {
		Expect(SeverityWarn).To(Equal("warn"))
		Expect(SeverityError).To(Equal("error"))
	})
})
