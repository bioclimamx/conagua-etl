package cmd

// Direct specs for the validate verb's flag resolution — the checks
// that run before the database is opened — and the summary printer.
// The rule ids come from the validate registry, never re-enumerated
// here; the period is any YYYY-YYYY window.

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

var _ = Describe("validate flag resolution", func() {
	Describe("resolveSkip", func() {
		It("accepts every registry rule id, trimming whitespace and dropping empty entries", func() {
			var ids []string
			for _, r := range validate.AllRules(validate.DefaultPeriod) {
				ids = append(ids, " "+r.ID+" ")
			}
			ids = append(ids, "", "  ")
			skip, err := resolveSkip(ids)
			Expect(err).NotTo(HaveOccurred())
			Expect(skip).To(HaveLen(len(validate.AllRules(validate.DefaultPeriod))))
			for _, r := range validate.AllRules(validate.DefaultPeriod) {
				Expect(skip).To(HaveKeyWithValue(r.ID, true))
			}
		})

		It("returns an empty set for no ids", func() {
			skip, err := resolveSkip(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(skip).To(BeEmpty())
		})

		It("refuses an unknown id, naming it and listing the valid ids in execution order", func() {
			_, err := resolveSkip([]string{"bbox", "nope"})
			Expect(err).To(MatchError(`--skip: unknown rule "nope" (valid: ` + strings.Join(validateRuleIDs(), ", ") + ")"))
		})
	})

	Describe("resolvePeriod", func() {
		It("accepts each CONAGUA normals period and any other YYYY-YYYY window, a single year included", func() {
			for _, p := range append(validMonthlyPeriods(), "2001-2020", "1990-2019", "2020-2020", "0001-9999") {
				Expect(resolvePeriod(p)).To(Succeed(), p)
			}
		})

		It("refuses a shape other than YYYY-YYYY, quoting it", func() {
			for _, p := range []string{"1991", "1991-2020-x", "19912020", " 1991-2020", "1991-2020 ", "1991–2020", "abcd-efgh", "91-20", ""} {
				Expect(resolvePeriod(p)).To(MatchError(fmt.Sprintf("--period: %q is not a YYYY-YYYY window", p)), p)
			}
		})

		It("refuses a window that ends before it starts", func() {
			Expect(resolvePeriod("2020-1991")).To(MatchError("--period: 2020-1991 ends before it starts"))
		})
	})

	Describe("checkDBExists", func() {
		It("accepts an existing file and refuses a missing one by name, never creating it", func() {
			dir := GinkgoT().TempDir()
			present := filepath.Join(dir, "present.db")
			Expect(os.WriteFile(present, nil, 0o644)).To(Succeed())
			Expect(checkDBExists(present)).To(Succeed())
			missing := filepath.Join(dir, "missing.db")
			Expect(checkDBExists(missing)).To(MatchError("--db: " + missing + " does not exist (validate never creates a database)"))
			Expect(missing).NotTo(BeAnExistingFile())
		})
	})

	It("lists the registry's rule ids in execution order", func() {
		want := make([]string, 0)
		for _, r := range validate.AllRules(validate.DefaultPeriod) {
			want = append(want, r.ID)
		}
		Expect(validateRuleIDs()).To(Equal(want))
		Expect(want).To(ContainElements("orphan-runs", "bbox", "wmo-month-completeness", "daily-sanity", "cross-period"))
	})
})

var _ = Describe("printValidateReport", func() {
	report := &validate.Report{
		StartedAt:     time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC),
		FinishedAt:    time.Date(2026, 8, 30, 10, 4, 15, 149_000_000, time.UTC),
		Period:        "1991-2020",
		WarningsTotal: 81757,
		ErrorsTotal:   0,
		PerRule: []validate.RuleReport{
			{ID: "orphan-runs", Scanned: 0, Warnings: 0, Errors: 0, Elapsed: 0},
			{ID: "bbox", Scanned: 5524, Warnings: 0, Errors: 0, Elapsed: 5 * time.Millisecond},
			{ID: "wmo-month-completeness", Scanned: 927354, Warnings: 9581, Errors: 0, Elapsed: 33548 * time.Millisecond},
			{ID: "daily-sanity", Scanned: 1365692, Warnings: 71991, Errors: 0, Elapsed: 221148 * time.Millisecond},
			{ID: "cross-period", Scanned: 72240, Warnings: 185, Errors: 0, Elapsed: 195 * time.Millisecond},
		},
	}

	It("prints the summary table for a complete run", func() {
		var out bytes.Buffer
		printValidateReport(&out, report, true)
		Expect(out.String()).To(Equal("\n" +
			"validate complete\n" +
			"  period            : 1991-2020\n" +
			"  warnings          : 81757\n" +
			"  errors            : 0\n" +
			"  elapsed           : 4m15.149s\n" +
			"\n" +
			"  rule                     scanned   warn   error  elapsed\n" +
			"  orphan-runs                    0      0       0  0s\n" +
			"  bbox                        5524      0       0  5ms\n" +
			"  wmo-month-completeness    927354   9581       0  33.548s\n" +
			"  daily-sanity             1365692  71991       0  3m41.148s\n" +
			"  cross-period               72240    185       0  195ms\n"))
	})

	It("heads an incomplete run as aborted and omits the elapsed line while FinishedAt is unset", func() {
		partial := *report
		partial.FinishedAt = time.Time{}
		partial.PerRule = partial.PerRule[:2]
		var out bytes.Buffer
		printValidateReport(&out, &partial, false)
		Expect(out.String()).To(HavePrefix("\nvalidate aborted\n  period            : 1991-2020\n"))
		Expect(out.String()).NotTo(ContainSubstring("elapsed           :"))
		Expect(out.String()).To(HaveSuffix("  orphan-runs                    0      0       0  0s\n" +
			"  bbox                        5524      0       0  5ms\n"))
	})
})
