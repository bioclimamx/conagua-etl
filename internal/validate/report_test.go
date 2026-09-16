package validate_test

import (
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// The report smoke fixture — the markers an
// operator looks at first — with the rendering of each slot pinned, the
// escaping, and the atomic write.
var _ = Describe("WriteHTMLReport", func() {
	smoke := func() *validate.Report {
		stid := int64(42)
		return &validate.Report{
			StartedAt:     time.Date(2026, 4, 28, 10, 0, 0, 0, time.UTC),
			FinishedAt:    time.Date(2026, 4, 28, 10, 0, 30, 0, time.UTC),
			Period:        "1991-2020",
			WarningsTotal: 3,
			ErrorsTotal:   1,
			PerRule: []validate.RuleReport{
				{
					ID: "bbox", Name: "Lat/lon plausibility",
					Scanned: 5512, Warnings: 2, Errors: 1,
					Elapsed: 50 * time.Millisecond,
					Findings: []validate.Finding{
						{StationID: &stid, RuleID: "bbox", Severity: validate.SeverityError,
							Issue: "station x at impossible lat=99.0 lon=-100.0"},
						{StationID: &stid, RuleID: "bbox", Severity: validate.SeverityWarn,
							Issue: "station y outside MX bbox"},
					},
				},
				{
					ID: "wmo-month-completeness", Name: "WMO §4.4.1 within-month completeness",
					Scanned: 100000, Warnings: 1, Errors: 0,
					Elapsed: 200 * time.Millisecond,
				},
			},
		}
	}

	render := func(r *validate.Report) (string, string) {
		GinkgoHelper()
		path := filepath.Join(GinkgoT().TempDir(), "report.html")
		Expect(validate.WriteHTMLReport(path, r)).To(Succeed())
		data, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		return string(data), path
	}

	It("renders the reference smoke fixture: title, period, rule ids and names, sample issues, scanned counts", func() {
		html, _ := render(smoke())
		for _, want := range []string{
			"Validate report",
			"1991-2020",
			"bbox",
			"Lat/lon plausibility",
			"WMO §4.4.1 within-month completeness",
			"impossible lat=99.0",
			"outside MX bbox",
			"5512",
		} {
			Expect(html).To(ContainSubstring(want))
		}
		Expect(html).To(HavePrefix("<!DOCTYPE html>\n<html lang=\"en\">"))
		Expect(html).To(ContainSubstring("<title>Validate report — 2026-04-28T10:00:00Z</title>"))
		Expect(html).To(ContainSubstring("2026-04-28T10:00:00Z → 2026-04-28T10:00:30Z\n     · 30s\n     · period 1991-2020"))
		Expect(html).To(ContainSubstring(`<dt>warnings</dt><dd class="warn">3</dd>`))
		Expect(html).To(ContainSubstring(`<dt>errors</dt><dd class="fail">1</dd>`))
		Expect(html).To(ContainSubstring("<td>bbox</td>\n      <td>Lat/lon plausibility</td>\n      <td class=\"num\">5512</td>\n" +
			"      <td class=\"num warn\">2</td>\n      <td class=\"num fail\">1</td>\n      <td class=\"num\">50ms</td>"))
		Expect(html).To(ContainSubstring("<td>wmo-month-completeness</td>\n      <td>WMO §4.4.1 within-month completeness</td>\n" +
			"      <td class=\"num\">100000</td>\n      <td class=\"num warn\">1</td>\n      <td class=\"num \">0</td>\n      <td class=\"num\">200ms</td>"))
		Expect(html).To(ContainSubstring("<h3>bbox — Lat/lon plausibility</h3>\n    <p class=\"meta\">scanned 5512 · 2 warning(s), 1 error(s) · 50ms</p>"))
		Expect(html).To(ContainSubstring(`<td class="fail">error</td>` + "\n        <td>42</td>\n        <td class=\"long\">station x at impossible lat=99.0 lon=-100.0</td>"))
		Expect(html).To(ContainSubstring(`<td class="warn">warn</td>` + "\n        <td>42</td>\n        <td class=\"long\">station y outside MX bbox</td>"))
		Expect(html).To(ContainSubstring("<h3>wmo-month-completeness — WMO §4.4.1 within-month completeness</h3>\n" +
			"    <p class=\"meta\">scanned 100000 · 1 warning(s), 0 error(s) · 200ms</p>\n    \n    <p class=\"empty\">No findings.</p>"))
		Expect(html).To(HaveSuffix("</main>\n</body>\n</html>\n"))
	})

	It("renders a station-less finding as a dash, a clean error count as ok, and escapes issue text", func() {
		r := &validate.Report{
			StartedAt: time.Date(2026, 4, 28, 10, 0, 0, 0, time.UTC), FinishedAt: time.Date(2026, 4, 28, 10, 0, 1, 500_000_000, time.UTC),
			Period: "1961-1990", WarningsTotal: 1,
			PerRule: []validate.RuleReport{{
				ID: "orphan-runs", Name: "Orphan run reconciliation", Scanned: 1, Warnings: 1, Elapsed: 1234 * time.Microsecond,
				Findings: []validate.Finding{{RuleID: "orphan-runs", Severity: validate.SeverityWarn,
					Issue: "ingest_runs.id=7 started 2020-01-01T00:00:00Z, status='running' beyond 24h0m0s — reconciled to 'aborted' <b>"}},
			}},
		}
		html, _ := render(r)
		Expect(html).To(ContainSubstring("· 1.5s\n     · period 1961-1990"))
		Expect(html).To(ContainSubstring(`<dt>errors</dt><dd class="ok">0</dd>`))
		Expect(html).To(ContainSubstring(`<td class="warn">warn</td>` + "\n        <td>—</td>\n        <td class=\"long\">ingest_runs.id=7 started 2020-01-01T00:00:00Z, status=&#39;running&#39; beyond 24h0m0s — reconciled to &#39;aborted&#39; &lt;b&gt;</td>"))
		Expect(html).To(ContainSubstring(`<td class="num">1ms</td>`))
		Expect(html).NotTo(ContainSubstring("No findings."))
	})

	It("lands world-readable (0644)", func() {
		_, path := render(smoke())
		info, err := os.Stat(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Mode().Perm()).To(Equal(os.FileMode(0o644)))
	})

	It("writes atomically: an existing report is replaced and no temp sibling remains", func() {
		first := smoke()
		html, path := render(first)
		Expect(html).To(ContainSubstring("station y outside MX bbox"))
		second := smoke()
		second.PerRule[0].Findings = nil
		second.WarningsTotal, second.ErrorsTotal = 0, 0
		Expect(validate.WriteHTMLReport(path, second)).To(Succeed())
		data, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).NotTo(ContainSubstring("station y outside MX bbox"))
		Expect(string(data)).To(ContainSubstring(`<dd class="ok">0</dd>`))
		entries, err := os.ReadDir(filepath.Dir(path))
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(HaveLen(1))
		Expect(entries[0].Name()).To(Equal("report.html"))
	})

	It("refuses an unwritable path without touching anything else", func() {
		dir := GinkgoT().TempDir()
		blocker := filepath.Join(dir, "report.html")
		Expect(os.Mkdir(blocker, 0o755)).To(Succeed())
		err := validate.WriteHTMLReport(blocker, smoke())
		Expect(err).To(MatchError(HavePrefix("html report: write file " + blocker)))
	})
})
