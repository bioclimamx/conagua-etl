package validate_test

// The HTML report parsed rather than grepped: the summary totals, the
// Rules table — one row per rule, in order, six cells each — and one
// section per rule whose findings table carries the rule's findings
// row for row (severity with its class, the station or the dash, the
// issue text unescaped back to what was written). The Run → report seam
// is proven with the DB as the oracle: a report rendered from a real
// Run's Report lists exactly the rows the run wrote, per rule.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"golang.org/x/net/html"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// domRow is one table row: its cells' text and each cell's class.
type domRow struct {
	Cells   []string
	Classes []string
}

// domSection is one per-rule section of the report.
type domSection struct {
	Heading  string
	Meta     string
	Findings []domRow
	Empty    string // the "No findings." marker, when present
}

// reportDOM is the report's content, as parsed.
type reportDOM struct {
	Title    string
	Sub      string
	Summary  []domRow // dt/dd pairs: [label, value] with the dd's class
	Rules    []domRow // the Rules table, header excluded
	Sections []domSection
}

func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

func text(n *html.Node) string {
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.TextNode {
			b.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return strings.TrimSpace(b.String())
}

// children lists n's element children of the given tag, one level deep.
func children(n *html.Node, tag string) []*html.Node {
	var out []*html.Node
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && c.Data == tag {
			out = append(out, c)
		}
	}
	return out
}

// find returns every element under n with the given tag, in document
// order.
func find(n *html.Node, tag string) []*html.Node {
	var out []*html.Node
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == tag {
			out = append(out, n)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return out
}

func first(n *html.Node, tag string) *html.Node {
	GinkgoHelper()
	all := find(n, tag)
	Expect(all).NotTo(BeEmpty(), tag)
	return all[0]
}

// tableRows parses a table's rows, dropping the header row (th cells).
func tableRows(table *html.Node) []domRow {
	// html.Parse inserts the implicit tbody.
	var trs []*html.Node
	for _, tbody := range children(table, "tbody") {
		trs = append(trs, children(tbody, "tr")...)
	}
	trs = append(trs, children(table, "tr")...)
	var out []domRow
	for _, tr := range trs {
		tds := children(tr, "td")
		if len(tds) == 0 {
			continue
		}
		var r domRow
		for _, td := range tds {
			r.Cells = append(r.Cells, text(td))
			r.Classes = append(r.Classes, strings.TrimSpace(attr(td, "class")))
		}
		out = append(out, r)
	}
	return out
}

// parseReport parses the report at path into its content.
func parseReport(path string) reportDOM {
	GinkgoHelper()
	data, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	Expect(string(data)).To(HavePrefix("<!DOCTYPE html>\n"))
	doc, err := html.Parse(strings.NewReader(string(data)))
	Expect(err).NotTo(HaveOccurred())

	var out reportDOM
	out.Title = text(first(doc, "title"))
	main := first(doc, "main")
	Expect(text(first(main, "h1"))).To(Equal("Validate report"))
	out.Sub = text(first(main, "p"))

	dl := first(main, "dl")
	dts, dds := children(dl, "dt"), children(dl, "dd")
	Expect(dds).To(HaveLen(len(dts)))
	for i := range dts {
		out.Summary = append(out.Summary, domRow{
			Cells:   []string{text(dts[i]), text(dds[i])},
			Classes: []string{attr(dts[i], "class"), attr(dds[i], "class")},
		})
	}

	out.Rules = tableRows(children(main, "table")[0])

	for _, div := range children(main, "div") {
		if attr(div, "class") != "rule" {
			continue
		}
		sec := domSection{Heading: text(first(div, "h3")), Meta: text(first(div, "p"))}
		if tables := children(div, "table"); len(tables) > 0 {
			sec.Findings = tableRows(tables[0])
		}
		for _, p := range children(div, "p") {
			if attr(p, "class") == "empty" {
				sec.Empty = text(p)
			}
		}
		out.Sections = append(out.Sections, sec)
	}
	return out
}

func writeReport(r *validate.Report) string {
	GinkgoHelper()
	path := filepath.Join(GinkgoT().TempDir(), "report.html")
	Expect(validate.WriteHTMLReport(path, r)).To(Succeed())
	return path
}

// findingRow is the expected findings-table row for one finding.
func findingRow(f validate.Finding) domRow {
	severityClass := "warn"
	if f.Severity == validate.SeverityError {
		severityClass = "fail"
	}
	station := "—"
	if f.StationID != nil {
		station = fmt.Sprint(*f.StationID)
	}
	return domRow{Cells: []string{f.Severity, station, f.Issue}, Classes: []string{severityClass, "", "long"}}
}

// ruleRow is the expected Rules-table row for one RuleReport.
func ruleRow(rr validate.RuleReport) domRow {
	// The template renders `class="num "` (a trailing space) for a clean
	// rule; the parser trims cell classes.
	errClass := "num"
	if rr.Errors > 0 {
		errClass = "num fail"
	}
	return domRow{
		Cells: []string{rr.ID, rr.Name, fmt.Sprint(rr.Scanned), fmt.Sprint(rr.Warnings), fmt.Sprint(rr.Errors),
			rr.Elapsed.Round(time.Millisecond).String()},
		Classes: []string{"", "", "num", "num warn", errClass, "num"},
	}
}

func expectReportOf(dom reportDOM, r *validate.Report) {
	GinkgoHelper()
	errClass := "ok"
	if r.ErrorsTotal > 0 {
		errClass = "fail"
	}
	Expect(dom.Summary).To(Equal([]domRow{
		{Cells: []string{"warnings", fmt.Sprint(r.WarningsTotal)}, Classes: []string{"", "warn"}},
		{Cells: []string{"errors", fmt.Sprint(r.ErrorsTotal)}, Classes: []string{"", errClass}},
	}))
	var rules []domRow
	var sections []domSection
	for _, rr := range r.PerRule {
		rules = append(rules, ruleRow(rr))
		sec := domSection{
			Heading: rr.ID + " — " + rr.Name,
			Meta: fmt.Sprintf("scanned %d · %d warning(s), %d error(s) · %s",
				rr.Scanned, rr.Warnings, rr.Errors, rr.Elapsed.Round(time.Millisecond)),
		}
		if len(rr.Findings) == 0 {
			sec.Empty = "No findings."
		}
		for _, f := range rr.Findings {
			sec.Findings = append(sec.Findings, findingRow(f))
		}
		sections = append(sections, sec)
	}
	Expect(dom.Rules).To(Equal(rules))
	Expect(dom.Sections).To(Equal(sections))
}

var _ = Describe("WriteHTMLReport, parsed", func() {
	It("renders the totals, one Rules row per rule in order, and per rule its findings row for row — severity class, station or dash, issue — or the empty marker", func() {
		sid := int64(42)
		r := &validate.Report{
			StartedAt:     time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC),
			FinishedAt:    time.Date(2026, 8, 30, 12, 4, 15, 149_000_000, time.UTC),
			Period:        "1981-2010",
			WarningsTotal: 3,
			ErrorsTotal:   1,
			PerRule: []validate.RuleReport{
				{ID: "orphan-runs", Name: "Orphan run reconciliation", Scanned: 1, Warnings: 1, Elapsed: 400 * time.Microsecond,
					Findings: []validate.Finding{
						{RuleID: "orphan-runs", Severity: validate.SeverityWarn,
							Issue: "power_runs.id=1 started 2026-06-09T00:00:00Z, status='running' beyond 24h0m0s — reconciled to 'aborted'"},
					}},
				{ID: "bbox", Name: "Lat/lon plausibility", Scanned: 5524, Warnings: 2, Errors: 1, Elapsed: 5 * time.Millisecond,
					Findings: []validate.Finding{
						{RuleID: "bbox", Severity: validate.SeverityWarn, StationID: &sid, Issue: `station conagua_conventional/1001 ("A & B") has NULL lat`},
						{RuleID: "bbox", Severity: validate.SeverityError, StationID: &sid, Issue: "impossible <lat>"},
						{RuleID: "bbox", Severity: validate.SeverityWarn, Issue: "a station-less warn"},
					}},
				{ID: "cross-period", Name: "Cross-period consistency", Scanned: 72240, Elapsed: 195 * time.Millisecond},
			},
		}
		dom := parseReport(writeReport(r))
		Expect(dom.Title).To(Equal("Validate report — 2026-08-30T12:00:00Z"))
		Expect(dom.Sub).To(Equal("2026-08-30T12:00:00Z → 2026-08-30T12:04:15Z\n     · 4m15.149s\n     · period 1981-2010"))
		expectReportOf(dom, r)
		// Spelled out once, so the helper's expectations are themselves
		// visible.
		Expect(dom.Rules[1]).To(Equal(domRow{
			Cells:   []string{"bbox", "Lat/lon plausibility", "5524", "2", "1", "5ms"},
			Classes: []string{"", "", "num", "num warn", "num fail", "num"},
		}))
		Expect(dom.Sections[1].Findings).To(Equal([]domRow{
			{Cells: []string{"warn", "42", `station conagua_conventional/1001 ("A & B") has NULL lat`}, Classes: []string{"warn", "", "long"}},
			{Cells: []string{"error", "42", "impossible <lat>"}, Classes: []string{"fail", "", "long"}},
			{Cells: []string{"warn", "—", "a station-less warn"}, Classes: []string{"warn", "", "long"}},
		}))
		Expect(dom.Sections[2]).To(Equal(domSection{
			Heading: "cross-period — Cross-period consistency",
			Meta:    "scanned 72240 · 0 warning(s), 0 error(s) · 195ms",
			Empty:   "No findings.",
		}))
	})

	It("lists, per rule, exactly the rows a Run wrote — the Run → report seam with the DB as the oracle", func() {
		db, _ := openTempDB()
		s := seedVerb(db)
		insertStationCell(db, 777, cellA)
		report, err := validate.Run(context.Background(), db, validate.Options{})
		Expect(err).NotTo(HaveOccurred())
		dom := parseReport(writeReport(report))
		expectReportOf(dom, report)

		Expect(dom.Sections).To(HaveLen(len(verbOrder)))
		for i, sec := range dom.Sections {
			id := verbOrder[i]
			Expect(sec.Heading).To(HavePrefix(id + " — "))
			var want []domRow
			for _, row := range readWarnings(db, "validate:"+id) {
				station := "—"
				if row.StationID.Valid {
					station = fmt.Sprint(row.StationID.Int64)
				}
				class := "warn"
				if row.Severity == validate.SeverityError {
					class = "fail"
				}
				want = append(want, domRow{Cells: []string{row.Severity, station, row.Issue}, Classes: []string{class, "", "long"}})
			}
			Expect(sec.Findings).To(Equal(want), id)
		}
		Expect(dom.Summary[0].Cells[1]).To(Equal("6"), "seedVerb's six warns")
		Expect(dom.Summary[1].Cells[1]).To(Equal("3"), "the (0, 0) station, the dangling link, the wind direction")
		Expect(len(verbRows(s))).To(Equal(8))
	})

	It("carries every error and the first 50 warns of a rule, in emission order", func() {
		// RuleReport keeps every error and caps warns alone, so a
		// refusal names each error: over 60 warns and 5 errors the page
		// shows 55 rows, not a flat first 50.
		db, _ := openTempDB()
		var warnIDs []int64
		for i := range 60 {
			warnIDs = append(warnIDs, station(db, fmt.Sprintf("w%03d", i), "no-coords", nil, nil))
		}
		for i := range 5 {
			station(db, fmt.Sprintf("e%03d", i), "wherever", f64(0.0), f64(0.0))
		}
		report, err := validate.Run(context.Background(), db, validate.Options{Skip: nativeAnchors()})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.WarningsTotal).To(Equal(60))
		Expect(report.ErrorsTotal).To(Equal(5))
		Expect(countWarnings(db, "validate:bbox")).To(Equal(65), "the DB carries every finding")

		dom := parseReport(writeReport(report))
		expectReportOf(dom, report)
		bbox := dom.Sections[1]
		Expect(bbox.Heading).To(Equal("bbox — Lat/lon plausibility"))
		Expect(bbox.Meta).To(HavePrefix("scanned 65 · 60 warning(s), 5 error(s) · "))
		Expect(bbox.Findings).To(HaveLen(55))
		var warns, errs []domRow
		for _, row := range bbox.Findings {
			if row.Cells[0] == "warn" {
				warns = append(warns, row)
			} else {
				errs = append(errs, row)
			}
		}
		Expect(errs).To(HaveLen(5))
		Expect(warns).To(HaveLen(50))
		for i, row := range warns {
			Expect(row.Cells[1]).To(Equal(fmt.Sprint(warnIDs[i])), "the first 50 warns, in station order")
		}
		Expect(dom.Rules[1].Cells[3:5]).To(Equal([]string{"60", "5"}), "the Rules table counts every finding")
	})

	It("escapes issue text on the way out and yields it back verbatim on the way in", func() {
		issue := `a <script>alert("x")</script> & 'quotes' — reconciled to 'aborted'`
		r := &validate.Report{
			StartedAt: time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC), FinishedAt: time.Date(2026, 8, 30, 12, 0, 1, 0, time.UTC),
			Period: "1991-2020", WarningsTotal: 1,
			PerRule: []validate.RuleReport{{ID: "orphan-runs", Name: "Orphan run reconciliation", Scanned: 1, Warnings: 1,
				Findings: []validate.Finding{{RuleID: "orphan-runs", Severity: validate.SeverityWarn, Issue: issue}}}},
		}
		path := writeReport(r)
		raw, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(raw)).NotTo(ContainSubstring("<script>"))
		Expect(strings.Count(string(raw), "<table>")).To(Equal(2))
		dom := parseReport(path)
		Expect(dom.Sections[0].Findings).To(Equal([]domRow{
			{Cells: []string{"warn", "—", issue}, Classes: []string{"warn", "", "long"}},
		}))
	})
})
