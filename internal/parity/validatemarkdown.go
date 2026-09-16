package parity

import (
	"fmt"
	"strconv"
	"strings"
)

// Markdown renders the validate comparison as a self-contained document:
// a per-rule summary over the reference rules, the two tools' per-rule
// summaries side by side, the native rules' counts, then a findings
// section per reference rule. Sample lists cap at DBSampleCap; every
// truncation is noted explicitly so a capped list can never be mistaken
// for a complete one.
func (c *ValidateComparison) Markdown() string {
	var b strings.Builder
	b.WriteString("# Validate parity report\n\n")
	fmt.Fprintf(&b, "- Base DB (the reference validate ran on it): `%s`\n", c.BaseLabel)
	fmt.Fprintf(&b, "- New DB (this repo's validate ran on it): `%s`\n\n", c.NewLabel)
	b.WriteString("`parsing_warnings` rows under `source_file LIKE 'validate:%'` are compared\n" +
		"as multisets keyed by (station `source/external_id` through the stations\n" +
		"join — a NULL `station_id` compares as NULL —, `source_file`, `severity`,\n" +
		"`issue`),\n" +
		"one tally per reference rule. Surrogate ids and\n" +
		"the `line` column never take part. A tuple whose multiplicity differs\n" +
		"appears under the side that carries the excess rows, with both counts.\n" +
		"`orphan-runs` is wall-clock dependent: it reconciles the stranded runs it\n" +
		"finds, so only a first pass over a copy reports them.\n\n")

	b.WriteString("## Summary\n\n")
	b.WriteString("| Rule | Base rows | New rows | Identical | Base-only rows | New-only rows |\n")
	b.WriteString("|---|--:|--:|--:|--:|--:|\n")
	for _, r := range c.Rules {
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d |\n",
			r.ID, r.BaseRows, r.NewRows, r.Identical, r.BaseOnlyRows, r.NewOnlyRows)
	}
	baseOnly, newOnly := 0, 0
	for _, r := range c.Rules {
		baseOnly += r.BaseOnlyRows
		newOnly += r.NewOnlyRows
	}
	fmt.Fprintf(&b, "| **total** | %d | %d | %d | %d | %d |\n\n",
		c.BaseRows(), c.NewRows(), c.Identical(), baseOnly, newOnly)

	writeValidateSummaries(&b, c)
	writeNativeSection(&b, c.Native)

	for _, r := range c.Rules {
		writeValidateRuleSection(&b, r)
	}
	return b.String()
}

// writeValidateSummaries renders both tools' per-rule summaries: the
// base side's warn / error tallies recovered from its rows (its
// Scanned is not stored anywhere), and this repo's Scanned / warn /
// error from the Report the caller supplied — dashes when it did not.
func writeValidateSummaries(b *strings.Builder, c *ValidateComparison) {
	b.WriteString("## Per-rule tool summaries\n\n")
	b.WriteString("Base warn / error are the base rows' severity tallies; the reference\n" +
		"build's scanned counts are not stored in the DB. This repo's columns come\n" +
		"from its Run report" + summaryAvailability(c) + ".\n\n")
	b.WriteString("| Rule | Base warn | Base error | Ours scanned | Ours warn | Ours error |\n")
	b.WriteString("|---|--:|--:|--:|--:|--:|\n")
	for _, r := range c.Rules {
		scanned, warn, errs := "—", "—", "—"
		if s, ok := c.Summary(r.ID); ok {
			scanned = strconv.Itoa(s.Scanned)
			warn = strconv.Itoa(s.Warnings)
			errs = strconv.Itoa(s.Errors)
		}
		fmt.Fprintf(b, "| %s | %d | %d | %s | %s | %s |\n",
			r.ID, r.BaseWarnings, r.BaseErrors, scanned, warn, errs)
	}
	b.WriteString("\n")
}

// summaryAvailability phrases whether the new side's summaries were
// supplied.
func summaryAvailability(c *ValidateComparison) string {
	if len(c.NewSummary) == 0 {
		return " (not supplied for this comparison; a skipped rule has no row)"
	}
	return " (a skipped rule has no row)"
}

// writeNativeSection renders the rule ids with no reference counterpart —
// counted per side, never compared.
func writeNativeSection(b *strings.Builder, native []ValidateNativeRule) {
	b.WriteString("## Native rules (reported, not compared)\n\n")
	if len(native) == 0 {
		b.WriteString("No `validate:` rows outside the reference rule set on either side.\n\n")
		return
	}
	b.WriteString("| Rule | Base rows | New rows |\n|---|--:|--:|\n")
	for _, n := range native {
		fmt.Fprintf(b, "| %s | %d | %d |\n", n.ID, n.BaseRows, n.NewRows)
	}
	b.WriteString("\n")
}

// writeValidateRuleSection renders one reference rule's findings, omitting
// empty categories.
func writeValidateRuleSection(b *strings.Builder, r ValidateRule) {
	fmt.Fprintf(b, "## %s\n\n", r.ID)
	if r.Clean() {
		fmt.Fprintf(b, "No drift: all %d rows identical.\n\n", r.Identical)
		return
	}
	writeRowDiffs(b, "Base-only tuples", r.BaseOnly, r.BaseOnlyRows)
	writeRowDiffs(b, "New-only tuples", r.NewOnly, r.NewOnlyRows)
}

// writeRowDiffs renders a sampled one-side-only tuple list as a table
// carrying both sides' multiplicities.
func writeRowDiffs(b *strings.Builder, title string, diffs Sampled[ValidateRowDiff], rows int) {
	if diffs.Total == 0 {
		return
	}
	fmt.Fprintf(b, "### %s (%d, %d rows)\n\n", title, diffs.Total, rows)
	b.WriteString("| Station | Severity | Issue | Base | New |\n|---|---|---|--:|--:|\n")
	for _, d := range diffs.Samples {
		station := d.Warning.Station
		if station == "" {
			station = renderedNull
		}
		fmt.Fprintf(b, "| %s | %s | %s | %d | %d |\n",
			cell(station), d.Warning.Severity, cell(d.Warning.Issue), d.Base, d.New)
	}
	sampleTruncNote(b, diffs)
	b.WriteString("\n")
}
