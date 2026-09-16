package parity

import (
	"fmt"
	"strings"
)

// Markdown renders the supplement drift comparison as a self-contained
// document: a per-table summary, then per-parameter tallies, revised
// samples, and one-side-only rows per table. The report is evidence,
// never a gate — nothing in it implies a pass/fail.
func (d *PowerDrift) Markdown() string {
	var b strings.Builder
	b.WriteString("# POWER supplement drift report\n\n")
	fmt.Fprintf(&b, "- Base DB: `%s`\n", d.BaseLabel)
	fmt.Fprintf(&b, "- New DB: `%s`\n\n", d.NewLabel)
	b.WriteString("Report-only evidence: POWER is a\n" +
		"re-versioning reanalysis product, so drift between two pulls is\n" +
		"upstream movement, never a port defect. Every value (row × column)\n" +
		"is classified: identical (equal, including NULL on both sides),\n" +
		"revised (non-NULL on both sides, different), appended (non-NULL in\n" +
		"new only), removed (non-NULL in base only). Values in rows present\n" +
		"on one side only count as appended/removed. `power_run_id` is a\n" +
		"per-side run pointer and is not compared.\n\n")

	b.WriteString("## Summary\n\n")
	b.WriteString("| Table | Base rows | New rows | Aligned | Base-only | New-only | Identical | Revised | Appended | Removed |\n")
	b.WriteString("|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|\n")
	for _, t := range d.Tables() {
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d | %d | %d | %d | %d |\n",
			t.Table, t.BaseRows(), t.NewRows(), t.RowsAligned,
			t.BaseOnly.Total, t.NewOnly.Total,
			t.Values.Identical, t.Values.Revised, t.Values.Appended, t.Values.Removed)
	}
	b.WriteString("\n")

	for _, t := range d.Tables() {
		writeDriftSection(&b, t)
	}
	return b.String()
}

// writeDriftSection renders one table's per-parameter tallies and
// samples.
func writeDriftSection(b *strings.Builder, t DriftTable) {
	fmt.Fprintf(b, "## %s\n\n", t.Table)
	if t.BaseRows() == 0 && t.NewRows() == 0 {
		b.WriteString("No rows on either side.\n\n")
		return
	}

	b.WriteString("### Per-parameter drift\n\n")
	b.WriteString("| Column | Identical | Revised | Appended | Removed |\n")
	b.WriteString("|---|--:|--:|--:|--:|\n")
	for _, c := range t.Columns {
		fmt.Fprintf(b, "| %s | %d | %d | %d | %d |\n",
			c.Column, c.Identical, c.Revised, c.Appended, c.Removed)
	}
	b.WriteString("\n")

	if t.RevisedSamples.Total > 0 {
		fmt.Fprintf(b, "### Revised samples (%d)\n\n", t.RevisedSamples.Total)
		b.WriteString("| Key | Column | Base | New |\n|---|---|---|---|\n")
		for _, s := range t.RevisedSamples.Samples {
			fmt.Fprintf(b, "| %s | %s | %s | %s |\n",
				cell(s.Key.String()), s.Column, cell(s.Base), cell(s.New))
		}
		sampleTruncNote(b, t.RevisedSamples)
		b.WriteString("\n")
	}

	writeKeyList(b, "Base-only rows", t.BaseOnly)
	writeKeyList(b, "New-only rows", t.NewOnly)
}
