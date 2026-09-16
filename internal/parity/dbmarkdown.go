package parity

import (
	"fmt"
	"strconv"
	"strings"
)

// Markdown renders the DB comparison as a self-contained document: a
// per-table summary, both sides' latest ingest-run counters, then a
// findings section per table. Sample lists cap at DBSampleCap; every
// truncation is noted explicitly so a capped list can never be
// mistaken for a complete one.
func (c *DBComparison) Markdown() string {
	var b strings.Builder
	b.WriteString("# Ingest DB parity report\n\n")
	fmt.Fprintf(&b, "- Base DB: `%s`\n", c.BaseLabel)
	fmt.Fprintf(&b, "- New DB: `%s`\n\n", c.NewLabel)
	b.WriteString("Rows are aligned on the `(source, external_id)` natural key;\n" +
		"surrogate ids differ between\n" +
		"ingests by construction and are never compared. `parsing_warnings`\n" +
		"rows are distinct (station, source_file, line, severity, issue)\n" +
		"tuples; a multiplicity mismatch surfaces as a `count` value diff.\n\n")

	b.WriteString("## Summary\n\n")
	b.WriteString("| Table | Base rows | New rows | Compared | Identical | Value diffs | Base-only | New-only |\n")
	b.WriteString("|---|--:|--:|--:|--:|--:|--:|--:|\n")
	for _, t := range c.Tables() {
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d | %d | %d |\n",
			t.Table, t.BaseRows(), t.NewRows(), t.RowsCompared, t.RowsIdentical,
			t.ValueDiffs.Total, t.BaseOnly.Total, t.NewOnly.Total)
	}
	b.WriteString("\n")

	writeRunSection(&b, c.BaseRun, c.NewRun)

	for _, t := range c.Tables() {
		writeTableSection(&b, t)
	}
	return b.String()
}

// runFields itemizes every ingest_runs column for the side-by-side run
// table, in the schema's column order.
var runFields = []struct {
	name   string
	render func(*RunCounters) string
}{
	{"id", func(r *RunCounters) string { return strconv.FormatInt(r.ID, 10) }},
	{"started_at", func(r *RunCounters) string { return r.StartedAt }},
	{"finished_at", func(r *RunCounters) string { return renderNullStr(r.FinishedAt) }},
	{"snapshot_date", func(r *RunCounters) string { return r.SnapshotDate }},
	{"sink_kind", func(r *RunCounters) string { return r.SinkKind }},
	{"etl_git_sha", func(r *RunCounters) string { return renderNullStr(r.ETLGitSHA) }},
	{"status", func(r *RunCounters) string { return r.Status }},
	{"stations_attempted", func(r *RunCounters) string { return renderNullInt(r.StationsAttempted) }},
	{"stations_succeeded", func(r *RunCounters) string { return renderNullInt(r.StationsSucceeded) }},
	{"stations_failed", func(r *RunCounters) string { return renderNullInt(r.StationsFailed) }},
	{"daily_rows", func(r *RunCounters) string { return renderNullInt(r.DailyRows) }},
	{"normals_rows", func(r *RunCounters) string { return renderNullInt(r.NormalsRows) }},
	{"extras_rows", func(r *RunCounters) string { return renderNullInt(r.ExtrasRows) }},
	{"warnings_total", func(r *RunCounters) string { return renderNullInt(r.WarningsTotal) }},
}

// writeRunSection renders both sides' latest ingest_runs row verbatim.
// Runs are build metadata, not compared data — the section exists for
// the reader's eye, never for assertions.
func writeRunSection(b *strings.Builder, baseRun, newRun *RunCounters) {
	b.WriteString("## Latest ingest run (reported, not compared)\n\n")
	if baseRun == nil && newRun == nil {
		b.WriteString("Neither database carries an ingest_runs row.\n\n")
		return
	}
	side := func(r *RunCounters, render func(*RunCounters) string) string {
		if r == nil {
			return "— (no run)"
		}
		return render(r)
	}
	b.WriteString("| Column | Base | New |\n|---|---|---|\n")
	for _, f := range runFields {
		fmt.Fprintf(b, "| %s | %s | %s |\n",
			f.name, cell(side(baseRun, f.render)), cell(side(newRun, f.render)))
	}
	b.WriteString("\n")
}

// writeTableSection renders one table's findings, omitting empty
// categories.
func writeTableSection(b *strings.Builder, t TableComparison) {
	fmt.Fprintf(b, "## %s\n\n", t.Table)
	if t.Clean() {
		fmt.Fprintf(b, "No drift: all %d compared rows identical.\n\n", t.RowsCompared)
		return
	}

	if t.ValueDiffs.Total > 0 {
		fmt.Fprintf(b, "### Value diffs (%d)\n\n", t.ValueDiffs.Total)
		b.WriteString("| Key | Column | Base | New |\n|---|---|---|---|\n")
		for _, d := range t.ValueDiffs.Samples {
			fmt.Fprintf(b, "| %s | %s | %s | %s |\n",
				cell(d.Key.String()), d.Column, cell(d.Base), cell(d.New))
		}
		sampleTruncNote(b, t.ValueDiffs)
		b.WriteString("\n")
	}

	writeKeyList(b, "Base-only rows", t.BaseOnly)
	writeKeyList(b, "New-only rows", t.NewOnly)
}

// writeKeyList renders a sampled one-side-only key list as a table.
func writeKeyList(b *strings.Builder, title string, keys Sampled[DBKey]) {
	if keys.Total == 0 {
		return
	}
	fmt.Fprintf(b, "### %s (%d)\n\n", title, keys.Total)
	b.WriteString("| Key |\n|---|\n")
	for _, k := range keys.Samples {
		fmt.Fprintf(b, "| %s |\n", cell(k.String()))
	}
	sampleTruncNote(b, keys)
	b.WriteString("\n")
}

// sampleTruncNote appends the explicit truncation marker after a
// sampled table.
func sampleTruncNote[T any](b *strings.Builder, s Sampled[T]) {
	if s.Truncated() {
		fmt.Fprintf(b, "\n_showing first %d of %d_\n", len(s.Samples), s.Total)
	}
}
