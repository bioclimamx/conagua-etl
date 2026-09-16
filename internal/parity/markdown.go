package parity

import (
	"fmt"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

// Markdown renders the report as a self-contained document: a summary
// table over every kind first, then a findings section per
// value-compared kind. Example lists are capped at ExampleCap entries;
// every truncation is noted inline so a capped list can never be
// mistaken for a complete one.
func (r Report) Markdown() string {
	var b strings.Builder
	b.WriteString("# Cross-snapshot parity report\n\n")
	fmt.Fprintf(&b, "- Base snapshot: `%s`\n", r.BaseDir)
	fmt.Fprintf(&b, "- New snapshot: `%s`\n\n", r.NewDir)
	b.WriteString("Comparison is value-level through the `internal/conagua` parsers:\n" +
		"parsed fields are compared one by one, never raw bytes\n" +
		"or sha256 digests. CONAGUA's per-emission header stamps are excluded by\n" +
		"construction — the parsers do not capture them.\n\n")

	b.WriteString("## Summary\n\n")
	b.WriteString("| Kind | Mode | Base files | New files | Both | Base-only | New-only | Parse errors | Stations identical | Stations revised |\n")
	b.WriteString("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|\n")
	for _, kr := range r.Kinds {
		if kr.ValueCompared {
			fmt.Fprintf(&b, "| %s | value | %d | %d | %d | %d | %d | %d | %d | %d |\n",
				kr.Kind, kr.BaseFiles, kr.NewFiles, kr.StationsBoth,
				kr.BaseOnly.Total, kr.NewOnly.Total, kr.ParseErrors.Total,
				kr.StationsIdentical, kr.StationsRevised)
		} else {
			fmt.Fprintf(&b, "| %s | presence-only | %d | %d | %d | %d | %d | — | — | — |\n",
				kr.Kind, kr.BaseFiles, kr.NewFiles, kr.StationsBoth,
				kr.BaseOnly.Total, kr.NewOnly.Total)
		}
	}
	b.WriteString("\n")
	b.WriteString("`monthly` and `extremes` are **presence-only**: neither this repo nor\n" +
		"the reference build has a parser for those kinds, so their values are *not*\n" +
		"compared — the file-presence counts above are the entire claim for them.\n\n")

	for _, kr := range r.Kinds {
		if kr.ValueCompared {
			writeKindSection(&b, kr)
		}
	}
	return b.String()
}

// writeKindSection renders the findings for one value-compared kind,
// omitting empty categories.
func writeKindSection(b *strings.Builder, kr KindReport) {
	fmt.Fprintf(b, "## %s\n\n", kr.Kind)

	if kr.Kind == conagua.KindDaily {
		b.WriteString("| Rows identical | Rows appended (new-only dates) | Rows removed (base-only dates) | Rows revised |\n|--:|--:|--:|--:|\n")
		fmt.Fprintf(b, "| %d | %d | %d | %d |\n\n",
			kr.RowsIdentical, kr.RowsAppended.Total, kr.RowsRemoved.Total, kr.RowsRevised)
	}

	if kindClean(kr) {
		fmt.Fprintf(b, "No drift: all %d stations present on both sides parsed identically.\n\n", kr.StationsBoth)
		return
	}

	if kr.ParseErrors.Total > 0 {
		fmt.Fprintf(b, "### Parse errors (%d)\n\n", kr.ParseErrors.Total)
		b.WriteString("| Station | Side | Error |\n|---|---|---|\n")
		for _, pe := range kr.ParseErrors.Examples {
			fmt.Fprintf(b, "| %s | %s | %s |\n", pe.Station, pe.Side, cell(pe.Err))
		}
		truncNote(b, kr.ParseErrors)
		b.WriteString("\n")
	}

	if !kr.UniverseIdentical() {
		b.WriteString("### Station universe drift\n\n")
		writeStationList(b, "Base-only", kr.BaseOnly)
		writeStationList(b, "New-only", kr.NewOnly)
		b.WriteString("\n")
	}

	if kr.Revisions.Total > 0 {
		fmt.Fprintf(b, "### Field revisions (%d)\n\n", kr.Revisions.Total)
		if kr.Kind == conagua.KindDaily {
			b.WriteString("| Station | Date | Field | Base | New |\n|---|---|---|---|---|\n")
			for _, rev := range kr.Revisions.Examples {
				fmt.Fprintf(b, "| %s | %s | %s | %s | %s |\n",
					rev.Station, rev.Date, rev.Field, cell(rev.Base), cell(rev.New))
			}
		} else {
			b.WriteString("| Station | Month | Field | Base | New |\n|---|--:|---|---|---|\n")
			for _, rev := range kr.Revisions.Examples {
				fmt.Fprintf(b, "| %s | %d | %s | %s | %s |\n",
					rev.Station, rev.Month, rev.Field, cell(rev.Base), cell(rev.New))
			}
		}
		truncNote(b, kr.Revisions)
		b.WriteString("\n")
	}

	writeRowRefs(b, "Appended rows (dates only in new)", kr.RowsAppended)
	writeRowRefs(b, "Removed rows (dates only in base)", kr.RowsRemoved)

	if kr.HeaderDiffs.Total > 0 {
		fmt.Fprintf(b, "### Header diffs (%d)\n\n", kr.HeaderDiffs.Total)
		b.WriteString("| Station | Field | Base | New |\n|---|---|---|---|\n")
		for _, rev := range kr.HeaderDiffs.Examples {
			fmt.Fprintf(b, "| %s | %s | %s | %s |\n",
				rev.Station, rev.Field, cell(rev.Base), cell(rev.New))
		}
		truncNote(b, kr.HeaderDiffs)
		b.WriteString("\n")
	}

	if kr.WarningDiffs.Total > 0 {
		fmt.Fprintf(b, "### Warning-count diffs (%d)\n\n", kr.WarningDiffs.Total)
		b.WriteString("| Station | Base warnings | New warnings |\n|---|--:|--:|\n")
		for _, wd := range kr.WarningDiffs.Examples {
			fmt.Fprintf(b, "| %s | %d | %d |\n", wd.Station, wd.Base, wd.New)
		}
		truncNote(b, kr.WarningDiffs)
		b.WriteString("\n")
	}
}

// kindClean reports whether a value-compared kind showed no findings at
// all. StationsRevised covers row, header, and warning drift (any of
// them marks the station revised), so three counters suffice.
func kindClean(kr KindReport) bool {
	return kr.ParseErrors.Total == 0 && kr.UniverseIdentical() && kr.StationsRevised == 0
}

// writeRowRefs renders a capped appended/removed row list as a table.
func writeRowRefs(b *strings.Builder, title string, refs Capped[RowRef]) {
	if refs.Total == 0 {
		return
	}
	fmt.Fprintf(b, "### %s (%d)\n\n", title, refs.Total)
	b.WriteString("| Station | Date |\n|---|---|\n")
	for _, ref := range refs.Examples {
		fmt.Fprintf(b, "| %s | %s |\n", ref.Station, ref.Date)
	}
	truncNote(b, refs)
	b.WriteString("\n")
}

// writeStationList renders one side's universe-drift station list.
func writeStationList(b *strings.Builder, label string, stations Capped[string]) {
	if stations.Total == 0 {
		return
	}
	fmt.Fprintf(b, "- %s (%d): ", label, stations.Total)
	for i, st := range stations.Examples {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(b, "`%s`", st)
	}
	if stations.Truncated() {
		fmt.Fprintf(b, " … (+%d more)", stations.Total-len(stations.Examples))
	}
	b.WriteString("\n")
}

// truncNote appends the explicit truncation marker after a capped table.
func truncNote[T any](b *strings.Builder, c Capped[T]) {
	if c.Truncated() {
		fmt.Fprintf(b, "\n_showing first %d of %d_\n", len(c.Examples), c.Total)
	}
}

// cell escapes the markdown table delimiter inside a cell value.
func cell(s string) string {
	return strings.ReplaceAll(s, "|", `\|`)
}
