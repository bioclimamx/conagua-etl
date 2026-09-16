package parity

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
)

// Markdown renders the power parity comparison as a self-contained
// document: the cell-math summary, a findings section per table, then
// every complete run's manifest side by side with the manifest
// findings. Sample lists cap at DBSampleCap with explicit truncation
// notes.
func (c *PowerComparison) Markdown() string {
	var b strings.Builder
	b.WriteString("# Power parity report\n\n")
	fmt.Fprintf(&b, "- DB: `%s`\n\n", c.Label)
	b.WriteString("The offline power parity gate: cells\n" +
		"and station links derived by this repo's ported math from the\n" +
		"stations table must reproduce the stored `nasa_power_grid_cells`\n" +
		"and `station_power_cell` rows float-exactly (609 cells and 5,524\n" +
		"links nationally), and every complete `power_runs` manifest must\n" +
		"equal the pinned constants, the parameter registry, and a BuildURL\n" +
		"round-trip.\n\n")

	b.WriteString("## Summary\n\n")
	fmt.Fprintf(&b, "Stations compared (source `%s`, with coordinates): %d\n\n",
		string(ingest.SourceConaguaConventional), c.StationsCompared)
	b.WriteString("| Table | Derived rows | Stored rows | Compared | Identical | Value diffs | Derived-only | Stored-only |\n")
	b.WriteString("|---|--:|--:|--:|--:|--:|--:|--:|\n")
	for _, t := range c.Tables() {
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d | %d | %d |\n",
			t.Table, t.DerivedRows(), t.StoredRows(), t.RowsCompared, t.RowsIdentical,
			t.ValueDiffs.Total, t.DerivedOnly.Total, t.StoredOnly.Total)
	}
	fmt.Fprintf(&b, "\nComplete power_runs manifests checked: %d; findings: %d\n\n",
		len(c.Runs), c.ManifestFindings.Total)

	for _, t := range c.Tables() {
		writePowerTableSection(&b, t)
	}
	writeManifestSection(&b, c)
	return b.String()
}

// writePowerTableSection renders one cell-math table's findings,
// omitting empty categories.
func writePowerTableSection(b *strings.Builder, t PowerTable) {
	fmt.Fprintf(b, "## %s\n\n", t.Table)
	if t.Clean() {
		fmt.Fprintf(b, "No findings: all %d compared rows identical.\n\n", t.RowsCompared)
		return
	}

	if t.ValueDiffs.Total > 0 {
		fmt.Fprintf(b, "### Value diffs (%d)\n\n", t.ValueDiffs.Total)
		b.WriteString("| Key | Column | Derived | Stored |\n|---|---|---|---|\n")
		for _, d := range t.ValueDiffs.Samples {
			fmt.Fprintf(b, "| %s | %s | %s | %s |\n",
				cell(d.Key.String()), d.Column, cell(d.Derived), cell(d.Stored))
		}
		sampleTruncNote(b, t.ValueDiffs)
		b.WriteString("\n")
	}

	writeKeyList(b, "Derived-only rows", t.DerivedOnly)
	writeKeyList(b, "Stored-only rows", t.StoredOnly)
}

// powerRunFields itemizes the manifest columns for the side-by-side
// run table, in the schema's column order. unit_conversions renders as
// an entry count — its full content is redundant with the registry
// when clean, and any mismatch appears in the findings with exact
// values.
var powerRunFields = []struct {
	name   string
	render func(*PowerRunManifest) string
}{
	{"temporal_mode", func(r *PowerRunManifest) string { return r.Temporal }},
	{"endpoint_url", func(r *PowerRunManifest) string { return r.EndpointURL }},
	{"parameters", func(r *PowerRunManifest) string { return r.Parameters }},
	{"community", func(r *PowerRunManifest) string { return r.Community }},
	{"period_start_year", func(r *PowerRunManifest) string { return strconv.FormatInt(r.PeriodStartYear, 10) }},
	{"period_end_year", func(r *PowerRunManifest) string { return strconv.FormatInt(r.PeriodEndYear, 10) }},
	{"period_start_date", func(r *PowerRunManifest) string { return renderNullStr(r.PeriodStartDate) }},
	{"period_end_date", func(r *PowerRunManifest) string { return renderNullStr(r.PeriodEndDate) }},
	{"grid_resolution", func(r *PowerRunManifest) string { return r.GridResolution }},
	{"solar_conversion", func(r *PowerRunManifest) string { return renderFloat(r.SolarConversion) }},
	{"unit_conversions", summarizeConversions},
}

// summarizeConversions renders the unit_conversions column as its
// entry count for the manifest table.
func summarizeConversions(r *PowerRunManifest) string {
	if !r.UnitConversions.Valid {
		return renderedNull
	}
	var entries map[string]json.RawMessage
	if err := json.Unmarshal([]byte(r.UnitConversions.String), &entries); err != nil {
		return "unparseable JSON"
	}
	return fmt.Sprintf("%d entries", len(entries))
}

// writeManifestSection renders every complete run's manifest columns
// side by side, then the manifest findings.
func writeManifestSection(b *strings.Builder, c *PowerComparison) {
	b.WriteString("## power_runs manifests (status='complete')\n\n")
	if len(c.Runs) == 0 {
		b.WriteString("No complete power_runs rows.\n\n")
	} else {
		b.WriteString("| Column |")
		for i := range c.Runs {
			fmt.Fprintf(b, " run %d |", c.Runs[i].ID)
		}
		b.WriteString("\n|---|")
		b.WriteString(strings.Repeat("---|", len(c.Runs)))
		b.WriteString("\n")
		for _, f := range powerRunFields {
			fmt.Fprintf(b, "| %s |", f.name)
			for i := range c.Runs {
				fmt.Fprintf(b, " %s |", cell(f.render(&c.Runs[i])))
			}
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}

	if c.ManifestFindings.Total == 0 {
		b.WriteString("No manifest findings: every checked field equals its pinned value.\n\n")
		return
	}
	fmt.Fprintf(b, "### Manifest findings (%d)\n\n", c.ManifestFindings.Total)
	b.WriteString("| Run | Field | Got | Want |\n|---|---|---|---|\n")
	for _, f := range c.ManifestFindings.Samples {
		fmt.Fprintf(b, "| %d | %s | %s | %s |\n", f.RunID, f.Field, cell(f.Got), cell(f.Want))
	}
	sampleTruncNote(b, c.ManifestFindings)
	b.WriteString("\n")
}
