// Package publish is the export layer that builds the Zenodo v0.1
// release artifacts from a bioclima.db opened read-only. It owns
// everything the per-file spec derives from schema.sql without touching
// the DDL: the per-file column specs (export rename map + fixed
// precision annotation, natural keys first, DDL order after), the
// fixed-decimal formatter that makes CSV and JSON byte-reproducible,
// the streaming CSV writer, the per-state scoping (lowercase state code
// in paths, the stored uppercase code in values), the four scope
// folders of a state's tabular archive — conagua/, nasa_power/,
// combined/ (the CONAGUA-spine LEFT join), and provenance/ (the run
// files, global) — assembled by TabularEntries, and the state's JSON
// archive — the per-station citable profile.json (the derived annual
// slot, the daily summary) and its sibling daily.json under combined/,
// plus provenance/ as JSON —
// assembled by JSONEntries; and, in a full run, the deposit-wide
// archives: national-csv.zip and national-json.zip (every state's
// per-unit files copied raw from the state archives, the whole-scope
// tables regenerated at national scope — NationalCSVEntries,
// NationalJSONEntries), national-parquet.zip (the ten tables regenerated
// nationally — NationalParquetEntries), national-sqlite.zip (the full
// canonical database — NationalDBEntry), and the raw snapshot archive
// (the pull output verbatim, held to the snapshot layout —
// RawSnapshotEntries); and the docs group's QA-REPORT.md (LoadQA,
// RenderQA), the lean QA report every number of which this package
// computes from the shipped DB. The archives and the docs files are the
// selectable artifact groups (Group; --only), each built as its own
// fail-soft artifact by Run — per state for the per-state groups, once
// for the rest — under Zenodo's per-record file cap (MaxTopLevelFiles).
// Run's precondition is the read-only QA gate (validate.Gate):
// evaluated read-only over the same handle before anything is written,
// an error-severity finding refuses the run as a GateError, and the
// gate's result rides in the Report and in the QA report.
//
// Every rename and every drop lives here, so reversing one is a
// one-place change with no schema consequence. The DDL lockstep specs
// hold the column specs to the embedded schema so a DDL change cannot
// land unexported or mis-formatted.
package publish
