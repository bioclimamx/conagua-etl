package publish

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// RunLabelColumn is the export name of provenance/power_runs' natural
// key. It is the one exported column no DDL column backs — RunLabel
// derives it from temporal_mode and the period years — so its
// Column carries an empty DB, which the lockstep specs allow for exactly
// this column.
const RunLabelColumn = "run_label"

// The provenance/ folder's file specs: the runs the shipped rows trace
// to, identified by natural label, never by surrogate id. The row sets
// are global — identical in every archive.
var (
	// ProvenanceIngestRuns is provenance/ingest_runs: the latest complete
	// ingest run per snapshot_date, keyed by snapshot_date; the surrogate
	// id is dropped.
	ProvenanceIngestRuns = newFileSpec("provenance/ingest_runs", "ingest_runs", []colDef{
		{db: "snapshot_date", kind: KindDate, key: true},
		{db: "started_at", kind: KindText},
		{db: "finished_at", kind: KindText},
		{db: "sink_kind", kind: KindText},
		{db: "etl_git_sha", kind: KindText},
		{db: "status", kind: KindText},
		{db: "stations_attempted", kind: KindInt},
		{db: "stations_succeeded", kind: KindInt},
		{db: "stations_failed", kind: KindInt},
		{db: "daily_rows", kind: KindInt},
		{db: "normals_rows", kind: KindInt},
		{db: "extras_rows", kind: KindInt},
		{db: "warnings_total", kind: KindInt},
	})

	// ProvenancePowerRuns is provenance/power_runs: the runs at least one
	// supplement row references, keyed by run_label; the surrogate id and
	// the documented constant solar_conversion are dropped. parameters
	// (the URL fragment a reproducer pastes — order is load-bearing) and
	// unit_conversions (the stored JSON text) pass through verbatim,
	// RFC-4180 quoted by the writer.
	ProvenancePowerRuns = withRunLabel(newFileSpec("provenance/power_runs", "power_runs", []colDef{
		{db: "started_at", kind: KindText},
		{db: "finished_at", kind: KindText},
		{db: "status", kind: KindText},
		{db: "endpoint_url", kind: KindText},
		{db: "parameters", kind: KindText},
		{db: "community", kind: KindText},
		{db: "period_start_year", kind: KindInt},
		{db: "period_end_year", kind: KindInt},
		{db: "grid_resolution", kind: KindText},
		{db: "unit_conversions", kind: KindText},
		{db: "temporal_mode", kind: KindText},
		{db: "period_start_date", kind: KindDate},
		{db: "period_end_date", kind: KindDate},
		{db: "cells_attempted", kind: KindInt},
		{db: "cells_succeeded", kind: KindInt},
		{db: "cells_failed", kind: KindInt},
		{db: "supplement_rows", kind: KindInt},
		{db: "etl_git_sha", kind: KindText},
	}))
)

// withRunLabel prepends the synthesized run_label key to a spec whose
// declared columns are all DDL-backed, keeping the natural-key-first
// order rule for the one key the DDL does not hold.
func withRunLabel(spec FileSpec) FileSpec {
	spec.Columns = append([]Column{{Name: RunLabelColumn, Kind: KindText, Key: true}}, spec.Columns...)
	return spec
}

// fields is one CSV record's scan targets keyed by export column name,
// so a record is assembled by name and the spec's column order alone
// decides where each value lands — a field order in a Go struct can
// never reorder a file.
type fields map[string]any

func cellText(s string) *sql.NullString {
	return &sql.NullString{String: s, Valid: true}
}

func cellTextPtr(p *string) *sql.NullString {
	if p == nil {
		return &sql.NullString{}
	}
	return cellText(*p)
}

func cellInt(n int64) *sql.NullInt64 {
	return &sql.NullInt64{Int64: n, Valid: true}
}

func cellIntPtr(p *int64) *sql.NullInt64 {
	if p == nil {
		return &sql.NullInt64{}
	}
	return cellInt(*p)
}

func ingestRunFields(r IngestRunRef) fields {
	return fields{
		"snapshot_date":      cellText(r.SnapshotDate),
		"started_at":         cellText(r.StartedAt),
		"finished_at":        cellTextPtr(r.FinishedAt),
		"sink_kind":          cellText(r.SinkKind),
		"etl_git_sha":        cellTextPtr(r.ETLGitSHA),
		"status":             cellText(r.Status),
		"stations_attempted": cellIntPtr(r.StationsAttempted),
		"stations_succeeded": cellIntPtr(r.StationsSucceeded),
		"stations_failed":    cellIntPtr(r.StationsFailed),
		"daily_rows":         cellIntPtr(r.DailyRows),
		"normals_rows":       cellIntPtr(r.NormalsRows),
		"extras_rows":        cellIntPtr(r.ExtrasRows),
		"warnings_total":     cellIntPtr(r.WarningsTotal),
	}
}

// powerRunFields renders one run; unit_conversions is the stored JSON
// text as bytes, never decoded and re-serialized, and a nil message is
// the column's NULL.
func powerRunFields(r PowerRunRef) fields {
	conversions := &sql.NullString{}
	if r.UnitConversions != nil {
		conversions = cellText(string(r.UnitConversions))
	}
	return fields{
		RunLabelColumn:      cellText(r.RunLabel),
		"started_at":        cellText(r.StartedAt),
		"finished_at":       cellTextPtr(r.FinishedAt),
		"status":            cellText(r.Status),
		"endpoint_url":      cellText(r.EndpointURL),
		"parameters":        cellText(r.Parameters),
		"community":         cellText(r.Community),
		"period_start_year": cellInt(r.PeriodStartYear),
		"period_end_year":   cellInt(r.PeriodEndYear),
		"grid_resolution":   cellText(r.GridResolution),
		"unit_conversions":  conversions,
		"temporal_mode":     cellText(r.TemporalMode),
		"period_start_date": cellTextPtr(r.PeriodStartDate),
		"period_end_date":   cellTextPtr(r.PeriodEndDate),
		"cells_attempted":   cellIntPtr(r.CellsAttempted),
		"cells_succeeded":   cellIntPtr(r.CellsSucceeded),
		"cells_failed":      cellIntPtr(r.CellsFailed),
		"supplement_rows":   cellIntPtr(r.SupplementRows),
		"etl_git_sha":       cellTextPtr(r.ETLGitSHA),
	}
}

// writeFields writes the header and one record per row, every cell
// through the same formatter the DB-backed files use (pinned decimals,
// NULL as the empty field), so a provenance file obeys the format
// contract by construction rather than by a second implementation. A
// row whose field set is not exactly the spec's columns is a
// programming error in the builders above and is refused rather than
// written short or shifted.
func writeFields(w io.Writer, spec FileSpec, rows []fields) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(spec.Header()); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	record := make([]string, len(spec.Columns))
	for n, row := range rows {
		if len(row) != len(spec.Columns) {
			return fmt.Errorf("row %d: %d fields for %d columns", n+1, len(row), len(spec.Columns))
		}
		for i, c := range spec.Columns {
			target, ok := row[c.Name]
			if !ok {
				return fmt.Errorf("row %d: no field for column %s", n+1, c.Name)
			}
			cell, err := formatCell(c, target)
			if err != nil {
				return fmt.Errorf("row %d: %w", n+1, err)
			}
			record[i] = cell
		}
		if err := cw.Write(record); err != nil {
			return fmt.Errorf("write row %d: %w", n+1, err)
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

// ProvenanceEntries returns the provenance/ CSV entries, Path-sorted,
// rendered from runs — the row sets LoadRuns already selected and
// ordered: the latest complete ingest run per snapshot_date in
// snapshot_date order, the referenced power runs in run_label order —
// so they need no DB access at write time and are byte-identical in
// every archive of the build. Integers are written as digits, a nil
// pointer as the empty field, unit_conversions as its stored text.
func ProvenanceEntries(runs Runs) []archive.Entry {
	return []archive.Entry{
		{
			Path: ProvenanceIngestRuns.Name + ".csv",
			Write: func(w io.Writer) error {
				rows := make([]fields, len(runs.Ingest))
				for i, r := range runs.Ingest {
					rows[i] = ingestRunFields(r)
				}
				if err := writeFields(w, ProvenanceIngestRuns, rows); err != nil {
					return fmt.Errorf("%s: %w", ProvenanceIngestRuns.Name, err)
				}
				return nil
			},
		},
		{
			Path: ProvenancePowerRuns.Name + ".csv",
			Write: func(w io.Writer) error {
				rows := make([]fields, len(runs.Power))
				for i, r := range runs.Power {
					rows[i] = powerRunFields(r)
				}
				if err := writeFields(w, ProvenancePowerRuns, rows); err != nil {
					return fmt.Errorf("%s: %w", ProvenancePowerRuns.Name, err)
				}
				return nil
			},
		},
	}
}
