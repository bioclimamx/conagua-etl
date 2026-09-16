// Package validate is the QC surface of the ETL: a fixed rule set over
// the ingested database, each rule emitting findings at one of two
// severities.
//
// Two paths share the rule model:
//
//   - The publish gate — GateRules run through Gate — is strictly
//     read-only: it evaluates the error-severity integrity checks
//     publish refuses on, over a schema.OpenReadOnly handle, and writes
//     nothing (no parsing_warnings rows, no *_runs status change).
//   - The validate verb — AllRules run through Run — is the full QC: the
//     five core QC rules, then the gate's integrity anchors. It records
//     every finding into parsing_warnings under
//     source_file = 'validate:<rule-id>' (the prior validate rows are
//     cleared first, so a re-run is a fresh read), reconciles stranded
//     runs through orphan-runs — the one mutating rule — and renders the
//     self-contained HTML report (WriteHTMLReport). It writes, so it
//     opens through schema.Open.
//
// Severity model: 'error' is reserved for what makes the data unfit to
// publish — impossible coordinates (the core rule set's only error),
// a run in flight, or a failed integrity anchor; 'warn' is everything
// else, surfaced in the reports and never blocking. CONAGUA's quirks
// are mirrored faithfully and warned about, never corrected.
package validate

import (
	"context"
	"database/sql"
)

// Severity literals match the parsing_warnings.severity CHECK
// constraint. Repeated here (rather than imported from ingest) so a
// finding's severity is meaningful without the writer's package.
const (
	SeverityWarn  = "warn"
	SeverityError = "error"
)

// Finding is one thing a rule found. StationID is set when the finding
// is anchored to one station (bbox, the per-station QC rules) and nil
// for aggregate or run-level findings; Issue is the English,
// self-contained text the reports and a refusal message carry verbatim.
// On the validate verb's write path a Finding becomes a
// parsing_warnings row with source_file = SourceFile().
type Finding struct {
	RuleID    string
	Severity  string
	StationID *int64
	Issue     string
}

// RuleResult is what one rule produces: its findings, in the order the
// rule emitted them, and Scanned — the rows it examined, or for the
// run-ledger rules the rows it found — surfaced in the reports.
type RuleResult struct {
	Findings []Finding
	Scanned  int
}

// Rule is one QC check: a stable ID (the parsing_warnings namespace on
// the write path, the label in every report), a human name, and the
// check itself. A Run must be idempotent — the same DB yields the same
// findings — and a gate rule must never write.
type Rule struct {
	ID   string
	Name string
	Run  func(ctx context.Context, db *sql.DB) (RuleResult, error)
}
