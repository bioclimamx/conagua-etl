package validate

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// sampleCap is the per-rule cap on warn-severity findings a RuleReport
// carries. Every error is kept — a refusal must name each one — while a
// noisy warn rule cannot balloon the QA report.
const sampleCap = 50

// GateRules returns the read-only gate rule set in execution order:
// bbox (the same rule the validate verb runs), the run ledger (runs in
// flight, then the complete-ingest anchor), and the integrity anchors —
// referential first (station, cell, and run references; run_label
// uniqueness), value anchors last (the POWER fill leak, wind direction
// range). Every rule reads; none writes.
func GateRules() []Rule {
	return []Rule{
		{ID: "bbox", Name: "Lat/lon plausibility", Run: ruleBBox},
		{ID: "runs-in-flight", Name: "Runs in flight / stranded", Run: ruleRunsInFlight},
		{ID: "ingest-complete", Name: "Complete ingest run", Run: ruleIngestComplete},
		{ID: "station-refs", Name: "Station references", Run: ruleStationRefs},
		{ID: "cell-refs", Name: "POWER cell references", Run: ruleCellRefs},
		{ID: "run-refs", Name: "POWER run references", Run: ruleRunRefs},
		{ID: "run-label-unique", Name: "POWER run_label uniqueness", Run: ruleRunLabelUnique},
		{ID: "fill-leak", Name: "POWER fill-value leak", Run: ruleFillLeak},
		{ID: "wind-range", Name: "Wind direction range", Run: ruleWindRange},
	}
}

// RuleReport is one rule's slot in a GateReport or a validate Report:
// its counts, wall time, and Findings — every error-severity finding
// plus up to sampleCap warn-severity ones, in the rule's emission order.
type RuleReport struct {
	ID       string
	Name     string
	Scanned  int
	Warnings int
	Errors   int
	Elapsed  time.Duration
	Findings []Finding
}

// GateReport is the record of one gate evaluation: the per-rule
// reports in execution order and the totals across them. Errors > 0 is
// the refusal signal publish acts on.
type GateReport struct {
	StartedAt  time.Time
	FinishedAt time.Time
	Rules      []RuleReport
	Warnings   int
	Errors     int
}

// ErrorFindings returns every error-severity finding across the rules,
// in execution order — complete, since a RuleReport keeps every error.
func (r *GateReport) ErrorFindings() []Finding {
	var out []Finding
	for _, rule := range r.Rules {
		for _, f := range rule.Findings {
			if f.Severity == SeverityError {
				out = append(out, f)
			}
		}
	}
	return out
}

// Gate evaluates GateRules over db — publish's read-only handle — and
// returns the report. onRule, if non-nil, receives each RuleReport as
// its rule completes, so a caller can render progress on a multi-second
// rule. Gate never writes: a rule that reached for a write would fail
// under the read-only handle and surface here as its error.
//
// A rule's own error (a missing table, a lost connection, cancellation)
// aborts the evaluation and is returned wrapped with the rule id, the
// report holding the rules that completed before it; a finding is never
// an error.
func Gate(ctx context.Context, db *sql.DB, onRule func(RuleReport)) (*GateReport, error) {
	r := &GateReport{StartedAt: time.Now().UTC()}
	finish := func(err error) (*GateReport, error) {
		r.FinishedAt = time.Now().UTC()
		return r, err
	}
	for _, rule := range GateRules() {
		if err := ctx.Err(); err != nil {
			return finish(fmt.Errorf("rule %s: %w", rule.ID, err))
		}
		start := time.Now()
		res, err := rule.Run(ctx, db)
		elapsed := time.Since(start)
		if err != nil {
			return finish(fmt.Errorf("rule %s: %w", rule.ID, err))
		}
		rr, err := summarize(rule, res, elapsed)
		if err != nil {
			return finish(err)
		}
		r.Rules = append(r.Rules, rr)
		r.Warnings += rr.Warnings
		r.Errors += rr.Errors
		if onRule != nil {
			onRule(rr)
		}
	}
	return finish(nil)
}

// summarize folds a rule's result into its report slot. A severity
// outside the two literals is a rule bug: it is refused rather than
// counted as neither, so a finding can never slip past the gate
// uncounted.
func summarize(rule Rule, res RuleResult, elapsed time.Duration) (RuleReport, error) {
	rr := RuleReport{ID: rule.ID, Name: rule.Name, Scanned: res.Scanned, Elapsed: elapsed}
	for _, f := range res.Findings {
		switch f.Severity {
		case SeverityError:
			rr.Errors++
			rr.Findings = append(rr.Findings, f)
		case SeverityWarn:
			rr.Warnings++
			if rr.Warnings <= sampleCap {
				rr.Findings = append(rr.Findings, f)
			}
		default:
			return RuleReport{}, fmt.Errorf("rule %s: invalid severity %q", rule.ID, f.Severity)
		}
	}
	return rr, nil
}
