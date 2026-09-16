package validate

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// DefaultPeriod is the normals window the period-scoped rules
// (wmo-month-completeness, daily-sanity) read when Options.Period is
// empty — the period publish targets.
const DefaultPeriod = "1991-2020"

// Options configures one validate invocation.
type Options struct {
	// Period scopes wmo-month-completeness and daily-sanity to a single
	// normals window, "YYYY-YYYY". Empty means DefaultPeriod.
	Period string

	// Skip is an optional set of rule IDs to omit (e.g. for dev when
	// one rule is being iterated on). Empty = run all.
	Skip map[string]bool

	// OnRuleStart, if non-nil, is invoked just before each rule runs.
	// The CLI uses it to print a starting line so the operator sees
	// progress on multi-minute rules (daily-sanity over 71M daily rows
	// takes a while).
	OnRuleStart func(ruleID, ruleName string)

	// OnRuleDone, if non-nil, is invoked after each rule completes with
	// its full result and wall time. The CLI uses it to print a per-rule
	// summary line.
	OnRuleDone func(ruleID string, res RuleResult, elapsed time.Duration)
}

func (o Options) withDefaults() Options {
	if o.Period == "" {
		o.Period = DefaultPeriod
	}
	return o
}

// Report is the full record of one validate invocation: the per-rule
// reports in execution order and the totals across them. It drives the
// verb's summary and the HTML report; the findings themselves are in
// parsing_warnings.
type Report struct {
	StartedAt  time.Time
	FinishedAt time.Time
	Period     string

	PerRule []RuleReport

	// Aggregates across rules.
	WarningsTotal int
	ErrorsTotal   int
}

// AllRules returns the verb's rule set in execution order: the five
// core QC rules — orphan-runs first, so a 'running' row whose run
// crashed is reconciled before the rules that read the ledgers; bbox;
// then the three warn-level QC rules — followed by the gate's integrity
// anchors from GateRules, minus bbox (already listed) and
// runs-in-flight. runs-in-flight is omitted because the verb's orphan-runs
// already scans the ledgers, reconciling only rows
// past the 24 h threshold — a younger running row stays publish's to
// refuse. period scopes the two period-scoped rules; empty means
// DefaultPeriod.
func AllRules(period string) []Rule {
	if period == "" {
		period = DefaultPeriod
	}
	rules := []Rule{
		{ID: "orphan-runs", Name: "Orphan run reconciliation", Run: ruleOrphanRuns},
		{ID: "bbox", Name: "Lat/lon plausibility", Run: ruleBBox},
		{ID: "wmo-month-completeness", Name: "WMO §4.4.1 within-month completeness", Run: ruleWMOMonthCompleteness(period)},
		{ID: "daily-sanity", Name: "Daily-series sanity", Run: ruleDailySanity(period)},
		{ID: "cross-period", Name: "Cross-period consistency", Run: ruleCrossPeriod},
	}
	listed := map[string]bool{"runs-in-flight": true}
	for _, r := range rules {
		listed[r.ID] = true
	}
	for _, r := range GateRules() {
		if !listed[r.ID] {
			rules = append(rules, r)
		}
	}
	return rules
}

// Run is the validate verb's entry point. It clears the prior validate
// rows, runs every non-skipped rule of AllRules in order — the hooks
// firing around each — records every finding into parsing_warnings in
// one batch once the last rule has run, and returns the Report. db must
// be a writer handle (schema.Open): the verb writes its findings and
// orphan-runs reconciles.
//
// The Report is non-nil even on failure — it holds the rules that
// completed, so a caller can print what there is — and a rule's own
// error is returned wrapped with its id. A cancelled ctx is observed
// before each rule starts, as in Gate, so cancellation never depends on
// the next rule reaching its first query. A failure after the clear
// leaves no validate rows behind, since the batch is written last.
func Run(ctx context.Context, db *sql.DB, opts Options) (*Report, error) {
	opts = opts.withDefaults()
	r := &Report{StartedAt: time.Now().UTC(), Period: opts.Period}

	if err := clearPriorWarnings(ctx, db); err != nil {
		return r, fmt.Errorf("clear prior validate warnings: %w", err)
	}

	var batch []Finding
	for _, rule := range AllRules(opts.Period) {
		if opts.Skip[rule.ID] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return r, fmt.Errorf("rule %s: %w", rule.ID, err)
		}
		if opts.OnRuleStart != nil {
			opts.OnRuleStart(rule.ID, rule.Name)
		}
		start := time.Now()
		res, err := rule.Run(ctx, db)
		elapsed := time.Since(start)
		if err != nil {
			return r, fmt.Errorf("rule %s: %w", rule.ID, err)
		}
		if opts.OnRuleDone != nil {
			opts.OnRuleDone(rule.ID, res, elapsed)
		}
		rr, err := summarize(rule, res, elapsed)
		if err != nil {
			return r, err
		}
		r.PerRule = append(r.PerRule, rr)
		r.WarningsTotal += rr.Warnings
		r.ErrorsTotal += rr.Errors
		batch = append(batch, res.Findings...)
	}

	if err := writeWarnings(ctx, db, batch); err != nil {
		return r, fmt.Errorf("write warnings: %w", err)
	}

	r.FinishedAt = time.Now().UTC()
	return r, nil
}

// clearPriorWarnings removes the validate-namespaced rows so each
// invocation produces a fresh snapshot; ingest's rows are untouched.
func clearPriorWarnings(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx,
		`DELETE FROM parsing_warnings WHERE source_file LIKE ?`, sourcePrefix+"%")
	return err
}
