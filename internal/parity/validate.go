package parity

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// CompareValidate compares the validate-written parsing_warnings of two
// databases — the base one the reference validate build ran on, the new
// one this repo's verb ran on — as multisets keyed by ValidateWarning,
// one tally per reference rule: both builds must write the same
// parsing_warnings set, counts per rule plus issue text. Rule ids
// outside ReferenceValidateRules are counted per side and never compared:
// the reference build has no such rules, so a difference there is not a
// parity signal. Both handles may be read-only; the comparator never
// writes.
//
// Rows are aligned in sorted tuple order, so the retained samples are
// deterministic across runs. The orphan-runs rule is wall-clock
// dependent by construction — it reconciles what it finds, so the same
// database yields its rows on the first pass only — which is the
// caller's to account for when preparing the two copies.
func CompareValidate(ctx context.Context, baseDB, newDB *sql.DB) (*ValidateComparison, error) {
	base, err := loadValidateWarnings(ctx, baseDB)
	if err != nil {
		return nil, fmt.Errorf("base: %w", err)
	}
	updated, err := loadValidateWarnings(ctx, newDB)
	if err != nil {
		return nil, fmt.Errorf("new: %w", err)
	}

	reference := make(map[string]bool, len(ReferenceValidateRules))
	for _, id := range ReferenceValidateRules {
		reference[id] = true
	}

	tuples := make(map[ValidateWarning]struct{}, len(base)+len(updated))
	for t := range base {
		tuples[t] = struct{}{}
	}
	for t := range updated {
		tuples[t] = struct{}{}
	}
	sorted := slices.SortedFunc(maps.Keys(tuples), compareValidateWarnings)

	byRule := make(map[string]*ValidateRule, len(ReferenceValidateRules))
	cmp := &ValidateComparison{Rules: make([]ValidateRule, len(ReferenceValidateRules))}
	for i, id := range ReferenceValidateRules {
		cmp.Rules[i].ID = id
		byRule[id] = &cmp.Rules[i]
	}
	native := map[string]*ValidateNativeRule{}

	for _, t := range sorted {
		b, n := base[t], updated[t]
		id := t.RuleID()
		if !reference[id] {
			nr, ok := native[id]
			if !ok {
				nr = &ValidateNativeRule{ID: id}
				native[id] = nr
			}
			nr.BaseRows += b
			nr.NewRows += n
			continue
		}
		r := byRule[id]
		r.BaseRows += b
		r.NewRows += n
		tallySeverity(r, t.Severity, b, n)
		r.Identical += min(b, n)
		switch {
		case b > n:
			r.BaseOnly.Add(ValidateRowDiff{Warning: t, Base: b, New: n})
			r.BaseOnlyRows += b - n
		case n > b:
			r.NewOnly.Add(ValidateRowDiff{Warning: t, Base: b, New: n})
			r.NewOnlyRows += n - b
		}
	}

	cmp.Native = make([]ValidateNativeRule, 0, len(native))
	for _, id := range slices.Sorted(maps.Keys(native)) {
		cmp.Native = append(cmp.Native, *native[id])
	}
	return cmp, nil
}

// tallySeverity folds one tuple's per-side multiplicities into the
// rule's severity counts. Any other literal is impossible under the
// column's CHECK constraint and is left uncounted rather than guessed.
func tallySeverity(r *ValidateRule, severity string, base, updated int) {
	switch severity {
	case "warn":
		r.BaseWarnings += base
		r.NewWarnings += updated
	case "error":
		r.BaseErrors += base
		r.NewErrors += updated
	}
}

// compareValidateWarnings orders tuples by rule, then station (NULL
// first), severity, and issue — the report's row order.
func compareValidateWarnings(a, b ValidateWarning) int {
	if c := strings.Compare(a.SourceFile, b.SourceFile); c != 0 {
		return c
	}
	if c := strings.Compare(a.Station, b.Station); c != 0 {
		return c
	}
	if c := strings.Compare(a.Severity, b.Severity); c != 0 {
		return c
	}
	return strings.Compare(a.Issue, b.Issue)
}

// loadValidateWarnings reads every 'validate:%' row of one side as a
// multiset of tuples. The station resolves to its (source, external_id)
// natural key through a LEFT JOIN — the key the DB comparator aligns on
// — so a NULL station_id stays NULL (an empty Station) and a dangling
// one is surfaced by its surrogate rather than dropped.
func loadValidateWarnings(ctx context.Context, db *sql.DB) (map[ValidateWarning]int, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT w.station_id, s.source, s.external_id, w.source_file, w.severity, w.issue
		FROM parsing_warnings w LEFT JOIN stations s ON s.id = w.station_id
		WHERE w.source_file LIKE ?`, validateSourcePrefix+"%")
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	counts := make(map[ValidateWarning]int)
	for rows.Next() {
		var stationID sql.NullInt64
		var source, externalID sql.NullString
		var t ValidateWarning
		if err := rows.Scan(&stationID, &source, &externalID, &t.SourceFile, &t.Severity, &t.Issue); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		switch {
		case externalID.Valid:
			t.Station = StationKey(source.String, externalID.String)
		case stationID.Valid:
			t.Station = unresolvedStationPrefix + strconv.FormatInt(stationID.Int64, 10)
		}
		counts[t]++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return counts, nil
}
