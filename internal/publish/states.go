package publish

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
)

// State is one CONAGUA state present in the DB: Code as stored
// (uppercase), Slug lowercase (used for filenames and
// the in-archive shard), Name the official name
// (conagua.StateCode.DisplayName — the codes in the DB are the uppercase
// form of conagua.AllStates).
type State struct {
	Code, Slug, Name string
}

// LoadStates returns the distinct states of the CONAGUA conventional
// stations, ordered by code (bytewise). A NULL state is an error: the
// per-state artifact set has no home for such a station, the national
// DB carries none, and a silently skipped station would be a lost row.
// A code outside conagua.AllStates is likewise an error — ingest only
// writes the catalog's 32 codes, so anything else is corruption.
func LoadStates(ctx context.Context, db *sql.DB) ([]State, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT DISTINCT state FROM stations WHERE source = ? ORDER BY state`,
		string(ingest.SourceConaguaConventional))
	if err != nil {
		return nil, fmt.Errorf("load states: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	var states []State
	for rows.Next() {
		var code sql.NullString
		if err := rows.Scan(&code); err != nil {
			return nil, fmt.Errorf("load states: scan: %w", err)
		}
		if !code.Valid {
			return nil, fmt.Errorf("load states: a %s station has a NULL state", ingest.SourceConaguaConventional)
		}
		st, err := stateFromCode(code.String)
		if err != nil {
			return nil, fmt.Errorf("load states: %w", err)
		}
		states = append(states, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load states: %w", err)
	}
	return states, nil
}

// stateFromCode builds the State for a stored (uppercase) code.
func stateFromCode(code string) (State, error) {
	slug := strings.ToLower(code)
	name := conagua.StateCode(slug).DisplayName()
	if name == "" {
		return State{}, fmt.Errorf("unknown state code %q in stations (not one of the 32 CONAGUA codes)", code)
	}
	return State{Code: code, Slug: slug, Name: name}, nil
}

// ResolveStates selects from all the states named in requested,
// matched case-insensitively against the stored code ("yuc" and "YUC"
// both select YUC). The result keeps all's order and drops duplicates,
// so the artifact order never depends on how flags were typed; an empty
// request selects every state. An unknown code is an error that lists
// the valid codes.
func ResolveStates(all []State, requested []string) ([]State, error) {
	if len(requested) == 0 {
		return slices.Clone(all), nil
	}
	wanted := make(map[string]bool, len(requested))
	for _, r := range requested {
		code := strings.ToUpper(r)
		if !slices.ContainsFunc(all, func(s State) bool { return s.Code == code }) {
			return nil, fmt.Errorf("unknown state %q (valid: %s)", r, strings.Join(codes(all), ", "))
		}
		wanted[code] = true
	}
	var out []State
	for _, s := range all {
		if wanted[s.Code] {
			out = append(out, s)
		}
	}
	return out, nil
}

func codes(states []State) []string {
	out := make([]string, len(states))
	for i, s := range states {
		out[i] = s.Code
	}
	return out
}
