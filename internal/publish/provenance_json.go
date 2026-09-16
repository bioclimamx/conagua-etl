package publish

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// provenanceJSONEntries returns the provenance/ entries of a JSON
// archive — ingest_runs.json and power_runs.json, the two row sets
// ProvenanceEntries renders as CSV, in the JSON archive's own format —
// as arrays of run objects, rendered from the Runs loaded
// once for the whole build, so they need no DB access at write time and
// are byte-identical in every archive. Each object's keys follow the
// struct's field order, which is the CSV column order, so the JSON and
// CSV renderings of a run agree field for field; a NULL column is an
// explicit null, never an omitted key; unit_conversions is the stored
// JSON text embedded raw and re-indented by the encoder, exactly as
// manifest.json carries it. An empty row set is an empty array.
func provenanceJSONEntries(runs Runs) []archive.Entry {
	return []archive.Entry{
		{
			Path: ProvenanceIngestRuns.Name + ".json",
			Write: func(w io.Writer) error {
				return encodeJSONArray(w, ProvenanceIngestRuns.Name, runs.Ingest)
			},
		},
		{
			Path: ProvenancePowerRuns.Name + ".json",
			Write: func(w io.Writer) error {
				return encodeJSONArray(w, ProvenancePowerRuns.Name, runs.Power)
			},
		},
	}
}

// encodeJSONArray writes list as a two-space-indented JSON array with a
// trailing newline and HTML escaping off — the manifest's encoding, so
// an endpoint URL's '&' reads as written. A nil list is an empty array,
// never null: the file is a table, and an empty table has no rows.
func encodeJSONArray[T any](w io.Writer, name string, list []T) error {
	if list == nil {
		list = []T{}
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(list); err != nil {
		return fmt.Errorf("%s: encode: %w", name, err)
	}
	return nil
}
