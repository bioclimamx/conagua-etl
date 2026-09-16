package publish_test

// Specs that hold profile.json's serialized contract to the Profile
// type and close the two field shapes the round-trip specs left
// unexercised. The rule that a key is never omitted is pinned by
// reflection over every struct reachable from Profile — a json tag on
// every field, no tag option (so never omitempty), English snake_case
// names, unique per object — so an added field cannot slip in untagged
// or optional. Then, against seeded values: a WMO period with exactly
// one score NULL renders as an object with that member null, never as a
// null period; a station with no observed rows on a cell that has daily
// rows reports observed coverage null beside the cell's own reanalysis
// extent, its daily.json the empty series; and a profile entry written
// under a cancelled context surfaces the cancellation from its first
// query, as the daily.json entry does.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// profileStructs is every struct type the profile encoder walks, by
// name — the fixed-shape objects of the document. The self-marshalling
// cells and month blocks are leaves and are not listed.
var profileStructs = []string{
	"Profile", "Identity", "WMOCompleteness", "WMOScore", "NormalsBlock", "ExtrasBlock",
	"PowerCellBlock", "PowerMonthlyBlock", "DailySummary", "Coverage", "ObservedCoverage",
	"DaysByVariable", "ReanalysisCoverage", "Extremes", "Record", "DrySpell", "MetaBlock", "RunLabels",
}

// walkProfileStructs visits every struct type reachable from t through
// fields, pointers, slices, and map values — stopping at a type that
// marshals itself — once each, with its fields in declaration order.
func walkProfileStructs(t reflect.Type, seen map[reflect.Type]bool, visit func(reflect.Type, []reflect.StructField)) {
	marshaler := reflect.TypeFor[json.Marshaler]()
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Map {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct || t.Implements(marshaler) || reflect.PointerTo(t).Implements(marshaler) || seen[t] {
		return
	}
	seen[t] = true
	fields := make([]reflect.StructField, 0, t.NumField())
	for i := range t.NumField() {
		fields = append(fields, t.Field(i))
	}
	visit(t, fields)
	for _, f := range fields {
		walkProfileStructs(f.Type, seen, visit)
	}
}

var _ = Describe("profile.json's serialized contract", func() {
	It("tags every field of every block with an English snake_case key and no tag option — never omitempty — unique per object", func() {
		var visited []string
		walkProfileStructs(reflect.TypeFor[publish.Profile](), map[reflect.Type]bool{},
			func(t reflect.Type, fields []reflect.StructField) {
				visited = append(visited, t.Name())
				keys := map[string]bool{}
				for _, f := range fields {
					where := t.Name() + "." + f.Name
					Expect(f.IsExported()).To(BeTrue(), where)
					tag, ok := f.Tag.Lookup("json")
					Expect(ok).To(BeTrue(), "%s has no json tag", where)
					key, opts, _ := strings.Cut(tag, ",")
					Expect(key).To(MatchRegexp(`^[a-z][a-z0-9_]*$`), where)
					Expect(opts).To(BeEmpty(), "%s carries the tag option %q: a profile key is never omitted", where, opts)
					Expect(keys).NotTo(HaveKey(key), "%s repeats the key %q", t.Name(), key)
					keys[key] = true
				}
			})
		Expect(visited).To(ConsistOf(profileStructs))
	})

	It("renders a WMO period with exactly one score NULL as an object with that member null, never as a null period", func() {
		db := openTempDB()
		id := upsertStation(db, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "31004", Name: "Ticul", State: "YUC",
		})
		mustExec(db, `UPDATE stations SET wmo_completeness_bin_1961_1990 = 0.25, wmo_completeness_cont_1991_2020 = ?
		  WHERE id = ?`, 1079.0/1080.0, id)
		got := decodeProfile(profileBytes(db, yucatan, "31004"))
		Expect(obj(got, "wmo_completeness", "periods")).To(Equal(map[string]any{
			"1961-1990": map[string]any{"bin": num("0.2500"), "cont": nil},
			"1971-2000": nil,
			"1981-2010": nil,
			"1991-2020": map[string]any{"bin": nil, "cont": num("0.9991")},
		}))
	})

	It("reports reanalysis coverage as the cell's own extent for a station with no observed rows, its daily.json empty", func() {
		db := openTempDB()
		id := upsertStation(db, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "31004", Name: "Ticul", State: "YUC",
		})
		insertCell(db, cellShared, 21.5, -89.375)
		insertStationCell(db, id, cellShared, 4.25)
		// Three daily rows out of order, one all-NULL: the extent is the
		// row set's, and days is its row count.
		insertPowerDaily(db, cellShared, "2020-01-03", powerSeqRow(400))
		insertPowerDaily(db, cellShared, "1981-01-01", fullPowerRow)
		insertPowerDaily(db, cellShared, "1999-12-31", allNullPowerRow)

		got := decodeProfile(profileBytes(db, yucatan, "31004"))
		Expect(obj(got, "daily_summary")).To(Equal(map[string]any{
			"source": "bioclima_derived",
			"coverage": map[string]any{
				"observed":   nil,
				"reanalysis": map[string]any{"first_date": "1981-01-01", "last_date": "2020-01-03", "days": num("3")},
			},
			"extremes": map[string]any{
				"source": "conagua_observed", "record_tmax_c": nil, "record_tmin_c": nil, "record_precip_mm_1day": nil,
			},
			"dry_spell": nil,
		}))
		Expect(obj(got, "power_cell")).To(Equal(map[string]any{
			"source": "bioclima_derived", "cell_id": cellShared,
			"lat": num("21.500"), "lon": num("-89.375"), "distance_km": num("4.250"),
		}))

		// The spine is the observed dates: no observations, no rows —
		// the cell's dates are never emitted under the station's name.
		entries, err := publish.DailyJSONEntries(context.Background(), db, yucatan)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(render(entryByPath(entries, "combined/yuc/31004/daily.json")))).To(Equal("[\n]\n"))
	})

	It("honours cancellation at write time: a profile entry built under a cancelled context surfaces it from its first query", func() {
		db := openTempDB()
		seedProfile(db)
		ctx, cancel := context.WithCancel(context.Background())
		entries, err := publish.ProfileEntries(ctx, db, yucatan, profileMeta())
		Expect(err).NotTo(HaveOccurred())
		cancel()
		err = entryByPath(entries, "combined/yuc/31001/profile.json").Write(io.Discard)
		Expect(errors.Is(err, context.Canceled)).To(BeTrue(), err)
		Expect(err).To(MatchError(HavePrefix("identity: ")))
	})
})
