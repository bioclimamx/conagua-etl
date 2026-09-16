package publish_test

// Specs for the provenance/ JSON entries: the two files; every field
// of a seeded Runs value round-tripped through encoding/json, the
// embedded unit_conversions content preserved; the object keys in the
// published column order; LoadRuns' row order kept; every field equal to the
// CSV rendering of the same runs, cell for cell; the empty set; and the
// encoding — two-space indent, trailing LF, no HTML escaping.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// compact returns raw with insignificant whitespace removed, so an
// embedded object re-indented by the encoder compares to its stored text.
func compact(raw []byte) string {
	GinkgoHelper()
	var buf bytes.Buffer
	Expect(json.Compact(&buf, raw)).To(Succeed(), string(raw))
	return buf.String()
}

// fieldText renders one decoded JSON field as the CSV carries it: a
// string verbatim, a number's literal, "" for null, an object compacted.
func fieldText(raw json.RawMessage) string {
	GinkgoHelper()
	switch {
	case string(raw) == "null":
		return ""
	case raw[0] == '"':
		var s string
		Expect(json.Unmarshal(raw, &s)).To(Succeed())
		return s
	case raw[0] == '{':
		return compact(raw)
	default:
		return string(raw)
	}
}

// parseObjects decodes a JSON array of objects keeping each object's
// key order.
func parseObjects(data []byte) []*jsonObject {
	GinkgoHelper()
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	arr, ok := decodeOrdered(dec).([]any)
	Expect(ok).To(BeTrue(), "top level is not an array")
	Expect(dec.More()).To(BeFalse(), "trailing content")
	objs := make([]*jsonObject, len(arr))
	for i, v := range arr {
		objs[i], ok = v.(*jsonObject)
		Expect(ok).To(BeTrue(), "element %d is not an object", i)
	}
	return objs
}

var _ = Describe("provenanceJSONEntries", func() {
	It("lists provenance/ingest_runs.json and provenance/power_runs.json", func() {
		entries := publish.ProvenanceJSONEntries(publish.Runs{})
		Expect(entries).To(HaveLen(2))
		Expect(entries[0].Path).To(Equal("provenance/ingest_runs.json"))
		Expect(entries[1].Path).To(Equal("provenance/power_runs.json"))
	})

	It("writes an empty run set as an empty array with a trailing newline, never null", func() {
		for _, runs := range []publish.Runs{
			{},
			{Ingest: []publish.IngestRunRef{}, Power: []publish.PowerRunRef{}},
		} {
			for _, e := range publish.ProvenanceJSONEntries(runs) {
				Expect(string(render(e))).To(Equal("[]\n"), e.Path)
			}
		}
	})

	It("indents two spaces, ends with LF, re-indents the embedded unit_conversions, and never HTML-escapes", func() {
		runs := publish.Runs{
			Ingest: []publish.IngestRunRef{{
				SnapshotDate: "2026-06-08", StartedAt: "2026-06-09T01:00:00Z", FinishedAt: str("2026-06-09T02:00:00Z"),
				SinkKind: "local", ETLGitSHA: nil, Status: "complete",
				StationsAttempted: i64(5), StationsSucceeded: i64(4), StationsFailed: i64(1),
				DailyRows: i64(0), NormalsRows: nil, ExtrasRows: i64(7), WarningsTotal: nil,
			}},
			Power: []publish.PowerRunRef{{
				RunLabel: "power-monthly-1981-2010", StartedAt: "2026-07-02T00:00:00Z", FinishedAt: nil,
				Status: "complete", EndpointURL: "https://power.larc.nasa.gov/api?a=1&b=<2>", Parameters: "T2M,WS2M",
				Community: "AG", PeriodStartYear: 1981, PeriodEndYear: 2010, GridResolution: "0.5x0.625",
				UnitConversions: json.RawMessage(`{"T2M":{"power_unit":"C","stored_unit":"C","factor":1},"X":[1,2.50]}`),
				TemporalMode:    "monthly", PeriodStartDate: nil, PeriodEndDate: str("2010-12-31"),
				CellsAttempted: i64(10), CellsSucceeded: nil, CellsFailed: i64(0), SupplementRows: i64(120),
				ETLGitSHA: str("abc"),
			}},
		}
		entries := publish.ProvenanceJSONEntries(runs)
		Expect(string(render(entries[0]))).To(Equal(`[
  {
    "snapshot_date": "2026-06-08",
    "started_at": "2026-06-09T01:00:00Z",
    "finished_at": "2026-06-09T02:00:00Z",
    "sink_kind": "local",
    "etl_git_sha": null,
    "status": "complete",
    "stations_attempted": 5,
    "stations_succeeded": 4,
    "stations_failed": 1,
    "daily_rows": 0,
    "normals_rows": null,
    "extras_rows": 7,
    "warnings_total": null
  }
]
`))
		Expect(string(render(entries[1]))).To(Equal(`[
  {
    "run_label": "power-monthly-1981-2010",
    "started_at": "2026-07-02T00:00:00Z",
    "finished_at": null,
    "status": "complete",
    "endpoint_url": "https://power.larc.nasa.gov/api?a=1&b=<2>",
    "parameters": "T2M,WS2M",
    "community": "AG",
    "period_start_year": 1981,
    "period_end_year": 2010,
    "grid_resolution": "0.5x0.625",
    "unit_conversions": {
      "T2M": {
        "power_unit": "C",
        "stored_unit": "C",
        "factor": 1
      },
      "X": [
        1,
        2.50
      ]
    },
    "temporal_mode": "monthly",
    "period_start_date": null,
    "period_end_date": "2010-12-31",
    "cells_attempted": 10,
    "cells_succeeded": null,
    "cells_failed": 0,
    "supplement_rows": 120,
    "etl_git_sha": "abc"
  }
]
`))
	})

	It("writes a NULL unit_conversions as null", func() {
		entries := publish.ProvenanceJSONEntries(publish.Runs{Power: []publish.PowerRunRef{{RunLabel: "power-daily-1981-2026"}}})
		Expect(string(render(entries[1]))).To(ContainSubstring("\n    \"unit_conversions\": null,\n"))
	})

	Context("from a seeded DB", func() {
		var (
			db      *sql.DB
			runs    publish.Runs
			entries []archive.Entry
			csvs    []archive.Entry
		)

		BeforeEach(func() {
			db = openTempDB()
			seedDistinctRuns(db)
			var err error
			runs, err = publish.LoadRuns(context.Background(), db)
			Expect(err).NotTo(HaveOccurred())
			Expect(runs.Ingest).To(HaveLen(3))
			Expect(runs.Power).To(HaveLen(3))
			entries = publish.ProvenanceJSONEntries(runs)
			csvs = publish.ProvenanceEntries(runs)
		})

		It("round-trips every field of every run, unit_conversions content preserved, in LoadRuns' order", func() {
			var ingest []publish.IngestRunRef
			Expect(json.Unmarshal(render(entryByPath(entries, "provenance/ingest_runs.json")), &ingest)).To(Succeed())
			Expect(ingest).To(Equal(runs.Ingest))

			var pow []publish.PowerRunRef
			Expect(json.Unmarshal(render(entryByPath(entries, "provenance/power_runs.json")), &pow)).To(Succeed())
			Expect(pow).To(HaveLen(len(runs.Power)))
			for i := range pow {
				got, want := pow[i], runs.Power[i]
				Expect(got.RunLabel).To(Equal(want.RunLabel))
				if want.UnitConversions == nil {
					Expect(got.UnitConversions).To(Equal(json.RawMessage("null")))
				} else {
					Expect(compact(got.UnitConversions)).To(Equal(compact(want.UnitConversions)), want.RunLabel)
				}
				got.UnitConversions, want.UnitConversions = nil, nil
				Expect(got).To(Equal(want), want.RunLabel)
			}

			var labels, snapshots []string
			for _, r := range pow {
				labels = append(labels, r.RunLabel)
			}
			for _, r := range ingest {
				snapshots = append(snapshots, r.SnapshotDate)
			}
			Expect(labels).To(Equal([]string{"power-daily-1985-2019", "power-monthly-1981-2010", "power-monthly-1991-2020"}))
			Expect(snapshots).To(Equal([]string{"2025-12-31", "2026-01-15", "2026-06-08"}))
		})

		It("keys every object in the published column order — the CSV header — with every key present", func() {
			for _, obj := range parseObjects(render(entryByPath(entries, "provenance/ingest_runs.json"))) {
				Expect(obj.keys).To(Equal(publish.ProvenanceIngestRuns.Header()))
			}
			for _, obj := range parseObjects(render(entryByPath(entries, "provenance/power_runs.json"))) {
				Expect(obj.keys).To(Equal(publish.ProvenancePowerRuns.Header()))
			}
		})

		It("equals the CSV rendering of the same runs, cell for cell", func() {
			for _, name := range []string{"provenance/ingest_runs", "provenance/power_runs"} {
				csvRows := records(render(entryByPath(csvs, name+".csv")))
				var jsonRows []map[string]json.RawMessage
				Expect(json.Unmarshal(render(entryByPath(entries, name+".json")), &jsonRows)).To(Succeed())
				Expect(jsonRows).To(HaveLen(len(csvRows) - 1))
				for i, row := range jsonRows {
					Expect(row).To(HaveLen(len(csvRows[0])), "%s row %d", name, i)
					for j, col := range csvRows[0] {
						want := csvRows[i+1][j]
						if col == "unit_conversions" && want != "" {
							want = compact([]byte(want))
						}
						Expect(fieldText(row[col])).To(Equal(want), "%s row %d column %s", name, i, col)
					}
				}
			}
		})

		It("writes identical bytes on every write", func() {
			for _, e := range entries {
				Expect(render(e)).To(Equal(render(e)), e.Path)
			}
		})
	})
})
