package publish_test

// Specs for the provenance/ entries: the two run files rendered from an
// already-loaded Runs value — every column round-tripped field by field,
// NULL pointers as empty fields, integers as digits, unit_conversions as
// its verbatim JSON text and the comma-joined parameters both RFC-4180
// quoted — and the seam from LoadRuns, which fixes the row sets and the
// natural-key order the files carry.

import (
	"context"
	"encoding/json"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// The CSV renderings of the manifest specs' expected run lists — the
// same values wantIngestRuns / wantPowerRuns pin for manifest.json.
var (
	wantIngestRunsCSV = [][]string{
		publish.ProvenanceIngestRuns.Header(),
		{"2026-05-01", "2026-05-01T10:00:00Z", "2026-05-01T12:00:00Z", "r2", "0ld5ha", "complete",
			"5000", "4990", "10", "70000000", "200000", "200000", "40"},
		{"2026-06-08", "2026-06-10T01:00:00Z", "2026-06-10T02:30:00Z", "local", "", "complete",
			"5524", "5523", "1", "71399000", "250000", "249988", ""},
	}
	wantPowerRunsCSV = [][]string{
		publish.ProvenancePowerRuns.Header(),
		{"power-daily-1981-2026", "2026-07-02T00:00:00Z", "", "complete",
			"https://power.larc.nasa.gov/api/temporal/daily/point?x=1&y=2", "T2M", "AG",
			"1981", "2026", "0.5x0.625", "", "daily", "1981-01-01", "2026-06-08",
			"2400", "", "2", "", ""},
		{"power-monthly-1981-2010", "2026-07-01T00:00:00Z", "2026-07-01T06:00:00Z", "complete",
			"https://power.larc.nasa.gov/api/temporal/monthly/point", "T2M,ALLSKY_SFC_SW_DWN", "AG",
			"1981", "2010", "0.5x0.625", conversionsText, "monthly", "", "",
			"2400", "2400", "0", "28800", "p0w3r"},
	}
)

var _ = Describe("ProvenanceEntries", func() {
	runs := publish.Runs{Ingest: wantIngestRuns, Power: wantPowerRuns}

	It("lists the two provenance/ files Path-sorted", func() {
		entries := publish.ProvenanceEntries(runs)
		var paths []string
		for _, e := range entries {
			paths = append(paths, e.Path)
		}
		Expect(paths).To(Equal([]string{"provenance/ingest_runs.csv", "provenance/power_runs.csv"}))
	})

	It("round-trips every ingest_runs column in the given order, NULL as the empty field", func() {
		got := records(render(entryByPath(publish.ProvenanceEntries(runs), "provenance/ingest_runs.csv")))
		Expect(got).To(Equal(wantIngestRunsCSV))
	})

	It("round-trips every power_runs column, run_label first, unit_conversions as its stored text", func() {
		got := records(render(entryByPath(publish.ProvenanceEntries(runs), "provenance/power_runs.csv")))
		Expect(got).To(Equal(wantPowerRunsCSV))
		Expect(json.Valid([]byte(got[2][10]))).To(BeTrue())
	})

	It("quotes the JSON text and the comma-joined parameters per RFC 4180, LF-terminated, no BOM (raw bytes)", func() {
		data := string(render(entryByPath(publish.ProvenanceEntries(runs), "provenance/power_runs.csv")))
		Expect(data).NotTo(HavePrefix("\xef\xbb\xbf"))
		Expect(data).NotTo(ContainSubstring("\r"))
		lines := strings.Split(strings.TrimSuffix(data, "\n"), "\n")
		Expect(lines).To(HaveLen(3))
		Expect(lines[0]).To(Equal(strings.Join(publish.ProvenancePowerRuns.Header(), ",")))
		Expect(lines[1]).To(Equal("power-daily-1981-2026,2026-07-02T00:00:00Z,,complete," +
			"https://power.larc.nasa.gov/api/temporal/daily/point?x=1&y=2,T2M,AG,1981,2026,0.5x0.625,,daily," +
			"1981-01-01,2026-06-08,2400,,2,,"))
		Expect(lines[2]).To(Equal("power-monthly-1981-2010,2026-07-01T00:00:00Z,2026-07-01T06:00:00Z,complete," +
			`https://power.larc.nasa.gov/api/temporal/monthly/point,"T2M,ALLSKY_SFC_SW_DWN",AG,1981,2010,0.5x0.625,` +
			`"` + strings.ReplaceAll(conversionsText, `"`, `""`) + `",monthly,,,2400,2400,0,28800,p0w3r`))
	})

	It("writes headers only for an empty Runs", func() {
		entries := publish.ProvenanceEntries(publish.Runs{})
		Expect(records(render(entries[0]))).To(Equal([][]string{publish.ProvenanceIngestRuns.Header()}))
		Expect(records(render(entries[1]))).To(Equal([][]string{publish.ProvenancePowerRuns.Header()}))
	})

	It("renders the same bytes on every write (byte-reproducible)", func() {
		entries := publish.ProvenanceEntries(runs)
		for _, e := range entries {
			Expect(render(e)).To(Equal(render(e)), e.Path)
		}
	})
})

var _ = Describe("LoadRuns through ProvenanceEntries", func() {
	It("renders the DB's provenance row sets in natural-key order, the same rows manifest.json carries", func() {
		db := openTempDB()
		seedRuns(db)
		runs, err := publish.LoadRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(runs.Ingest).To(Equal(wantIngestRuns))
		expectPowerRunsEqual(runs.Power, wantPowerRuns)

		entries := publish.ProvenanceEntries(runs)
		Expect(records(render(entryByPath(entries, "provenance/ingest_runs.csv")))).To(Equal(wantIngestRunsCSV))
		Expect(records(render(entryByPath(entries, "provenance/power_runs.csv")))).To(Equal(wantPowerRunsCSV))
	})
})
