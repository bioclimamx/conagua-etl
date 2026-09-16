package publish_test

// Specs for the manifest seam: the run_label derivation, the two
// provenance row sets (latest complete ingest run per snapshot; the
// power runs supplement rows reference — with the label-collision
// fail-fast), the per-table counts held in lockstep with the DDL, and
// the manifest file itself round-tripped field by field.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

func str(s string) *string { return &s }
func i64(n int64) *int64   { return &n }

// ingestRunSeed is one ingest_runs row; nil pointers seed NULL.
type ingestRunSeed struct {
	startedAt    string
	finishedAt   *string
	snapshotDate string
	sinkKind     string
	gitSHA       *string
	status       string
	counters     []*int64 // attempted, succeeded, failed, daily, normals, extras, warnings
}

func insertIngestRun(db *sql.DB, s ingestRunSeed) int64 {
	GinkgoHelper()
	Expect(s.counters).To(HaveLen(7))
	var id int64
	err := db.QueryRowContext(context.Background(), `
INSERT INTO ingest_runs (started_at, finished_at, snapshot_date, sink_kind, etl_git_sha, status,
  stations_attempted, stations_succeeded, stations_failed, daily_rows, normals_rows, extras_rows, warnings_total)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`,
		s.startedAt, s.finishedAt, s.snapshotDate, s.sinkKind, s.gitSHA, s.status,
		s.counters[0], s.counters[1], s.counters[2], s.counters[3], s.counters[4], s.counters[5], s.counters[6],
	).Scan(&id)
	Expect(err).NotTo(HaveOccurred())
	return id
}

// powerRunSeed is one power_runs row; nil pointers seed NULL.
type powerRunSeed struct {
	startedAt       string
	finishedAt      *string
	status          string
	endpoint        string
	parameters      string
	community       string
	startYear       int64
	endYear         int64
	gridResolution  string
	unitConversions *string
	temporalMode    string
	startDate       *string
	endDate         *string
	gitSHA          *string
	counters        []*int64 // attempted, succeeded, failed, supplement_rows
}

func insertPowerRun(db *sql.DB, s powerRunSeed) int64 {
	GinkgoHelper()
	Expect(s.counters).To(HaveLen(4))
	var id int64
	err := db.QueryRowContext(context.Background(), `
INSERT INTO power_runs (started_at, finished_at, status, endpoint_url, parameters, community,
  period_start_year, period_end_year, grid_resolution, solar_conversion, unit_conversions,
  temporal_mode, period_start_date, period_end_date,
  cells_attempted, cells_succeeded, cells_failed, supplement_rows, etl_git_sha)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`,
		s.startedAt, s.finishedAt, s.status, s.endpoint, s.parameters, s.community,
		s.startYear, s.endYear, s.gridResolution, 1e6/86400.0, s.unitConversions,
		s.temporalMode, s.startDate, s.endDate,
		s.counters[0], s.counters[1], s.counters[2], s.counters[3], s.gitSHA,
	).Scan(&id)
	Expect(err).NotTo(HaveOccurred())
	return id
}

func insertCell(db *sql.DB, cellID string, lat, lon float64) {
	GinkgoHelper()
	mustExec(db, `INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution)
	  VALUES (?, ?, ?, '0.5x0.625')`, cellID, lat, lon)
}

func insertMonthlySupplement(db *sql.DB, cellID, period string, month int, runID any) {
	GinkgoHelper()
	mustExec(db, `INSERT INTO monthly_supplement (cell_id, period, month, t2m_c, power_run_id)
	  VALUES (?, ?, ?, 24.48, ?)`, cellID, period, month, runID)
}

func insertDailySupplement(db *sql.DB, cellID, date string, runID any) {
	GinkgoHelper()
	mustExec(db, `INSERT INTO daily_supplement (cell_id, date, t2m_c, power_run_id)
	  VALUES (?, ?, 25.1, ?)`, cellID, date, runID)
}

// The stored unit_conversions text: compact, keys in registry order, a
// factor with a long tail — the shape power writes with json.Marshal.
const conversionsText = `{"T2M":{"power_unit":"C","stored_unit":"C","factor":1},` +
	`"ALLSKY_SFC_SW_DWN":{"power_unit":"MJ/m^2/day","stored_unit":"W/m^2","factor":11.574074074074074}}`

// seedRuns' referenced power runs land at these ids in a fresh DB — the
// monthly run first, the daily run second — and seedPower's supplement
// rows reference them by constant (insertPowerMonthly, insertPowerDaily);
// seedRuns asserts the ids it got, so the coupling is checked rather
// than assumed.
const (
	seededMonthlyRunID int64 = 1
	seededDailyRunID   int64 = 2
)

// seedRuns gives a DB the provenance the manifest reads: two snapshots
// of ingest runs (the latest complete per date must win over an earlier
// complete one, and over a newer aborted one), three power runs of
// which two are referenced by supplement rows, and the cells those rows
// key on — a fixture the gate passes, so every Run spec builds on it.
// Returns the referenced power run ids by label. seedRunEdges adds the
// rows the gate refuses.
func seedRuns(db *sql.DB) map[string]int64 {
	GinkgoHelper()
	insertIngestRun(db, ingestRunSeed{
		startedAt: "2026-05-01T10:00:00Z", finishedAt: str("2026-05-01T12:00:00Z"),
		snapshotDate: "2026-05-01", sinkKind: "r2", gitSHA: str("0ld5ha"), status: "complete",
		counters: []*int64{i64(5000), i64(4990), i64(10), i64(70000000), i64(200000), i64(200000), i64(40)},
	})
	insertIngestRun(db, ingestRunSeed{
		startedAt: "2026-06-09T01:00:00Z", finishedAt: str("2026-06-09T03:00:00Z"),
		snapshotDate: "2026-06-08", sinkKind: "local", gitSHA: str("f1r5t"), status: "complete",
		counters: []*int64{i64(5524), i64(5524), i64(0), i64(71400000), i64(250000), i64(250000), i64(12)},
	})
	insertIngestRun(db, ingestRunSeed{
		startedAt: "2026-06-10T01:00:00Z", finishedAt: str("2026-06-10T02:30:00Z"),
		snapshotDate: "2026-06-08", sinkKind: "local", gitSHA: nil, status: "complete",
		counters: []*int64{i64(5524), i64(5523), i64(1), i64(71399000), i64(250000), i64(249988), nil},
	})
	insertIngestRun(db, ingestRunSeed{
		startedAt: "2026-06-11T01:00:00Z", finishedAt: str("2026-06-11T01:05:00Z"),
		snapshotDate: "2026-06-08", sinkKind: "local", gitSHA: str("ab0rt"), status: "aborted",
		counters: []*int64{i64(100), i64(90), i64(10), i64(1), i64(1), i64(1), i64(1)},
	})

	ids := map[string]int64{}
	ids["power-monthly-1981-2010"] = insertPowerRun(db, powerRunSeed{
		startedAt: "2026-07-01T00:00:00Z", finishedAt: str("2026-07-01T06:00:00Z"), status: "complete",
		endpoint: "https://power.larc.nasa.gov/api/temporal/monthly/point", parameters: "T2M,ALLSKY_SFC_SW_DWN",
		community: "AG", startYear: 1981, endYear: 2010, gridResolution: "0.5x0.625",
		unitConversions: str(conversionsText), temporalMode: "monthly",
		gitSHA:   str("p0w3r"),
		counters: []*int64{i64(2400), i64(2400), i64(0), i64(28800)},
	})
	ids["power-daily-1981-2026"] = insertPowerRun(db, powerRunSeed{
		startedAt: "2026-07-02T00:00:00Z", finishedAt: nil, status: "complete",
		endpoint: "https://power.larc.nasa.gov/api/temporal/daily/point?x=1&y=2", parameters: "T2M",
		community: "AG", startYear: 1981, endYear: 2026, gridResolution: "0.5x0.625",
		unitConversions: nil, temporalMode: "daily",
		startDate: str("1981-01-01"), endDate: str("2026-06-08"),
		gitSHA:   nil,
		counters: []*int64{i64(2400), nil, i64(2), nil},
	})
	// Unreferenced: no supplement row points at it, so it is not provenance.
	insertPowerRun(db, powerRunSeed{
		startedAt: "2026-07-03T00:00:00Z", finishedAt: str("2026-07-03T00:01:00Z"), status: "aborted",
		endpoint: "https://power.larc.nasa.gov/api/temporal/monthly/point", parameters: "T2M",
		community: "AG", startYear: 1991, endYear: 2020, gridResolution: "0.5x0.625",
		unitConversions: str(`{}`), temporalMode: "monthly",
		counters: []*int64{i64(0), i64(0), i64(0), i64(0)},
	})

	Expect(ids["power-monthly-1981-2010"]).To(Equal(seededMonthlyRunID))
	Expect(ids["power-daily-1981-2026"]).To(Equal(seededDailyRunID))

	insertCell(db, "n21.75_w89.375", 21.75, -89.375)
	insertCell(db, "n20.75_w88.125", 20.75, -88.125)
	insertMonthlySupplement(db, "n21.75_w89.375", "1981-2010", 1, ids["power-monthly-1981-2010"])
	insertMonthlySupplement(db, "n21.75_w89.375", "1981-2010", 2, ids["power-monthly-1981-2010"])
	insertDailySupplement(db, "n20.75_w88.125", "2020-01-01", ids["power-daily-1981-2026"])
	return ids
}

// seedRunEdges adds to seedRuns the rows the provenance loaders must
// skip and the gate refuses: a running ingest run for the snapshot,
// newest of all, which must not win over the complete one, and a
// daily_supplement row with no run (the schema's forward-compat NULL),
// which references nothing and must not disturb the run list. A DB
// carrying them cannot pass the gate, so only the loader, QA, and
// state-database specs seed them.
func seedRunEdges(db *sql.DB) {
	GinkgoHelper()
	insertIngestRun(db, ingestRunSeed{
		startedAt: "2026-06-12T01:00:00Z", finishedAt: nil,
		snapshotDate: "2026-06-08", sinkKind: "local", gitSHA: str("runn1ng"), status: "running",
		counters: []*int64{nil, nil, nil, nil, nil, nil, nil},
	})
	insertDailySupplement(db, "n20.75_w88.125", "2020-01-02", nil)
}

// The provenance seedRuns yields, as the manifest must carry it.
var (
	wantIngestRuns = []publish.IngestRunRef{
		{
			SnapshotDate: "2026-05-01", StartedAt: "2026-05-01T10:00:00Z", FinishedAt: str("2026-05-01T12:00:00Z"),
			SinkKind: "r2", ETLGitSHA: str("0ld5ha"), Status: "complete",
			StationsAttempted: i64(5000), StationsSucceeded: i64(4990), StationsFailed: i64(10),
			DailyRows: i64(70000000), NormalsRows: i64(200000), ExtrasRows: i64(200000), WarningsTotal: i64(40),
		},
		{
			SnapshotDate: "2026-06-08", StartedAt: "2026-06-10T01:00:00Z", FinishedAt: str("2026-06-10T02:30:00Z"),
			SinkKind: "local", ETLGitSHA: nil, Status: "complete",
			StationsAttempted: i64(5524), StationsSucceeded: i64(5523), StationsFailed: i64(1),
			DailyRows: i64(71399000), NormalsRows: i64(250000), ExtrasRows: i64(249988), WarningsTotal: nil,
		},
	}
	wantPowerRuns = []publish.PowerRunRef{
		{
			RunLabel:  "power-daily-1981-2026",
			StartedAt: "2026-07-02T00:00:00Z", FinishedAt: nil, Status: "complete",
			EndpointURL: "https://power.larc.nasa.gov/api/temporal/daily/point?x=1&y=2", Parameters: "T2M",
			Community: "AG", PeriodStartYear: 1981, PeriodEndYear: 2026, GridResolution: "0.5x0.625",
			UnitConversions: nil, TemporalMode: "daily",
			PeriodStartDate: str("1981-01-01"), PeriodEndDate: str("2026-06-08"),
			CellsAttempted: i64(2400), CellsSucceeded: nil, CellsFailed: i64(2), SupplementRows: nil, ETLGitSHA: nil,
		},
		{
			RunLabel:  "power-monthly-1981-2010",
			StartedAt: "2026-07-01T00:00:00Z", FinishedAt: str("2026-07-01T06:00:00Z"), Status: "complete",
			EndpointURL: "https://power.larc.nasa.gov/api/temporal/monthly/point", Parameters: "T2M,ALLSKY_SFC_SW_DWN",
			Community: "AG", PeriodStartYear: 1981, PeriodEndYear: 2010, GridResolution: "0.5x0.625",
			UnitConversions: json.RawMessage(conversionsText), TemporalMode: "monthly",
			PeriodStartDate: nil, PeriodEndDate: nil,
			CellsAttempted: i64(2400), CellsSucceeded: i64(2400), CellsFailed: i64(0), SupplementRows: i64(28800),
			ETLGitSHA: str("p0w3r"),
		},
	}
)

// compactRaw normalizes a RawMessage's whitespace so a value re-indented
// by the manifest encoder compares against the stored text; key order
// and number literals are untouched by the normalization, so the
// comparison still proves the text was never re-serialized. A nil
// message encodes as null and decodes as the text "null" — one value.
func compactRaw(raw json.RawMessage) string {
	GinkgoHelper()
	if raw == nil {
		return "null"
	}
	var buf bytes.Buffer
	Expect(json.Compact(&buf, raw)).To(Succeed())
	return buf.String()
}

// expectPowerRunsEqual compares decoded power runs to the expected ones,
// the raw unit_conversions by compacted text and everything else exactly.
func expectPowerRunsEqual(got, want []publish.PowerRunRef) {
	GinkgoHelper()
	Expect(got).To(HaveLen(len(want)))
	for i := range want {
		g, w := got[i], want[i]
		Expect(compactRaw(g.UnitConversions)).To(Equal(compactRaw(w.UnitConversions)), w.RunLabel)
		g.UnitConversions, w.UnitConversions = nil, nil
		Expect(g).To(Equal(w))
	}
}

var _ = Describe("RunLabel", func() {
	It("derives power-<temporal_mode>-<start>-<end> for both modes", func() {
		Expect(publish.RunLabel("monthly", 1981, 2010)).To(Equal("power-monthly-1981-2010"))
		Expect(publish.RunLabel("monthly", 1991, 2020)).To(Equal("power-monthly-1991-2020"))
		Expect(publish.RunLabel("daily", 1981, 2026)).To(Equal("power-daily-1981-2026"))
	})
})

var _ = Describe("DatasetMetadata", func() {
	It("carries the BioclimaMX title, v0.1, CC-BY-4.0, the settled personal creator with ORCID, and the citation", func() {
		Expect(publish.DatasetMetadata(seededSnapshot, "")).To(Equal(publish.Dataset{
			Title:        "BioclimaMX Stations: Mexican Climate Station Records (CONAGUA), Augmented with NASA POWER",
			Version:      "0.1",
			License:      "CC-BY-4.0",
			Creator:      "Pablo Trinidad",
			CreatorORCID: "0009-0007-4050-494X",
			SuggestedCitation: "Pablo Trinidad (2026). BioclimaMX Stations: Mexican Climate Station Records " +
				"(CONAGUA), Augmented with NASA POWER. Version 0.1. Zenodo.",
		}))
	})

	It("names only the dataset under the BioclimaMX scheme: the provenance tag keeps the producing project's name", func() {
		Expect(publish.DatasetTitle).To(HavePrefix("BioclimaMX "))
		// bioclima_derived names the ETL that computed the value, not
		// the dataset, and is deliberately not renamed with the title.
		Expect(buildDictionary().Tags).To(ContainElement(HaveField("Tag", "bioclima_derived")))
	})

	It("omits the DOI clause while no DOI is baked in, and never stands a placeholder in for one", func() {
		d := publish.DatasetMetadata(seededSnapshot, "")
		Expect(d.DOI).To(BeEmpty())
		Expect(d.SuggestedCitation).To(HaveSuffix("Version 0.1. Zenodo."))
		Expect(d.SuggestedCitation).NotTo(ContainSubstring("DOI"))
		Expect(d.SuggestedCitation).NotTo(ContainSubstring("placeholder"))
		Expect(d.SuggestedCitation).NotTo(ContainSubstring("<"))
	})

	It("takes the citation year from the snapshot, never the clock", func() {
		later := time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)
		Expect(publish.DatasetMetadata(later, "").SuggestedCitation).To(HavePrefix("Pablo Trinidad (2027). "))
		Expect(publish.DatasetMetadata(later, "").SuggestedCitation).NotTo(ContainSubstring("2026"))
	})
})

var _ = Describe("ValidateDOI", func() {
	DescribeTable("accepts no DOI and bare DOIs",
		func(doi string) { Expect(publish.ValidateDOI(doi)).To(Succeed()) },
		Entry("none", ""),
		Entry("a Zenodo DOI", "10.5281/zenodo.1234567"),
		Entry("a Zenodo sandbox DOI", "10.5072/zenodo.42"),
		Entry("a suffix with the other DOI characters", "10.1000/ABC-def_1.2;(3)/4:5"),
	)

	DescribeTable("refuses anything a citation could not carry as an identifier",
		func(doi, want string) { Expect(publish.ValidateDOI(doi)).To(MatchError(ContainSubstring(want))) },
		Entry("a resolver URL, naming the bare form", "https://doi.org/10.5281/zenodo.1234567", `pass the bare DOI, "10.5281/zenodo.1234567"`),
		Entry("a dx resolver URL", "https://dx.doi.org/10.5281/zenodo.7", `pass the bare DOI, "10.5281/zenodo.7"`),
		Entry("a doi: prefix", "doi:10.5281/zenodo.1", `pass the bare DOI, "10.5281/zenodo.1"`),
		Entry("a Zenodo record URL", "https://zenodo.org/records/1234567", "is not a DOI"),
		Entry("a bare record number", "1234567", "is not a DOI"),
		Entry("no suffix", "10.5281/", "is not a DOI"),
		Entry("a short registrant code", "10.52/zenodo.1", "is not a DOI"),
		Entry("inner whitespace", "10.5281/zenodo 1", "is not a DOI"),
		Entry("a placeholder", "<concept-DOI placeholder>", "is not a DOI"),
	)
})

var _ = Describe("DatasetMetadata with a DOI", func() {
	It("carries the DOI and closes the citation with it", func() {
		d := publish.DatasetMetadata(seededSnapshot, "10.5281/zenodo.1234567")
		Expect(d.DOI).To(Equal("10.5281/zenodo.1234567"))
		Expect(d.SuggestedCitation).To(Equal("Pablo Trinidad (2026). BioclimaMX Stations: Mexican Climate Station " +
			"Records (CONAGUA), Augmented with NASA POWER. Version 0.1. Zenodo. DOI: 10.5281/zenodo.1234567"))
	})
})

var _ = Describe("the dataset DOI in manifest.json", func() {
	var path string

	BeforeEach(func() { path = filepath.Join(GinkgoT().TempDir(), "manifest.json") })

	// writeDataset writes a manifest carrying d and reads the file back,
	// returning both the raw bytes and the decoded dataset block.
	writeDataset := func(d publish.Dataset) (string, publish.Dataset) {
		GinkgoHelper()
		m := &publish.Manifest{SchemaVersion: schema.Version, GeneratedAt: "2026-08-29T12:34:56Z",
			SnapshotDate: "2026-06-08", Dataset: d}
		_, err := publish.WriteManifest(path, m)
		Expect(err).NotTo(HaveOccurred())
		data, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		var back publish.Manifest
		Expect(json.Unmarshal(data, &back)).To(Succeed())
		return string(data), back.Dataset
	}

	It("omits the doi key entirely while the DOI is empty", func() {
		text, back := writeDataset(publish.DatasetMetadata(seededSnapshot, ""))
		Expect(text).NotTo(ContainSubstring(`"doi"`))
		Expect(text).NotTo(ContainSubstring("placeholder"))
		Expect(back).To(Equal(publish.DatasetMetadata(seededSnapshot, "")))
		Expect(back.DOI).To(BeEmpty())
	})

	It("round-trips the reserved DOI, in key order after version, once it is set", func() {
		d := publish.DatasetMetadata(seededSnapshot, "")
		d.DOI = "10.5281/zenodo.9999999"
		d.SuggestedCitation += " DOI: " + d.DOI
		text, back := writeDataset(d)
		Expect(back).To(Equal(d))
		Expect(text).To(ContainSubstring("\"version\": \"0.1\",\n    \"doi\": \"10.5281/zenodo.9999999\",\n    \"license\""))
		Expect(text).To(ContainSubstring("Version 0.1. Zenodo. DOI: 10.5281/zenodo.9999999"))
	})
})

var _ = Describe("PowerParameters", func() {
	It("renders every registry row, field for field, in registry order", func() {
		got := publish.PowerParameters()
		Expect(got).To(HaveLen(len(power.Registry)))
		Expect(got).To(HaveLen(31))
		for i, p := range power.Registry {
			Expect(got[i]).To(Equal(publish.PowerParameter{
				ID: p.Name, Column: p.Column, PowerUnit: p.PowerUnit, StoredUnit: p.StoredUnit, Factor: p.Factor,
			}), p.Name)
		}
		// The two conversion classes the registry carries: identity, and
		// the MJ/m²/day → W/m² solar factor.
		Expect(got[0]).To(Equal(publish.PowerParameter{
			ID: "T2M", Column: "t2m_c", PowerUnit: "C", StoredUnit: "C", Factor: 1,
		}))
		Expect(got[15]).To(Equal(publish.PowerParameter{
			ID: "ALLSKY_SFC_SW_DWN", Column: "solar_ghi_wm2", PowerUnit: "MJ/m^2/day", StoredUnit: "W/m^2",
			Factor: 1e6 / 86400,
		}))
	})
})

var _ = Describe("LoadIngestRuns", func() {
	It("keeps the latest complete run per snapshot_date, every column round-tripped, in snapshot order", func() {
		db := openTempDB()
		seedRuns(db)
		seedRunEdges(db)
		got, err := publish.LoadIngestRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(wantIngestRuns))
	})

	It("breaks a started_at tie by the higher id", func() {
		db := openTempDB()
		for _, sha := range []string{"first", "second"} {
			insertIngestRun(db, ingestRunSeed{
				startedAt: "2026-06-09T01:00:00Z", snapshotDate: "2026-06-08", sinkKind: "local",
				gitSHA: str(sha), status: "complete", counters: make([]*int64, 7),
			})
		}
		got, err := publish.LoadIngestRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(1))
		Expect(got[0].ETLGitSHA).To(Equal(str("second")))
	})

	It("returns an empty list, never nil, when no run is complete", func() {
		db := openTempDB()
		insertIngestRun(db, ingestRunSeed{
			startedAt: "2026-06-09T01:00:00Z", snapshotDate: "2026-06-08", sinkKind: "local",
			status: "aborted", counters: make([]*int64, 7),
		})
		got, err := publish.LoadIngestRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]publish.IngestRunRef{}))
	})
})

var _ = Describe("LoadPowerRuns", func() {
	var db *sql.DB

	BeforeEach(func() {
		db = openTempDB()
		seedRuns(db)
		seedRunEdges(db)
	})

	It("lists only the runs supplement rows reference, labelled, sorted by label, every column round-tripped", func() {
		got, err := publish.LoadPowerRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(wantPowerRuns))
		// The stored text is embedded byte for byte — never decoded.
		Expect(string(got[1].UnitConversions)).To(Equal(conversionsText))
	})

	It("fails the build when two referenced runs share a run_label, naming both ids", func() {
		dup := insertPowerRun(db, powerRunSeed{
			startedAt: "2026-07-04T00:00:00Z", status: "complete",
			endpoint: "https://power.larc.nasa.gov/api/temporal/monthly/point", parameters: "T2M",
			community: "AG", startYear: 1981, endYear: 2010, gridResolution: "0.5x0.625",
			temporalMode: "monthly", counters: make([]*int64, 4),
		})
		insertMonthlySupplement(db, "n20.75_w88.125", "1981-2010", 1, dup)

		_, err := publish.LoadPowerRuns(context.Background(), db)
		Expect(err).To(MatchError(ContainSubstring(
			`power_runs 1 and 4 share run_label "power-monthly-1981-2010"`)))
	})

	It("leaves two runs of the same period alone when only one is referenced", func() {
		insertPowerRun(db, powerRunSeed{
			startedAt: "2026-07-04T00:00:00Z", status: "aborted",
			endpoint: "u", parameters: "T2M", community: "AG", startYear: 1981, endYear: 2010,
			gridResolution: "0.5x0.625", temporalMode: "monthly", counters: make([]*int64, 4),
		})
		got, err := publish.LoadPowerRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(2))
	})

	It("refuses a unit_conversions text that is not JSON rather than corrupt the manifest", func() {
		mustExec(db, `UPDATE power_runs SET unit_conversions = '{not json' WHERE id = 1`)
		_, err := publish.LoadPowerRuns(context.Background(), db)
		Expect(err).To(MatchError(ContainSubstring("power_runs 1: unit_conversions is not valid JSON")))
	})

	It("returns an empty list, never nil, when no supplement row exists", func() {
		mustExec(db, `DELETE FROM monthly_supplement`)
		mustExec(db, `DELETE FROM daily_supplement`)
		got, err := publish.LoadPowerRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]publish.PowerRunRef{}))
	})
})

var _ = Describe("LoadCounts", func() {
	It("counts every table", func() {
		db := openTempDB()
		seedTwoStates(db)
		seedRuns(db)
		seedRunEdges(db)
		mustExec(db, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
		  VALUES (1, 'daily/31001.txt', 3, 'warn', 'x')`)
		mustExec(db, `INSERT INTO station_power_cell (station_id, cell_id, distance_km)
		  VALUES (1, 'n21.75_w89.375', 11.2)`)

		got, err := publish.LoadCounts(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(publish.TableCounts{
			Stations:             5,
			MonthlyNormals:       7,
			MonthlyNormalsExtras: 5,
			DailyObservations:    6,
			ParsingWarnings:      1,
			IngestRuns:           5,
			PowerRuns:            3,
			NasaPowerGridCells:   2,
			StationPowerCell:     1,
			MonthlySupplement:    2,
			DailySupplement:      2,
		}))
	})

	It("covers exactly the tables the embedded DDL creates (lockstep)", func() {
		db := openTempDB()
		rows, err := db.Query(`SELECT name FROM sqlite_master
		  WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = rows.Close() }()
		var ddl []string
		for rows.Next() {
			var n string
			Expect(rows.Scan(&n)).To(Succeed())
			ddl = append(ddl, n)
		}
		Expect(rows.Err()).NotTo(HaveOccurred())

		t := reflect.TypeOf(publish.TableCounts{})
		var counted []string
		for i := 0; i < t.NumField(); i++ {
			counted = append(counted, t.Field(i).Tag.Get("json"))
		}
		sort.Strings(counted)
		Expect(counted).To(Equal(ddl))
	})
})

var _ = Describe("WriteManifest", func() {
	var (
		db   *sql.DB
		want *publish.Manifest
		path string
	)

	BeforeEach(func() {
		db = openTempDB()
		seedTwoStates(db)
		seedRuns(db)
		ingestRuns, err := publish.LoadIngestRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		powerRuns, err := publish.LoadPowerRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		counts, err := publish.LoadCounts(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		want = &publish.Manifest{
			SchemaVersion: schema.Version,
			ETLGitSHA:     "abc123",
			GeneratedAt:   "2026-08-29T12:34:56Z",
			SnapshotDate:  "2026-06-08",
			Dataset:       publish.DatasetMetadata(seededSnapshot, ""),
			States: []publish.ManifestState{
				{Code: "AGS", Name: "Aguascalientes", Artifacts: []string{}},
				{Code: "YUC", Name: "Yucatán", Artifacts: []string{"yuc-tabular.zip"}},
			},
			Runs:            publish.Runs{Ingest: ingestRuns, Power: powerRuns},
			PowerParameters: publish.PowerParameters(),
			Counts:          counts,
			Files: []publish.ManifestFile{
				{Name: "yuc-tabular.zip", SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", Bytes: 4321},
			},
		}
		path = filepath.Join(GinkgoT().TempDir(), "manifest.json")
	})

	It("round-trips every field through the file", func() {
		res, err := publish.WriteManifest(path, want)
		Expect(err).NotTo(HaveOccurred())

		data, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Bytes).To(Equal(int64(len(data))))
		Expect(res.SHA256).To(HaveLen(64))

		var got publish.Manifest
		Expect(json.Unmarshal(data, &got)).To(Succeed())
		expectPowerRunsEqual(got.Runs.Power, want.Runs.Power)
		got.Runs.Power, want.Runs.Power = nil, nil
		Expect(got).To(Equal(*want))
	})

	It("writes explicit nulls, two-space indentation, a trailing newline, and no HTML escaping", func() {
		_, err := publish.WriteManifest(path, want)
		Expect(err).NotTo(HaveOccurred())
		data, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		text := string(data)

		Expect(text).To(HavePrefix("{\n  \"schema_version\": 1,\n  \"etl_git_sha\": \"abc123\",\n"))
		Expect(text).To(HaveSuffix("}\n"))
		// NULL columns are written, never omitted.
		Expect(text).To(ContainSubstring("\"etl_git_sha\": null"))
		Expect(text).To(ContainSubstring("\"warnings_total\": null"))
		Expect(text).To(ContainSubstring("\"unit_conversions\": null"))
		Expect(text).To(ContainSubstring("\"finished_at\": null"))
		Expect(text).To(ContainSubstring("\"period_start_date\": null"))
		// A failed state's artifact list is an empty array, not null.
		Expect(text).To(ContainSubstring("\"artifacts\": []"))
		// The endpoint URL's query separator reads as written, and no
		// DOI placeholder stands anywhere in the file — a deposited file
		// is immutable, so a placeholder could never be patched.
		Expect(text).To(ContainSubstring("point?x=1&y=2"))
		Expect(text).NotTo(ContainSubstring("placeholder"))
		Expect(text).NotTo(ContainSubstring("<"))
		Expect(text).NotTo(ContainSubstring(`\u003c`))
		Expect(text).NotTo(ContainSubstring(`\u0026`))
		// Integers are JSON numbers; the embedded conversions keep their
		// key order and number literals.
		Expect(text).To(ContainSubstring("\"daily_rows\": 71399000"))
		Expect(text).To(MatchRegexp(`"T2M": \{[\s\S]*?"factor": 1\n[\s\S]*?"ALLSKY_SFC_SW_DWN": \{[\s\S]*?"factor": 11\.574074074074074\n`))
		// The registry's factors are written with the same literals the
		// stored unit_conversions carry, so the two agree byte for byte.
		Expect(text).To(ContainSubstring("{\n      \"id\": \"T2M\",\n      \"column\": \"t2m_c\",\n" +
			"      \"power_unit\": \"C\",\n      \"stored_unit\": \"C\",\n      \"factor\": 1\n    }"))
		Expect(text).To(ContainSubstring("\"column\": \"solar_ghi_wm2\",\n      \"power_unit\": \"MJ/m^2/day\",\n" +
			"      \"stored_unit\": \"W/m^2\",\n      \"factor\": 11.574074074074074\n"))
	})

	It("writes atomically: a failing encode leaves no file behind", func() {
		bad := *want
		bad.Runs.Power = []publish.PowerRunRef{{RunLabel: "x", UnitConversions: json.RawMessage("{oops")}}
		_, err := publish.WriteManifest(path, &bad)
		Expect(err).To(MatchError(ContainSubstring("write manifest")))
		_, statErr := os.Stat(path)
		Expect(os.IsNotExist(statErr)).To(BeTrue())
	})
})
