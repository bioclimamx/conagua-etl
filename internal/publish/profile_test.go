package publish_test

// Specs for the per-station profile.json: the derived annual against
// CONAGUA's own published annual column on the real normals fixture, the
// incomplete-year null policy and the circular wind annual through the
// profile, a round-trip of every block from a fully seeded station
// (numbers as their literal text, key order, every period key present),
// the all-null shape of a station with no cell and no daily rows, the
// trace-rain day the dry spell and the daily series beside it agree on,
// byte identity across two builds, the absence of any generation stamp,
// and the entry paths.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

var aguascalientes = publish.State{Code: "AGS", Slug: "ags", Name: "Aguascalientes"}

func profileMeta() publish.ProfileMeta {
	return publish.ProfileMeta{
		SchemaVersion: schema.Version, ETLGitSHA: "abc123", SnapshotDate: "2026-06-08",
		Runs: publish.Runs{
			Ingest: []publish.IngestRunRef{{SnapshotDate: "2026-05-01"}, {SnapshotDate: "2026-06-08"}},
			Power:  []publish.PowerRunRef{{RunLabel: "power-daily-1981-2026"}, {RunLabel: "power-monthly-1981-2010"}},
		},
		Dataset: publish.DatasetMetadata(time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC), ""),
	}
}

// profileBytes builds one state's profile entries and renders the
// station's.
func profileBytes(db *sql.DB, st publish.State, externalID string) []byte {
	GinkgoHelper()
	entries, err := publish.ProfileEntries(context.Background(), db, st, profileMeta())
	Expect(err).NotTo(HaveOccurred())
	return render(entryByPath(entries, "combined/"+st.Slug+"/"+externalID+"/profile.json"))
}

// decodeProfile parses profile bytes generically, numbers kept as their
// literal text so the fixed-decimal rendering is what is asserted.
func decodeProfile(b []byte) map[string]any {
	GinkgoHelper()
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var got map[string]any
	Expect(dec.Decode(&got)).To(Succeed())
	return got
}

func obj(v any, keys ...string) map[string]any {
	GinkgoHelper()
	for _, k := range keys {
		m, ok := v.(map[string]any)
		Expect(ok).To(BeTrue(), "not an object at %q", k)
		v = m[k]
	}
	m, ok := v.(map[string]any)
	Expect(ok).To(BeTrue(), "not an object: %v", v)
	return m
}

// rawObj decodes one object level, values left raw so key order can be
// read from each.
func rawObj(raw []byte) map[string]json.RawMessage {
	GinkgoHelper()
	var m map[string]json.RawMessage
	Expect(json.Unmarshal(raw, &m)).To(Succeed())
	return m
}

// objectKeys lists an object's keys in the order they were written.
func objectKeys(raw []byte) []string {
	GinkgoHelper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var keys []string
	depth := 0
	expectKey := false
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		Expect(err).NotTo(HaveOccurred())
		switch t := tok.(type) {
		case json.Delim:
			switch t {
			case '{', '[':
				depth++
				expectKey = depth == 1 && t == '{'
			default:
				depth--
				expectKey = depth == 1
			}
		default:
			if depth != 1 {
				continue
			}
			if expectKey {
				keys = append(keys, t.(string))
			}
			expectKey = !expectKey
		}
	}
	return keys
}

func num(s string) json.Number { return json.Number(s) }
func n1(v float64) json.Number { return num(fmt.Sprintf("%.1f", v)) }
func n2(v float64) json.Number { return num(fmt.Sprintf("%.2f", v)) }

// months builds a 13-slot expected series from a per-month value and
// the annual.
func months(f func(m int) any, annual any) []any {
	out := make([]any, 13)
	for m := 1; m <= 12; m++ {
		out[m-1] = f(m)
	}
	out[12] = annual
	return out
}

// firstOnly is a series with only slot 0 set.
func firstOnly(v any) []any {
	return months(func(m int) any {
		if m == 1 {
			return v
		}
		return nil
	}, nil)
}

var nullSeries = months(func(int) any { return nil }, nil)

func nullPeriods() map[string]any {
	return map[string]any{"1961-1990": nil, "1971-2000": nil, "1981-2010": nil, "1991-2020": nil}
}

func floatArg(p *float64) any {
	if p == nil {
		return nil
	}
	return *p
}

func intArg(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

func textArg(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}

// circularOf is the expected annual of twelve directions at one decimal.
func circularOf(vals ...float64) json.Number {
	GinkgoHelper()
	Expect(vals).To(HaveLen(12))
	return n1(power.CircularMeanDegrees(vals))
}

// seedProfile populates one Yucatán station with every profile input
// exercised — identity with a NULL municipality and a name that needs
// quoting, all eight WMO scores with one NULL pair, normals in three of
// four periods (a complete one, one with a NULL month, one with a
// single month), extras with every column once and an all-NULL month,
// a cell with two monthly periods (a distinct value per column and
// month, then a NULL-bearing month), daily rows inserted out of order
// with NULLs, an all-NULL day, a tie on the record tmax, a dry run
// broken by a NULL day and another by a missing date, and the cell's
// daily series — plus a second station with nothing, an EMA station
// sharing the external id, and another state's station, none of which
// may leak into the first's profile.
func seedProfile(db *sql.DB) map[string]int64 {
	GinkgoHelper()
	conv := ingest.SourceConaguaConventional
	ids := map[string]int64{}

	ids["31001"] = upsertStation(db, ingest.StationUpsert{
		Source: conv, ExternalID: "31001", Name: `Mérida, "La Plancha" <&>`, State: "YUC",
		Lat: f64(21.85027778), Lon: f64(-89.375), AltitudeM: f64(9), Status: "operating",
	})
	mustExec(db, `UPDATE stations SET first_year = 1951, last_year = 2026,
	  wmo_completeness_bin_1961_1990 = ?, wmo_completeness_bin_1971_2000 = 0.5,
	  wmo_completeness_bin_1981_2010 = NULL, wmo_completeness_bin_1991_2020 = 1,
	  wmo_completeness_cont_1961_1990 = 0.9324074074, wmo_completeness_cont_1971_2000 = 0,
	  wmo_completeness_cont_1981_2010 = NULL, wmo_completeness_cont_1991_2020 = ?
	  WHERE id = ?`, 35.0/36.0, 1.0/1080.0, ids["31001"])
	ids["31002"] = upsertStation(db, ingest.StationUpsert{Source: conv, ExternalID: "31002", Name: "Tizimín", State: "YUC"})
	ids["ema"] = upsertStation(db, ingest.StationUpsert{Source: ingest.SourceConaguaEMA, ExternalID: "31001", Name: "EMA", State: "YUC"})
	ids["1001"] = upsertStation(db, ingest.StationUpsert{Source: conv, ExternalID: "1001", Name: "Aguascalientes (OBS)", State: "AGS"})

	for m := 12; m >= 1; m-- {
		insertNormals(db, ids["31001"], "1981-2010", m, float64(m+20), float64(m+5), float64(m)+12.5, float64(10*m), float64(100+m))
		var tmax any = float64(m + 21)
		if m == 6 {
			tmax = nil
		}
		insertNormals(db, ids["31001"], "1991-2020", m, tmax, float64(m+6), float64(m)+13.5, float64(10*m+1), float64(101+m))
	}
	insertNormals(db, ids["31001"], "1961-1990", 3, 30.0, nil, 22.2, 0.0, 150.3)
	insertNormals(db, ids["ema"], "1971-2000", 1, 1.0, 1.0, 1.0, 1.0, 1.0)
	insertNormals(db, ids["1001"], "1971-2000", 1, 2.0, 2.0, 2.0, 2.0, 2.0)

	insertExtras(db, ids["31001"], "1981-2010", 12,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	insertExtras(db, ids["31001"], "1981-2010", 1,
		38.5, 1998, 42.0, "1998-05-14",
		8.25, 1985, 3.5, "1985-01-20",
		210.6, 2002, 98.7, "2002-09-23",
		28, 27, 27, 30, 25, 4.6, 29)
	insertExtras(db, ids["ema"], "1981-2010", 1,
		1.0, 1, 1.0, "1981-01-01", 1.0, 1, 1.0, "1981-01-01", 1.0, 1, 1.0, "1981-01-01", 1, 1, 1, 1, 1, 1.0, 1)

	insertCell(db, cellShared, 21.5, -89.375)
	insertCell(db, cellAGS, 22.0, -102.5)
	insertStationCell(db, ids["31001"], cellShared, 27.2524061152165)
	insertStationCell(db, ids["ema"], cellAGS, 1)
	for m := 12; m >= 1; m-- {
		insertPowerMonthly(db, cellShared, "1981-2010", m, powerSeqRow(float64(10*m)))
		if m == 1 {
			insertPowerMonthly(db, cellShared, "1991-2020", m, mixedPowerRow)
		} else {
			insertPowerMonthly(db, cellShared, "1991-2020", m, fullPowerRow)
		}
	}
	insertPowerMonthly(db, cellAGS, "1961-1990", 1, powerSeqRow(900))

	insertDaily(db, ids["31001"], "2020-01-06", 27.5, 16.0, 0.0, 3.8)
	insertDaily(db, ids["31001"], "2020-01-11", nil, nil, 12.4, nil)
	insertDaily(db, ids["31001"], "2020-01-01", 30.5, nil, 0.0, nil)
	insertDaily(db, ids["31001"], "2020-01-02", 31.0, 19.5, 0.0, 4.2)
	insertDaily(db, ids["31001"], "2020-01-03", 29.0, 18.0, nil, 4.0)
	insertDaily(db, ids["31001"], "2020-01-04", 31.0, 17.5, 0.0, 4.1)
	insertDaily(db, ids["31001"], "2020-01-05", 28.0, 17.0, 0.0, 3.9)
	insertDaily(db, ids["31001"], "2020-01-08", 26.0, 15.0, 0.0, 3.0)
	insertDaily(db, ids["31001"], "2020-01-09", 25.0, 14.0, 0.0, 2.9)
	insertDaily(db, ids["31001"], "2020-01-10", 24.0, 13.0, 0.0, 2.8)
	insertDaily(db, ids["31001"], "1999-12-31", nil, -0.04, nil, nil)
	insertDaily(db, ids["31001"], "2000-01-01", nil, nil, nil, nil)
	insertDaily(db, ids["ema"], "2020-01-01", 40.0, -10.0, 99.9, 9.9)
	insertDaily(db, ids["ema"], "1900-01-01", 40.0, -10.0, 0.0, 9.9)

	insertPowerDaily(db, cellShared, "2020-01-03", powerSeqRow(400))
	insertPowerDaily(db, cellShared, "2020-01-01", fullPowerRow)
	insertPowerDaily(db, cellShared, "1999-12-31", mixedPowerRow)
	insertPowerDaily(db, cellAGS, "1990-01-01", fullPowerRow)
	return ids
}

var _ = Describe("the derived annual on CONAGUA's published normals", func() {
	It("reproduces the fixture's own published annual column: mean for temperatures, sum for precip and evap", func() {
		f, err := os.Open("../conagua/testdata/normals/real_1991_2020_01001.txt")
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = f.Close() }()
		_, normals, extras, _, err := conagua.ParseNormalsFile(f, "1991-2020")
		Expect(err).NotTo(HaveOccurred())
		Expect(normals).To(HaveLen(12))
		Expect(extras).To(HaveLen(12))

		db := openTempDB()
		id := upsertStation(db, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "1001", Name: "AGUASCALIENTES (OBS)", State: "AGS",
		})
		for _, r := range normals {
			insertNormals(db, id, "1991-2020", r.Month,
				floatArg(r.Tmax), floatArg(r.Tmin), floatArg(r.Tmean), floatArg(r.Precip), floatArg(r.Evap))
		}
		for _, e := range extras {
			insertExtras(db, id, "1991-2020", e.Month,
				floatArg(e.TmaxMonthlyExtreme), intArg(e.TmaxMonthlyExtremeYear),
				floatArg(e.TmaxDailyExtreme), textArg(e.TmaxDailyExtremeDate),
				floatArg(e.TminMonthlyExtreme), intArg(e.TminMonthlyExtremeYear),
				floatArg(e.TminDailyExtreme), textArg(e.TminDailyExtremeDate),
				floatArg(e.PrecipMonthlyExtreme), intArg(e.PrecipMonthlyExtremeYear),
				floatArg(e.PrecipDailyExtreme), textArg(e.PrecipDailyExtremeDate),
				intArg(e.TmaxYearsWithData), intArg(e.TminYearsWithData), intArg(e.TmeanYearsWithData),
				intArg(e.PrecipYearsWithData), intArg(e.EvapYearsWithData),
				floatArg(e.RainDays), intArg(e.RainDaysYearsWithData))
		}

		got := decodeProfile(profileBytes(db, aguascalientes, "1001"))
		period := obj(got, "normals", "periods", "1991-2020")

		// CONAGUA's published annual row in the fixture: 27.4, 10.6, 19, 431.9, 1750.4.
		Expect(period["tmax_c"].([]any)[12]).To(Equal(num("27.4")))
		Expect(period["precip_mm"].([]any)[12]).To(Equal(num("431.9")))
		Expect(period["evap_mm"].([]any)[12]).To(Equal(num("1750.4")))

		mean := func(get func(conagua.MonthlyNormalsRow) *float64) json.Number {
			var sum float64
			for _, r := range normals {
				Expect(get(r)).NotTo(BeNil())
				sum += *get(r)
			}
			return n1(sum / 12)
		}
		Expect(period["tmin_c"].([]any)[12]).To(Equal(mean(func(r conagua.MonthlyNormalsRow) *float64 { return r.Tmin })))
		Expect(period["tmin_c"].([]any)[12]).To(Equal(num("10.6")))
		Expect(period["tmean_c"].([]any)[12]).To(Equal(mean(func(r conagua.MonthlyNormalsRow) *float64 { return r.Tmean })))
		Expect(period["tmean_c"].([]any)[12]).To(Equal(num("19.0")))

		// The months as published, one decimal, no gaps in this fixture.
		Expect(period["tmax_c"].([]any)[:12]).To(Equal(months(func(m int) any { return n1(*normals[m-1].Tmax) }, nil)[:12]))
		Expect(period["precip_mm"].([]any)[:12]).To(Equal(months(func(m int) any { return n1(*normals[m-1].Precip) }, nil)[:12]))
		for _, name := range []string{"tmax_c", "tmin_c", "tmean_c", "precip_mm", "evap_mm"} {
			Expect(period[name].([]any)).To(HaveLen(13))
			Expect(period[name].([]any)).NotTo(ContainElement(BeNil()), name)
		}
		Expect(obj(got, "normals", "periods")).To(HaveKeyWithValue("1961-1990", BeNil()))

		// The extras ride along from the same file, slot 12 null.
		ex := obj(got, "extras", "periods", "1991-2020")
		Expect(ex["tmax_daily_extreme_date"].([]any)[0]).To(Equal("2018-01-13"))
		Expect(ex["tmax_monthly_extreme_year"].([]any)[0]).To(Equal(num("2017")))
		Expect(ex["tmin_daily_extreme_c"].([]any)[0]).To(Equal(num("-9.00")))
		// The monthly extreme beside it is a normal, and stays at one.
		Expect(ex["tmin_monthly_extreme_c"].([]any)[0]).To(Equal(num("2.5")))
		Expect(ex["rain_days"].([]any)[0]).To(Equal(num("2.4")))
		Expect(ex["rain_days"].([]any)[12]).To(BeNil())
		Expect(ex["rain_days_years_with_data"].([]any)[0]).To(Equal(num("30")))
		for name, series := range ex {
			Expect(series.([]any)).To(HaveLen(13), name)
			Expect(series.([]any)[12]).To(BeNil(), name)
		}
	})
})

var _ = Describe("the annual slot's null policy and the circular wind mean, through the profile", func() {
	var got map[string]any

	BeforeEach(func() {
		db := openTempDB()
		id := upsertStation(db, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "31003", Name: "Progreso", State: "YUC",
		})
		for m := 1; m <= 12; m++ {
			var tmax, precip any = float64(m + 20), float64(10 * m)
			if m == 2 {
				tmax = nil
			}
			if m == 7 {
				precip = nil
			}
			insertNormals(db, id, "1981-2010", m, tmax, float64(m+5), float64(m)+12.5, precip, float64(100+m))
		}
		insertCell(db, cellSingle, 21.0, -89.625)
		insertStationCell(db, id, cellSingle, 5.5)
		for m := 1; m <= 12; m++ {
			row := withNulls(fullPowerRow)
			row["wd2m_deg"] = powerCase{in: 350.0}
			if m%2 == 0 {
				row["wd2m_deg"] = powerCase{in: 10.0}
			}
			if m == 5 {
				row["wd10m_deg"] = powerCase{in: nil}
			}
			if m == 3 {
				row["t2m_c"] = powerCase{in: nil}
			}
			insertPowerMonthly(db, cellSingle, "1981-2010", m, row)
		}
		got = decodeProfile(profileBytes(db, yucatan, "31003"))
	})

	It("leaves slot 12 null when any month is null, for a sum, a mean, and the circular mean (slot 12 null under a partial year)", func() {
		normals := obj(got, "normals", "periods", "1981-2010")
		Expect(normals["precip_mm"].([]any)[6]).To(BeNil())
		Expect(normals["precip_mm"].([]any)[12]).To(BeNil())
		Expect(normals["tmax_c"].([]any)[1]).To(BeNil())
		Expect(normals["tmax_c"].([]any)[12]).To(BeNil())
		pow := obj(got, "power_monthly", "periods", "1981-2010")
		Expect(pow["wd10m_deg"].([]any)[4]).To(BeNil())
		Expect(pow["wd10m_deg"].([]any)[12]).To(BeNil())
		Expect(pow["t2m_c"].([]any)[2]).To(BeNil())
		Expect(pow["t2m_c"].([]any)[12]).To(BeNil())
	})

	It("sums a complete monthly total and never means it", func() {
		normals := obj(got, "normals", "periods", "1981-2010")
		Expect(normals["evap_mm"].([]any)[12]).To(Equal(num("1278.0")))
		Expect(normals["evap_mm"].([]any)[12]).NotTo(Equal(num("106.5")))
		Expect(normals["tmin_c"].([]any)[12]).To(Equal(num("11.5")))
		Expect(normals["tmean_c"].([]any)[12]).To(Equal(num("19.0")))
	})

	It("takes the circular mean of wind direction exactly as power's function yields, never 180", func() {
		pow := obj(got, "power_monthly", "periods", "1981-2010")
		Expect(pow["wd2m_deg"].([]any)[12]).To(Equal(circularOf(350, 10, 350, 10, 350, 10, 350, 10, 350, 10, 350, 10)))
		Expect(pow["wd2m_deg"].([]any)[12]).To(Equal(num("360.0")))
		Expect(pow["wd2m_deg"].([]any)[12]).NotTo(Equal(num("180.0")))
		Expect(pow["precip_mmpd"].([]any)[12]).To(Equal(num("3.70")))
	})
})

var _ = Describe("the profile round-trip", func() {
	var (
		raw []byte
		got map[string]any
	)

	BeforeEach(func() {
		db := openTempDB()
		seedProfile(db)
		raw = profileBytes(db, yucatan, "31001")
		got = decodeProfile(raw)
	})

	It("carries station_id, identity, and the WMO scores with explicit nulls", func() {
		Expect(got["station_id"]).To(Equal("31001"))
		Expect(obj(got, "identity")).To(Equal(map[string]any{
			"name": `Mérida, "La Plancha" <&>`, "state": "YUC", "state_name": "Yucatán", "municipality": nil,
			"lat": num("21.850278"), "lon": num("-89.375000"), "altitude_m": num("9.0"), "status": "operating",
			"first_year": num("1951"), "last_year": num("2026"),
		}))
		Expect(obj(got, "wmo_completeness")).To(Equal(map[string]any{
			"source": "bioclima_derived",
			"periods": map[string]any{
				"1961-1990": map[string]any{"bin": num("0.9722"), "cont": num("0.9324")},
				"1971-2000": map[string]any{"bin": num("0.5000"), "cont": num("0.0000")},
				"1981-2010": nil,
				"1991-2020": map[string]any{"bin": num("1.0000"), "cont": num("0.0009")},
			},
		}))
	})

	It("carries the normals as 13-slot series per period, the annual per variable, absent periods null", func() {
		Expect(obj(got, "normals")["source"]).To(Equal("conagua_published"))
		Expect(obj(got, "normals")["annual_slot"]).To(Equal("bioclima_derived"))
		Expect(obj(got, "normals", "periods")).To(Equal(map[string]any{
			"1961-1990": map[string]any{
				"tmax_c":    months(func(m int) any { return map[bool]any{true: num("30.0")}[m == 3] }, nil),
				"tmin_c":    nullSeries,
				"tmean_c":   months(func(m int) any { return map[bool]any{true: num("22.2")}[m == 3] }, nil),
				"precip_mm": months(func(m int) any { return map[bool]any{true: num("0.0")}[m == 3] }, nil),
				"evap_mm":   months(func(m int) any { return map[bool]any{true: num("150.3")}[m == 3] }, nil),
			},
			"1971-2000": nil,
			"1981-2010": map[string]any{
				"tmax_c":    months(func(m int) any { return n1(float64(m + 20)) }, num("26.5")),
				"tmin_c":    months(func(m int) any { return n1(float64(m + 5)) }, num("11.5")),
				"tmean_c":   months(func(m int) any { return n1(float64(m) + 12.5) }, num("19.0")),
				"precip_mm": months(func(m int) any { return n1(float64(10 * m)) }, num("780.0")),
				"evap_mm":   months(func(m int) any { return n1(float64(100 + m)) }, num("1278.0")),
			},
			"1991-2020": map[string]any{
				"tmax_c":    months(func(m int) any { return map[bool]any{true: n1(float64(m + 21))}[m != 6] }, nil),
				"tmin_c":    months(func(m int) any { return n1(float64(m + 6)) }, num("12.5")),
				"tmean_c":   months(func(m int) any { return n1(float64(m) + 13.5) }, num("20.0")),
				"precip_mm": months(func(m int) any { return n1(float64(10*m + 1)) }, num("792.0")),
				"evap_mm":   months(func(m int) any { return n1(float64(101 + m)) }, num("1290.0")),
			},
		}))
	})

	It("carries every extras column as a 13-slot series with slot 12 null (no annual derived for extras)", func() {
		Expect(obj(got, "extras")["source"]).To(Equal("conagua_published"))
		Expect(obj(got, "extras", "periods")).To(Equal(map[string]any{
			"1961-1990": nil,
			"1971-2000": nil,
			"1981-2010": map[string]any{
				"tmax_monthly_extreme_c":      firstOnly(num("38.5")),
				"tmax_monthly_extreme_year":   firstOnly(num("1998")),
				"tmax_daily_extreme_c":        firstOnly(num("42.00")),
				"tmax_daily_extreme_date":     firstOnly("1998-05-14"),
				"tmin_monthly_extreme_c":      firstOnly(num("8.2")),
				"tmin_monthly_extreme_year":   firstOnly(num("1985")),
				"tmin_daily_extreme_c":        firstOnly(num("3.50")),
				"tmin_daily_extreme_date":     firstOnly("1985-01-20"),
				"precip_monthly_extreme_mm":   firstOnly(num("210.6")),
				"precip_monthly_extreme_year": firstOnly(num("2002")),
				"precip_daily_extreme_mm":     firstOnly(num("98.70")),
				"precip_daily_extreme_date":   firstOnly("2002-09-23"),
				"tmax_years_with_data":        firstOnly(num("28")),
				"tmin_years_with_data":        firstOnly(num("27")),
				"tmean_years_with_data":       firstOnly(num("27")),
				"precip_years_with_data":      firstOnly(num("30")),
				"evap_years_with_data":        firstOnly(num("25")),
				"rain_days":                   firstOnly(num("4.6")),
				"rain_days_years_with_data":   firstOnly(num("29")),
			},
			"1991-2020": nil,
		}))
	})

	It("carries the cell and the POWER-31 monthly series with the per-column annual, absent periods null", func() {
		Expect(obj(got, "power_cell")).To(Equal(map[string]any{
			"source": "bioclima_derived", "cell_id": cellShared,
			"lat": num("21.500"), "lon": num("-89.375"), "distance_km": num("27.252"),
		}))
		Expect(obj(got, "power_monthly")["source"]).To(Equal("nasa_power"))
		Expect(obj(got, "power_monthly")["annual_slot"]).To(Equal("bioclima_derived"))

		seq := map[string]any{}
		same := map[string]any{}
		for i, p := range power.Registry {
			col := p.Column
			var dirs []float64
			for m := 1; m <= 12; m++ {
				dirs = append(dirs, float64(10*m+i))
			}
			annual := any(n2(float64(65 + i)))
			if p.Circular {
				annual = circularOf(dirs...)
			}
			seq[col] = months(func(m int) any { return num(powerSeqRow(float64(10 * m))[col].want) }, annual)

			want := fullPowerRow[col].want
			annual = num(want)
			if p.Circular {
				annual = circularOf(fullPowerRow[col].in.(float64), fullPowerRow[col].in.(float64),
					fullPowerRow[col].in.(float64), fullPowerRow[col].in.(float64), fullPowerRow[col].in.(float64),
					fullPowerRow[col].in.(float64), fullPowerRow[col].in.(float64), fullPowerRow[col].in.(float64),
					fullPowerRow[col].in.(float64), fullPowerRow[col].in.(float64), fullPowerRow[col].in.(float64),
					fullPowerRow[col].in.(float64))
			}
			if mixedPowerRow[col].in == nil {
				same[col] = months(func(m int) any { return map[bool]any{true: num(want)}[m != 1] }, nil)
			} else {
				same[col] = months(func(int) any { return num(want) }, annual)
			}
		}
		Expect(obj(got, "power_monthly", "periods")).To(Equal(map[string]any{
			"1961-1990": nil, "1971-2000": nil, "1981-2010": seq, "1991-2020": same,
		}))
	})

	It("carries the daily summary: coverage, the observed records with earliest-date ties, the longest dry spell", func() {
		Expect(obj(got, "daily_summary")).To(Equal(map[string]any{
			"source": "bioclima_derived",
			"coverage": map[string]any{
				"observed": map[string]any{
					"first_date": "1999-12-31", "last_date": "2020-01-11", "days_with_obs": num("11"),
					"days_by_variable": map[string]any{
						"tmax_c": num("9"), "tmin_c": num("9"), "precip_mm": num("9"), "evap_mm": num("8"),
					},
				},
				"reanalysis": map[string]any{"first_date": "1999-12-31", "last_date": "2020-01-03", "days": num("3")},
			},
			"extremes": map[string]any{
				"source":                "conagua_observed",
				"record_tmax_c":         map[string]any{"value": num("31.00"), "date": "2020-01-02"},
				"record_tmin_c":         map[string]any{"value": num("-0.04"), "date": "1999-12-31"},
				"record_precip_mm_1day": map[string]any{"value": num("12.40"), "date": "2020-01-11"},
			},
			"dry_spell": map[string]any{
				"longest_dry_run_days": num("3"), "start_date": "2020-01-04", "end_date": "2020-01-06",
			},
		}))
	})

	It("carries the meta block with run labels, the license, and the citation, and no generation stamp (reproducible bytes)", func() {
		Expect(obj(got, "meta")).To(Equal(map[string]any{
			"schema_version": num(fmt.Sprint(schema.Version)),
			"etl_git_sha":    "abc123",
			"snapshot_date":  "2026-06-08",
			"runs": map[string]any{
				"ingest": []any{"2026-05-01", "2026-06-08"},
				"power":  []any{"power-daily-1981-2026", "power-monthly-1981-2010"},
			},
			"license":            "CC-BY-4.0",
			"suggested_citation": profileMeta().Dataset.SuggestedCitation,
		}))
		Expect(string(raw)).NotTo(ContainSubstring("generated"))
	})

	It("writes the blocks and their fields in the published order, period maps sorted, series keyed in spec order", func() {
		top := rawObj(raw)
		Expect(objectKeys(raw)).To(Equal([]string{
			"station_id", "identity", "wmo_completeness", "normals", "extras",
			"power_cell", "power_monthly", "daily_summary", "meta",
		}))
		Expect(objectKeys(top["identity"])).To(Equal([]string{
			"name", "state", "state_name", "municipality", "lat", "lon", "altitude_m", "status", "first_year", "last_year",
		}))
		Expect(objectKeys(top["wmo_completeness"])).To(Equal([]string{"source", "periods"}))
		Expect(objectKeys(top["normals"])).To(Equal([]string{"source", "annual_slot", "periods"}))
		Expect(objectKeys(top["extras"])).To(Equal([]string{"source", "periods"}))
		Expect(objectKeys(top["power_cell"])).To(Equal([]string{"source", "cell_id", "lat", "lon", "distance_km"}))
		Expect(objectKeys(top["power_monthly"])).To(Equal([]string{"source", "annual_slot", "periods"}))
		Expect(objectKeys(top["daily_summary"])).To(Equal([]string{"source", "coverage", "extremes", "dry_spell"}))
		Expect(objectKeys(top["meta"])).To(Equal([]string{
			"schema_version", "etl_git_sha", "snapshot_date", "runs", "license", "suggested_citation",
		}))
		summary := rawObj(top["daily_summary"])
		Expect(objectKeys(summary["coverage"])).To(Equal([]string{"observed", "reanalysis"}))
		Expect(objectKeys(rawObj(summary["coverage"])["observed"])).To(Equal([]string{
			"first_date", "last_date", "days_with_obs", "days_by_variable",
		}))
		Expect(objectKeys(rawObj(summary["extremes"])["record_tmax_c"])).To(Equal([]string{"value", "date"}))
		Expect(objectKeys(summary["dry_spell"])).To(Equal([]string{"longest_dry_run_days", "start_date", "end_date"}))
		Expect(objectKeys(rawObj(top["meta"])["runs"])).To(Equal([]string{"ingest", "power"}))

		periods := []string{"1961-1990", "1971-2000", "1981-2010", "1991-2020"}
		for _, block := range []string{"wmo_completeness", "normals", "extras", "power_monthly"} {
			Expect(objectKeys(rawObj(top[block])["periods"])).To(Equal(periods), block)
		}
		valueNames := func(spec publish.FileSpec) []string {
			var names []string
			for _, c := range spec.Columns {
				if !c.Key {
					names = append(names, c.Name)
				}
			}
			return names
		}
		Expect(objectKeys(rawObj(rawObj(top["normals"])["periods"])["1981-2010"])).To(Equal(valueNames(publish.MonthlyNormals)))
		Expect(objectKeys(rawObj(rawObj(top["extras"])["periods"])["1981-2010"])).To(Equal(valueNames(publish.MonthlyNormalsExtras)))
		Expect(objectKeys(rawObj(rawObj(top["power_monthly"])["periods"])["1981-2010"])).To(Equal(valueNames(publish.PowerMonthly)))
		Expect(objectKeys(rawObj(rawObj(top["power_monthly"])["periods"])["1981-2010"])).To(Equal(power31))
		Expect(objectKeys(rawObj(rawObj(summary["coverage"])["observed"])["days_by_variable"])).
			To(Equal(valueNames(publish.DailyObservations)))
	})

	It("is two-space indented UTF-8 with a trailing newline, fixed-decimal literals, and no HTML escaping", func() {
		Expect(raw).To(HaveSuffix("}\n"))
		Expect(raw).NotTo(ContainSubstring("\r"))
		lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
		Expect(lines[0]).To(Equal("{"))
		Expect(lines[1]).To(Equal(`  "station_id": "31001",`))
		Expect(lines[2]).To(Equal(`  "identity": {`))
		Expect(lines[3]).To(Equal(`    "name": "Mérida, \"La Plancha\" <&>",`))
		Expect(string(raw)).To(ContainSubstring(`"lon": -89.375000,`))
		Expect(string(raw)).To(ContainSubstring(`"altitude_m": 9.0,`))
		Expect(string(raw)).To(ContainSubstring(`"distance_km": 27.252`))
		cite := profileMeta().Dataset.SuggestedCitation
		Expect(string(raw)).To(ContainSubstring(`"suggested_citation": "` + cite + `"`))
		// This build names no DOI, so the citation carries no DOI clause,
		// and no placeholder ever stands in for one.
		Expect(cite).NotTo(ContainSubstring("DOI"))
		Expect(string(raw)).NotTo(ContainSubstring("placeholder"))
		Expect(string(raw)).NotTo(ContainSubstring(`\u003c`))
		Expect(string(raw)).NotTo(ContainSubstring(`\u0026`))
	})
})

// The trace-precipitation contract across the two per-station files: the
// profile's dry spell and the daily.json a reader would recompute it from
// state the same thing about the same day, which one decimal made
// impossible.
var _ = Describe("a trace-rain day across profile.json and daily.json", func() {
	It("keeps a trace-rain day out of the dry spell, and the daily series shipped beside it shows the same 0.01", func() {
		// A dry day is an observed zero, so CONAGUA's trace
		// ("inappreciable") 0.01 mm is a wet day and breaks a run. Under
		// the one-decimal observed export the daily.json of this same
		// archive rendered that day 0.0 — a dry day — so a reader
		// recomputing the spell from the series could not arrive at the
		// number the profile states. At two decimals the two agree.
		db := openTempDB()
		ids := seedProfile(db)
		mustExec(db, `UPDATE daily_observations SET precip = 0.01 WHERE station_id = ? AND date = '2020-01-05'`,
			ids["31001"])

		summary := obj(decodeProfile(profileBytes(db, yucatan, "31001")), "daily_summary")
		// 01-04 … 01-06 was the winning run; the trace day splits it, and
		// the later 01-08 … 01-10 run wins instead.
		Expect(summary["dry_spell"]).To(Equal(map[string]any{
			"longest_dry_run_days": num("3"), "start_date": "2020-01-08", "end_date": "2020-01-10",
		}))

		series, err := publish.DailyJSONEntries(context.Background(), db, yucatan)
		Expect(err).NotTo(HaveOccurred())
		daily := string(render(entryByPath(series, "combined/yuc/31001/daily.json")))
		Expect(daily).To(ContainSubstring(dailyRow("2020-01-05", []string{"28.00", "17.00", "0.01", "3.90"}, nil)))
		Expect(daily).To(ContainSubstring(dailyRow("2020-01-04", []string{"31.00", "17.50", "0.00", "4.10"}, nil)))
		// The wet day and the dry day beside it are distinguishable in
		// the shipped bytes — the whole point of the second decimal.
		Expect(daily).NotTo(ContainSubstring(`"date":"2020-01-05","observed":{"tmax_c":28.00,"tmin_c":17.00,"precip_mm":0.00`))
	})
})

var _ = Describe("a station with no cell, no normals, and no daily rows", func() {
	It("is every block present with explicit nulls, every period key present", func() {
		db := openTempDB()
		seedProfile(db)
		raw := profileBytes(db, yucatan, "31002")
		Expect(decodeProfile(raw)).To(Equal(map[string]any{
			"station_id": "31002",
			"identity": map[string]any{
				"name": "Tizimín", "state": "YUC", "state_name": "Yucatán", "municipality": nil,
				"lat": nil, "lon": nil, "altitude_m": nil, "status": nil, "first_year": nil, "last_year": nil,
			},
			"wmo_completeness": map[string]any{"source": "bioclima_derived", "periods": nullPeriods()},
			"normals":          map[string]any{"source": "conagua_published", "annual_slot": "bioclima_derived", "periods": nullPeriods()},
			"extras":           map[string]any{"source": "conagua_published", "periods": nullPeriods()},
			"power_cell":       nil,
			"power_monthly":    nil,
			"daily_summary": map[string]any{
				"source":   "bioclima_derived",
				"coverage": map[string]any{"observed": nil, "reanalysis": nil},
				"extremes": map[string]any{
					"source": "conagua_observed", "record_tmax_c": nil, "record_tmin_c": nil, "record_precip_mm_1day": nil,
				},
				"dry_spell": nil,
			},
			"meta": map[string]any{
				"schema_version": num(fmt.Sprint(schema.Version)), "etl_git_sha": "abc123", "snapshot_date": "2026-06-08",
				"runs":    map[string]any{"ingest": []any{"2026-05-01", "2026-06-08"}, "power": []any{"power-daily-1981-2026", "power-monthly-1981-2010"}},
				"license": "CC-BY-4.0", "suggested_citation": profileMeta().Dataset.SuggestedCitation,
			},
		}))
		Expect(string(raw)).NotTo(ContainSubstring("generated"))
	})

	It("reports a cell with no daily rows as null reanalysis coverage and its periods without rows as null", func() {
		db := openTempDB()
		id := upsertStation(db, ingest.StationUpsert{Source: ingest.SourceConaguaConventional, ExternalID: "31003", Name: "Progreso", State: "YUC"})
		insertCell(db, cellSingle, 21.0, -89.625)
		insertStationCell(db, id, cellSingle, 0)
		got := decodeProfile(profileBytes(db, yucatan, "31003"))
		Expect(obj(got, "power_cell")).To(Equal(map[string]any{
			"source": "bioclima_derived", "cell_id": cellSingle, "lat": num("21.000"), "lon": num("-89.625"), "distance_km": num("0.000"),
		}))
		Expect(obj(got, "power_monthly")).To(Equal(map[string]any{
			"source": "nasa_power", "annual_slot": "bioclima_derived", "periods": nullPeriods(),
		}))
		Expect(obj(got, "daily_summary", "coverage")).To(Equal(map[string]any{"observed": nil, "reanalysis": nil}))
	})

	It("renders empty run lists as [] and an unknown git SHA as the empty string, never null", func() {
		db := openTempDB()
		upsertStation(db, ingest.StationUpsert{Source: ingest.SourceConaguaConventional, ExternalID: "31003", Name: "Progreso", State: "YUC"})
		entries, err := publish.ProfileEntries(context.Background(), db, yucatan, publish.ProfileMeta{})
		Expect(err).NotTo(HaveOccurred())
		got := decodeProfile(render(entries[0]))
		Expect(obj(got, "meta")).To(Equal(map[string]any{
			"schema_version": num("0"), "etl_git_sha": "", "snapshot_date": "",
			"runs": map[string]any{"ingest": []any{}, "power": []any{}}, "license": "", "suggested_citation": "",
		}))
	})
})

var _ = Describe("ProfileEntries", func() {
	It("lists one profile per station of the state, under combined/<slug>/<station_id>/, Path-sorted", func() {
		db := openTempDB()
		seedProfile(db)
		entries, err := publish.ProfileEntries(context.Background(), db, yucatan, profileMeta())
		Expect(err).NotTo(HaveOccurred())
		var paths []string
		for _, e := range entries {
			paths = append(paths, e.Path)
		}
		Expect(paths).To(Equal([]string{"combined/yuc/31001/profile.json", "combined/yuc/31002/profile.json"}))
		entries, err = publish.ProfileEntries(context.Background(), db, aguascalientes, profileMeta())
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(HaveLen(1))
		Expect(entries[0].Path).To(Equal("combined/ags/1001/profile.json"))
	})

	It("builds byte-identical profiles on every write and across builds (byte-reproducible)", func() {
		db := openTempDB()
		seedProfile(db)
		first := profileBytes(db, yucatan, "31001")
		Expect(profileBytes(db, yucatan, "31001")).To(Equal(first))
		entries, err := publish.ProfileEntries(context.Background(), db, yucatan, profileMeta())
		Expect(err).NotTo(HaveOccurred())
		e := entryByPath(entries, "combined/yuc/31001/profile.json")
		Expect(render(e)).To(Equal(first))
		Expect(render(e)).To(Equal(first))
	})

	It("writes through the zip seam with the profile bytes intact", func() {
		db := openTempDB()
		seedProfile(db)
		entries, err := publish.ProfileEntries(context.Background(), db, yucatan, profileMeta())
		Expect(err).NotTo(HaveOccurred())
		path := GinkgoT().TempDir() + "/yuc-json.zip"
		res, err := archive.WriteZip(context.Background(), path, entries, archive.ZipOptions{Modified: time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Entries).To(Equal(2))
		_, contents := zipContents(path)
		Expect(contents["combined/yuc/31001/profile.json"]).To(Equal(profileBytes(db, yucatan, "31001")))
	})

	It("refuses a cell reference with no grid row, and a daily date that is not YYYY-MM-DD", func() {
		db := openTempDB()
		id := upsertStation(db, ingest.StationUpsert{Source: ingest.SourceConaguaConventional, ExternalID: "31003", Name: "Progreso", State: "YUC"})
		insertStationCell(db, id, "21.0N_89.6250W", 0)
		entries, err := publish.ProfileEntries(context.Background(), db, yucatan, profileMeta())
		Expect(err).NotTo(HaveOccurred())
		err = entries[0].Write(io.Discard)
		Expect(err).To(MatchError("power_cell: cell \"21.0N_89.6250W\" has no nasa_power_grid_cells row"))

		insertCell(db, "21.0N_89.6250W", 21.0, -89.625)
		insertDaily(db, id, "2020/01/01", 1.0, 1.0, 0.0, 1.0)
		err = entries[0].Write(io.Discard)
		Expect(err).To(MatchError(ContainSubstring(`daily_summary: daily_observations: date "2020/01/01" is not YYYY-MM-DD`)))
	})
})
