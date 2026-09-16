package publish_test

// Specs for daily.json: the per-station entry under
// combined/<slug>/<station_id>/; every row of a station on a cell
// round-tripped — the observed spine with per-field nulls, reanalysis
// null before POWER begins, the full 31-key object where the cell has
// the date, 31 explicit nulls for a matched all-NULL row, null where
// the cell lacks the date; a station with no cell; a cell that has rows
// but none on the station's dates; the row count held to the observed
// count read back from the DB, dates ascending; the fixed-decimal
// literals, the observed block at the two decimals that keep a trace
// rain day off a dry day; byte identity across writes; the empty
// series; and the query held to combinedDailySQL's spine and plan.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// observedKeys is the observed block's key order — DailyObservations'
// value columns under their export names.
var observedKeys = []string{"tmax_c", "tmin_c", "precip_mm", "evap_mm"}

// jsonBlock renders one {key:literal,…} object from CSV-shaped literals
// ("" is NULL and renders as null), the suite's one expected-JSON
// assembler.
func jsonBlock(keys, literals []string) string {
	GinkgoHelper()
	Expect(literals).To(HaveLen(len(keys)))
	parts := make([]string, len(keys))
	for i, k := range keys {
		v := literals[i]
		if v == "" {
			v = "null"
		}
		parts[i] = `"` + k + `":` + v
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// dailyRow renders one expected daily.json row; a nil reanalysis is the
// whole-object null, a non-nil one the 31-key block (all-"" = 31 nulls).
func dailyRow(date string, observed, reanalysis []string) string {
	GinkgoHelper()
	re := "null"
	if reanalysis != nil {
		re = jsonBlock(registryColumns(), reanalysis)
	}
	return `{"date":"` + date + `","observed":` + jsonBlock(observedKeys, observed) + `,"reanalysis":` + re + `}`
}

func dailyDoc(rows ...string) string {
	if len(rows) == 0 {
		return "[\n]\n"
	}
	return "[\n" + strings.Join(rows, ",\n") + "\n]\n"
}

// jsonObject is a decoded JSON object that keeps its key order, with
// numbers as json.Number so their literal text is asserted, not their
// float value.
type jsonObject struct {
	keys   []string
	values map[string]any
}

// decodeOrdered reads one JSON value from dec: objects as *jsonObject,
// arrays as []any, numbers as json.Number, strings, bools, and nil for
// null.
func decodeOrdered(dec *json.Decoder) any {
	GinkgoHelper()
	tok, err := dec.Token()
	Expect(err).NotTo(HaveOccurred())
	delim, ok := tok.(json.Delim)
	if !ok {
		return tok
	}
	switch delim {
	case '{':
		obj := &jsonObject{values: map[string]any{}}
		for dec.More() {
			keyTok, err := dec.Token()
			Expect(err).NotTo(HaveOccurred())
			key, ok := keyTok.(string)
			Expect(ok).To(BeTrue())
			Expect(obj.values).NotTo(HaveKey(key), "duplicate key")
			obj.keys = append(obj.keys, key)
			obj.values[key] = decodeOrdered(dec)
		}
		end, err := dec.Token()
		Expect(err).NotTo(HaveOccurred())
		Expect(end).To(Equal(json.Delim('}')))
		return obj
	case '[':
		arr := []any{}
		for dec.More() {
			arr = append(arr, decodeOrdered(dec))
		}
		end, err := dec.Token()
		Expect(err).NotTo(HaveOccurred())
		Expect(end).To(Equal(json.Delim(']')))
		return arr
	}
	Fail("unexpected delimiter " + delim.String())
	return nil
}

// parseRows decodes a daily.json document into its row objects with
// encoding/json (UseNumber), asserting it is exactly one top-level array.
func parseRows(data []byte) []*jsonObject {
	GinkgoHelper()
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	arr, ok := decodeOrdered(dec).([]any)
	Expect(ok).To(BeTrue(), "top level is not an array")
	Expect(dec.More()).To(BeFalse(), "trailing content")
	rows := make([]*jsonObject, len(arr))
	for i, v := range arr {
		rows[i], ok = v.(*jsonObject)
		Expect(ok).To(BeTrue(), "row %d is not an object", i)
	}
	return rows
}

// literals returns obj's values in key order as the CSV would render
// them: a number's literal text, "" for null.
func literals(obj *jsonObject) []string {
	GinkgoHelper()
	out := make([]string, len(obj.keys))
	for i, k := range obj.keys {
		switch v := obj.values[k].(type) {
		case nil:
		case json.Number:
			out[i] = v.String()
		default:
			Fail("value of " + k + " is neither a number nor null")
		}
	}
	return out
}

// expectRow asserts one parsed row's key order and every literal; a nil
// reanalysis expects the whole-object null.
func expectRow(row *jsonObject, date string, observed, reanalysis []string) {
	GinkgoHelper()
	Expect(row.keys).To(Equal([]string{"date", "observed", "reanalysis"}))
	Expect(row.values["date"]).To(Equal(date))
	obs, ok := row.values["observed"].(*jsonObject)
	Expect(ok).To(BeTrue(), "observed is not an object on %s", date)
	Expect(obs.keys).To(Equal(observedKeys))
	Expect(literals(obs)).To(Equal(observed), date)
	if reanalysis == nil {
		Expect(row.values["reanalysis"]).To(BeNil(), "reanalysis on %s", date)
		return
	}
	re, ok := row.values["reanalysis"].(*jsonObject)
	Expect(ok).To(BeTrue(), "reanalysis is not an object on %s", date)
	Expect(re.keys).To(Equal(registryColumns()))
	Expect(literals(re)).To(Equal(reanalysis), date)
}

var _ = Describe("daily.json", func() {
	var (
		db      *sql.DB
		ids     map[string]int64
		entries []archive.Entry
	)

	// The rows of 31001 (on cellShared), date ascending, as the LEFT join
	// renders them: a pre-1981 date; the three dates the cell has rows for
	// (a mixed-NULL row, a full row, an all-NULL row); a post-1981 date the
	// cell has no row for.
	rows31001 := func() []string {
		return []string{
			dailyRow("1967-05-19", []string{"", "", "0.00", ""}, nil),
			dailyRow("1999-12-31", []string{"", "-0.04", "", ""}, powerWant(mixedPowerRow)),
			dailyRow("2020-01-01", []string{"30.50", "", "12.40", ""}, powerWant(fullPowerRow)),
			dailyRow("2020-01-02", []string{"31.00", "19.50", "0.00", "4.20"}, powerWant(allNullPowerRow)),
			dailyRow("2020-01-03", []string{"28.00", "20.50", "3.50", "2.20"}, nil),
		}
	}

	BeforeEach(func() {
		db = openTempDB()
		ids = seedTwoStates(db)
		seedPower(db, ids)
		// A pre-1981 observation and a post-1981 date with no POWER row
		// for 31001; one observation for the cell-less 31002 on a date
		// cellShared does have a row for.
		insertDaily(db, ids["conv/31001"], "2020-01-03", 28.0, 20.5, 3.5, 2.2)
		insertDaily(db, ids["conv/31001"], "1967-05-19", nil, nil, 0.0, nil)
		insertDaily(db, ids["conv/31002"], "2020-01-01", 29.5, 18.0, 2.0, 5.0)
		var err error
		entries, err = publish.DailyJSONEntries(context.Background(), db, yucatan)
		Expect(err).NotTo(HaveOccurred())
	})

	It("names one daily.json per station under combined/<slug>/<station_id>/, in station order", func() {
		var paths []string
		for _, e := range entries {
			paths = append(paths, e.Path)
		}
		Expect(paths).To(Equal([]string{
			"combined/yuc/31001/daily.json",
			"combined/yuc/31002/daily.json",
			"combined/yuc/31003/daily.json",
			"combined/yuc/3101/daily.json",
		}))
	})

	It("writes a station on a cell byte for byte: the observed spine, reanalysis null before POWER and where the cell lacks the date, the 31-key object where it has it, 31 explicit nulls for a matched all-NULL row", func() {
		Expect(string(render(entryByPath(entries, "combined/yuc/31001/daily.json")))).To(Equal(dailyDoc(rows31001()...)))
	})

	It("parses back with every key in order and every value's literal text, row by row", func() {
		rows := parseRows(render(entryByPath(entries, "combined/yuc/31001/daily.json")))
		Expect(rows).To(HaveLen(5))
		expectRow(rows[0], "1967-05-19", []string{"", "", "0.00", ""}, nil)
		expectRow(rows[1], "1999-12-31", []string{"", "-0.04", "", ""}, powerWant(mixedPowerRow))
		expectRow(rows[2], "2020-01-01", []string{"30.50", "", "12.40", ""}, powerWant(fullPowerRow))
		expectRow(rows[3], "2020-01-02", []string{"31.00", "19.50", "0.00", "4.20"}, powerWant(allNullPowerRow))
		expectRow(rows[4], "2020-01-03", []string{"28.00", "20.50", "3.50", "2.20"}, nil)
	})

	It("holds every station's row count to its observed row count and its dates ascending", func() {
		for _, station := range []string{"31001", "31002", "31003", "3101"} {
			rows := parseRows(render(entryByPath(entries, "combined/yuc/"+station+"/daily.json")))
			Expect(rows).To(HaveLen(countRows(db, `SELECT COUNT(*) FROM daily_observations WHERE station_id = ?`,
				ids["conv/"+station])), station)
			dates := make([]string, len(rows))
			for i, r := range rows {
				dates[i] = r.values["date"].(string)
			}
			Expect(slices.IsSorted(dates)).To(BeTrue(), "%s: %v", station, dates)
			Expect(slices.Compact(slices.Clone(dates))).To(Equal(dates), "%s repeats a date", station)
		}
	})

	It("writes reanalysis null on every row of a station with no cell, even on a date some cell has", func() {
		Expect(countRows(db, `SELECT COUNT(*) FROM daily_supplement WHERE date = '2020-01-01'`)).To(BeNumerically(">", 0))
		Expect(string(render(entryByPath(entries, "combined/yuc/31002/daily.json")))).To(Equal(dailyDoc(
			dailyRow("2020-01-01", []string{"29.50", "18.00", "2.00", "5.00"}, nil),
		)))
	})

	It("writes reanalysis null where the station's own cell has no row for the date, though another cell does", func() {
		// 31003 sits on cellSingle, which has no daily row at all; cellShared
		// has 2020-01-01.
		insertDaily(db, ids["conv/31003"], "2020-01-01", 28.5, 17.5, 0.0, 3.0)
		insertDaily(db, ids["conv/31003"], "1999-12-31", 27.0, 16.0, 10.2, 2.1)
		Expect(string(render(entryByPath(entries, "combined/yuc/31003/daily.json")))).To(Equal(dailyDoc(
			dailyRow("1999-12-31", []string{"27.00", "16.00", "10.20", "2.10"}, nil),
			dailyRow("2020-01-01", []string{"28.50", "17.50", "0.00", "3.00"}, nil),
		)))
	})

	It("writes an empty series as an empty array on its own lines", func() {
		Expect(string(render(entryByPath(entries, "combined/yuc/31003/daily.json")))).To(Equal("[\n]\n"))
	})

	It("emits every number as its fixed-decimal literal: 0.00 not 0, 20.50 not 20.5, 31.00 not 31, the solar tail rounded, wind at one, a sub-precision negative as positive zero", func() {
		data := string(render(entryByPath(entries, "combined/yuc/31001/daily.json")))
		Expect(data).To(ContainSubstring(`"precip_mm":0.00,`))
		Expect(data).To(ContainSubstring(`"tmin_c":20.50,`))
		Expect(data).To(ContainSubstring(`"tmax_c":31.00,`))
		Expect(data).To(ContainSubstring(`"t2m_max_c":31.00,`))
		Expect(data).To(ContainSubstring(`"solar_ghi_wm2":228.47,`))
		Expect(data).To(ContainSubstring(`"t2m_min_c":0.00,`))
		Expect(data).To(ContainSubstring(`"ts_min_c":-3.35,`))
		Expect(data).To(ContainSubstring(`"wd10m_deg":180.2,`))
		Expect(data).NotTo(ContainSubstring("228.472"))
		Expect(data).NotTo(ContainSubstring(`:0,`))
		Expect(data).NotTo(ContainSubstring(`:31,`))
		// The observed block is never truncated to the one decimal the
		// normals carry: a 31.0 tmax cell would be that truncation.
		Expect(data).NotTo(ContainSubstring(`"tmax_c":31.0,`))
		// Positive zero still holds, for the values that round to zero:
		// POWER's -0.004 t2m_min_c is 0.00, never -0.00. A negative sign
		// survives only in front of a digit the file actually claims.
		Expect(data).NotTo(MatchRegexp(`-0\.0+[,}]`))
		// The stored -0.04 (1999-12-31 tmin) is one of those digits under
		// the two-decimal observed contract, not a rounding artefact: it
		// keeps its sign instead of flattening to 0.0.
		rows := parseRows([]byte(data))
		Expect(rows[1].values["observed"].(*jsonObject).values["tmin_c"]).To(Equal(json.Number("-0.04")))
	})

	It("carries a trace precipitation day to the file as 0.01, never as a dry 0.0", func() {
		// CONAGUA publishes "inappreciable" rain as 0.01 mm — over a
		// million station-days of it. At one decimal every one of them
		// shipped as 0.0, a dry day, while the SQLite artifact of the
		// same deposit held 0.01; the two-decimal observed contract is
		// what makes this file agree with the DB beside it.
		insertDaily(db, ids["conv/31003"], "2020-01-05", 30.0, 20.0, 0.01, nil)
		data := string(render(entryByPath(entries, "combined/yuc/31003/daily.json")))
		Expect(data).To(Equal(dailyDoc(
			dailyRow("2020-01-05", []string{"30.00", "20.00", "0.01", ""}, nil),
		)))
		Expect(data).NotTo(ContainSubstring(`"precip_mm":0.0,`))
		Expect(data).NotTo(ContainSubstring(`"precip_mm":0.00,`))
		var stored float64
		Expect(db.QueryRow(`SELECT precip FROM daily_observations WHERE station_id = ? AND date = '2020-01-05'`,
			ids["conv/31003"]).Scan(&stored)).To(Succeed())
		Expect(stored).To(Equal(0.01))
	})

	It("is UTF-8 with LF endings, one row per line, a trailing newline, and no CR", func() {
		data := string(render(entryByPath(entries, "combined/yuc/31001/daily.json")))
		Expect(data).NotTo(HavePrefix("\xef\xbb\xbf"))
		Expect(data).NotTo(ContainSubstring("\r"))
		Expect(data).To(HaveSuffix("}\n]\n"))
		lines := strings.Split(strings.TrimSuffix(data, "\n"), "\n")
		Expect(lines[0]).To(Equal("["))
		Expect(lines[len(lines)-1]).To(Equal("]"))
		Expect(lines[1 : len(lines)-1]).To(HaveLen(5))
		for _, l := range lines[1 : len(lines)-1] {
			Expect(l).To(HavePrefix(`{"date":"`))
		}
	})

	It("writes identical bytes on every write", func() {
		e := entryByPath(entries, "combined/yuc/31001/daily.json")
		Expect(render(e)).To(Equal(render(e)))
	})

	It("names the same station set as the CSV folders, station for station", func() {
		conagua, err := publish.ConaguaEntries(context.Background(), db, yucatan, nil)
		Expect(err).NotTo(HaveOccurred())
		var csvStations, jsonStations []string
		for _, e := range conagua {
			if rest, ok := strings.CutPrefix(e.Path, "conagua/daily_observations/yuc/daily-"); ok {
				csvStations = append(csvStations, strings.TrimSuffix(rest, ".csv"))
			}
		}
		for _, e := range entries {
			jsonStations = append(jsonStations, strings.TrimSuffix(strings.TrimPrefix(e.Path, "combined/yuc/"), "/daily.json"))
		}
		Expect(jsonStations).To(Equal(csvStations))
	})

	It("honours cancellation at write time", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancelable, err := publish.DailyJSONEntries(ctx, db, yucatan)
		Expect(err).NotTo(HaveOccurred())
		cancel()
		err = entryByPath(cancelable, "combined/yuc/31001/daily.json").Write(io.Discard)
		Expect(errors.Is(err, context.Canceled)).To(BeTrue(), err)
	})

	It("refuses to write a non-finite POWER value rather than emit +Inf", func() {
		mustExec(db, `UPDATE daily_supplement SET ps_kpa = 9e999 WHERE cell_id = ? AND date = '2020-01-01'`, cellShared)
		err := entryByPath(entries, "combined/yuc/31001/daily.json").Write(io.Discard)
		Expect(err).To(MatchError(ContainSubstring("row 3: column ps_kpa: non-finite value +Inf")))
	})

	It("refuses a stored date that is not valid UTF-8, naming the row", func() {
		insertDaily(db, ids["conv/31002"], "2020-01-0\xff", 1.0, 1.0, 0.0, 1.0)
		err := entryByPath(entries, "combined/yuc/31002/daily.json").Write(io.Discard)
		Expect(err).To(MatchError(`row 2: date "2020-01-0\xff" is not valid UTF-8`))
	})

	It("surfaces a failing writer", func() {
		err := entryByPath(entries, "combined/yuc/31001/daily.json").Write(failingWriter{})
		Expect(errors.Is(err, errWriterBroken)).To(BeTrue(), err)
	})
})

var errWriterBroken = errors.New("writer broken")

// failingWriter fails every write, so the writer's error path is seen
// from the caller's side.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errWriterBroken }

var _ = Describe("the daily.json query", func() {
	It("selects the date, the observed values, POWER-31 in registry order, then the supplement-side sentinel", func() {
		want := "SELECT d.date, d.tmax, d.tmin, d.precip, d.evap"
		for _, c := range power31 {
			want += ", ds." + c
		}
		want += ", ds.cell_id IS NOT NULL FROM "
		Expect(publish.DailyJSONSQL).To(HavePrefix(want))
	})

	It("shares combinedDailySQL's spine verbatim from its FROM clause on", func() {
		from := func(q string) string {
			i := strings.Index(q, " FROM ")
			Expect(i).To(BeNumerically(">", 0), q)
			return q[i:]
		}
		Expect(from(publish.DailyJSONSQL)).To(Equal(from(publish.CombinedDailySQL)))
		Expect(strings.Count(publish.DailyJSONSQL, "?")).To(Equal(2))
	})

	It("walks the daily spine's (station_id, date) key and probes the supplement's (cell_id, date) primary key, never a scan", func() {
		db := openTempDB()
		details := planDetails(db, publish.DailyJSONSQL, sql.NullString{}, int64(1))
		Expect(details).To(ContainElement(MatchRegexp(
			`^SEARCH d USING (COVERING )?INDEX (idx_daily_station_date|sqlite_autoindex_daily_observations_1) \(station_id=\?\)`)))
		Expect(details).To(ContainElement(MatchRegexp(
			`^SEARCH ds USING (INDEX sqlite_autoindex_daily_supplement_1|PRIMARY KEY) \(cell_id=\? AND date=\?\)`)))
		for _, d := range details {
			Expect(d).NotTo(HavePrefix("SCAN"), d)
		}
	})
})
