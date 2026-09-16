package publish_test

// Specs at the Run → archive seam for the JSON group: each state's
// {state}-json.zip held to the union of the three builders' entries —
// ProfileEntries, DailyJSONEntries, ProvenanceJSONEntries; never
// JSONEntries, the function Run itself calls — Path-sorted, each entry's
// bytes the builder's own rendering, the provenance pair byte-identical
// across archives; the archive's station folders and unit events held to
// the DB's own station list (one pair and one unit per conventional
// station of the state, in external_id order; the EMA station sharing an
// id adds nothing); and a station on a cell that has monthly rows but no
// daily rows, shipped with every daily.json reanalysis null on dates
// another cell does have, the cell and its climatology present, and
// reanalysis coverage null.

import (
	"context"
	"database/sql"
	"path/filepath"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// jsonBuilderEntries assembles a state's JSON archive from the three
// builders separately — never through JSONEntries — so the seam spec
// holds Run to the builders and not to the function Run itself calls.
func jsonBuilderEntries(db *sql.DB, st publish.State, runs publish.Runs, meta publish.ProfileMeta) []archive.Entry {
	GinkgoHelper()
	ctx := context.Background()
	profiles, err := publish.ProfileEntries(ctx, db, st, meta)
	Expect(err).NotTo(HaveOccurred())
	series, err := publish.DailyJSONEntries(ctx, db, st)
	Expect(err).NotTo(HaveOccurred())
	all := slices.Concat(profiles, series, publish.ProvenanceJSONEntries(runs))
	slices.SortFunc(all, func(a, b archive.Entry) int { return strings.Compare(a.Path, b.Path) })
	return all
}

// stationIDs reads a state's conventional station ids straight from the
// DB in export order — the list every per-station file set must match.
func stationIDs(db *sql.DB, st publish.State) []string {
	GinkgoHelper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT external_id FROM stations WHERE state = ? AND source = ? ORDER BY external_id`,
		st.Code, string(ingest.SourceConaguaConventional))
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		Expect(rows.Scan(&id)).To(Succeed())
		ids = append(ids, id)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return ids
}

var _ = Describe("Run's JSON archives against the three builders", func() {
	var (
		db     *sql.DB
		out    string
		rec    *recorder
		states []publish.State
		report *publish.Report
	)

	BeforeEach(func() {
		db = openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		// Zacatecas: a station with rows in the spine tables and no cell.
		insertNormals(db, ids["conv/32001"], "1981-2010", 3, 21.0, 6.0, 13.5, 9.5, 155.5)
		insertDaily(db, ids["conv/32001"], "2020-01-01", 18.0, 2.0, 0.0, 4.4)
		// Progreso (31003) sits on cellSingle, which has a monthly row and no
		// daily row; its observed dates are exactly the dates cellShared has
		// daily rows for.
		insertDaily(db, ids["conv/31003"], "2020-01-02", 27.5, 16.0, 0.0, 3.8)
		insertDaily(db, ids["conv/31003"], "1999-12-31", nil, -0.04, nil, nil)
		insertDaily(db, ids["conv/31003"], "2020-01-01", 30.5, nil, 12.4, nil)

		var err error
		states, err = publish.LoadStates(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(codesOf(states)).To(Equal([]string{"AGS", "YUC", "ZAC"}))
		out = filepath.Join(GinkgoT().TempDir(), "publish")
		rec = &recorder{}
		report, err = publish.Run(context.Background(), db,
			publish.Options{OutDir: out, ETLGitSHA: "abc123", Now: fixedClock, Only: stateGroups}, rec.record)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
		Expect(report.Artifacts).To(HaveLen(6))
	})

	It("writes, per state, exactly the union of the three builders' entries, Path-sorted, each entry's bytes as its builder renders them, the provenance pair identical across archives", func() {
		runs, err := publish.LoadRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		meta := runMeta(db, "abc123")
		provenance := map[string][][]byte{}
		for i, st := range states {
			want := jsonBuilderEntries(db, st, runs, meta)
			wantPaths := entryPaths(want)
			Expect(wantPaths).To(HaveLen(len(slices.Compact(slices.Clone(wantPaths)))), "%s: a duplicate path", st.Code)

			a := report.Artifacts[2*i+1]
			Expect(a.Name).To(Equal(st.Slug + "-json.zip"))
			Expect(a.Err).To(BeEmpty())
			Expect(a.Entries).To(Equal(len(want)))
			names, contents := zipContents(filepath.Join(out, a.Name))
			Expect(names).To(Equal(wantPaths), st.Code)
			for _, e := range want {
				Expect(contents[e.Path]).To(Equal(render(e)), "%s: %s", st.Code, e.Path)
			}
			for _, p := range []string{"provenance/ingest_runs.json", "provenance/power_runs.json"} {
				provenance[p] = append(provenance[p], contents[p])
			}
		}
		for p, renderings := range provenance {
			Expect(renderings).To(HaveLen(3), p)
			Expect(renderings[1]).To(Equal(renderings[0]), p)
			Expect(renderings[2]).To(Equal(renderings[0]), p)
		}
	})

	It("holds each archive's station folders and unit events to the DB's station list: one pair and one unit per conventional station, in external_id order", func() {
		for i, st := range states {
			ids := stationIDs(db, st)
			Expect(ids).NotTo(BeEmpty(), st.Code)
			a := report.Artifacts[2*i+1]
			names, _ := zipContents(filepath.Join(out, a.Name))
			var wantNames, wantUnits []string
			for _, id := range ids {
				folder := "combined/" + st.Slug + "/" + id + "/"
				wantNames = append(wantNames, folder+"daily.json", folder+"profile.json")
				wantUnits = append(wantUnits, folder+"profile.json")
			}
			wantNames = append(wantNames, "provenance/ingest_runs.json", "provenance/power_runs.json")
			Expect(names).To(Equal(wantNames), st.Code)
			Expect(a.Entries).To(Equal(2*len(ids)+2), st.Code)
			Expect(eventsOf(rec, a.Name)).To(Equal(append(
				unitEvents(a.Name, wantUnits...), publish.ProgressEvent{Artifact: a.Name})), st.Code)
		}
		// The EMA station sharing 31001's id adds no folder: Yucatán has
		// five stations rows and four conventional stations.
		Expect(stationIDs(db, yucatan)).To(Equal([]string{"31001", "31002", "31003", "3101"}))
		Expect(countRows(db, `SELECT COUNT(*) FROM stations WHERE state = 'YUC'`)).To(Equal(5))
	})

	It("ships a station on a cell with monthly rows but no daily rows: every daily.json reanalysis null on dates another cell has, the cell and its climatology present, reanalysis coverage null", func() {
		Expect(countRows(db, `SELECT COUNT(*) FROM daily_supplement WHERE cell_id = ?`, cellSingle)).To(BeZero())
		Expect(countRows(db, `SELECT COUNT(*) FROM daily_supplement WHERE cell_id = ?
		  AND date IN ('1999-12-31', '2020-01-01', '2020-01-02')`, cellShared)).To(Equal(3))

		_, contents := zipContents(filepath.Join(out, "yuc-json.zip"))
		Expect(string(contents["combined/yuc/31003/daily.json"])).To(Equal(dailyDoc(
			dailyRow("1999-12-31", []string{"", "-0.04", "", ""}, nil),
			dailyRow("2020-01-01", []string{"30.50", "", "12.40", ""}, nil),
			dailyRow("2020-01-02", []string{"27.50", "16.00", "0.00", "3.80"}, nil),
		)))

		got := decodeProfile(contents["combined/yuc/31003/profile.json"])
		Expect(obj(got, "power_cell")).To(Equal(map[string]any{
			"source": "bioclima_derived", "cell_id": cellSingle,
			"lat": num("21.000"), "lon": num("-89.625"), "distance_km": num("12.500"),
		}))
		// The one monthly row (June, rh2m_pct NULL); slot 12 null with
		// eleven months absent.
		june := map[string]any{}
		for _, p := range power.Registry {
			col := p.Column
			june[col] = months(func(m int) any {
				if m == 6 && noRHPowerRow[col].in != nil {
					return num(noRHPowerRow[col].want)
				}
				return nil
			}, nil)
		}
		Expect(obj(got, "power_monthly")).To(Equal(map[string]any{
			"source": "nasa_power", "annual_slot": "bioclima_derived",
			"periods": map[string]any{"1961-1990": nil, "1971-2000": nil, "1981-2010": june, "1991-2020": nil},
		}))
		Expect(obj(got, "daily_summary")).To(Equal(map[string]any{
			"source": "bioclima_derived",
			"coverage": map[string]any{
				"observed": map[string]any{
					"first_date": "1999-12-31", "last_date": "2020-01-02", "days_with_obs": num("3"),
					"days_by_variable": map[string]any{
						"tmax_c": num("2"), "tmin_c": num("2"), "precip_mm": num("2"), "evap_mm": num("1"),
					},
				},
				"reanalysis": nil,
			},
			"extremes": map[string]any{
				"source":                "conagua_observed",
				"record_tmax_c":         map[string]any{"value": num("30.50"), "date": "2020-01-01"},
				"record_tmin_c":         map[string]any{"value": num("-0.04"), "date": "1999-12-31"},
				"record_precip_mm_1day": map[string]any{"value": num("12.40"), "date": "2020-01-01"},
			},
			"dry_spell": map[string]any{
				"longest_dry_run_days": num("1"), "start_date": "2020-01-02", "end_date": "2020-01-02",
			},
		}))
	})
})
