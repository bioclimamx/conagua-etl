package publish

// In-package characterization of the scope refactor: every whole-scope
// query bound at a state's scope is, text for text, the query the state
// builders ran when each carried its own WHERE clause — the same select
// list, FROM, joins, predicate, and ORDER BY — so a national per-table
// file is the state file's query with the predicate swapped and nothing
// else; the per-unit queries and their argument binders are untouched;
// and at national scope every whole-scope query walks the stations
// natural-key index (source, external_id) in external_id order, with no
// temp B-tree for the sort and no table scan.

import (
	"context"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// The clauses the state builders carried before the scope refactor,
// verbatim, so the spec holds the refactored text to the old one rather
// than to itself.
const (
	statePredicateText = "WHERE s.state = ? AND s.source = ?"
	stateCellsText     = `SELECT DISTINCT m.cell_id FROM station_power_cell m` +
		` JOIN stations s ON s.id = m.station_id WHERE s.state = ? AND s.source = ?`
	stateStationsText = `
SELECT s.id, s.external_id, spc.cell_id, spc.distance_km
  FROM stations s LEFT JOIN station_power_cell spc ON spc.station_id = s.id
 WHERE s.state = ? AND s.source = ? ORDER BY s.external_id`
)

// squeeze folds runs of whitespace to one space, for the one query whose
// pre-refactor text was laid out over several lines.
func squeeze(s string) string { return strings.Join(strings.Fields(s), " ") }

var _ = ginkgo.Describe("the scoped queries against their pre-refactor text", func() {
	yuc := stateScope(State{Code: "YUC", Slug: "yuc", Name: "Yucatán"})

	ginkgo.It("keeps conagua/stations as the full literal, select list included", func() {
		Expect(stationsSQL(yuc)).To(Equal("SELECT s.external_id, s.name, s.state, s.municipality, s.lat, s.lon, s.altitude_m, s.status, " +
			"s.first_year, s.last_year, " +
			"s.wmo_completeness_bin_1961_1990, s.wmo_completeness_bin_1971_2000, " +
			"s.wmo_completeness_bin_1981_2010, s.wmo_completeness_bin_1991_2020, " +
			"s.wmo_completeness_cont_1961_1990, s.wmo_completeness_cont_1971_2000, " +
			"s.wmo_completeness_cont_1981_2010, s.wmo_completeness_cont_1991_2020" +
			" FROM stations s WHERE s.state = ? AND s.source = ? ORDER BY s.external_id"))
	})

	ginkgo.It("keeps the two station-keyed normals tables on childTableSQL's shape", func() {
		child := func(spec FileSpec) string {
			return "SELECT " + selectList(spec, func(c Column) string {
				if c.DB == "station_id" {
					return "s.external_id"
				}
				return "t." + c.DB
			}) + " FROM " + spec.Table + " t JOIN stations s ON s.id = t.station_id" +
				" WHERE s.state = ? AND s.source = ? ORDER BY s.external_id, t.period, t.month"
		}
		Expect(normalsSQL(yuc)).To(Equal(child(MonthlyNormals)))
		Expect(normalsSQL(yuc)).To(Equal("SELECT s.external_id, t.period, t.month, t.tmax, t.tmin, t.tmean, t.precip, t.evap" +
			" FROM monthly_normals t JOIN stations s ON s.id = t.station_id" +
			" WHERE s.state = ? AND s.source = ? ORDER BY s.external_id, t.period, t.month"))
		Expect(extrasSQL(yuc)).To(Equal(child(MonthlyNormalsExtras)))
		Expect(extrasSQL(yuc)).To(HavePrefix("SELECT s.external_id, t.period, t.month, t.tmax_monthly_extreme, t.tmax_monthly_extreme_year, "))
		Expect(extrasSQL(yuc)).To(HaveSuffix(", t.rain_days, t.rain_days_years_with_data" +
			" FROM monthly_normals_extras t JOIN stations s ON s.id = t.station_id" +
			" WHERE s.state = ? AND s.source = ? ORDER BY s.external_id, t.period, t.month"))
	})

	ginkgo.It("keeps the nasa_power/ tables over the same cell subquery", func() {
		Expect(cellSetSQL(yuc)).To(Equal(stateCellsText))
		Expect(cellsSQL(yuc)).To(Equal("SELECT c.cell_id, c.lat, c.lon FROM nasa_power_grid_cells c" +
			" WHERE c.cell_id IN (" + stateCellsText + ") ORDER BY c.cell_id"))
		Expect(stationCellMapSQL(yuc)).To(Equal("SELECT s.external_id, m.cell_id, m.distance_km" +
			" FROM station_power_cell m JOIN stations s ON s.id = m.station_id" +
			" WHERE s.state = ? AND s.source = ? ORDER BY s.external_id"))
		Expect(powerMonthlySQL(yuc)).To(Equal("SELECT " + selectList(PowerMonthly, func(c Column) string { return "p." + c.DB }) +
			" FROM monthly_supplement p WHERE p.cell_id IN (" + stateCellsText + ")" +
			" ORDER BY p.cell_id, p.period, p.month"))
		Expect(powerMonthlySQL(yuc)).To(HavePrefix("SELECT p.cell_id, p.period, p.month, p.t2m_c, p.t2m_max_c, "))
	})

	ginkgo.It("keeps combined/combined_monthly's two LEFT hops and its sort", func() {
		Expect(combinedMonthlySQL(yuc)).To(Equal("SELECT " + selectList(CombinedMonthly.FileSpec, func(c Column) string {
			switch {
			case c.DB == "station_id":
				return "s.external_id"
			case CombinedMonthly.Source(c) == "station_power_cell":
				return "spc." + c.DB
			case CombinedMonthly.Source(c) == "monthly_supplement":
				return "ms." + c.DB
			default:
				return "n." + c.DB
			}
		}) + " FROM monthly_normals n JOIN stations s ON s.id = n.station_id" +
			" LEFT JOIN station_power_cell spc ON spc.station_id = n.station_id" +
			" LEFT JOIN monthly_supplement ms ON ms.cell_id = spc.cell_id AND ms.period = n.period AND ms.month = n.month" +
			" WHERE s.state = ? AND s.source = ? ORDER BY s.external_id, n.period, n.month"))
		Expect(combinedMonthlySQL(yuc)).To(HavePrefix(
			"SELECT s.external_id, n.period, n.month, spc.cell_id, spc.distance_km, n.tmax, n.tmin, n.tmean, n.precip, n.evap, ms.t2m_c, "))
	})

	ginkgo.It("keeps the station list — the one whose text was re-laid on one line — equal up to whitespace", func() {
		Expect(squeeze(stationListSQL(yuc))).To(Equal(squeeze(stateStationsText)))
		Expect(stationListSQL(yuc)).To(Equal("SELECT s.id, s.external_id, spc.cell_id, spc.distance_km" +
			" FROM stations s LEFT JOIN station_power_cell spc ON spc.station_id = s.id" +
			" WHERE s.state = ? AND s.source = ? ORDER BY s.external_id"))
	})

	ginkgo.It("binds every state query as (state, source) and every national one as (source), the predicate replaced once", func() {
		for name, q := range map[string]func(scope) string{
			"stations": stationsSQL, "monthly_normals": normalsSQL, "monthly_normals_extras": extrasSQL,
			"cells": cellsSQL, "station_cell_map": stationCellMapSQL, "monthly": powerMonthlySQL,
			"combined_monthly": combinedMonthlySQL, "station list": stationListSQL, "cell set": cellSetSQL,
		} {
			state := q(yuc)
			Expect(state).To(ContainSubstring(statePredicateText), name)
			Expect(q(nationalScope)).To(Equal(strings.Replace(state, statePredicateText, "WHERE s.source = ?", 1)), name)
			Expect(q(nationalScope)).NotTo(ContainSubstring("s.state = ?"), name)
		}
		Expect(yuc.args()).To(Equal([]any{"YUC", "conagua_conventional"}))
		Expect(nationalScope.args()).To(Equal([]any{"conagua_conventional"}))
	})

	ginkgo.It("leaves the per-unit queries and their binders untouched", func() {
		Expect(dailySQL).To(Equal("SELECT ?, d.date, d.tmax, d.tmin, d.precip, d.evap" +
			" FROM daily_observations d WHERE d.station_id = ? ORDER BY d.date"))
		Expect(powerDailySQL).To(HavePrefix("SELECT d.cell_id, d.date, d.t2m_c, d.t2m_max_c, d.t2m_min_c, "))
		Expect(powerDailySQL).To(HaveSuffix(", d.gwet_root, d.gwet_prof FROM daily_supplement d WHERE d.cell_id = ? ORDER BY d.date"))
		Expect(strings.Count(powerDailySQL, "?")).To(Equal(1))
		Expect(combinedDailySQL).To(HavePrefix("SELECT ?, d.date, ?, ?, d.tmax, d.tmin, d.precip, d.evap, ds.t2m_c, ds.t2m_max_c, "))
		Expect(combinedDailySQL).To(HaveSuffix(", ds.gwet_prof" + combinedDailyFromSQL))
		Expect(combinedDailyFromSQL).To(Equal(" FROM daily_observations d" +
			" LEFT JOIN daily_supplement ds ON ds.cell_id = ? AND ds.date = d.date" +
			" WHERE d.station_id = ? ORDER BY d.date"))
		Expect(strings.Count(combinedDailySQL, "?")).To(Equal(5))

		s := stationRef{id: 7, externalID: "31001"}
		s.cellID.String, s.cellID.Valid = "21.5N_89.3750W", true
		s.distanceKm.Float64, s.distanceKm.Valid = 27.25, true
		Expect(dailyArgs(s)).To(Equal([]any{"31001", int64(7)}))
		Expect(combinedDailyArgs(s)).To(Equal([]any{"31001", s.cellID, s.distanceKm, s.cellID, int64(7)}))
		none := stationRef{id: 8, externalID: "31002"}
		Expect(combinedDailyArgs(none)).To(Equal([]any{"31002", none.cellID, none.distanceKm, none.cellID, int64(8)}))
	})
})

var _ = ginkgo.Describe("the national queries' plans", func() {
	// planLines is the detail column of EXPLAIN QUERY PLAN over query
	// bound at sc, on an empty schema DB (the planner reads no
	// statistics, so the plan is the DDL's, whatever the data).
	planLines := func(query string, sc scope) []string {
		ginkgo.GinkgoHelper()
		db, err := schema.Open(filepath.Join(ginkgo.GinkgoT().TempDir(), "plan.db"))
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = db.Close() }()
		rows, err := db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+query, sc.args()...)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = rows.Close() }()
		var lines []string
		for rows.Next() {
			var id, parent, notUsed int
			var detail string
			Expect(rows.Scan(&id, &parent, &notUsed, &detail)).To(Succeed())
			lines = append(lines, detail)
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		return lines
	}
	stationsIndex := regexp.MustCompile(`^SEARCH s USING (COVERING )?INDEX (sqlite_autoindex_stations_1|idx_stations_source) \(source=\?\)$`)

	ginkgo.It("walk the stations natural-key index at national scope with no temp B-tree for the sort and no scan, as the state queries do", func() {
		queries := map[string]func(scope) string{
			"stations": stationsSQL, "monthly_normals": normalsSQL, "monthly_normals_extras": extrasSQL,
			"cells": cellsSQL, "station_cell_map": stationCellMapSQL, "monthly": powerMonthlySQL,
			"combined_monthly": combinedMonthlySQL, "station list": stationListSQL,
		}
		for name, q := range queries {
			for _, sc := range []scope{nationalScope, stateScope(State{Code: "YUC"})} {
				lines := planLines(q(sc), sc)
				Expect(lines).To(ContainElement(MatchRegexp(stationsIndex.String())), "%s at %s: %v", name, sc.label(), lines)
				for _, l := range lines {
					Expect(l).NotTo(ContainSubstring("TEMP B-TREE FOR ORDER BY"), "%s at %s", name, sc.label())
					Expect(l).NotTo(HavePrefix("SCAN"), "%s at %s", name, sc.label())
				}
			}
		}
		// The sorted cell set (loadCells) sorts through its DISTINCT
		// B-tree; the sort itself needs no second one.
		lines := planLines(cellSetSQL(nationalScope)+" ORDER BY m.cell_id", nationalScope)
		Expect(lines).To(ContainElement(MatchRegexp(stationsIndex.String())))
		Expect(lines).NotTo(ContainElement(ContainSubstring("TEMP B-TREE FOR ORDER BY")))
	})
})
