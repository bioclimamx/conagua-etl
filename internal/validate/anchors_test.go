package validate_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// expectErrors asserts every finding is an error of rule id, anchored
// to no station, and returns the issue texts.
func expectErrors(fs []validate.Finding, id string) []string {
	GinkgoHelper()
	for _, f := range fs {
		Expect(f.Severity).To(Equal(validate.SeverityError))
		Expect(f.RuleID).To(Equal(id))
		Expect(f.StationID).To(BeNil())
	}
	return issues(fs)
}

var _ = Describe("station-refs", func() {
	It("is silent on a clean DB and counts the rows of the five tables", func() {
		db, _ := openTempDB()
		seedClean(db)
		res := runRule(db, "station-refs")
		Expect(res.Findings).To(BeEmpty())
		// 3 observations + 1 normals + 1 extras + 2 station_power_cell +
		// the 1 warning with a station_id (the NULL one is not a reference).
		Expect(res.Scanned).To(Equal(8))
	})

	It("names each table whose station_id dangles, with the distinct ids and the truncation", func() {
		db, _ := openTempDB()
		s := seedClean(db)
		mustExec(db, `DELETE FROM stations WHERE id = ?`, s.progreso)
		gone := s.progreso
		insertObservation(db, 900, "2020-01-01")
		insertObservation(db, 901, "2020-01-01")
		insertObservation(db, 901, "2020-01-02")
		insertObservation(db, 902, "2020-01-01")
		insertNormals(db, 950, "1981-2010", 6)
		insertExtras(db, 950, "1981-2010", 6)
		insertWarning(db, 960, "conagua-raw/2026-06-08/normals/960.txt")

		res := runRule(db, "station-refs")
		Expect(res.Scanned).To(Equal(8 + 4 + 1 + 1 + 1))
		Expect(expectErrors(res.Findings, "station-refs")).To(Equal([]string{
			fmt.Sprintf("daily_observations: 5 rows with a station_id naming no stations row (4 distinct: %d, 900, 901, …)", gone),
			"monthly_normals: 1 row with a station_id naming no stations row (1 distinct: 950)",
			"monthly_normals_extras: 1 row with a station_id naming no stations row (1 distinct: 950)",
			fmt.Sprintf("station_power_cell: 1 row with a station_id naming no stations row (1 distinct: %d)", gone),
			"parsing_warnings: 1 row with a station_id naming no stations row (1 distinct: 960)",
		}))
	})
})

var _ = Describe("cell-refs", func() {
	It("is silent on a clean DB and counts the rows of the three tables", func() {
		db, _ := openTempDB()
		seedClean(db)
		res := runRule(db, "cell-refs")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(2 + 2 + 2))
	})

	It("names each table whose cell_id has no grid cell", func() {
		db, _ := openTempDB()
		s := seedClean(db)
		mustExec(db, `DELETE FROM nasa_power_grid_cells WHERE cell_id = ?`, cellB)
		insertMonthly(db, "19.0N_99.3750W", "1991-2020", 2, s.monthlyRun, nil)
		insertDaily(db, "19.0N_99.3750W", "2020-01-02", s.dailyRun, nil)
		insertDaily(db, "19.0N_99.3750W", "2020-01-03", s.dailyRun, nil)

		res := runRule(db, "cell-refs")
		Expect(res.Scanned).To(Equal(3 + 4 + 2))
		Expect(expectErrors(res.Findings, "cell-refs")).To(Equal([]string{
			"monthly_supplement: 2 rows with a cell_id naming no nasa_power_grid_cells row (2 distinct: 19.0N_99.3750W, " + cellB + ")",
			"daily_supplement: 3 rows with a cell_id naming no nasa_power_grid_cells row (2 distinct: 19.0N_99.3750W, " + cellB + ")",
			"station_power_cell: 1 row with a cell_id naming no nasa_power_grid_cells row (1 distinct: " + cellB + ")",
		}))
	})
})

var _ = Describe("run-refs", func() {
	It("is silent on a clean DB and counts the supplement rows", func() {
		db, _ := openTempDB()
		seedClean(db)
		res := runRule(db, "run-refs")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(4))
	})

	It("errors on a NULL power_run_id, naming the rows by key", func() {
		db, _ := openTempDB()
		s := seedClean(db)
		insertMonthly(db, cellA, "1981-2010", 12, nil, nil)
		insertDaily(db, cellA, "2020-01-04", nil, nil)
		insertDaily(db, cellA, "2020-01-02", nil, nil)
		insertDaily(db, cellA, "2020-01-03", nil, nil)
		insertDaily(db, cellB, "2020-01-02", s.dailyRun, nil)

		res := runRule(db, "run-refs")
		Expect(res.Scanned).To(Equal(3 + 6))
		Expect(expectErrors(res.Findings, "run-refs")).To(Equal([]string{
			"monthly_supplement: 1 row with a NULL power_run_id (" + cellA + "/1981-2010/12)",
			"daily_supplement: 3 rows with a NULL power_run_id (" + cellA + "/2020-01-02, " + cellA + "/2020-01-03, " + cellA + "/2020-01-04)",
		}))
	})

	It("errors on a power_run_id with no power_runs row, naming the distinct ids", func() {
		db, _ := openTempDB()
		s := seedClean(db)
		insertDaily(db, cellA, "2020-01-02", 42, nil)
		insertDaily(db, cellA, "2020-01-03", 42, nil)
		insertDaily(db, cellB, "2020-01-02", 7, nil)
		insertMonthly(db, cellA, "1981-2010", 1, s.monthlyRun+100, nil)
		insertMonthly(db, cellA, "1981-2010", 2, nil, nil)

		res := runRule(db, "run-refs")
		Expect(res.Scanned).To(Equal(4 + 5))
		Expect(expectErrors(res.Findings, "run-refs")).To(Equal([]string{
			"monthly_supplement: 1 row with a NULL power_run_id (" + cellA + "/1981-2010/2)",
			fmt.Sprintf("monthly_supplement: 1 row with a power_run_id naming no power_runs row (1 distinct: %d)", s.monthlyRun+100),
			"daily_supplement: 3 rows with a power_run_id naming no power_runs row (2 distinct: 7, 42)",
		}))
	})

	It("counts every distinct dangling id while naming only the first three, in id order", func() {
		db, _ := openTempDB()
		seedClean(db)
		for i, id := range []int64{900, 42, 7, 300, 42} {
			insertDaily(db, cellA, fmt.Sprintf("2020-02-%02d", i+1), id, nil)
		}

		res := runRule(db, "run-refs")
		Expect(res.Scanned).To(Equal(2 + 7))
		Expect(expectErrors(res.Findings, "run-refs")).To(Equal([]string{
			"daily_supplement: 5 rows with a power_run_id naming no power_runs row (4 distinct: 7, 42, 300, …)",
		}))
	})
})

var _ = Describe("run-label-unique", func() {
	It("is silent when every referenced run has its own label, ignoring an unreferenced duplicate", func() {
		db, _ := openTempDB()
		seedClean(db)
		insertPowerRun(db, "complete", "monthly", 1991, 2020)
		res := runRule(db, "run-label-unique")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(2))
	})

	It("errors on two referenced runs sharing (temporal_mode, start, end)", func() {
		db, _ := openTempDB()
		s := seedClean(db)
		dup := insertPowerRun(db, "complete", "monthly", 1991, 2020)
		insertMonthly(db, cellA, "1991-2020", 2, dup, nil)
		// A daily run with the same years is a different label.
		daily := insertPowerRun(db, "complete", "daily", 1991, 2020)
		insertDaily(db, cellA, "1991-01-01", daily, nil)

		res := runRule(db, "run-label-unique")
		Expect(res.Scanned).To(Equal(4))
		Expect(expectErrors(res.Findings, "run-label-unique")).To(Equal([]string{
			fmt.Sprintf("power_runs %d, %d share temporal_mode/period (monthly 1991-2020): one run_label for 2 referenced runs "+
				"(re-run power for that period to convergence)", s.monthlyRun, dup),
		}))
	})
})

var _ = Describe("fill-leak", func() {
	It("is silent on a clean DB and ignores a value near the fill", func() {
		db, _ := openTempDB()
		s := seedClean(db)
		insertDaily(db, cellA, "2020-01-02", s.dailyRun, map[string]any{"t2m_c": -998.9, "gwet_prof": -999.1})
		insertMonthly(db, cellA, "1981-2010", 1, s.monthlyRun, map[string]any{"ps_kpa": -998.9})
		res := runRule(db, "fill-leak")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(6))
	})

	It("catches -999 in any of the 31 columns of either table, one finding per column", func() {
		db, _ := openTempDB()
		s := seedClean(db)
		var want []string
		for i, p := range power.Registry {
			period := []string{"1961-1990", "1971-2000", "1981-2010"}[i/12]
			month := i%12 + 1
			insertMonthly(db, cellA, period, month, s.monthlyRun, map[string]any{p.Column: -999.0})
			want = append(want, fmt.Sprintf("monthly_supplement.%s: 1 row holding POWER's -999 fill (%s/%s/%d)",
				p.Column, cellA, period, month))
		}
		for i, p := range power.Registry {
			date := fmt.Sprintf("2021-03-%02d", i+1)
			insertDaily(db, cellB, date, s.dailyRun, map[string]any{p.Column: -999})
			want = append(want, fmt.Sprintf("daily_supplement.%s: 1 row holding POWER's -999 fill (%s/%s)",
				p.Column, cellB, date))
		}
		res := runRule(db, "fill-leak")
		Expect(res.Scanned).To(Equal(4 + 62))
		Expect(expectErrors(res.Findings, "fill-leak")).To(Equal(want))
	})

	It("aggregates a column's leak to one finding with three sample keys", func() {
		db, _ := openTempDB()
		s := seedClean(db)
		for _, date := range []string{"2020-02-04", "2020-02-01", "2020-02-03", "2020-02-02"} {
			insertDaily(db, cellA, date, s.dailyRun, map[string]any{"rh2m_pct": -999.0, "ws2m_ms": -999.0})
		}
		res := runRule(db, "fill-leak")
		Expect(expectErrors(res.Findings, "fill-leak")).To(Equal([]string{
			"daily_supplement.rh2m_pct: 4 rows holding POWER's -999 fill (" + cellA + "/2020-02-01, " + cellA + "/2020-02-02, " + cellA + "/2020-02-03, …)",
			"daily_supplement.ws2m_ms: 4 rows holding POWER's -999 fill (" + cellA + "/2020-02-01, " + cellA + "/2020-02-02, " + cellA + "/2020-02-03, …)",
		}))
	})
})

var _ = Describe("wind-range", func() {
	It("accepts exactly 0.0 and 360.0 in both columns of both tables", func() {
		db, _ := openTempDB()
		seedClean(db)
		res := runRule(db, "wind-range")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(4))
	})

	It("rejects 360.1 and -0.1, naming the row and its value", func() {
		db, _ := openTempDB()
		s := seedClean(db)
		insertMonthly(db, cellA, "1981-2010", 3, s.monthlyRun, map[string]any{"wd10m_deg": -0.1})
		insertDaily(db, cellA, "2020-01-02", s.dailyRun, map[string]any{"wd2m_deg": 360.1})
		insertDaily(db, cellA, "2020-01-03", s.dailyRun, map[string]any{"wd2m_deg": -0.1, "wd10m_deg": 720.0})

		res := runRule(db, "wind-range")
		Expect(res.Scanned).To(Equal(7))
		Expect(expectErrors(res.Findings, "wind-range")).To(Equal([]string{
			"monthly_supplement.wd10m_deg: 1 row outside [0, 360] (" + cellA + "/1981-2010/3 = -0.1)",
			"daily_supplement.wd2m_deg: 2 rows outside [0, 360] (" + cellA + "/2020-01-02 = 360.1, " + cellA + "/2020-01-03 = -0.1)",
			"daily_supplement.wd10m_deg: 1 row outside [0, 360] (" + cellA + "/2020-01-03 = 720)",
		}))
	})

	It("checks exactly the registry's circular columns", func() {
		var circular []string
		for _, p := range power.Registry {
			if p.Circular {
				circular = append(circular, p.Column)
			}
		}
		Expect(circular).To(Equal([]string{"wd2m_deg", "wd10m_deg"}))
	})
})
