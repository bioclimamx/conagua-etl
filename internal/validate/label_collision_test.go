package validate_test

// The run_label collision at its edge: the label is a function of
// (temporal_mode, period_start_year, period_end_year), and the rule
// counts only the runs at least one supplement row references — so a
// duplicate stays invisible until a row traces to it, whichever table
// that row lives in, and a collision names every referenced run of the
// label in id order, one finding per label.

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

var _ = Describe("run-label-unique at the reference edge", func() {
	It("stays silent while the duplicate is unreferenced and errors the moment one supplement row references it", func() {
		db, _ := openTempDB()
		s := seedClean(db)
		dup := insertPowerRun(db, "complete", "monthly", 1991, 2020)

		res := runRule(db, "run-label-unique")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(2), "the unreferenced duplicate is not scanned")

		// One daily_supplement row is enough — the referenced set is the
		// union over both tables, whatever the run's mode.
		insertDaily(db, cellA, "2020-01-02", dup, nil)
		res = runRule(db, "run-label-unique")
		Expect(res.Scanned).To(Equal(3))
		Expect(expectErrors(res.Findings, "run-label-unique")).To(Equal([]string{
			fmt.Sprintf("power_runs %d, %d share temporal_mode/period (monthly 1991-2020): one run_label for 2 referenced runs "+
				"(re-run power for that period to convergence)", s.monthlyRun, dup),
		}))
	})

	It("ignores an unreferenced duplicate with the lower id: the referenced run alone owns the label", func() {
		db, _ := openTempDB()
		insertPowerRun(db, "aborted", "monthly", 1961, 1990)
		owner := insertPowerRun(db, "complete", "monthly", 1961, 1990)
		seedClean(db)
		insertMonthly(db, cellA, "1961-1990", 1, owner, nil)
		res := runRule(db, "run-label-unique")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(3))
	})

	It("names every referenced run of a label in id order, one finding per colliding label, labels in mode order", func() {
		db, _ := openTempDB()
		s := seedClean(db)
		dupM1 := insertPowerRun(db, "complete", "monthly", 1991, 2020)
		dupM2 := insertPowerRun(db, "complete", "monthly", 1991, 2020)
		dupD := insertPowerRun(db, "complete", "daily", 1981, 2025)
		insertMonthly(db, cellB, "1991-2020", 2, dupM1, nil)
		insertMonthly(db, cellB, "1991-2020", 3, dupM2, nil)
		insertDaily(db, cellB, "2020-01-02", dupD, nil)
		// A same-years run of the other mode is its own label.
		other := insertPowerRun(db, "complete", "daily", 1991, 2020)
		insertDaily(db, cellB, "2020-01-03", other, nil)

		res := runRule(db, "run-label-unique")
		Expect(res.Scanned).To(Equal(6))
		Expect(expectErrors(res.Findings, "run-label-unique")).To(Equal([]string{
			fmt.Sprintf("power_runs %d, %d share temporal_mode/period (daily 1981-2025): one run_label for 2 referenced runs "+
				"(re-run power for that period to convergence)", s.dailyRun, dupD),
			fmt.Sprintf("power_runs %d, %d, %d share temporal_mode/period (monthly 1991-2020): one run_label for 3 referenced runs "+
				"(re-run power for that period to convergence)", s.monthlyRun, dupM1, dupM2),
		}))
		Expect(res.Findings).To(HaveLen(2))
		Expect(res.Findings[0].Severity).To(Equal(validate.SeverityError))
	})
})
