package validate_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

func sha256Of(path string) string {
	GinkgoHelper()
	b, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

var gateOrder = []string{
	"bbox", "runs-in-flight", "ingest-complete", "station-refs", "cell-refs",
	"run-refs", "run-label-unique", "fill-leak", "wind-range",
}

var _ = Describe("GateRules", func() {
	It("lists the read-only gate rule set in execution order", func() {
		rules := validate.GateRules()
		ids := make([]string, len(rules))
		for i, r := range rules {
			ids[i] = r.ID
			Expect(r.Name).NotTo(BeEmpty(), r.ID)
			Expect(r.Run).NotTo(BeNil(), r.ID)
		}
		Expect(ids).To(Equal(gateOrder))
	})
})

var _ = Describe("Gate", func() {
	It("passes a clean DB with every rule reported, in order, and no findings", func() {
		db, _ := openTempDB()
		seedClean(db)
		var seen []string
		before := time.Now().UTC()
		report, err := validate.Gate(context.Background(), db, func(rr validate.RuleReport) { seen = append(seen, rr.ID) })
		Expect(err).NotTo(HaveOccurred())
		Expect(seen).To(Equal(gateOrder))
		Expect(report.Errors).To(BeZero())
		Expect(report.Warnings).To(BeZero())
		Expect(report.ErrorFindings()).To(BeEmpty())
		Expect(report.Rules).To(HaveLen(len(gateOrder)))
		for i, rr := range report.Rules {
			Expect(rr.ID).To(Equal(gateOrder[i]))
			Expect(rr.Name).To(Equal(validate.GateRules()[i].Name))
			Expect(rr.Findings).To(BeEmpty())
			Expect(rr.Errors).To(BeZero())
			Expect(rr.Warnings).To(BeZero())
			Expect(rr.Elapsed).To(BeNumerically(">=", 0))
		}
		scanned := map[string]int{}
		for _, rr := range report.Rules {
			scanned[rr.ID] = rr.Scanned
		}
		Expect(scanned).To(Equal(map[string]int{
			"bbox": 2, "runs-in-flight": 0, "ingest-complete": 1, "station-refs": 8, "cell-refs": 6,
			"run-refs": 4, "run-label-unique": 2, "fill-leak": 4, "wind-range": 4,
		}))
		Expect(report.StartedAt).To(BeTemporally(">=", before.Truncate(time.Second)))
		Expect(report.FinishedAt).To(BeTemporally(">=", report.StartedAt))
	})

	It("aggregates per-rule counts and totals, keeping every error and capping warns", func() {
		db, _ := openTempDB()
		s := seedClean(db)
		// bbox: one warn, one error.
		station(db, "outside", "London", f64(51.5), f64(-0.1))
		station(db, "zerozero", "wherever", f64(0.0), f64(0.0))
		// runs-in-flight: one error.
		insertIngestRun(db, "running", "2026-06-09T09:59:00Z")
		// wind-range: one error.
		insertDaily(db, cellA, "2020-01-02", s.dailyRun, map[string]any{"wd2m_deg": 360.1})

		report, err := validate.Gate(context.Background(), db, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Warnings).To(Equal(1))
		Expect(report.Errors).To(Equal(3))

		byID := map[string]validate.RuleReport{}
		for _, rr := range report.Rules {
			byID[rr.ID] = rr
		}
		Expect(byID["bbox"].Warnings).To(Equal(1))
		Expect(byID["bbox"].Errors).To(Equal(1))
		Expect(byID["bbox"].Findings).To(HaveLen(2))
		Expect(byID["runs-in-flight"].Errors).To(Equal(1))
		Expect(byID["wind-range"].Errors).To(Equal(1))

		errs := report.ErrorFindings()
		Expect(errs).To(HaveLen(3))
		Expect(errs[0].RuleID).To(Equal("bbox"))
		Expect(errs[0].Issue).To(Equal(`station conagua_conventional/zerozero ("wherever") at impossible lat=0.0000 lon=0.0000 — error blocks publish`))
		Expect(errs[1].RuleID).To(Equal("runs-in-flight"))
		Expect(errs[1].Issue).To(HavePrefix("ingest_runs.id="))
		Expect(errs[2].RuleID).To(Equal("wind-range"))
		Expect(errs[2].Issue).To(Equal("daily_supplement.wd2m_deg: 1 row outside [0, 360] (" + cellA + "/2020-01-02 = 360.1)"))
	})

	It("carries every error and at most 50 warn samples per rule", func() {
		db, _ := openTempDB()
		seedClean(db)
		for i := range 60 {
			station(db, fmt.Sprintf("w%03d", i), "no-coords", nil, nil)
		}
		for i := range 55 {
			station(db, fmt.Sprintf("e%03d", i), "wherever", f64(0.0), f64(0.0))
		}
		report, err := validate.Gate(context.Background(), db, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Warnings).To(Equal(60))
		Expect(report.Errors).To(Equal(55))
		bbox := report.Rules[0]
		Expect(bbox.ID).To(Equal("bbox"))
		Expect(bbox.Scanned).To(Equal(2 + 60 + 55))
		Expect(bbox.Findings).To(HaveLen(50 + 55))
		var warns, errs int
		for _, f := range bbox.Findings {
			switch f.Severity {
			case validate.SeverityWarn:
				warns++
			case validate.SeverityError:
				errs++
			}
		}
		Expect(warns).To(Equal(50))
		Expect(errs).To(Equal(55))
		Expect(report.ErrorFindings()).To(HaveLen(55))
	})

	It("never writes: it runs to completion over a read-only handle and leaves the file byte-identical", func() {
		db, path := openTempDB()
		seedClean(db)
		// Findings of every severity, so the paths that would tempt a
		// write (a warning row, a stranded run to reconcile) are exercised.
		station(db, "outside", "London", f64(51.5), f64(-0.1))
		station(db, "zerozero", "wherever", f64(0.0), f64(0.0))
		stranded := insertIngestRun(db, "running", "2020-01-01T00:00:00Z")
		Expect(db.Close()).To(Succeed())
		before := sha256Of(path)

		ro, err := schema.OpenReadOnly(path)
		Expect(err).NotTo(HaveOccurred())
		report, err := validate.Gate(context.Background(), ro, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(ro.Close()).To(Succeed())
		Expect(report.Errors).To(Equal(2))
		Expect(report.Warnings).To(Equal(1))
		Expect(sha256Of(path)).To(Equal(before))

		// Re-read through a fresh handle: no validate rows, the stranded
		// run untouched.
		again, err := schema.OpenReadOnly(path)
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(again.Close()).To(Succeed()) }()
		var n int
		Expect(again.QueryRow(`SELECT COUNT(*) FROM parsing_warnings WHERE source_file LIKE 'validate:%'`).Scan(&n)).To(Succeed())
		Expect(n).To(BeZero())
		var status string
		var finished sql.NullString
		Expect(again.QueryRow(`SELECT status, finished_at FROM ingest_runs WHERE id = ?`, stranded).Scan(&status, &finished)).To(Succeed())
		Expect(status).To(Equal("running"))
		Expect(finished.Valid).To(BeFalse())
	})

	It("surfaces a rule's SQL error with the rule id and the rules that completed", func() {
		db, _ := openTempDB()
		seedClean(db)
		mustExec(db, `DROP TABLE nasa_power_grid_cells`)
		report, err := validate.Gate(context.Background(), db, nil)
		Expect(err).To(MatchError(ContainSubstring("rule cell-refs: ")))
		Expect(err).To(MatchError(ContainSubstring("no such table: nasa_power_grid_cells")))
		Expect(report).NotTo(BeNil())
		Expect(report.Rules).To(HaveLen(4))
		Expect(report.Rules[3].ID).To(Equal("station-refs"))
		Expect(report.FinishedAt).NotTo(BeZero())
	})

	It("honours cancellation before the first rule", func() {
		db, _ := openTempDB()
		seedClean(db)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		report, err := validate.Gate(ctx, db, nil)
		Expect(err).To(MatchError(context.Canceled))
		Expect(err).To(MatchError(ContainSubstring("rule bbox: ")))
		Expect(report.Rules).To(BeEmpty())
	})
})
