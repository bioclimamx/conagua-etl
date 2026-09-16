package validate_test

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// The verb's rule set: the five core rules in their fixed order, then
// the gate's anchors — runs-in-flight excluded.
var verbOrder = []string{
	"orphan-runs", "bbox", "wmo-month-completeness", "daily-sanity", "cross-period",
	"ingest-complete", "station-refs", "cell-refs", "run-refs", "run-label-unique", "fill-leak", "wind-range",
}

var _ = Describe("AllRules", func() {
	It("lists the five core rules then the gate's anchors, without runs-in-flight", func() {
		rules := validate.AllRules("1991-2020")
		ids := make([]string, len(rules))
		for i, r := range rules {
			ids[i] = r.ID
			Expect(r.Name).NotTo(BeEmpty(), r.ID)
			Expect(r.Run).NotTo(BeNil(), r.ID)
		}
		Expect(ids).To(Equal(verbOrder))
		Expect(ids).NotTo(ContainElement("runs-in-flight"))

		gate := map[string]validate.Rule{}
		for _, r := range validate.GateRules() {
			gate[r.ID] = r
		}
		for _, r := range rules[5:] {
			Expect(r.Name).To(Equal(gate[r.ID].Name), r.ID)
		}
		Expect(rules[1].Name).To(Equal(gate["bbox"].Name))
		Expect(rules[0].Name).To(Equal("Orphan run reconciliation"))
		Expect(rules[2].Name).To(Equal("WMO §4.4.1 within-month completeness"))
		Expect(rules[3].Name).To(Equal("Daily-series sanity"))
		Expect(rules[4].Name).To(Equal("Cross-period consistency"))
	})

	It("scopes the period-scoped rules to DefaultPeriod when the period is empty", func() {
		Expect(validate.DefaultPeriod).To(Equal("1991-2020"))
		db, _ := openTempDB()
		sid := station(db, "x", "test", f64(19.4), f64(-99.2))
		for d := 1; d <= 20; d++ {
			insertObs(db, sid, 1992, 1, d, 25.0, 10.0)
		}
		res := runVerbRule(db, "", "wmo-month-completeness")
		Expect(issues(res.Findings)).To(Equal([]string{
			"WMO §4.4.1 fail in period 1991-2020, calendar month=01: 1" + wmoIssueTail + "1992 (11/31 missing, gap=11)",
		}))
	})
})

// verbSeed is seedClean plus one finding for each core rule and one
// anchor error: a station outside MX and one at (0, 0), a stranded
// ingest run, a 40 °C diurnal swing at Mérida, an 8 °C tmax jump between
// two of Mérida's periods, and a wind direction past 360.
type verbSeed struct {
	cleanSeed
	london, zerozero, stranded int64
}

func seedVerb(db *sql.DB) verbSeed {
	GinkgoHelper()
	s := verbSeed{cleanSeed: seedClean(db)}
	s.london = station(db, "outside", "London", f64(51.5), f64(-0.1))
	s.zerozero = station(db, "zerozero", "wherever", f64(0.0), f64(0.0))
	s.stranded = insertIngestRun(db, "running", "2020-01-01T00:00:00Z")
	insertObs(db, s.merida, 2020, 1, 3, 50.0, 10.0)
	insertNormalsTemps(db, s.merida, "1981-2010", 1, 25.0, nil, nil)
	insertDaily(db, cellA, "2020-01-02", s.dailyRun, map[string]any{"wd2m_deg": 360.1})
	return s
}

// verbRows is what Run writes over seedVerb, in write order: rule order,
// then each rule's emission order. The orphan row is first and, once
// the run is reconciled, gone.
func verbRows(s verbSeed) []warningRow {
	return []warningRow{
		validateRow(nil, "orphan-runs", validate.SeverityWarn,
			fmt.Sprintf("ingest_runs.id=%d started 2020-01-01T00:00:00Z, status='running' beyond 24h0m0s — reconciled to 'aborted'", s.stranded)),
		validateRow(i64(s.london), "bbox", validate.SeverityWarn,
			`station conagua_conventional/outside ("London") at lat=51.5000 lon=-0.1000 outside MX bbox [14.5,32.8]×[-118.5,-86.7] — likely typo`),
		validateRow(i64(s.zerozero), "bbox", validate.SeverityError,
			`station conagua_conventional/zerozero ("wherever") at impossible lat=0.0000 lon=0.0000 — error blocks publish`),
		validateRow(i64(s.merida), "wmo-month-completeness", validate.SeverityWarn,
			"WMO §4.4.1 fail in period 1991-2020, calendar month=01: 1"+wmoIssueTail+"2020 (28/31 missing, gap=28)"),
		validateRow(i64(s.progreso), "wmo-month-completeness", validate.SeverityWarn,
			"WMO §4.4.1 fail in period 1991-2020, calendar month=01: 1"+wmoIssueTail+"2020 (30/31 missing, gap=30)"),
		validateRow(i64(s.merida), "daily-sanity", validate.SeverityWarn,
			"diurnal range > 25°C on 1 day(s); top: 2020-01-03 (40.0°C)"),
		validateRow(i64(s.merida), "cross-period", validate.SeverityWarn,
			"tmax cross-period delta exceeds ±3.0°C in 1 (month, period-pair)(s); top: m=01 1981-2010 vs 1991-2020: 25.0 vs 33.0 (Δ-8.0°C)"),
		validateRow(nil, "wind-range", validate.SeverityError,
			"daily_supplement.wd2m_deg: 1 row outside [0, 360] ("+cellA+"/2020-01-02 = 360.1)"),
	}
}

func ruleIDs(rr []validate.RuleReport) []string {
	out := make([]string, len(rr))
	for i, r := range rr {
		out[i] = r.ID
	}
	return out
}

var _ = Describe("Run", func() {
	It("writes every finding as a parsing_warnings row — station_id, source_file, NULL line, severity, issue — in rule order, and reports the totals", func() {
		db, _ := openTempDB()
		s := seedVerb(db)
		before := time.Now().UTC().Truncate(time.Second)
		report, err := validate.Run(context.Background(), db, validate.Options{})
		Expect(err).NotTo(HaveOccurred())

		Expect(readWarnings(db, "validate:%")).To(Equal(verbRows(s)))

		Expect(report.Period).To(Equal("1991-2020"))
		Expect(report.WarningsTotal).To(Equal(6))
		Expect(report.ErrorsTotal).To(Equal(2))
		Expect(report.StartedAt).To(BeTemporally(">=", before))
		Expect(report.FinishedAt).To(BeTemporally(">=", report.StartedAt))
		Expect(ruleIDs(report.PerRule)).To(Equal(verbOrder))
		counts := map[string][3]int{}
		for _, rr := range report.PerRule {
			counts[rr.ID] = [3]int{rr.Scanned, rr.Warnings, rr.Errors}
			Expect(rr.Elapsed).To(BeNumerically(">=", 0))
			Expect(rr.Findings).To(HaveLen(rr.Warnings + rr.Errors))
		}
		Expect(counts).To(Equal(map[string][3]int{
			"orphan-runs": {1, 1, 0}, "bbox": {4, 1, 1}, "wmo-month-completeness": {2, 2, 0},
			"daily-sanity": {1, 1, 0}, "cross-period": {1, 1, 0},
			"ingest-complete": {2, 0, 0}, "station-refs": {10, 0, 0}, "cell-refs": {7, 0, 0},
			"run-refs": {5, 0, 0}, "run-label-unique": {2, 0, 0}, "fill-leak": {5, 0, 0}, "wind-range": {5, 0, 1},
		}))
		// The per-rule findings are the rows, in the same order.
		var fromReport []warningRow
		for _, rr := range report.PerRule {
			for _, f := range rr.Findings {
				fromReport = append(fromReport, validateRow(f.StationID, f.RuleID, f.Severity, f.Issue))
			}
		}
		Expect(fromReport).To(Equal(verbRows(s)))
	})

	It("clears its own prior rows and leaves ingest's untouched", func() {
		db, _ := openTempDB()
		s := seedVerb(db)
		insertWarning(db, s.merida, "validate:bbox")
		insertWarning(db, nil, "validate:stale-rule")
		Expect(countWarnings(db, "validate:%")).To(Equal(2))

		_, err := validate.Run(context.Background(), db, validate.Options{})
		Expect(err).NotTo(HaveOccurred())
		Expect(readWarnings(db, "validate:%")).To(Equal(verbRows(s)))
		Expect(readWarnings(db, "conagua-raw/%")).To(Equal([]warningRow{
			ingestRow(i64(s.merida), "conagua-raw/2026-06-08/normals/31019.txt"),
			ingestRow(nil, "conagua-raw/2026-06-08/catalog.html"),
		}))
		Expect(countWarnings(db, "%")).To(Equal(2 + len(verbRows(s))))
	})

	It("converges: a second run replaces its rows with the same set, and the reconciled run is found no more", func() {
		db, _ := openTempDB()
		s := seedVerb(db)
		first, err := validate.Run(context.Background(), db, validate.Options{})
		Expect(err).NotTo(HaveOccurred())
		Expect(first.WarningsTotal).To(Equal(6))
		status, finished := runStatus(db, "ingest_runs", s.stranded)
		Expect(status).To(Equal("aborted"))
		Expect(finished.Valid).To(BeTrue())

		second, err := validate.Run(context.Background(), db, validate.Options{})
		Expect(err).NotTo(HaveOccurred())
		Expect(second.WarningsTotal).To(Equal(5))
		Expect(second.ErrorsTotal).To(Equal(2))
		Expect(second.PerRule[0].ID).To(Equal("orphan-runs"))
		Expect(second.PerRule[0].Scanned).To(BeZero())
		Expect(second.PerRule[0].Findings).To(BeEmpty())
		afterSecond := readWarnings(db, "validate:%")
		Expect(afterSecond).To(Equal(verbRows(s)[1:]))

		third, err := validate.Run(context.Background(), db, validate.Options{})
		Expect(err).NotTo(HaveOccurred())
		Expect(third.WarningsTotal).To(Equal(second.WarningsTotal))
		Expect(third.ErrorsTotal).To(Equal(second.ErrorsTotal))
		Expect(readWarnings(db, "validate:%")).To(Equal(afterSecond))
		Expect(countWarnings(db, "%")).To(Equal(2+len(afterSecond)), "replaced, not appended")
		status, again := runStatus(db, "ingest_runs", s.stranded)
		Expect(status).To(Equal("aborted"))
		Expect(again).To(Equal(finished))
	})

	It("omits a skipped rule entirely: no hook, no report slot, no rows", func() {
		db, _ := openTempDB()
		s := seedVerb(db)
		var started []string
		report, err := validate.Run(context.Background(), db, validate.Options{
			Skip:        map[string]bool{"daily-sanity": true, "wind-range": true},
			OnRuleStart: func(id, _ string) { started = append(started, id) },
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(started).NotTo(ContainElements("daily-sanity", "wind-range"))
		Expect(started).To(HaveLen(len(verbOrder) - 2))
		Expect(ruleIDs(report.PerRule)).To(Equal(started))
		Expect(report.WarningsTotal).To(Equal(5))
		Expect(report.ErrorsTotal).To(Equal(1))
		want := verbRows(s)
		Expect(readWarnings(db, "validate:%")).To(Equal(append(want[:5:5], want[6])))
		Expect(countWarnings(db, "validate:daily-sanity")).To(BeZero())
		Expect(countWarnings(db, "validate:wind-range")).To(BeZero())
	})

	It("fires the hooks around each rule, in order, with the rule's result and its wall time", func() {
		db, _ := openTempDB()
		seedVerb(db)
		type done struct {
			id       string
			findings int
			scanned  int
		}
		var events []string
		var names []string
		var dones []done
		report, err := validate.Run(context.Background(), db, validate.Options{
			OnRuleStart: func(id, name string) {
				events = append(events, "start:"+id)
				names = append(names, name)
			},
			OnRuleDone: func(id string, res validate.RuleResult, elapsed time.Duration) {
				events = append(events, "done:"+id)
				Expect(elapsed).To(BeNumerically(">=", 0))
				dones = append(dones, done{id: id, findings: len(res.Findings), scanned: res.Scanned})
			},
		})
		Expect(err).NotTo(HaveOccurred())
		var wantEvents []string
		for _, id := range verbOrder {
			wantEvents = append(wantEvents, "start:"+id, "done:"+id)
		}
		Expect(events).To(Equal(wantEvents))
		rules := validate.AllRules("")
		for i, r := range rules {
			Expect(names[i]).To(Equal(r.Name))
		}
		Expect(dones).To(HaveLen(len(verbOrder)))
		for i, d := range dones {
			rr := report.PerRule[i]
			Expect(d.id).To(Equal(rr.ID))
			Expect(d.findings).To(Equal(rr.Warnings + rr.Errors))
			Expect(d.scanned).To(Equal(rr.Scanned))
		}
	})

	It("counts the core rules' error (an impossible coordinate) and the anchors' errors alike in ErrorsTotal, each written under its rule's id", func() {
		db, _ := openTempDB()
		s := seedVerb(db)
		insertStationCell(db, 777, cellA)
		report, err := validate.Run(context.Background(), db, validate.Options{})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.ErrorsTotal).To(Equal(3))
		Expect(report.WarningsTotal).To(Equal(6))
		var errs []warningRow
		for _, r := range readWarnings(db, "validate:%") {
			if r.Severity == validate.SeverityError {
				errs = append(errs, r)
			}
		}
		Expect(errs).To(Equal([]warningRow{
			verbRows(s)[2],
			validateRow(nil, "station-refs", validate.SeverityError,
				"station_power_cell: 1 row with a station_id naming no stations row (1 distinct: 777)"),
			verbRows(s)[7],
		}))
	})

	It("surfaces a rule's own error with its id, keeps the rules that completed, and leaves no validate rows behind", func() {
		db, _ := openTempDB()
		s := seedVerb(db)
		insertWarning(db, s.merida, "validate:bbox")
		mustExec(db, `DROP TABLE monthly_normals`)
		report, err := validate.Run(context.Background(), db, validate.Options{})
		Expect(err).To(MatchError(ContainSubstring("rule cross-period: ")))
		Expect(err).To(MatchError(ContainSubstring("no such table: monthly_normals")))
		Expect(report).NotTo(BeNil())
		Expect(ruleIDs(report.PerRule)).To(Equal(verbOrder[:4]))
		Expect(report.FinishedAt).To(BeZero())
		Expect(report.WarningsTotal).To(Equal(5))
		Expect(report.ErrorsTotal).To(Equal(1))
		Expect(countWarnings(db, "validate:%")).To(BeZero(), "the clear ran; the batch is written only after the last rule")
		Expect(countWarnings(db, "conagua-raw/%")).To(Equal(2))
	})

	It("rejects a malformed period at the first period-scoped rule", func() {
		db, _ := openTempDB()
		seedVerb(db)
		report, err := validate.Run(context.Background(), db, validate.Options{Period: "1991"})
		Expect(err).To(MatchError(`rule wmo-month-completeness: malformed period "1991"`))
		Expect(ruleIDs(report.PerRule)).To(Equal([]string{"orphan-runs", "bbox"}))
		Expect(report.Period).To(Equal("1991"))
	})

	It("scopes the period-scoped rules to the period given and stamps it on the report", func() {
		db, _ := openTempDB()
		s := seedVerb(db)
		report, err := validate.Run(context.Background(), db, validate.Options{Period: "1961-1990"})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Period).To(Equal("1961-1990"))
		Expect(countWarnings(db, "validate:wmo-month-completeness")).To(BeZero())
		Expect(countWarnings(db, "validate:daily-sanity")).To(BeZero())
		Expect(report.PerRule[2].Scanned).To(BeZero())
		Expect(report.PerRule[3].Scanned).To(BeZero())
		want := verbRows(s)
		Expect(readWarnings(db, "validate:%")).To(Equal([]warningRow{want[0], want[1], want[2], want[6], want[7]}))
	})

	It("honours cancellation before the first rule", func() {
		db, _ := openTempDB()
		seedVerb(db)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		report, err := validate.Run(ctx, db, validate.Options{})
		Expect(err).To(MatchError(context.Canceled))
		Expect(report).NotTo(BeNil())
		Expect(report.PerRule).To(BeEmpty())
	})

	It("needs the writer handle: over the read-only handle the clear is refused before any rule runs", func() {
		db, path := openTempDB()
		seedVerb(db)
		Expect(db.Close()).To(Succeed())
		before := sha256Of(path)

		ro, err := schema.OpenReadOnly(path)
		Expect(err).NotTo(HaveOccurred())
		var started []string
		report, err := validate.Run(context.Background(), ro, validate.Options{
			OnRuleStart: func(id, _ string) { started = append(started, id) },
		})
		Expect(ro.Close()).To(Succeed())
		Expect(err).To(MatchError(ContainSubstring("clear prior validate warnings: ")))
		Expect(err).To(MatchError(ContainSubstring("readonly")))
		Expect(report.PerRule).To(BeEmpty())
		Expect(started).To(BeEmpty())
		Expect(sha256Of(path)).To(Equal(before))
	})
})
