package validate_test

// The end-to-end Run fixture: a station outside MX (London) and one at
// (0, 0), nothing else. The pinned assertions — at least one warning,
// exactly one error (the (0, 0) station), the persisted rows equal to
// the report's totals, a second Run replacing rather than appending —
// with every row read back.
//
// The five core rules alone report ErrorsTotal == 1 over this fixture;
// the full verb reports 2, because AllRules appends the gate's integrity
// anchors after the five and ingest-complete errors on a DB with no
// ingest run. Both totals are pinned so the difference is never silent.

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

const (
	londonIssue = `station conagua_conventional/outside ("London") at lat=51.5000 lon=-0.1000 outside MX bbox [14.5,32.8]×[-118.5,-86.7] — likely typo`
	brokenIssue = `station conagua_conventional/zz ("broken") at impossible lat=0.0000 lon=0.0000 — error blocks publish`
	noIngestRun = "ingest_runs has no complete row (0 rows in total): nothing to publish"
)

var _ = Describe("Run over the end-to-end London and (0, 0) fixture", func() {
	It("records the London warn and the (0, 0) error, persists exactly the report's totals, and replaces its rows on a second run", func() {
		db, _ := openTempDB()
		london := station(db, "outside", "London", f64(51.5), f64(-0.1))
		broken := station(db, "zz", "broken", f64(0.0), f64(0.0))

		report, err := validate.Run(context.Background(), db, validate.Options{Period: "1991-2020"})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.WarningsTotal).To(Equal(1), "the pinned '≥ 1 warning' is exactly London")
		bbox := report.PerRule[1]
		Expect(bbox.ID).To(Equal("bbox"))
		Expect(bbox.Errors).To(Equal(1), "the pinned 'ErrorsTotal == 1' is the (0, 0) station")
		Expect(bbox.Warnings).To(Equal(1))
		Expect(report.ErrorsTotal).To(Equal(2), "the verb's extension: ingest-complete over a DB with no ingest run")

		persisted := countWarnings(db, "validate:%")
		Expect(persisted).To(Equal(report.WarningsTotal + report.ErrorsTotal))
		rows := readWarnings(db, "validate:%")
		Expect(rows).To(Equal([]warningRow{
			validateRow(i64(london), "bbox", validate.SeverityWarn, londonIssue),
			validateRow(i64(broken), "bbox", validate.SeverityError, brokenIssue),
			validateRow(nil, "ingest-complete", validate.SeverityError, noIngestRun),
		}))

		// Idempotence — the second run replaces the first run's rows,
		// not appends.
		report2, err := validate.Run(context.Background(), db, validate.Options{Period: "1991-2020"})
		Expect(err).NotTo(HaveOccurred())
		persisted2 := countWarnings(db, "validate:%")
		Expect(persisted2).To(Equal(report2.WarningsTotal + report2.ErrorsTotal))
		Expect(persisted2).To(Equal(persisted))
		Expect(readWarnings(db, "validate:%")).To(Equal(rows))
		Expect(countWarnings(db, "%")).To(Equal(persisted))
	})

	It("reproduces the pinned totals exactly — 1 warning, 1 error, 2 rows — when the five core rules run alone", func() {
		db, _ := openTempDB()
		london := station(db, "outside", "London", f64(51.5), f64(-0.1))
		broken := station(db, "zz", "broken", f64(0.0), f64(0.0))

		report, err := validate.Run(context.Background(), db, validate.Options{Period: "1991-2020", Skip: nativeAnchors()})
		Expect(err).NotTo(HaveOccurred())
		Expect(ruleIDs(report.PerRule)).To(Equal(coreRules))
		Expect(report.WarningsTotal).To(Equal(1))
		Expect(report.ErrorsTotal).To(Equal(1))
		Expect(countWarnings(db, "validate:%")).To(Equal(2))
		Expect(readWarnings(db, "validate:%")).To(Equal([]warningRow{
			validateRow(i64(london), "bbox", validate.SeverityWarn, londonIssue),
			validateRow(i64(broken), "bbox", validate.SeverityError, brokenIssue),
		}))
	})
})
