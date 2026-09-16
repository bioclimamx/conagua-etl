package validate_test

// The five core QC rules' SQL, pinned statement by statement, byte for
// byte. Every
// statement a rule prepares is captured through the recording driver
// and compared whole, so a drifted WHERE clause, ORDER BY, GROUP BY,
// HAVING floor, or column list — anything that changes the finding set
// or its order — fails here before the national parity harness has to
// say so. The bound thresholds (25 °C, 3σ, the period bounds, the
// orphan cutoff) ride as parameters and are pinned by value in
// pinned_constants_test.go. Run's own two statements — the clear and
// the insert — are pinned beside the rules, with the statement sequence
// of a whole run over the five core rules.

import (
	"context"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// The pinned statements, verbatim — the leading newline is part of each
// literal the rules prepare.
const (
	pinnedOrphanScanSQL = `
SELECT id, started_at FROM %s
 WHERE status = 'running' AND started_at < ?`

	pinnedOrphanReconcileSQL = `UPDATE %s SET status = 'aborted', finished_at = ? WHERE id = ?`

	pinnedBBoxSQL = `
SELECT id, name, source, external_id, lat, lon
  FROM stations
 ORDER BY id`

	pinnedWMOSQL = `
SELECT station_id,
       CAST(strftime('%Y', date) AS INTEGER) AS y,
       CAST(strftime('%m', date) AS INTEGER) AS m,
       COUNT(*) AS days,
       GROUP_CONCAT(strftime('%d', date), ',') AS day_list
  FROM daily_observations
 WHERE date BETWEEN ? AND ?
   AND tmax IS NOT NULL
 GROUP BY station_id, y, m`

	pinnedDiurnalSQL = `
SELECT station_id, date, tmax, tmin
  FROM daily_observations
 WHERE date BETWEEN ? AND ?
   AND tmax IS NOT NULL AND tmin IS NOT NULL
   AND (tmax - tmin) > ?
 ORDER BY station_id, (tmax - tmin) DESC, date`

	// %[1]s is the variable — tmax, then tmin — as the rule formats
	// it.
	pinnedZScoreSQL = `
WITH stats AS (
  SELECT station_id,
         CAST(strftime('%%m', date) AS INTEGER) AS m,
         AVG(%[1]s) AS mean,
         AVG(%[1]s * %[1]s) - AVG(%[1]s) * AVG(%[1]s) AS var
    FROM daily_observations
   WHERE date BETWEEN ? AND ?
     AND %[1]s IS NOT NULL
   GROUP BY station_id, m
   HAVING COUNT(*) >= 30 AND var > 0
)
SELECT d.station_id, d.date, d.%[1]s, s.mean, s.var
  FROM daily_observations d
  JOIN stats s
    ON s.station_id = d.station_id
   AND s.m = CAST(strftime('%%m', d.date) AS INTEGER)
 WHERE d.date BETWEEN ? AND ?
   AND d.%[1]s IS NOT NULL
   AND ABS(d.%[1]s - s.mean) > ? * SQRT(s.var)
 ORDER BY d.station_id, ABS(d.%[1]s - s.mean) / SQRT(s.var) DESC, d.date`

	pinnedCrossPeriodSQL = `
SELECT a.station_id, a.month, a.period AS period_a, b.period AS period_b,
       a.tmax, b.tmax, a.tmin, b.tmin, a.tmean, b.tmean
  FROM monthly_normals a
  JOIN monthly_normals b
    ON a.station_id = b.station_id
   AND a.month = b.month
   AND a.period < b.period
 ORDER BY a.station_id, a.month, a.period, b.period`

	// The clear binds the pattern 'validate:%' as a parameter rather than
	// inlining it as a literal; the plan and the rows it deletes are the
	// same either way.
	verbClearSQL = `DELETE FROM parsing_warnings WHERE source_file LIKE ?`

	pinnedInsertSQL = `
INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
VALUES (?, ?, NULL, ?, ?)`
)

// coreRules is the five core QC rules, in their execution order.
var coreRules = []string{"orphan-runs", "bbox", "wmo-month-completeness", "daily-sanity", "cross-period"}

// nativeAnchors is what AllRules adds after the five core rules.
func nativeAnchors() map[string]bool {
	out := map[string]bool{}
	for _, r := range validate.AllRules("") {
		out[r.ID] = true
	}
	for _, id := range coreRules {
		delete(out, id)
	}
	return out
}

func orphanSQL(table string) (scan, reconcile string) {
	return strings.Replace(pinnedOrphanScanSQL, "%s", table, 1),
		strings.Replace(pinnedOrphanReconcileSQL, "%s", table, 1)
}

func zScoreSQL(variable string) string {
	return strings.ReplaceAll(strings.ReplaceAll(pinnedZScoreSQL, "%[1]s", variable), "%%m", "%m")
}

// recordVerbRule evaluates one verb rule at period over a recording
// handle on the closed DB at path, returning its result and the
// statements it prepared.
func recordVerbRule(path, period, id string) (validate.RuleResult, []string) {
	GinkgoHelper()
	rec := openRecording(path)
	recording.reset()
	res, err := verbRule(period, id).Run(context.Background(), rec)
	Expect(err).NotTo(HaveOccurred())
	return res, recording.statements()
}

var _ = Describe("pinned SQL", func() {
	It("orphan-runs scans each ledger, then reconciles that ledger's orphans one UPDATE at a time, ingest_runs before power_runs", func() {
		db, path := openTempDB()
		insertIngestRun(db, "running", "2020-01-01T00:00:00Z")
		insertPowerRunAt(db, "running", "2020-01-01T00:00:00Z")
		insertPowerRunAt(db, "running", "2020-01-02T00:00:00Z")
		Expect(db.Close()).To(Succeed())

		res, stmts := recordVerbRule(path, "", "orphan-runs")
		Expect(res.Findings).To(HaveLen(3))
		ingestScan, ingestUpdate := orphanSQL("ingest_runs")
		powerScan, powerUpdate := orphanSQL("power_runs")
		Expect(stmts).To(Equal([]string{ingestScan, ingestUpdate, powerScan, powerUpdate, powerUpdate}))
	})

	It("orphan-runs issues no UPDATE when nothing is stranded", func() {
		db, path := openTempDB()
		seedClean(db)
		Expect(db.Close()).To(Succeed())
		_, stmts := recordVerbRule(path, "", "orphan-runs")
		ingestScan, _ := orphanSQL("ingest_runs")
		powerScan, _ := orphanSQL("power_runs")
		Expect(stmts).To(Equal([]string{ingestScan, powerScan}))
	})

	It("bbox is one ordered scan of stations", func() {
		db, path := openTempDB()
		seedClean(db)
		Expect(db.Close()).To(Succeed())
		_, stmts := recordVerbRule(path, "", "bbox")
		Expect(stmts).To(Equal([]string{pinnedBBoxSQL}))
	})

	It("wmo-month-completeness is one grouped scan of daily_observations on tmax", func() {
		db, path := openTempDB()
		seedClean(db)
		Expect(db.Close()).To(Succeed())
		_, stmts := recordVerbRule(path, "1991-2020", "wmo-month-completeness")
		Expect(stmts).To(Equal([]string{pinnedWMOSQL}))
	})

	It("daily-sanity is the diurnal scan, then the tmax and the tmin z-score passes", func() {
		db, path := openTempDB()
		seedClean(db)
		Expect(db.Close()).To(Succeed())
		_, stmts := recordVerbRule(path, "1991-2020", "daily-sanity")
		Expect(stmts).To(Equal([]string{pinnedDiurnalSQL, zScoreSQL("tmax"), zScoreSQL("tmin")}))
	})

	It("cross-period is one ordered self-join of monthly_normals over ascending period pairs", func() {
		db, path := openTempDB()
		seedClean(db)
		Expect(db.Close()).To(Succeed())
		_, stmts := recordVerbRule(path, "", "cross-period")
		Expect(stmts).To(Equal([]string{pinnedCrossPeriodSQL}))
	})

	It("Run over the five core rules prepares the clear first, the rules' statements in rule order, and one insert last", func() {
		db, path := openTempDB()
		s := seedVerb(db)
		Expect(db.Close()).To(Succeed())

		rec := openRecording(path)
		recording.reset()
		report, err := validate.Run(context.Background(), rec, validate.Options{Skip: nativeAnchors()})
		Expect(err).NotTo(HaveOccurred())
		Expect(ruleIDs(report.PerRule)).To(Equal(coreRules))

		ingestScan, ingestUpdate := orphanSQL("ingest_runs")
		powerScan, _ := orphanSQL("power_runs")
		Expect(recording.statements()).To(Equal([]string{
			verbClearSQL,
			ingestScan, ingestUpdate, powerScan,
			pinnedBBoxSQL,
			pinnedWMOSQL,
			pinnedDiurnalSQL, zScoreSQL("tmax"), zScoreSQL("tmin"),
			pinnedCrossPeriodSQL,
			pinnedInsertSQL,
		}))
		// The one insert carried every finding of the five core rules.
		Expect(readWarnings(rec, "validate:%")).To(Equal(verbRows(s)[:7]))
	})

	It("Run over an empty DB prepares no insert: an empty batch pays no transaction", func() {
		db, path := openTempDB()
		Expect(db.Close()).To(Succeed())

		rec := openRecording(path)
		recording.reset()
		report, err := validate.Run(context.Background(), rec, validate.Options{Skip: nativeAnchors()})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.WarningsTotal + report.ErrorsTotal).To(BeZero())
		stmts := recording.statements()
		Expect(stmts[0]).To(Equal(verbClearSQL))
		Expect(stmts).NotTo(ContainElement(pinnedInsertSQL))
		Expect(stmts).To(HaveLen(1 + 2 + 1 + 1 + 3 + 1))
	})
})
