package validate_test

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

func i64(v int64) *int64 { return &v }

// verbRule looks a verb rule up by id through AllRules at period.
func verbRule(period, id string) validate.Rule {
	GinkgoHelper()
	for _, r := range validate.AllRules(period) {
		if r.ID == id {
			return r
		}
	}
	Fail(fmt.Sprintf("no verb rule %q", id))
	return validate.Rule{}
}

// runVerbRule evaluates one verb rule over db at period.
func runVerbRule(db *sql.DB, period, id string) validate.RuleResult {
	GinkgoHelper()
	res, err := verbRule(period, id).Run(context.Background(), db)
	Expect(err).NotTo(HaveOccurred())
	return res
}

// insertObs seeds one daily_observations row by calendar day with the
// temperature pair (nil = NULL).
func insertObs(db *sql.DB, stationID int64, year, month, day int, tmax, tmin any) {
	GinkgoHelper()
	date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
	mustExec(db, `INSERT INTO daily_observations (station_id, date, tmax, tmin) VALUES (?, ?, ?, ?)`,
		stationID, date, tmax, tmin)
}

// insertNormalsTemps seeds one monthly_normals row with the three
// temperature normals (nil = NULL) and no precip / evap.
func insertNormalsTemps(db *sql.DB, stationID int64, period string, month int, tmax, tmin, tmean any) {
	GinkgoHelper()
	mustExec(db, `INSERT INTO monthly_normals (station_id, period, month, tmax, tmin, tmean)
	  VALUES (?, ?, ?, ?, ?, ?)`, stationID, period, month, tmax, tmin, tmean)
}

// insertPowerRunAt seeds a monthly power_runs row with the status and
// started_at given.
func insertPowerRunAt(db *sql.DB, status, startedAt string) int64 {
	GinkgoHelper()
	var id int64
	Expect(db.QueryRowContext(context.Background(), `
INSERT INTO power_runs (started_at, status, endpoint_url, parameters, community,
    period_start_year, period_end_year, grid_resolution, solar_conversion)
VALUES (?, ?, 'u', 'p', 'AG', 1991, 2020, '0.5x0.625', ?) RETURNING id`,
		startedAt, status, power.SolarMJpm2dToWm2).Scan(&id)).To(Succeed())
	return id
}

// runStatus reads one ledger row's status and finished_at back.
func runStatus(db *sql.DB, table string, id int64) (status string, finished sql.NullString) {
	GinkgoHelper()
	Expect(db.QueryRowContext(context.Background(),
		fmt.Sprintf(`SELECT status, finished_at FROM %s WHERE id = ?`, table), id).Scan(&status, &finished)).To(Succeed())
	return status, finished
}

// warningRow is one parsing_warnings row read back in full.
type warningRow struct {
	StationID  sql.NullInt64
	SourceFile sql.NullString
	Line       sql.NullInt64
	Severity   string
	Issue      string
}

// readWarnings returns the parsing_warnings rows whose source_file
// matches like, in id order, every column read back.
func readWarnings(db *sql.DB, like string) []warningRow {
	GinkgoHelper()
	rows, err := db.QueryContext(context.Background(), `
SELECT station_id, source_file, line, severity, issue FROM parsing_warnings
 WHERE source_file LIKE ? ORDER BY id`, like)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	var out []warningRow
	for rows.Next() {
		var r warningRow
		Expect(rows.Scan(&r.StationID, &r.SourceFile, &r.Line, &r.Severity, &r.Issue)).To(Succeed())
		out = append(out, r)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

// validateRow is the row the verb writes for a finding: station_id as
// given (nil = NULL), source_file 'validate:<rule>', line NULL.
func validateRow(stationID *int64, ruleID, severity, issue string) warningRow {
	r := warningRow{
		SourceFile: sql.NullString{String: "validate:" + ruleID, Valid: true},
		Severity:   severity,
		Issue:      issue,
	}
	if stationID != nil {
		r.StationID = sql.NullInt64{Int64: *stationID, Valid: true}
	}
	return r
}

// ingestRow is a row ingest wrote, as seedClean's insertWarning shapes
// it: line 3, warn, 'blank month'.
func ingestRow(stationID *int64, sourceFile string) warningRow {
	r := warningRow{
		SourceFile: sql.NullString{String: sourceFile, Valid: true},
		Line:       sql.NullInt64{Int64: 3, Valid: true},
		Severity:   validate.SeverityWarn,
		Issue:      "blank month",
	}
	if stationID != nil {
		r.StationID = sql.NullInt64{Int64: *stationID, Valid: true}
	}
	return r
}

// countWarnings counts the parsing_warnings rows whose source_file
// matches like.
func countWarnings(db *sql.DB, like string) int {
	GinkgoHelper()
	var n int
	Expect(db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM parsing_warnings WHERE source_file LIKE ?`, like).Scan(&n)).To(Succeed())
	return n
}
