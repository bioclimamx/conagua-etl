package validate_test

// The value anchors are one pass per supplement table, not one per
// column: every statement a rule prepares is captured through the
// recording driver, and each table's counting
// statement is held to naming every checked column exactly once behind a
// single FROM, and to a query plan of one scan of the table. The
// bounded sample queries appear only for a column that leaked.

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

var supplementTableNames = []string{"monthly_supplement", "daily_supplement"}

// planOf returns the EXPLAIN QUERY PLAN detail lines of stmt.
func planOf(db *sql.DB, stmt string) []string {
	GinkgoHelper()
	rows, err := db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+stmt)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		Expect(rows.Scan(&id, &parent, &notUsed, &detail)).To(Succeed())
		out = append(out, detail)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

// occurrences counts whole-word occurrences of column in stmt.
func occurrences(stmt, column string) int {
	return len(regexp.MustCompile(`\b`+regexp.QuoteMeta(column)+`\b`).FindAllString(stmt, -1))
}

// expectOnePass holds stmt to the one-pass shape over table: a single
// FROM naming the table, every column of columns named perColumn times
// (once per operand of its predicate), and a plan of exactly one full
// scan of the table.
func expectOnePass(db *sql.DB, stmt, table string, columns []string, perColumn int) {
	GinkgoHelper()
	Expect(strings.Count(stmt, "FROM ")).To(Equal(1), stmt)
	Expect(stmt).To(HaveSuffix("FROM "+table), stmt)
	Expect(stmt).To(HavePrefix("SELECT COUNT(*), "), stmt)
	for _, col := range columns {
		Expect(occurrences(stmt, col)).To(Equal(perColumn), table+"."+col)
	}
	Expect(planOf(db, stmt)).To(Equal([]string{"SCAN " + table}))
}

// recordRule evaluates one gate rule over a recording handle on the DB
// at path, returning its result and the statements it prepared.
func recordRule(path, id string) (validate.RuleResult, []string) {
	GinkgoHelper()
	rec := openRecording(path)
	recording.reset()
	res := runRule(rec, id)
	return res, recording.statements()
}

// statementsOver filters stmts to those whose FROM names table.
func statementsOver(stmts []string, table string) []string {
	var out []string
	for _, s := range stmts {
		if strings.Contains(s, "FROM "+table) {
			out = append(out, s)
		}
	}
	return out
}

// circularColumns is the registry's circular columns, in registry order.
func circularColumns() []string {
	var out []string
	for _, p := range power.Registry {
		if p.Circular {
			out = append(out, p.Column)
		}
	}
	return out
}

var _ = Describe("one pass per supplement table", func() {
	It("fill-leak counts all 31 columns of each table in one statement — a single scan — and nothing else on a clean DB", func() {
		db, path := openTempDB()
		seedClean(db)
		Expect(db.Close()).To(Succeed())

		res, stmts := recordRule(path, "fill-leak")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(4))
		Expect(stmts).To(HaveLen(len(supplementTableNames)), strings.Join(stmts, "\n"))
		rec := openRecording(path)
		for i, table := range supplementTableNames {
			stmt := stmts[i]
			expectOnePass(rec, stmt, table, registryColumns(), 1)
			Expect(strings.Count(stmt, "= -999)")).To(Equal(len(power.Registry)), table)
			Expect(strings.Count(stmt, "COUNT(*) FILTER (WHERE ")).To(Equal(len(power.Registry)), table)
		}
	})

	It("wind-range counts both circular columns of each table in one statement — a single scan — and nothing else on a clean DB", func() {
		db, path := openTempDB()
		seedClean(db)
		Expect(db.Close()).To(Succeed())
		circulars := circularColumns()
		Expect(circulars).To(Equal([]string{"wd2m_deg", "wd10m_deg"}))

		res, stmts := recordRule(path, "wind-range")
		Expect(res.Findings).To(BeEmpty())
		Expect(res.Scanned).To(Equal(4))
		Expect(stmts).To(HaveLen(len(supplementTableNames)), strings.Join(stmts, "\n"))
		rec := openRecording(path)
		for i, table := range supplementTableNames {
			stmt := stmts[i]
			// Each direction is named twice: once per bound of the closed interval.
			expectOnePass(rec, stmt, table, circulars, 2)
			Expect(strings.Count(stmt, "COUNT(*) FILTER (WHERE ")).To(Equal(len(circulars)), table)
			Expect(strings.Count(stmt, " < 0 OR ")).To(Equal(len(circulars)), table)
			Expect(strings.Count(stmt, " > 360)")).To(Equal(len(circulars)), table)
			// No other registry column rides along.
			for _, col := range registryColumns() {
				if col == "wd2m_deg" || col == "wd10m_deg" {
					continue
				}
				Expect(occurrences(stmt, col)).To(BeZero(), table+"."+col)
			}
		}
	})

	It("fill-leak adds one bounded sample per leaked column, never a second counting pass", func() {
		db, path := openTempDB()
		s := seedClean(db)
		for _, date := range []string{"2020-02-01", "2020-02-02", "2020-02-03", "2020-02-04"} {
			insertDaily(db, cellA, date, s.dailyRun, map[string]any{"rh2m_pct": -999.0, "ws2m_ms": -999.0})
		}
		Expect(db.Close()).To(Succeed())

		res, stmts := recordRule(path, "fill-leak")
		Expect(res.Findings).To(HaveLen(2))
		Expect(res.Scanned).To(Equal(8))
		Expect(stmts).To(HaveLen(2+2), strings.Join(stmts, "\n"))
		Expect(statementsOver(stmts, "monthly_supplement")).To(HaveLen(1))
		daily := statementsOver(stmts, "daily_supplement")
		Expect(daily).To(HaveLen(3))
		Expect(strings.Count(daily[0], "COUNT(*) FILTER")).To(Equal(len(power.Registry)), "the one counting pass")
		for i, col := range []string{"rh2m_pct", "ws2m_ms"} {
			Expect(daily[1+i]).To(ContainSubstring(
				"SELECT cell_id, date FROM daily_supplement WHERE " + col + " = -999 ORDER BY cell_id, date LIMIT 3"))
		}
	})

	It("wind-range adds one bounded sample per offending column, never a second counting pass", func() {
		db, path := openTempDB()
		s := seedClean(db)
		insertMonthly(db, cellA, "1981-2010", 3, s.monthlyRun, map[string]any{"wd10m_deg": -0.1})
		insertMonthly(db, cellA, "1981-2010", 4, s.monthlyRun, map[string]any{"wd10m_deg": 361.0})
		Expect(db.Close()).To(Succeed())

		res, stmts := recordRule(path, "wind-range")
		Expect(res.Findings).To(HaveLen(1))
		Expect(res.Scanned).To(Equal(6))
		Expect(stmts).To(HaveLen(2+1), strings.Join(stmts, "\n"))
		monthly := statementsOver(stmts, "monthly_supplement")
		Expect(monthly).To(HaveLen(2))
		Expect(strings.Count(monthly[0], "COUNT(*) FILTER")).To(Equal(2), "the one counting pass")
		Expect(monthly[1]).To(ContainSubstring(
			"SELECT cell_id, period, month, wd10m_deg FROM monthly_supplement WHERE wd10m_deg < 0 OR wd10m_deg > 360 " +
				"ORDER BY cell_id, period, month LIMIT 3"))
		Expect(statementsOver(stmts, "daily_supplement")).To(HaveLen(1))
	})

	It("run-refs counts the NULL, dangling, and distinct dangling references in one pass per table, sampling a failing table with one bounded query", func() {
		db, path := openTempDB()
		seedClean(db)
		for i, id := range []int64{900, 42, 7, 300} {
			insertDaily(db, cellA, fmt.Sprintf("2020-02-%02d", i+1), id, nil)
		}
		Expect(db.Close()).To(Succeed())

		res, stmts := recordRule(path, "run-refs")
		Expect(res.Findings).To(HaveLen(1))
		Expect(res.Scanned).To(Equal(2 + 6))
		Expect(stmts).To(HaveLen(2+1), strings.Join(stmts, "\n"))
		Expect(statementsOver(stmts, "monthly_supplement")).To(HaveLen(1))
		daily := statementsOver(stmts, "daily_supplement")
		Expect(daily).To(HaveLen(2))
		Expect(strings.Count(daily[0], "FROM ")).To(Equal(1), daily[0])
		Expect(daily[0]).To(ContainSubstring("COUNT(DISTINCT CASE WHEN s.power_run_id IS NOT NULL AND r.id IS NULL THEN s.power_run_id END)"))
		Expect(daily[1]).To(HaveSuffix("GROUP BY power_run_id ORDER BY power_run_id LIMIT 3"))
	})
})
