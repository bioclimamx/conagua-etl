// In-package specs for the surfaces the external ingest_test package
// cannot reach: the unexported pullErrorIssue formatter, the writer's
// prepared-statement fields (positional column order is the invariant
// under test), and clearWarningsForSourceFile. ginkgo is imported by
// name, not dot-imported, because its Report would collide with
// ingest.Report — the parity package sets the precedent.
package ingest

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// newSpecDB opens a fresh file-backed DB in a spec-scoped temp dir.
func newSpecDB() *sql.DB {
	ginkgo.GinkgoHelper()
	path := filepath.Join(ginkgo.GinkgoT().TempDir(), "bioclima.db")
	db, err := schema.Open(path)
	Expect(err).NotTo(HaveOccurred())
	ginkgo.DeferCleanup(func() { _ = db.Close() })
	return db
}

func newSpecTx(db *sql.DB) *sql.Tx {
	ginkgo.GinkgoHelper()
	tx, err := db.BeginTx(context.Background(), nil)
	Expect(err).NotTo(HaveOccurred())
	ginkgo.DeferCleanup(func() { _ = tx.Rollback() })
	return tx
}

func specStation(tx *sql.Tx, externalID string) int64 {
	ginkgo.GinkgoHelper()
	id, err := UpsertStation(context.Background(), tx, StationUpsert{
		Source: SourceConaguaConventional, ExternalID: externalID, Name: "t",
	})
	Expect(err).NotTo(HaveOccurred())
	return id
}

var _ = ginkgo.Describe("pullErrorIssue", func() {
	ginkgo.It("formats attempts, HTTP status, and last_error", func() {
		got := pullErrorIssue(snapshot.FileState{
			Outcome:   snapshot.OutcomeError,
			HTTPCode:  500,
			Attempts:  3,
			LastError: "unexpected status 500",
		})
		Expect(got).To(Equal("pull errored after 3 attempt(s), http=500: unexpected status 500"))
	})

	ginkgo.It("omits the http= token for transport failures and floors attempts at 1", func() {
		got := pullErrorIssue(snapshot.FileState{
			Outcome:   snapshot.OutcomeError,
			Attempts:  0, // a ledger written before attempts were tracked
			LastError: "context deadline exceeded",
		})
		Expect(got).To(Equal("pull errored after 1 attempt(s): context deadline exceeded"))
	})

	ginkgo.It("truncates a long last_error at 240 chars with an ellipsis", func() {
		long := strings.Repeat("x", 500)
		got := pullErrorIssue(snapshot.FileState{
			Outcome: snapshot.OutcomeError, Attempts: 1, LastError: long,
		})
		Expect(got).To(Equal("pull errored after 1 attempt(s): " + strings.Repeat("x", 240) + "…"))
	})

	ginkgo.It("truncates rune-wise, never splitting a multi-byte character", func() {
		long := strings.Repeat("á", 300) // 2 bytes per rune: a byte cut would corrupt
		got := pullErrorIssue(snapshot.FileState{
			Outcome: snapshot.OutcomeError, Attempts: 1, LastError: long,
		})
		Expect(got).To(Equal("pull errored after 1 attempt(s): " + strings.Repeat("á", 240) + "…"))
	})
})

var _ = ginkgo.Describe("ingestWriter", func() {
	ctx := context.Background()

	newWriter := func(db *sql.DB) *ingestWriter {
		ginkgo.GinkgoHelper()
		w, err := newIngestWriter(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = w.Close() })
		return w
	}

	ginkgo.It("round-trips a daily row through the rebound prepared statement", func() {
		db := newSpecDB()
		w := newWriter(db)
		tx := newSpecTx(db)
		id := specStation(tx, "1001")

		// Distinct sentinel per column so a positional swap cannot pass.
		_, err := tx.Stmt(w.insertDaily).ExecContext(ctx, id, "2020-01-01", 25.1, 10.2, 0.3, 5.4)
		Expect(err).NotTo(HaveOccurred())
		Expect(tx.Commit()).To(Succeed())

		var gotStation int64
		var gotDate string
		var tmax, tmin, precip, evap float64
		Expect(db.QueryRow(`SELECT station_id, date, tmax, tmin, precip, evap
			FROM daily_observations`).
			Scan(&gotStation, &gotDate, &tmax, &tmin, &precip, &evap)).To(Succeed())
		Expect(gotStation).To(Equal(id))
		Expect(gotDate).To(Equal("2020-01-01"))
		Expect(tmax).To(Equal(25.1))
		Expect(tmin).To(Equal(10.2))
		Expect(precip).To(Equal(0.3))
		Expect(evap).To(Equal(5.4))
	})

	ginkgo.It("round-trips a monthly_normals row through the rebound prepared statement", func() {
		db := newSpecDB()
		w := newWriter(db)
		tx := newSpecTx(db)
		id := specStation(tx, "1001")

		_, err := tx.Stmt(w.insertMonthlyNormal).ExecContext(ctx,
			id, "1991-2020", 3, 25.5, 10.5, 17.75, 33.3, 101.5)
		Expect(err).NotTo(HaveOccurred())
		Expect(tx.Commit()).To(Succeed())

		var gotStation int64
		var period string
		var month int
		var tmax, tmin, tmean, precip, evap float64
		Expect(db.QueryRow(`SELECT station_id, period, month, tmax, tmin, tmean, precip, evap
			FROM monthly_normals`).
			Scan(&gotStation, &period, &month, &tmax, &tmin, &tmean, &precip, &evap)).To(Succeed())
		Expect(gotStation).To(Equal(id))
		Expect(period).To(Equal("1991-2020"))
		Expect(month).To(Equal(3))
		Expect(tmax).To(Equal(25.5))
		Expect(tmin).To(Equal(10.5))
		Expect(tmean).To(Equal(17.75))
		Expect(precip).To(Equal(33.3))
		Expect(evap).To(Equal(101.5))
	})

	ginkgo.It("round-trips every extras column with distinct sentinels", func() {
		db := newSpecDB()
		w := newWriter(db)
		tx := newSpecTx(db)
		id := specStation(tx, "1001")

		// 22 positional args, one distinct sentinel per column. The
		// evap/rain_days tail is where a positional slip historically
		// hides — the sentinels make any swap visible.
		_, err := tx.Stmt(w.insertNormalsExtras).ExecContext(ctx,
			id, "1991-2020", 7,
			31.1, 2001, 32.2, "2001-07-13",
			1.1, 2002, -2.2, "2002-07-17",
			101.5, 2003, 55.5, "2003-07-02",
			21, 22, 23, 24, 25,
			6.5, 26,
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(tx.Commit()).To(Succeed())

		var gotStation int64
		var period string
		var month int
		var tmaxME, tmaxDE, tminME, tminDE, precipME, precipDE, rainDays float64
		var tmaxMEYear, tminMEYear, precipMEYear int
		var tmaxDEDate, tminDEDate, precipDEDate string
		var yTmax, yTmin, yTmean, yPrecip, yEvap, yRain int
		Expect(db.QueryRow(`SELECT station_id, period, month,
			tmax_monthly_extreme, tmax_monthly_extreme_year, tmax_daily_extreme, tmax_daily_extreme_date,
			tmin_monthly_extreme, tmin_monthly_extreme_year, tmin_daily_extreme, tmin_daily_extreme_date,
			precip_monthly_extreme, precip_monthly_extreme_year, precip_daily_extreme, precip_daily_extreme_date,
			tmax_years_with_data, tmin_years_with_data, tmean_years_with_data,
			precip_years_with_data, evap_years_with_data,
			rain_days, rain_days_years_with_data
			FROM monthly_normals_extras`).
			Scan(&gotStation, &period, &month,
				&tmaxME, &tmaxMEYear, &tmaxDE, &tmaxDEDate,
				&tminME, &tminMEYear, &tminDE, &tminDEDate,
				&precipME, &precipMEYear, &precipDE, &precipDEDate,
				&yTmax, &yTmin, &yTmean, &yPrecip, &yEvap,
				&rainDays, &yRain)).To(Succeed())
		Expect(gotStation).To(Equal(id))
		Expect(period).To(Equal("1991-2020"))
		Expect(month).To(Equal(7))
		Expect(tmaxME).To(Equal(31.1))
		Expect(tmaxMEYear).To(Equal(2001))
		Expect(tmaxDE).To(Equal(32.2))
		Expect(tmaxDEDate).To(Equal("2001-07-13"))
		Expect(tminME).To(Equal(1.1))
		Expect(tminMEYear).To(Equal(2002))
		Expect(tminDE).To(Equal(-2.2))
		Expect(tminDEDate).To(Equal("2002-07-17"))
		Expect(precipME).To(Equal(101.5))
		Expect(precipMEYear).To(Equal(2003))
		Expect(precipDE).To(Equal(55.5))
		Expect(precipDEDate).To(Equal("2003-07-02"))
		Expect(yTmax).To(Equal(21))
		Expect(yTmin).To(Equal(22))
		Expect(yTmean).To(Equal(23))
		Expect(yPrecip).To(Equal(24))
		Expect(yEvap).To(Equal(25))
		Expect(rainDays).To(Equal(6.5))
		Expect(yRain).To(Equal(26))
	})

	ginkgo.It("accepts the 1961-1990 period in both normals statements", func() {
		db := newSpecDB()
		w := newWriter(db)
		tx := newSpecTx(db)
		id := specStation(tx, "1001")

		_, err := tx.Stmt(w.insertMonthlyNormal).ExecContext(ctx,
			id, "1961-1990", 6, 22.5, 8.0, 15.25, 12.0, 90.0)
		Expect(err).NotTo(HaveOccurred())
		_, err = tx.Stmt(w.insertNormalsExtras).ExecContext(ctx,
			id, "1961-1990", 6,
			nil, nil, nil, nil,
			nil, nil, nil, nil,
			nil, nil, nil, nil,
			27, 26, nil, 28, nil,
			nil, nil,
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(tx.Commit()).To(Succeed())

		var tmax, tmin, tmean, precip, evap float64
		Expect(db.QueryRow(`SELECT tmax, tmin, tmean, precip, evap FROM monthly_normals
			WHERE station_id=? AND period='1961-1990' AND month=6`, id).
			Scan(&tmax, &tmin, &tmean, &precip, &evap)).To(Succeed())
		Expect([]float64{tmax, tmin, tmean, precip, evap}).
			To(Equal([]float64{22.5, 8.0, 15.25, 12.0, 90.0}))

		var yTmax, yTmin, yPrecip int
		var yTmean, yEvap sql.NullInt64
		Expect(db.QueryRow(`SELECT tmax_years_with_data, tmin_years_with_data,
			tmean_years_with_data, precip_years_with_data, evap_years_with_data
			FROM monthly_normals_extras
			WHERE station_id=? AND period='1961-1990' AND month=6`, id).
			Scan(&yTmax, &yTmin, &yTmean, &yPrecip, &yEvap)).To(Succeed())
		Expect(yTmax).To(Equal(27))
		Expect(yTmin).To(Equal(26))
		Expect(yTmean.Valid).To(BeFalse())
		Expect(yPrecip).To(Equal(28))
		Expect(yEvap.Valid).To(BeFalse())
	})

	ginkgo.Describe("WriteWarnings", func() {
		ginkgo.It("round-trips a batch, storing line 0 and nil station_id as NULL", func() {
			db := newSpecDB()
			w := newWriter(db)
			tx := newSpecTx(db)
			id := specStation(tx, "1001")

			warns := []Warning{
				{StationID: &id, SourceFile: "conagua-raw/2026-04-21/daily/01001.txt", Line: 42,
					Severity: SeverityWarn, Issue: `malformed TMAX "abc"`},
				{StationID: &id, SourceFile: "conagua-raw/2026-04-21/daily/01001.txt", Line: 0,
					Severity: SeverityError, Issue: "file had no station header"},
				{StationID: nil, SourceFile: "qc", Line: 0,
					Severity: SeverityWarn, Issue: "station has no observations"},
			}
			Expect(w.WriteWarnings(ctx, tx, warns)).To(Succeed())
			Expect(tx.Commit()).To(Succeed())

			rows, err := db.Query(`SELECT station_id, source_file, line, severity, issue
				FROM parsing_warnings ORDER BY id`)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close() //nolint:errcheck // read-side close in a spec

			type got struct {
				stationID sql.NullInt64
				source    string
				line      sql.NullInt64
				severity  string
				issue     string
			}
			var all []got
			for rows.Next() {
				var g got
				Expect(rows.Scan(&g.stationID, &g.source, &g.line, &g.severity, &g.issue)).To(Succeed())
				all = append(all, g)
			}
			Expect(rows.Err()).NotTo(HaveOccurred())
			Expect(all).To(Equal([]got{
				{sql.NullInt64{Int64: id, Valid: true}, "conagua-raw/2026-04-21/daily/01001.txt",
					sql.NullInt64{Int64: 42, Valid: true}, "warn", `malformed TMAX "abc"`},
				{sql.NullInt64{Int64: id, Valid: true}, "conagua-raw/2026-04-21/daily/01001.txt",
					sql.NullInt64{}, "error", "file had no station header"},
				{sql.NullInt64{}, "qc", sql.NullInt64{}, "warn", "station has no observations"},
			}))
		})

		ginkgo.It("rejects a severity outside {warn, error} before writing any row", func() {
			db := newSpecDB()
			w := newWriter(db)
			tx := newSpecTx(db)

			err := w.WriteWarnings(ctx, tx, []Warning{
				{SourceFile: "x", Severity: "fatal", Issue: "oh no"},
			})
			Expect(err).To(MatchError(ContainSubstring(`invalid severity "fatal"`)))

			var n int
			Expect(tx.QueryRow(`SELECT COUNT(*) FROM parsing_warnings`).Scan(&n)).To(Succeed())
			Expect(n).To(BeZero())
		})
	})

	ginkgo.It("closes idempotently and refuses WriteWarnings afterwards", func() {
		db := newSpecDB()
		w, err := newIngestWriter(ctx, db)
		Expect(err).NotTo(HaveOccurred())

		Expect(w.Close()).To(Succeed())
		Expect(w.Close()).To(Succeed(), "second Close must be a no-op")

		tx := newSpecTx(db)
		err = w.WriteWarnings(ctx, tx, []Warning{
			{SourceFile: "x", Severity: SeverityWarn, Issue: "y"},
		})
		Expect(err).To(MatchError(ContainSubstring("writer is closed")))
	})
})

var _ = ginkgo.Describe("clearWarningsForSourceFile", func() {
	ginkgo.It("deletes only the rows keyed to the given sink key", func() {
		ctx := context.Background()
		db := newSpecDB()
		w, err := newIngestWriter(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = w.Close() })
		tx := newSpecTx(db)

		Expect(w.WriteWarnings(ctx, tx, []Warning{
			{SourceFile: "conagua-raw/2026-04-21/daily/01001.txt", Severity: SeverityWarn, Issue: "a"},
			{SourceFile: "conagua-raw/2026-04-21/daily/01001.txt", Severity: SeverityWarn, Issue: "b"},
			{SourceFile: "conagua-raw/2026-04-21/daily/01002.txt", Severity: SeverityWarn, Issue: "c"},
		})).To(Succeed())

		Expect(clearWarningsForSourceFile(ctx, tx, "conagua-raw/2026-04-21/daily/01001.txt")).To(Succeed())

		var n int
		Expect(tx.QueryRow(`SELECT COUNT(*) FROM parsing_warnings
			WHERE source_file = 'conagua-raw/2026-04-21/daily/01001.txt'`).Scan(&n)).To(Succeed())
		Expect(n).To(BeZero())

		// The sibling file's row survives with its values intact.
		var source, severity, issue string
		Expect(tx.QueryRow(`SELECT source_file, severity, issue FROM parsing_warnings`).
			Scan(&source, &severity, &issue)).To(Succeed())
		Expect(source).To(Equal("conagua-raw/2026-04-21/daily/01002.txt"))
		Expect(severity).To(Equal("warn"))
		Expect(issue).To(Equal("c"))
	})
})
