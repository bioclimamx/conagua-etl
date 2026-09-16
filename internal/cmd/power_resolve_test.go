package cmd

// Direct specs for resolveDailyEndDate: the three-way contract behind
// the daily-mode --end-date default — the
// newest complete ingest snapshot date when one exists, today UTC when
// the DB simply has no complete run, and a hard failure on any real DB
// error (fail-fast rather than silently fetching a window the
// operator's DB does not warrant). The binary-level resolution specs
// in power_test.go cover the first two through the CLI; the error path
// needs an injected broken handle, only reachable here.

import (
	"context"
	"database/sql"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/schema"
)

var _ = Describe("resolveDailyEndDate", func() {
	openResolveDB := func() *sql.DB {
		db, err := schema.Open(filepath.Join(GinkgoT().TempDir(), "bioclima.db"))
		Expect(err).NotTo(HaveOccurred())
		return db
	}

	It("returns the newest complete ingest snapshot date", func() {
		db := openResolveDB()
		DeferCleanup(func() { Expect(db.Close()).To(Succeed()) })
		for _, run := range [][2]string{
			{"2026-07-01T00:00:00Z", "2026-07-01"},
			{"2026-07-18T00:00:00Z", "2026-07-18"},
		} {
			_, err := db.Exec(
				`INSERT INTO ingest_runs (started_at, snapshot_date, sink_kind, status)
				 VALUES (?, ?, 'local', 'complete')`, run[0], run[1])
			Expect(err).NotTo(HaveOccurred())
		}
		// A newer but incomplete run must not win.
		_, err := db.Exec(
			`INSERT INTO ingest_runs (started_at, snapshot_date, sink_kind, status)
			 VALUES ('2026-07-19T00:00:00Z', '2026-07-19', 'local', 'running')`)
		Expect(err).NotTo(HaveOccurred())

		got, err := resolveDailyEndDate(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal("2026-07-18"))
	})

	It("falls back to today UTC when no complete run exists", func() {
		db := openResolveDB()
		DeferCleanup(func() { Expect(db.Close()).To(Succeed()) })

		got, err := resolveDailyEndDate(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(time.Now().UTC().Format(powerDateLayout)))
	})

	It("propagates a real DB error instead of falling back", func() {
		db := openResolveDB()
		Expect(db.Close()).To(Succeed())

		_, err := resolveDailyEndDate(context.Background(), db)
		Expect(err).To(MatchError(ContainSubstring("resolve --end-date default")))
	})
})
