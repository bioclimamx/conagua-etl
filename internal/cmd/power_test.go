package cmd_test

// CLI-surface specs for the power verb, driven through the real binary
// (the command tree is the single source of truth for flags, so the
// operator-visible surface is what gets proven): --temporal and period
// validation, the cross-mode flag rejections, and the --end-date
// default resolution against ingest_runs. The monthly period set is
// derived from its conagua owners (NormalsKinds + PeriodForKind) —
// never re-enumerated here.

import (
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// powerDeadEndpoint never serves: validation must fail before any POWER
// request, and the resolution specs run against zero cells, so a
// request reaching it would surface as a run failure.
const powerDeadEndpoint = "http://127.0.0.1:9/"

// conaguaPeriods derives the valid monthly period labels from the
// vocabulary owners.
func conaguaPeriods() []string {
	out := make([]string, 0, len(conagua.NormalsKinds))
	for _, k := range conagua.NormalsKinds {
		p, ok := conagua.PeriodForKind(k)
		ExpectWithOffset(1, ok).To(BeTrue(), string(k))
		out = append(out, p)
	}
	return out
}

func freshPowerDBPath() string {
	return filepath.Join(GinkgoT().TempDir(), "bioclima.db")
}

var _ = Describe("conagua-etl power CLI", func() {
	Context("flag validation", func() {
		It("rejects an unknown --temporal", func() {
			session := runPowerToExit(freshPowerDBPath(), powerDeadEndpoint, "--temporal", "weekly")
			Expect(session.ExitCode()).To(Equal(1))
			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring("error:"))
			Expect(stderr).To(ContainSubstring("temporal"))
			Expect(stderr).To(ContainSubstring("weekly"))
		})

		It("rejects a monthly year pair outside the CONAGUA period set, listing the valid periods", func() {
			session := runPowerToExit(freshPowerDBPath(), powerDeadEndpoint,
				"--start-year", "1990", "--end-year", "2019")
			Expect(session.ExitCode()).To(Equal(1))
			stderr := string(session.Err.Contents())
			Expect(stderr).To(ContainSubstring("error:"))
			// The error must teach the operator the whole valid set.
			periods := conaguaPeriods()
			Expect(periods).To(HaveLen(4))
			for _, p := range periods {
				Expect(stderr).To(ContainSubstring(p))
			}
		})

		It("rejects daily-mode date flags under --temporal monthly", func() {
			for _, flags := range [][]string{
				{"--start-date", "1991-01-01"},
				{"--end-date", "2020-12-31"},
			} {
				session := runPowerToExit(freshPowerDBPath(), powerDeadEndpoint, flags...)
				Expect(session.ExitCode()).To(Equal(1), strings.Join(flags, " "))
				Expect(string(session.Err.Contents())).To(
					ContainSubstring(strings.TrimPrefix(flags[0], "--")), strings.Join(flags, " "))
			}
		})

		It("rejects monthly-mode year flags under --temporal daily, even at their default values", func() {
			for _, flags := range [][]string{
				{"--start-year", "1991"},
				{"--end-year", "2020"},
			} {
				args := append([]string{"--temporal", "daily"}, flags...)
				session := runPowerToExit(freshPowerDBPath(), powerDeadEndpoint, args...)
				// Explicitly setting the flag is the operator error —
				// the value matching the default earns no pass.
				Expect(session.ExitCode()).To(Equal(1), strings.Join(flags, " "))
				Expect(string(session.Err.Contents())).To(
					ContainSubstring(strings.TrimPrefix(flags[0], "--")), strings.Join(flags, " "))
			}
		})

		It("rejects a malformed --start-date", func() {
			session := runPowerToExit(freshPowerDBPath(), powerDeadEndpoint,
				"--temporal", "daily", "--start-date", "01/02/2020", "--end-date", "2020-01-10")
			Expect(session.ExitCode()).To(Equal(1))
			stderr := string(session.Err.Contents())
			Expect(stderr).To(MatchRegexp(`(?i)start.?date`))
			Expect(stderr).To(ContainSubstring("01/02/2020"))
		})
	})

	Context("--end-date default resolution", func() {
		It("uses the newest complete ingest run's snapshot_date", func() {
			dbPath := freshPowerDBPath()
			db, err := schema.Open(dbPath)
			Expect(err).NotTo(HaveOccurred())
			for _, run := range []struct{ started, snapshot, status string }{
				{"2026-07-01T00:00:00Z", "2026-07-01", "complete"},
				{"2026-07-18T00:00:00Z", "2026-07-18", "complete"},
				// A newer aborted run must never supply the default.
				{"2026-07-19T00:00:00Z", "2026-07-19", "aborted"},
			} {
				_, err := db.Exec(`
INSERT INTO ingest_runs (started_at, snapshot_date, sink_kind, status)
VALUES (?, ?, 'local', ?)`, run.started, run.snapshot, run.status)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(db.Close()).To(Succeed())

			// No station carries coordinates, so the run resolves zero
			// cells and completes without one POWER request — while
			// still recording the manifest the defaults resolved to.
			session := runPowerToExit(dbPath, powerDeadEndpoint, "--temporal", "daily")
			Expect(session.ExitCode()).To(Equal(0))

			run := readPowerRun(openIngestDB(dbPath), 1)
			Expect(run.status).To(Equal("complete"))
			Expect(run.temporalMode).To(Equal("daily"))
			Expect(run.endDate.String).To(Equal("2026-07-18"))
			Expect(run.startDate.String).To(Equal("1981-01-01")) // the daily default
			Expect(run.startYear).To(Equal(1981))
			Expect(run.endYear).To(Equal(2026))
			Expect(run.endpoint).To(Equal(powerDeadEndpoint))
			Expect(run.cellsAttempted.Int64).To(BeZero())
			Expect(run.cellsFailed.Int64).To(BeZero())
			Expect(run.supplementRows.Int64).To(BeZero())
		})

		It("falls back to today UTC when no complete ingest run exists", func() {
			dbPath := freshPowerDBPath()
			before := time.Now().UTC().Format("2006-01-02")
			session := runPowerToExit(dbPath, powerDeadEndpoint, "--temporal", "daily")
			after := time.Now().UTC().Format("2006-01-02")
			Expect(session.ExitCode()).To(Equal(0))

			run := readPowerRun(openIngestDB(dbPath), 1)
			Expect(run.status).To(Equal("complete"))
			// Bracketed so a midnight rollover mid-spec cannot flake.
			Expect(run.endDate.String).To(SatisfyAny(Equal(before), Equal(after)))
			Expect(run.startDate.String).To(Equal("1981-01-01"))
		})
	})
})
