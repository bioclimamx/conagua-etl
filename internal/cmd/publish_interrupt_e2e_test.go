package cmd_test

// The cancellation path on the real binary (SIGINT and SIGTERM cancel):
// a publish interrupted inside an archive exits 1 with
// the aborted summary, leaves no partial file at a final path and no
// temp residue, writes no manifest or CHECKSUMS, and moves not one byte
// of the database. The seed carries enough daily rows that the archive
// in flight at the signal cannot complete before the signal lands, so
// the interrupt is mid-run by construction rather than by timing luck.

import (
	"context"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// heavyDailyRows is the size of the synthetic daily series the publish
// interrupt spec adds to one station: its tabular archive then holds
// about two seconds of work past its first unit line on the reference
// machine (the station's daily and combined files, the Parquet twins),
// against a signal that lands within tens of milliseconds.
const heavyDailyRows = 200_000

// interruptPoll is how often the interrupt specs re-read the child's
// stderr for the line that triggers the signal, and how long they wait
// for it — generous, since the suite may share the machine with other
// work; the seed's size, not the timing, is what makes the interrupt
// land mid-run.
const (
	interruptPoll = 10 * time.Millisecond
	interruptWait = 120 * time.Second
)

// addHeavyDaily appends heavyDailyRows consecutive daily rows for the
// station with the given external id through one recursive CTE, dated
// from 1800-01-01 so they collide with no seeded row.
func addHeavyDaily(dbPath, externalID string) {
	db, err := schema.Open(dbPath)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	_, err = db.ExecContext(context.Background(), `
WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM seq WHERE n + 1 < ?)
INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
SELECT s.id, date('1800-01-01', '+' || n || ' days'), 20.0 + n % 7, 5.0 + n % 5, n % 3, 4.0
  FROM seq, stations s WHERE s.external_id = ?`, heavyDailyRows, externalID)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	ExpectWithOffset(1, db.Close()).To(Succeed())
}

var _ = Describe("conagua-etl publish interrupted mid-run", func() {
	It("exits 1 on SIGINT with the aborted summary: the archive in flight discarded whole, no temp residue, no manifest, the DB untouched", func() {
		// The heavy station is AGS's, so the first archive is the heavy
		// one: its first unit line — the state database, built on disk
		// ahead of the stream — prints early, and the archive still has
		// seconds of work (the heavy daily and combined files, the Parquet
		// twins) when the signal lands.
		dbPath := seedPublishDB()
		addHeavyDaily(dbPath, "1010")
		dbSHA := fileSHA(dbPath)
		out := filepath.Join(GinkgoT().TempDir(), "publish")

		session := startPublish(dbPath, out, "--only", "tabular,json")
		Eventually(func() string { return string(session.Err.Contents()) }, interruptWait, interruptPoll).
			Should(MatchRegexp(publishUnitLineRE.String()))
		session.Interrupt()
		Eventually(session, interruptWait).Should(gexec.Exit(1))

		stderr := string(session.Err.Contents())
		stdout := string(session.Out.Contents())

		// The gate passed and the first archive was in flight: its units
		// started landing, no artifact completed, and the archive's FAIL
		// line carries the cancellation, as does the run-level error.
		Expect(stderr).To(ContainSubstring("db=" + dbPath + " (read-only)  out=" + out + "  root=./snapshots  states=all  only=tabular,json  doi=none\n"))
		Expect(stderr).NotTo(ContainSubstring("gate FAIL"))
		units := publishUnitLineRE.FindAllStringSubmatch(stderr, -1)
		Expect(units).NotTo(BeEmpty())
		for _, u := range units {
			Expect(u[3]).To(Equal("ags-tabular.zip"))
		}
		artifacts := publishArtifactLineRE.FindAllStringSubmatch(stderr, -1)
		Expect(artifacts).To(HaveLen(1))
		Expect(artifacts[0][1:3]).To(Equal([]string{"FAIL", "ags-tabular.zip"}))
		Expect(artifacts[0][6]).To(MatchRegexp(`context canceled|interrupted`))
		Expect(stderr).To(MatchRegexp(`(?m)^error: aborted after 1 of 4 artifacts: .*(context canceled|interrupted)`))

		Expect(stdout).To(ContainSubstring("\npublish aborted\n"))
		Expect(stdout).To(ContainSubstring("  snapshot date        : " + publishSnapshotDate + "\n"))
		Expect(stdout).To(ContainSubstring("  gate                 : passed · 9 rules · "))
		Expect(stdout).To(ContainSubstring("  states               : AGS,YUC\n"))
		Expect(stdout).To(ContainSubstring("  groups               : tabular,json\n"))
		Expect(stdout).To(ContainSubstring("  artifacts            : 0 ok / 1 failed / 1 attempted\n"))
		Expect(stdout).To(ContainSubstring("  failed artifacts:\n    ags-tabular.zip: "))

		// Nothing at a final path — the archive in flight was discarded
		// whole, no later archive started, no manifest or CHECKSUMS —
		// and no temp file or run directory survives.
		Expect(listDir(out)).To(BeEmpty())
		expectNoTempResidue(out)
		Expect(fileSHA(dbPath)).To(Equal(dbSHA))
	})
})
