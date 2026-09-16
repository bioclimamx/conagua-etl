package schema

// Specs for the two shipping primitives over a WAL-mode source whose
// -wal file holds frames not yet checkpointed into the main file — a
// writer still open on it, or a file left with its log after a close
// that never checkpointed — so the copy must carry the live content the
// log holds, never the stale main file alone, without checkpointing the
// source to get it.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// walSource builds a WAL-mode database at path whose main file holds
// the schema and seedEveryTable's rows and whose -wal file alone holds
// n further daily_observations rows: auto-checkpointing is switched
// off, the base content is checkpointed by hand, and the extra rows
// are written after. The writer is returned open, with the live
// content every table then holds; the caller closes it.
func walSource(path string, n int) (*sql.DB, map[string][][]any) {
	GinkgoHelper()
	db := mustOpen(path)
	_, err := db.Exec("PRAGMA wal_autocheckpoint = 0")
	Expect(err).NotTo(HaveOccurred())
	seedEveryTable(db)
	var busy, logFrames, checkpointed int
	Expect(db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed)).To(Succeed())
	Expect(busy).To(BeZero())
	for i := range n {
		_, err := db.Exec(`INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
		  VALUES (1, date('2021-01-01', ? || ' days'), ?, NULL, 0.0, ?)`, i, 20.0+float64(i)*0.1, float64(i)*1.5)
		Expect(err).NotTo(HaveOccurred())
	}
	return db, snapshot(path)
}

// copyFile duplicates src at dst byte for byte.
func copyFile(src, dst string) {
	GinkgoHelper()
	data, err := os.ReadFile(src)
	Expect(err).NotTo(HaveOccurred())
	Expect(os.WriteFile(dst, data, 0o600)).To(Succeed())
}

// dailyRowsOfMainFile counts daily_observations in a copy of the main
// file alone — what a reader that ignored the log would see.
func dailyRowsOfMainFile(path string) int {
	GinkgoHelper()
	alone := filepath.Join(GinkgoT().TempDir(), "main-only.db")
	copyFile(path, alone)
	ro, err := OpenReadOnly(alone)
	Expect(err).NotTo(HaveOccurred())
	defer mustClose(ro)
	var n int
	Expect(ro.QueryRow(`SELECT COUNT(*) FROM daily_observations`).Scan(&n)).To(Succeed())
	return n
}

var _ = Describe("the shipping primitives over a WAL with un-checkpointed frames", func() {
	const extra = 50
	var (
		ctx  = context.Background()
		path string
	)

	BeforeEach(func() {
		path = tempDBPath()
	})

	It("VacuumInto copies the live content a still-open writer has not checkpointed, leaving the source's main file and log untouched", func() {
		db, want := walSource(path, extra)
		defer mustClose(db)
		walPath := path + "-wal"
		Expect(walPath).To(BeAnExistingFile())
		Expect(want["daily_observations"]).To(HaveLen(2 + extra))
		Expect(dailyRowsOfMainFile(path)).To(Equal(2), "the extra rows live in the log alone")
		mainBefore, walBefore := fileSHA256(path), fileSHA256(walPath)

		dst := filepath.Join(GinkgoT().TempDir(), "copy.db")
		Expect(VacuumInto(ctx, path, dst)).To(Succeed())

		got := snapshot(dst)
		for t, rows := range want {
			Expect(got[t]).To(Equal(rows), t)
		}
		Expect(dailyRowsOfMainFile(dst)).To(Equal(2+extra), "the copy is one whole file")
		Expect(fileSHA256(path)).To(Equal(mainBefore))
		Expect(fileSHA256(walPath)).To(Equal(walBefore), "a read-only connection never checkpoints")
		// The writer is unaffected: it goes on writing into the same log.
		_, err := db.Exec(`INSERT INTO daily_observations (station_id, date) VALUES (1, '2030-01-01')`)
		Expect(err).NotTo(HaveOccurred())
	})

	It("VacuumInto copies the live content of a closed file left with its log, leaving the log's bytes as they were", func() {
		db, want := walSource(path, extra)
		dir := GinkgoT().TempDir()
		left := filepath.Join(dir, "left.db")
		copyFile(path, left)
		copyFile(path+"-wal", left+"-wal")
		mustClose(db)
		Expect(dirListing(dir, true)).To(Equal([]string{"left.db", "left.db-wal"}))
		Expect(dailyRowsOfMainFile(left)).To(Equal(2))
		mainBefore, walBefore := fileSHA256(left), fileSHA256(left+"-wal")

		dst := filepath.Join(GinkgoT().TempDir(), "copy.db")
		Expect(VacuumInto(ctx, left, dst)).To(Succeed())

		got := snapshot(dst)
		for t, rows := range want {
			Expect(got[t]).To(Equal(rows), t)
		}
		Expect(fileSHA256(left)).To(Equal(mainBefore))
		Expect(fileSHA256(left + "-wal")).To(Equal(walBefore))
		Expect(dirListing(dir, false)).To(Equal([]string{"left.db"}), "nothing beyond SQLite's side files appears")
	})

	It("FinalizeShipped checkpoints a closed file's leftover log into the main file, removes the side files, and keeps the live content", func() {
		db, want := walSource(path, extra)
		dir := GinkgoT().TempDir()
		ship := filepath.Join(dir, "ship.db")
		copyFile(path, ship)
		copyFile(path+"-wal", ship+"-wal")
		mustClose(db)
		Expect(dailyRowsOfMainFile(ship)).To(Equal(2))

		Expect(FinalizeShipped(ctx, ship)).To(Succeed())

		Expect(dirListing(dir, true)).To(Equal([]string{"ship.db"}))
		Expect(dailyRowsOfMainFile(ship)).To(Equal(2+extra), "the log is in the main file now")
		ro, err := OpenReadOnly(ship)
		Expect(err).NotTo(HaveOccurred())
		defer mustClose(ro)
		Expect(journalMode(ro)).To(Equal("delete"))
		Expect(userVersion(ro)).To(Equal(Version))
		Expect(integrityCheck(ro)).To(Equal("ok"))
		for t, rows := range want {
			Expect(tableRows(ro, t)).To(Equal(rows), t)
		}
		Expect(dirListing(dir, true)).To(Equal([]string{"ship.db"}), "a rollback-journal file opens with no side file")
	})

	It("refuses to finalize a file a writer still holds open, leaving it in WAL mode with its content intact", func() {
		db, want := walSource(path, extra)
		defer mustClose(db)
		// Leaving WAL mode needs the file exclusively; the idle writer's
		// connection still holds it, so SQLite refuses the switch.
		Expect(FinalizeShipped(ctx, path)).To(MatchError(HavePrefix(
			fmt.Sprintf("finalize %s: PRAGMA journal_mode = DELETE: database is locked", path))))
		Expect(journalMode(db)).To(Equal("wal"))
		Expect(tableRows(db, "daily_observations")).To(Equal(want["daily_observations"]))
	})
})
