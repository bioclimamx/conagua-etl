package publish_test

// Specs for the national bioclima.db: the full canonical database —
// every table's rows equal to the source's, every column, the sequence
// table too; the shipped file's properties (rollback journal, the
// stamp, integrity, FKs resolvable, no side files); the source never
// written and its header left as it was; no temp residue on success or
// on the failures the copy can meet; two builds identical in content;
// and the entry crossing the archive seam.

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// setHeaderVersion writes user_version on the source through a raw
// connection — the one way a fixture can stand in for a header the
// writer would refuse or never stamps (the production DB's 0).
func setHeaderVersion(path string, version int) {
	GinkgoHelper()
	raw, err := sql.Open("sqlite", path)
	Expect(err).NotTo(HaveOccurred())
	_, err = raw.Exec("PRAGMA user_version = " + strconv.Itoa(version))
	Expect(err).NotTo(HaveOccurred())
	Expect(raw.Close()).To(Succeed())
}

// buildNationalDB renders the national entry into memory and returns
// the bytes and the entry path.
func buildNationalDB(ctx context.Context, srcPath, tmpDir string) (data []byte, entryPath string) {
	GinkgoHelper()
	e := publish.NationalDBEntry(ctx, srcPath, tmpDir)
	var buf bytes.Buffer
	Expect(e.Write(&buf)).To(Succeed())
	return buf.Bytes(), e.Path
}

var _ = Describe("NationalDBEntry", func() {
	var (
		ctx     = context.Background()
		srcPath string
		srcDir  string
		tmpDir  string
		before  string
		listing []string
	)

	BeforeEach(func() {
		srcPath, _ = seedStateDBSource()
		srcDir = filepath.Dir(srcPath)
		tmpDir = filepath.Join(GinkgoT().TempDir(), "out")
		before = sha256Of(srcPath)
		listing = dirNames(srcDir)
	})

	// sourceUntouched asserts the source's bytes are as seeded and that
	// nothing beyond SQLite's WAL coordination files appeared beside it.
	sourceUntouched := func() {
		GinkgoHelper()
		Expect(sha256Of(srcPath)).To(Equal(before))
		var others []string
		for _, n := range dirNames(srcDir) {
			if !strings.HasSuffix(n, "-shm") && !strings.HasSuffix(n, "-wal") {
				others = append(others, n)
			}
		}
		Expect(others).To(Equal(listing))
	}

	Describe("the copy", func() {
		var (
			src  *sql.DB
			got  *sql.DB
			path string
		)

		BeforeEach(func() {
			data, entryPath := buildNationalDB(ctx, srcPath, tmpDir)
			Expect(entryPath).To(Equal("bioclima.db"))
			path = writeDB(data, "bioclima.db")
			src = openRO(srcPath)
			got = openRO(path)
		})

		It("carries every table's rows equal to the source's, every column, and the sequence table", func() {
			var covered []string
			for _, t := range ddlTableNames {
				want := selectAll(src, t, "")
				Expect(want).NotTo(BeEmpty(), t)
				Expect(selectAll(got, t, "")).To(Equal(want), t)
				covered = append(covered, t)
			}
			Expect(covered).To(ConsistOf(ddlTableNames))
			Expect(selectAll(got, "sqlite_sequence", "")).To(Equal(selectAll(src, "sqlite_sequence", "")))
			// The full DB, not a state's: both states' stations and the EMA
			// station, the no-station warning, every cell.
			Expect(count(got, `SELECT COUNT(DISTINCT state) FROM stations`)).To(Equal(3))
			Expect(count(got, `SELECT COUNT(*) FROM stations WHERE source <> 'conagua_conventional'`)).To(Equal(1))
			Expect(count(got, `SELECT COUNT(*) FROM parsing_warnings WHERE station_id IS NULL`)).To(Equal(1))
			Expect(count(got, `SELECT COUNT(*) FROM nasa_power_grid_cells`)).To(Equal(count(src, `SELECT COUNT(*) FROM nasa_power_grid_cells`)))
			Expect(count(got, `SELECT COUNT(*) FROM nasa_power_grid_cells`)).To(BeNumerically(">", 2), "more cells than any state references")
		})

		It("is a shipped file: rollback journal, stamped, integrity ok, FKs resolvable, no side files", func() {
			Expect(pragmaText(got, "journal_mode")).To(Equal("delete"))
			var version int
			Expect(got.QueryRow("PRAGMA user_version").Scan(&version)).To(Succeed())
			Expect(version).To(Equal(schema.Version))
			Expect(pragmaText(got, "integrity_check")).To(Equal("ok"))
			rows, err := got.Query(`PRAGMA foreign_key_check`)
			Expect(err).NotTo(HaveOccurred())
			Expect(rows.Next()).To(BeFalse(), "a foreign key that does not resolve in-file")
			Expect(rows.Err()).NotTo(HaveOccurred())
			Expect(rows.Close()).To(Succeed())
			Expect(dirNames(filepath.Dir(path))).To(Equal([]string{"bioclima.db"}))
			Expect(count(got, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_%'`)).To(Equal(8))
		})

		It("leaves the source untouched and no residue under the temp dir", func() {
			sourceUntouched()
			Expect(dirNames(tmpDir)).To(BeEmpty())
		})
	})

	It("stamps a copy of a source whose header reads 0 — the production DB's — leaving the source's header at 0", func() {
		setHeaderVersion(srcPath, 0)
		before = sha256Of(srcPath)
		listing = dirNames(srcDir)

		data, _ := buildNationalDB(ctx, srcPath, tmpDir)
		got := openRO(writeDB(data, "bioclima.db"))
		var version int
		Expect(got.QueryRow("PRAGMA user_version").Scan(&version)).To(Succeed())
		Expect(version).To(Equal(schema.Version))
		src := openRO(srcPath)
		Expect(src.QueryRow("PRAGMA user_version").Scan(&version)).To(Succeed())
		Expect(version).To(BeZero())
		Expect(selectAll(got, "stations", "")).To(Equal(selectAll(src, "stations", "")))
		sourceUntouched()
	})

	It("builds the same content twice", func() {
		first, _ := buildNationalDB(ctx, srcPath, tmpDir)
		second, _ := buildNationalDB(ctx, srcPath, tmpDir)
		a := openRO(writeDB(first, "a.db"))
		b := openRO(writeDB(second, "b.db"))
		for _, t := range ddlTableNames {
			Expect(selectAll(b, t, "")).To(Equal(selectAll(a, t, "")), t)
		}
		Expect(selectAll(b, "sqlite_sequence", "")).To(Equal(selectAll(a, "sqlite_sequence", "")))
		// Reported, not asserted: SQLite is content-reproducible only.
		GinkgoWriter.Printf("NationalDBEntry twice: bytes identical = %t (%d bytes)\n", bytes.Equal(first, second), len(first))
		sourceUntouched()
		Expect(dirNames(tmpDir)).To(BeEmpty())
	})

	Describe("failures", func() {
		// expectCleanFailure runs the entry, asserts it failed with err
		// matching m, wrote nothing to w, left no residue under the temp
		// dir, and never touched the source.
		expectCleanFailure := func(e archive.Entry, m types.GomegaMatcher) {
			GinkgoHelper()
			var buf bytes.Buffer
			err := e.Write(&buf)
			Expect(err).To(m)
			Expect(buf.Len()).To(BeZero(), "nothing is streamed before the copy completes")
			Expect(dirNames(tmpDir)).To(BeEmpty())
			sourceUntouched()
		}

		It("refuses a missing source and creates nothing", func() {
			missing := filepath.Join(srcDir, "missing.db")
			expectCleanFailure(publish.NationalDBEntry(ctx, missing, tmpDir),
				MatchError(ContainSubstring("vacuum "+missing+" into ")))
			Expect(sha256Of(srcPath)).To(Equal(before))
		})

		It("refuses a source another schema version stamped, through the writer's check, after the copy", func() {
			setHeaderVersion(srcPath, 99)
			before = sha256Of(srcPath)
			listing = dirNames(srcDir)
			expectCleanFailure(publish.NationalDBEntry(ctx, srcPath, tmpDir),
				MatchError(ContainSubstring("user_version 99 does not match schema version")))
		})

		It("honours cancellation", func() {
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			expectCleanFailure(publish.NationalDBEntry(cancelled, srcPath, tmpDir), MatchError(context.Canceled))
		})

		It("refuses a temp dir it cannot create", func() {
			blocked := filepath.Join(GinkgoT().TempDir(), "file")
			Expect(os.WriteFile(blocked, []byte("x"), 0o600)).To(Succeed())
			e := publish.NationalDBEntry(ctx, srcPath, filepath.Join(blocked, "out"))
			var buf bytes.Buffer
			Expect(e.Write(&buf)).To(MatchError(HavePrefix("create temp dir: ")))
			Expect(buf.Len()).To(BeZero())
			sourceUntouched()
		})
	})

	It("crosses the archive seam: the one entry of the zip is the database the entry renders", func() {
		e := publish.NationalDBEntry(ctx, srcPath, tmpDir)
		zipPath := filepath.Join(tmpDir, "national-sqlite.zip")
		res, err := archive.WriteZip(ctx, zipPath, []archive.Entry{e},
			archive.ZipOptions{Modified: time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Entries).To(Equal(1))
		names, contents := zipContents(zipPath)
		Expect(names).To(Equal([]string{"bioclima.db"}))
		Expect(dirNames(tmpDir)).To(Equal([]string{"national-sqlite.zip"}), "the build's temp dir is gone")

		got := openRO(writeDB(contents["bioclima.db"], "bioclima.db"))
		src := openRO(srcPath)
		Expect(pragmaText(got, "integrity_check")).To(Equal("ok"))
		Expect(pragmaText(got, "journal_mode")).To(Equal("delete"))
		for _, t := range ddlTableNames {
			Expect(selectAll(got, t, "")).To(Equal(selectAll(src, t, "")), t)
		}
		sourceUntouched()
	})
})
