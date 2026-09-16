package schema

// Specs for the two shipping primitives: VacuumInto — a faithful,
// compact copy over a read-only connection that never touches the
// source and never leaves a partial destination — and FinalizeShipped
// — the DDL, the stamp, and the rollback-journal switch that lets a
// shipped file open from a read-only medium, content unchanged.

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// seedEveryTable populates a row or more in every DDL table, with NULLs
// where the schema allows them, so a copy comparison exercises every
// column class the artifact carries.
func seedEveryTable(db *sql.DB) {
	GinkgoHelper()
	insertStation(db, probeStation)
	for _, q := range []string{
		`INSERT INTO stations (source, external_id, name, state) VALUES ('conagua_ema', '1001', 'EMA', NULL)`,
		`INSERT INTO monthly_normals (station_id, period, month, tmax, tmin, tmean, precip, evap)
		 VALUES (1, '1991-2020', 1, 27.4, NULL, 19.0, 0.0, 150.25)`,
		`INSERT INTO monthly_normals_extras (station_id, period, month, tmax_monthly_extreme,
		 tmax_daily_extreme_date, rain_days, rain_days_years_with_data)
		 VALUES (1, '1991-2020', 1, 38.5, '1998-05-14', 4.6, NULL)`,
		`INSERT INTO daily_observations (station_id, date, tmax, tmin, precip, evap)
		 VALUES (1, '2020-01-01', 30.5, NULL, 12.4, NULL), (1, '2020-01-02', -0.04, 19.5, 0.0, 4.2)`,
		`INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
		 VALUES (1, 'daily/1001.txt', 3, 'warn', 'x'), (NULL, 'catalog.html', NULL, 'error', 'y')`,
		`INSERT INTO ingest_runs (started_at, finished_at, snapshot_date, sink_kind, etl_git_sha, status,
		 stations_attempted, warnings_total)
		 VALUES ('2026-06-09T01:00:00Z', NULL, '2026-06-08', 'local', NULL, 'complete', 5524, 12)`,
		`INSERT INTO power_runs (started_at, status, endpoint_url, parameters, community,
		 period_start_year, period_end_year, grid_resolution, solar_conversion, unit_conversions)
		 VALUES ('2026-07-01T00:00:00Z', 'complete', 'https://power.example', 'T2M', 'AG',
		 1981, 2010, '0.5x0.625', 11.574074074074074, NULL)`,
		`INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution)
		 VALUES ('21.5N_89.3750W', 21.5, -89.375, '0.5x0.625')`,
		`INSERT INTO station_power_cell (station_id, cell_id, distance_km) VALUES (1, '21.5N_89.3750W', 27.2524061152165)`,
		`INSERT INTO monthly_supplement (cell_id, period, month, t2m_c, wd2m_deg, power_run_id)
		 VALUES ('21.5N_89.3750W', '1981-2010', 1, 24.48, NULL, 1)`,
		`INSERT INTO daily_supplement (cell_id, date, t2m_c, solar_ghi_wm2, power_run_id)
		 VALUES ('21.5N_89.3750W', '2020-01-01', 25.1, 231.48148148148147, NULL)`,
	} {
		_, err := db.Exec(q)
		Expect(err).NotTo(HaveOccurred(), q)
	}
}

// tableNames lists the DDL tables of db, sorted.
func tableNames(db *sql.DB) []string {
	GinkgoHelper()
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		Expect(rows.Scan(&n)).To(Succeed())
		names = append(names, n)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return names
}

// tableRows reads every column of every row of table, in a total order
// over all columns, as the driver's native values — so two databases
// compare column by column, NULLs and REAL bits included.
func tableRows(db *sql.DB, table string) [][]any {
	GinkgoHelper()
	var n int
	Expect(db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?)`, table).Scan(&n)).To(Succeed())
	Expect(n).To(BeNumerically(">", 0), table)
	order := make([]string, n)
	for i := range order {
		order[i] = fmt.Sprint(i + 1)
	}
	rows, err := db.Query("SELECT * FROM " + table + " ORDER BY " + strings.Join(order, ", "))
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	var out [][]any
	for rows.Next() {
		row := make([]any, n)
		ptrs := make([]any, n)
		for i := range row {
			ptrs[i] = &row[i]
		}
		Expect(rows.Scan(ptrs...)).To(Succeed())
		out = append(out, row)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

// snapshot captures every table's rows plus sqlite_sequence, keyed by
// table, through a read-only open of path.
func snapshot(path string) map[string][][]any {
	GinkgoHelper()
	db, err := OpenReadOnly(path)
	Expect(err).NotTo(HaveOccurred())
	defer mustClose(db)
	out := map[string][][]any{}
	for _, t := range tableNames(db) {
		out[t] = tableRows(db, t)
	}
	out["sqlite_sequence"] = tableRows(db, "sqlite_sequence")
	return out
}

// dirListing lists dir's entries, minus SQLite's -shm/-wal coordination
// files when withSideFiles is false.
func dirListing(dir string, withSideFiles bool) []string {
	GinkgoHelper()
	entries, err := os.ReadDir(dir)
	Expect(err).NotTo(HaveOccurred())
	var names []string
	for _, e := range entries {
		n := e.Name()
		if !withSideFiles && (strings.HasSuffix(n, "-shm") || strings.HasSuffix(n, "-wal")) {
			continue
		}
		names = append(names, n)
	}
	return names
}

func journalMode(db *sql.DB) string {
	GinkgoHelper()
	var mode string
	Expect(db.QueryRow("PRAGMA journal_mode").Scan(&mode)).To(Succeed())
	return strings.ToLower(mode)
}

func integrityCheck(db *sql.DB) string {
	GinkgoHelper()
	var result string
	Expect(db.QueryRow("PRAGMA integrity_check").Scan(&result)).To(Succeed())
	return result
}

// seededSource writes a seeded database at a fresh path through the
// writer and closes it, so the WAL is checkpointed and the main file is
// the whole content.
func seededSource() string {
	GinkgoHelper()
	path := tempDBPath()
	db := mustOpen(path)
	seedEveryTable(db)
	mustClose(db)
	return path
}

var _ = Describe("VacuumInto", func() {
	var (
		ctx = context.Background()
		src string
		dst string
	)

	BeforeEach(func() {
		src = seededSource()
		dst = filepath.Join(GinkgoT().TempDir(), "copy.db")
	})

	It("copies every table's rows, every column, and the sequence table, without writing the source", func() {
		before := fileSHA256(src)
		listing := dirListing(filepath.Dir(src), true)
		want := snapshot(src)

		Expect(VacuumInto(ctx, src, dst)).To(Succeed())

		got := snapshot(dst)
		Expect(slices.Sorted(maps.Keys(got))).To(Equal(slices.Sorted(maps.Keys(want))))
		for t, rows := range want {
			Expect(rows).NotTo(BeEmpty(), t)
			Expect(got[t]).To(Equal(rows), t)
		}
		Expect(fileSHA256(src)).To(Equal(before))
		// The read-only open of a WAL source leaves SQLite's -shm/-wal
		// beside it; nothing else may appear.
		Expect(dirListing(filepath.Dir(src), false)).To(Equal(listing))
		Expect(dirListing(filepath.Dir(dst), true)).To(Equal([]string{"copy.db"}), "the copy is a single file")
	})

	It("copies user_version as-is: a stamped source stays stamped, an unstamped one stays 0", func() {
		Expect(VacuumInto(ctx, src, dst)).To(Succeed())
		ro, err := OpenReadOnly(dst)
		Expect(err).NotTo(HaveOccurred())
		Expect(userVersion(ro)).To(Equal(Version))
		mustClose(ro)

		unstamped := tempDBPath()
		raw, err := sql.Open("sqlite", unstamped)
		Expect(err).NotTo(HaveOccurred())
		_, err = raw.Exec(ddl)
		Expect(err).NotTo(HaveOccurred())
		mustClose(raw)
		dst0 := filepath.Join(GinkgoT().TempDir(), "copy0.db")
		Expect(VacuumInto(ctx, unstamped, dst0)).To(Succeed())
		ro, err = OpenReadOnly(dst0)
		Expect(err).NotTo(HaveOccurred())
		defer mustClose(ro)
		Expect(userVersion(ro)).To(Equal(0))
	})

	It("refuses an existing destination, leaving it untouched", func() {
		Expect(os.WriteFile(dst, []byte("not a database"), 0o600)).To(Succeed())
		before := fileSHA256(dst)
		err := VacuumInto(ctx, src, dst)
		Expect(err).To(MatchError(fs.ErrExist))
		Expect(err.Error()).To(ContainSubstring(dst))
		Expect(fileSHA256(dst)).To(Equal(before))
	})

	It("refuses a missing source and creates nothing", func() {
		missing := filepath.Join(GinkgoT().TempDir(), "missing.db")
		Expect(VacuumInto(ctx, missing, dst)).To(MatchError(fs.ErrNotExist))
		Expect(dst).NotTo(BeAnExistingFile())
		Expect(missing).NotTo(BeAnExistingFile())
	})

	It("honours a cancelled context and leaves no destination behind", func() {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		Expect(VacuumInto(cancelled, src, dst)).To(MatchError(context.Canceled))
		Expect(dst).NotTo(BeAnExistingFile())
	})

	It("removes a partial destination when the statement fails", func() {
		// A source that is not a database: the copy statement fails
		// after the open, and the destination it may have created must
		// not survive.
		bogus := filepath.Join(GinkgoT().TempDir(), "bogus.db")
		Expect(os.WriteFile(bogus, []byte("not a database"), 0o600)).To(Succeed())
		Expect(VacuumInto(ctx, bogus, dst)).To(HaveOccurred())
		Expect(dst).NotTo(BeAnExistingFile())
	})
})

var _ = Describe("FinalizeShipped", func() {
	var ctx = context.Background()

	// expectShipped asserts the properties a shipped file carries.
	expectShipped := func(path string, want map[string][][]any) {
		GinkgoHelper()
		Expect(dirListing(filepath.Dir(path), true)).To(Equal([]string{filepath.Base(path)}), "no -wal/-shm beside the file")
		ro, err := OpenReadOnly(path)
		Expect(err).NotTo(HaveOccurred())
		defer mustClose(ro)
		Expect(journalMode(ro)).To(Equal("delete"))
		Expect(userVersion(ro)).To(Equal(Version))
		Expect(integrityCheck(ro)).To(Equal("ok"))
		for t, rows := range want {
			Expect(tableRows(ro, t)).To(Equal(rows), t)
		}
		Expect(dirListing(filepath.Dir(path), true)).To(Equal([]string{filepath.Base(path)}),
			"a read-only open of a rollback-journal file creates no side files")
	}

	It("stamps, switches a VacuumInto copy of an unstamped WAL source to delete mode, and keeps its content", func() {
		unstamped := tempDBPath()
		raw, err := sql.Open("sqlite", unstamped)
		Expect(err).NotTo(HaveOccurred())
		_, err = raw.Exec("PRAGMA journal_mode = WAL")
		Expect(err).NotTo(HaveOccurred())
		_, err = raw.Exec(ddl)
		Expect(err).NotTo(HaveOccurred())
		seedEveryTable(raw)
		mustClose(raw)
		want := snapshot(unstamped)

		copyPath := filepath.Join(GinkgoT().TempDir(), "national.db")
		Expect(VacuumInto(ctx, unstamped, copyPath)).To(Succeed())
		Expect(FinalizeShipped(ctx, copyPath)).To(Succeed())
		expectShipped(copyPath, want)
	})

	It("leaves no side files behind a database the writer built in WAL mode", func() {
		path := tempDBPath()
		db := mustOpen(path)
		seedEveryTable(db)
		mustClose(db)
		want := snapshot(path)

		Expect(FinalizeShipped(ctx, path)).To(Succeed())
		expectShipped(path, want)
	})

	It("is idempotent in content and properties on a second call", func() {
		path := seededSource()
		want := snapshot(path)
		Expect(FinalizeShipped(ctx, path)).To(Succeed())
		first := fileSHA256(path)
		Expect(FinalizeShipped(ctx, path)).To(Succeed())
		expectShipped(path, want)
		// Reported, not asserted: each call is a write transaction and
		// SQLite's header change counter moves with it.
		GinkgoWriter.Printf("FinalizeShipped twice: bytes identical = %t\n", fileSHA256(path) == first)
	})

	It("refuses a file another schema version stamped, through the writer's check", func() {
		path := tempDBPath()
		raw, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		_, err = raw.Exec(fmt.Sprintf("PRAGMA user_version = %d", Version+1))
		Expect(err).NotTo(HaveOccurred())
		mustClose(raw)
		before := fileSHA256(path)
		Expect(FinalizeShipped(ctx, path)).To(MatchError(ContainSubstring("migration required")))
		Expect(fileSHA256(path)).To(Equal(before))
	})

	It("honours a cancelled context before touching the file", func() {
		path := seededSource()
		before := fileSHA256(path)
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		Expect(FinalizeShipped(cancelled, path)).To(MatchError(context.Canceled))
		Expect(fileSHA256(path)).To(Equal(before))
	})
})
