package publish_test

// Specs for the filtered per-state {state}.db: every table's row set
// held, column for column, to the source filtered the spec's own way;
// the other states' rows, cells, and warnings absent; a cell two states
// share present once with all its rows; both run tables whole; the
// shipped file's properties (foreign keys resolvable in-file, integrity,
// the stamp, rollback journal, a sane sequence table); the source never
// written and no temp residue left on success or on failures injected
// mid-build; two builds identical in content; and the entry crossing the
// archive seam.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

var zacatecas = publish.State{Code: "ZAC", Slug: "zac", Name: "Zacatecas"}

// ddlTableNames is the DDL's table set — the state database must carry
// every one of them.
var ddlTableNames = []string{
	"stations", "monthly_normals", "monthly_normals_extras", "daily_observations", "parsing_warnings",
	"ingest_runs", "power_runs", "nasa_power_grid_cells", "station_power_cell",
	"monthly_supplement", "daily_supplement",
}

// seedStateDBSource writes the two-state fixture — stations, normals,
// extras, daily, cells, supplements, runs — to a fresh file through the
// writer, adds what the per-state scoping needs beyond it (a cell one
// Aguascalientes station shares with Yucatán; parsing warnings for
// stations of both states, for the EMA station, and with no station;
// Zacatecas rows), closes the writer so the main file is the whole
// content, and returns the path and the surrogate ids.
func seedStateDBSource() (path string, ids map[string]int64) {
	GinkgoHelper()
	path = filepath.Join(GinkgoT().TempDir(), "bioclima.db")
	db, err := schema.Open(path)
	Expect(err).NotTo(HaveOccurred())
	ids = seedTwoStates(db)
	seedPower(db, ids)
	seedRuns(db)
	// The database copy takes what the source holds; the gate is Run's
	// concern, so the rows it refuses ride along here.
	seedRunEdges(db)

	ids["conv/1002"] = upsertStation(db, ingest.StationUpsert{
		Source: ingest.SourceConaguaConventional, ExternalID: "1002", Name: "Calvillo", State: "AGS",
	})
	insertStationCell(db, ids["conv/1002"], cellShared, 40.0)
	insertNormals(db, ids["conv/32001"], "1981-2010", 3, 21.0, 6.0, 13.5, 9.5, 155.5)
	insertDaily(db, ids["conv/32001"], "2020-01-01", 18.0, 2.0, 0.0, 4.4)

	for _, w := range []struct {
		station any
		file    string
		line    any
		sev     string
		issue   string
	}{
		{ids["conv/31001"], "daily/31001.txt", 3, "warn", "unparseable value"},
		{ids["conv/31001"], "normals/31001.txt", nil, "error", "period mismatch"},
		{ids["conv/3101"], "daily/3101.txt", 10, "warn", "duplicate date"},
		{ids["conv/1001"], "daily/1001.txt", 1, "warn", "other state"},
		{ids["ema/31001"], "ema/31001.csv", 1, "warn", "EMA source"},
		{nil, "catalog.html", nil, "error", "no station"},
	} {
		mustExec(db, `INSERT INTO parsing_warnings (station_id, source_file, line, severity, issue)
		  VALUES (?, ?, ?, ?, ?)`, w.station, w.file, w.line, w.sev, w.issue)
	}
	Expect(db.Close()).To(Succeed())
	return path, ids
}

// buildStateDB renders the state's entry from a read-only open of the
// source — the handle publish holds — into memory, closing the handle
// afterwards, and returns the bytes and the entry path.
func buildStateDB(ctx context.Context, srcPath string, st publish.State, tmpDir string) (data []byte, entryPath string) {
	GinkgoHelper()
	ro, err := schema.OpenReadOnly(srcPath)
	Expect(err).NotTo(HaveOccurred())
	defer func() { Expect(ro.Close()).To(Succeed()) }()
	e := publish.StateDBEntry(ctx, ro, st, tmpDir)
	var buf bytes.Buffer
	Expect(e.Write(&buf)).To(Succeed())
	return buf.Bytes(), e.Path
}

// writeDB lands data at <fresh dir>/<name> and returns the path.
func writeDB(data []byte, name string) string {
	GinkgoHelper()
	path := filepath.Join(GinkgoT().TempDir(), name)
	Expect(os.WriteFile(path, data, 0o600)).To(Succeed())
	return path
}

func openRO(path string) *sql.DB {
	GinkgoHelper()
	db, err := schema.OpenReadOnly(path)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = db.Close() })
	return db
}

// selectAll reads every column of table's rows matching where, in a
// total order over all columns, as the driver's native values — the
// comparison that catches a dropped column, a swapped one, or a REAL
// that lost a bit.
func selectAll(db *sql.DB, table, where string, args ...any) [][]any {
	GinkgoHelper()
	var n int
	Expect(db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?)`, table).Scan(&n)).To(Succeed())
	Expect(n).To(BeNumerically(">", 0), table)
	order := make([]string, n)
	for i := range order {
		order[i] = fmt.Sprint(i + 1)
	}
	q := "SELECT * FROM " + table
	if where != "" {
		q += " WHERE " + where
	}
	rows, err := db.Query(q+" ORDER BY "+strings.Join(order, ", "), args...)
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

func count(db *sql.DB, query string, args ...any) int {
	GinkgoHelper()
	var n int
	Expect(db.QueryRow(query, args...).Scan(&n)).To(Succeed())
	return n
}

func pragmaText(db *sql.DB, pragma string) string {
	GinkgoHelper()
	var v string
	Expect(db.QueryRow("PRAGMA " + pragma).Scan(&v)).To(Succeed())
	return strings.ToLower(v)
}

func sha256Of(path string) string {
	GinkgoHelper()
	b, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

// placeholders renders n bound-parameter markers for an IN list.
func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

func anyValues(ids []int64) []any {
	out := make([]any, len(ids))
	for i, id := range ids {
		out[i] = id
	}
	return out
}

var _ = Describe("StateDBEntry", func() {
	var (
		ctx     = context.Background()
		srcPath string
		srcDir  string
		ids     map[string]int64
		tmpDir  string
		before  string
		listing []string
	)

	BeforeEach(func() {
		srcPath, ids = seedStateDBSource()
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

	Describe("Yucatán's database", func() {
		var (
			src  *sql.DB
			got  *sql.DB
			path string
			yuc  []int64
		)

		BeforeEach(func() {
			data, entryPath := buildStateDB(ctx, srcPath, yucatan, tmpDir)
			Expect(entryPath).To(Equal("yuc.db"))
			path = writeDB(data, "yuc.db")
			src = openRO(srcPath)
			got = openRO(path)
			yuc = []int64{ids["conv/31001"], ids["conv/31002"], ids["conv/3101"], ids["conv/31003"]}
		})

		It("carries the published row set of every table, every column equal to the source's", func() {
			inYuc := "station_id IN (" + placeholders(len(yuc)) + ")"
			inCells := "cell_id IN (?, ?)"
			cells := []any{cellShared, cellSingle}
			cases := []struct {
				table string
				where string
				args  []any
			}{
				{"stations", "id IN (" + placeholders(len(yuc)) + ")", anyValues(yuc)},
				{"monthly_normals", inYuc, anyValues(yuc)},
				{"monthly_normals_extras", inYuc, anyValues(yuc)},
				{"daily_observations", inYuc, anyValues(yuc)},
				{"parsing_warnings", inYuc, anyValues(yuc)},
				{"station_power_cell", inYuc, anyValues(yuc)},
				{"nasa_power_grid_cells", inCells, cells},
				{"monthly_supplement", inCells, cells},
				{"daily_supplement", inCells, cells},
				{"ingest_runs", "", nil},
				{"power_runs", "", nil},
			}
			var covered []string
			for _, c := range cases {
				want := selectAll(src, c.table, c.where, c.args...)
				Expect(want).NotTo(BeEmpty(), c.table)
				Expect(selectAll(got, c.table, "")).To(Equal(want), c.table)
				covered = append(covered, c.table)
			}
			Expect(covered).To(ConsistOf(ddlTableNames))
		})

		It("holds only the state's CONAGUA conventional stations and what hangs off them", func() {
			Expect(count(got, `SELECT COUNT(*) FROM stations WHERE state <> 'YUC' OR source <> ?`,
				string(ingest.SourceConaguaConventional))).To(BeZero())
			Expect(count(got, `SELECT COUNT(*) FROM stations`)).To(Equal(len(yuc)))
			for _, t := range []string{"monthly_normals", "monthly_normals_extras", "daily_observations",
				"station_power_cell", "parsing_warnings"} {
				Expect(count(got, `SELECT COUNT(*) FROM `+t+` WHERE station_id IS NULL OR station_id NOT IN (SELECT id FROM stations)`)).
					To(BeZero(), t)
			}
			Expect(count(got, `SELECT COUNT(*) FROM parsing_warnings`)).To(Equal(3), "31001 twice, 3101 once")
			for _, cell := range []string{cellAGS, cellEMA, cellNone} {
				Expect(count(got, `SELECT COUNT(*) FROM nasa_power_grid_cells WHERE cell_id = ?`, cell)).To(BeZero(), cell)
				Expect(count(got, `SELECT COUNT(*) FROM monthly_supplement WHERE cell_id = ?`, cell)).To(BeZero(), cell)
				Expect(count(got, `SELECT COUNT(*) FROM daily_supplement WHERE cell_id = ?`, cell)).To(BeZero(), cell)
			}
		})

		It("ships a cell shared with the other state once, with all its supplement rows", func() {
			Expect(count(src, `SELECT COUNT(DISTINCT s.state) FROM station_power_cell m JOIN stations s ON s.id = m.station_id WHERE m.cell_id = ?`,
				cellShared)).To(Equal(2), "the fixture shares the cell across YUC and AGS")
			Expect(count(got, `SELECT COUNT(*) FROM nasa_power_grid_cells WHERE cell_id = ?`, cellShared)).To(Equal(1))
			Expect(selectAll(got, "monthly_supplement", "cell_id = ?", cellShared)).
				To(Equal(selectAll(src, "monthly_supplement", "cell_id = ?", cellShared)))
			Expect(selectAll(got, "daily_supplement", "cell_id = ?", cellShared)).
				To(Equal(selectAll(src, "daily_supplement", "cell_id = ?", cellShared)))
			// The other state's mapping onto the shared cell stays out.
			Expect(count(got, `SELECT COUNT(*) FROM station_power_cell WHERE station_id = ?`, ids["conv/1002"])).To(BeZero())
		})

		It("is a shipped file: FKs resolvable, integrity ok, stamped, rollback journal, no side files", func() {
			rows, err := got.Query(`PRAGMA foreign_key_check`)
			Expect(err).NotTo(HaveOccurred())
			Expect(rows.Next()).To(BeFalse(), "a foreign key that does not resolve in-file")
			Expect(rows.Err()).NotTo(HaveOccurred())
			Expect(rows.Close()).To(Succeed())
			Expect(pragmaText(got, "integrity_check")).To(Equal("ok"))
			Expect(pragmaText(got, "journal_mode")).To(Equal("delete"))
			var version int
			Expect(got.QueryRow("PRAGMA user_version").Scan(&version)).To(Succeed())
			Expect(version).To(Equal(schema.Version))
			Expect(dirNames(filepath.Dir(path))).To(Equal([]string{"yuc.db"}))

			var tables []string
			trows, err := got.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
			Expect(err).NotTo(HaveOccurred())
			for trows.Next() {
				var n string
				Expect(trows.Scan(&n)).To(Succeed())
				tables = append(tables, n)
			}
			Expect(trows.Close()).To(Succeed())
			Expect(tables).To(ConsistOf(ddlTableNames))
			Expect(count(got, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_%'`)).To(Equal(8))
		})

		It("keeps the sequence table sane: a later insert in a writable copy gets a fresh id", func() {
			var maxID, seq int64
			Expect(got.QueryRow(`SELECT MAX(id) FROM stations`).Scan(&maxID)).To(Succeed())
			Expect(got.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name = 'stations'`).Scan(&seq)).To(Succeed())
			Expect(seq).To(Equal(maxID))

			data, err := os.ReadFile(path)
			Expect(err).NotTo(HaveOccurred())
			writable := writeDB(data, "writable.db")
			w, err := schema.Open(writable)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = w.Close() }()
			var newID int64
			Expect(w.QueryRow(`INSERT INTO stations (source, external_id, name) VALUES ('meta', 'x', 'X') RETURNING id`).
				Scan(&newID)).To(Succeed())
			Expect(newID).To(BeNumerically(">", maxID))
		})

		It("leaves the source untouched and no residue under the temp dir", func() {
			sourceUntouched()
			Expect(dirNames(tmpDir)).To(BeEmpty())
		})
	})

	It("ships a state whose stations reference no cell with the POWER tables empty and the runs whole", func() {
		data, _ := buildStateDB(ctx, srcPath, zacatecas, tmpDir)
		got := openRO(writeDB(data, "zac.db"))
		src := openRO(srcPath)
		Expect(selectAll(got, "stations", "")).To(Equal(selectAll(src, "stations", "id = ?", ids["conv/32001"])))
		Expect(selectAll(got, "monthly_normals", "")).To(Equal(selectAll(src, "monthly_normals", "station_id = ?", ids["conv/32001"])))
		Expect(selectAll(got, "daily_observations", "")).To(Equal(selectAll(src, "daily_observations", "station_id = ?", ids["conv/32001"])))
		for _, t := range []string{"station_power_cell", "nasa_power_grid_cells", "monthly_supplement", "daily_supplement",
			"parsing_warnings", "monthly_normals_extras"} {
			Expect(count(got, `SELECT COUNT(*) FROM `+t)).To(BeZero(), t)
		}
		Expect(selectAll(got, "ingest_runs", "")).To(Equal(selectAll(src, "ingest_runs", "")))
		Expect(selectAll(got, "power_runs", "")).To(Equal(selectAll(src, "power_runs", "")))
		Expect(pragmaText(got, "integrity_check")).To(Equal("ok"))
	})

	It("builds the same content twice", func() {
		first, _ := buildStateDB(ctx, srcPath, yucatan, tmpDir)
		second, _ := buildStateDB(ctx, srcPath, yucatan, tmpDir)
		a := openRO(writeDB(first, "a.db"))
		b := openRO(writeDB(second, "b.db"))
		for _, t := range ddlTableNames {
			Expect(selectAll(b, t, "")).To(Equal(selectAll(a, t, "")), t)
		}
		Expect(selectAll(b, "sqlite_sequence", "")).To(Equal(selectAll(a, "sqlite_sequence", "")))
		// Reported, not asserted: SQLite is content-reproducible only.
		GinkgoWriter.Printf("StateDBEntry twice: bytes identical = %t (%d bytes)\n", bytes.Equal(first, second), len(first))
		sourceUntouched()
	})

	Describe("failures", func() {
		var ro *sql.DB

		BeforeEach(func() {
			ro = openRO(srcPath)
		})

		// expectCleanFailure runs the entry, asserts it failed with err
		// matching m, wrote nothing to w, left no residue (a temp dir
		// never created counts as none), and never touched the source.
		expectCleanFailure := func(e archive.Entry, m types.GomegaMatcher) {
			GinkgoHelper()
			var buf bytes.Buffer
			err := e.Write(&buf)
			Expect(err).To(m)
			Expect(buf.Len()).To(BeZero(), "nothing is streamed before the build completes")
			if _, statErr := os.Stat(tmpDir); statErr == nil {
				Expect(dirNames(tmpDir)).To(BeEmpty())
			} else {
				Expect(statErr).To(MatchError(os.ErrNotExist))
			}
			sourceUntouched()
		}

		It("refuses a source with a column the schema lacks, after the build began", func() {
			Expect(ro.Close()).To(Succeed())
			w, err := schema.Open(srcPath)
			Expect(err).NotTo(HaveOccurred())
			mustExec(w, `ALTER TABLE stations ADD COLUMN extra TEXT`)
			Expect(w.Close()).To(Succeed())
			before = sha256Of(srcPath)
			listing = dirNames(srcDir)
			ro = openRO(srcPath)

			expectCleanFailure(publish.StateDBEntry(ctx, ro, yucatan, tmpDir),
				MatchError(ContainSubstring("source table stations has columns")))
		})

		It("refuses a source missing a table, after the build began", func() {
			Expect(ro.Close()).To(Succeed())
			w, err := schema.Open(srcPath)
			Expect(err).NotTo(HaveOccurred())
			mustExec(w, `DROP TABLE parsing_warnings`)
			Expect(w.Close()).To(Succeed())
			before = sha256Of(srcPath)
			listing = dirNames(srcDir)
			ro = openRO(srcPath)

			expectCleanFailure(publish.StateDBEntry(ctx, ro, yucatan, tmpDir),
				MatchError(ContainSubstring("src has no table parsing_warnings")))
		})

		It("refuses a station_power_cell reference with no nasa_power_grid_cells row, after the build began", func() {
			// The supplement tables are scoped through the copied grid
			// cells, so a dangling reference would silently drop its
			// cell's rows from the database while the CSV / Parquet
			// twins, scoped through the map, carry them.
			Expect(ro.Close()).To(Succeed())
			w, err := schema.Open(srcPath)
			Expect(err).NotTo(HaveOccurred())
			insertStationCell(w, ids["conv/31002"], "9.9N_9.9W", 1.0)
			Expect(w.Close()).To(Succeed())
			before = sha256Of(srcPath)
			listing = dirNames(srcDir)
			ro = openRO(srcPath)

			expectCleanFailure(publish.StateDBEntry(ctx, ro, yucatan, tmpDir),
				MatchError(`build state database: cell "9.9N_9.9W" has no nasa_power_grid_cells row`))
		})

		It("honours cancellation", func() {
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			expectCleanFailure(publish.StateDBEntry(cancelled, ro, yucatan, tmpDir), MatchError(context.Canceled))
		})

		It("refuses a connection with no database file", func() {
			mem, err := sql.Open("sqlite", ":memory:")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = mem.Close() }()
			expectCleanFailure(publish.StateDBEntry(ctx, mem, yucatan, tmpDir),
				MatchError("resolve source database path: the connection has no database file"))
		})
	})

	It("crosses the archive seam: the entry inside a zip is the database the entry renders", func() {
		ro := openRO(srcPath)
		e := publish.StateDBEntry(ctx, ro, yucatan, tmpDir)
		zipPath := filepath.Join(tmpDir, "yuc-tabular.zip")
		res, err := archive.WriteZip(ctx, zipPath, []archive.Entry{e}, archive.ZipOptions{Modified: time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Entries).To(Equal(1))
		names, contents := zipContents(zipPath)
		Expect(names).To(Equal([]string{"yuc.db"}))
		Expect(dirNames(tmpDir)).To(Equal([]string{"yuc-tabular.zip"}), "the build's temp dir is gone")

		got := openRO(writeDB(contents["yuc.db"], "yuc.db"))
		src := openRO(srcPath)
		Expect(pragmaText(got, "integrity_check")).To(Equal("ok"))
		yuc := []int64{ids["conv/31001"], ids["conv/31002"], ids["conv/3101"], ids["conv/31003"]}
		Expect(selectAll(got, "stations", "")).To(Equal(selectAll(src, "stations", "id IN ("+placeholders(len(yuc))+")", anyValues(yuc)...)))
		sourceUntouched()
	})
})
