package schema

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

// stationValues carries every stations column (except the surrogate id)
// with one field per column, so an insert-then-select comparison catches
// column-order drift that a count or partial scan would hide.
type stationValues struct {
	source, externalID, name, state, municipality string
	lat, lon, altitudeM                           float64
	status                                        string
	firstYear, lastYear                           int
	bin6190, bin7100, bin8110, bin9120            float64
	cont6190, cont7100, cont8110, cont9120        float64
}

// probeStation populates every column with a distinct value.
var probeStation = stationValues{
	source: "conagua_conventional", externalID: "1001",
	name: "AGUASCALIENTES (OBS)", state: "AGS", municipality: "Aguascalientes",
	lat: 21.88, lon: -102.3, altitudeM: 1877.5,
	status: "operating", firstYear: 1947, lastYear: 2016,
	bin6190: 0.1, bin7100: 0.2, bin8110: 0.3, bin9120: 0.4,
	cont6190: 0.5, cont7100: 0.6, cont8110: 0.7, cont9120: 0.8,
}

func insertStation(db *sql.DB, v stationValues) {
	GinkgoHelper()
	_, err := db.Exec(`INSERT INTO stations
		(source, external_id, name, state, municipality, lat, lon, altitude_m,
		 status, first_year, last_year,
		 wmo_completeness_bin_1961_1990, wmo_completeness_bin_1971_2000,
		 wmo_completeness_bin_1981_2010, wmo_completeness_bin_1991_2020,
		 wmo_completeness_cont_1961_1990, wmo_completeness_cont_1971_2000,
		 wmo_completeness_cont_1981_2010, wmo_completeness_cont_1991_2020)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		v.source, v.externalID, v.name, v.state, v.municipality,
		v.lat, v.lon, v.altitudeM, v.status, v.firstYear, v.lastYear,
		v.bin6190, v.bin7100, v.bin8110, v.bin9120,
		v.cont6190, v.cont7100, v.cont8110, v.cont9120)
	Expect(err).NotTo(HaveOccurred())
}

func selectStation(db *sql.DB, source, externalID string) stationValues {
	GinkgoHelper()
	var v stationValues
	err := db.QueryRow(`SELECT
		source, external_id, name, state, municipality, lat, lon, altitude_m,
		status, first_year, last_year,
		wmo_completeness_bin_1961_1990, wmo_completeness_bin_1971_2000,
		wmo_completeness_bin_1981_2010, wmo_completeness_bin_1991_2020,
		wmo_completeness_cont_1961_1990, wmo_completeness_cont_1971_2000,
		wmo_completeness_cont_1981_2010, wmo_completeness_cont_1991_2020
		FROM stations WHERE source = ? AND external_id = ?`,
		source, externalID).Scan(
		&v.source, &v.externalID, &v.name, &v.state, &v.municipality,
		&v.lat, &v.lon, &v.altitudeM, &v.status, &v.firstYear, &v.lastYear,
		&v.bin6190, &v.bin7100, &v.bin8110, &v.bin9120,
		&v.cont6190, &v.cont7100, &v.cont8110, &v.cont9120)
	Expect(err).NotTo(HaveOccurred())
	return v
}

func fileSHA256(path string) string {
	GinkgoHelper()
	b, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

// normalsPeriods derives the DB period vocabulary from its Go owner,
// conagua.NormalsKinds — the specs below hold the DDL to this set.
func normalsPeriods() []string {
	GinkgoHelper()
	periods := make([]string, 0, len(conagua.NormalsKinds))
	for _, k := range conagua.NormalsKinds {
		p, ok := conagua.PeriodForKind(k)
		Expect(ok).To(BeTrue(), "NormalsKinds entry %q must map to a period", k)
		periods = append(periods, p)
	}
	return periods
}

var _ = Describe("Version", func() {
	It("parses 1 from the schema_version header", func() {
		Expect(Version).To(Equal(1))
	})
})

var _ = Describe("Open", func() {
	It("creates a fresh database with every table and index applied", func() {
		db := mustOpen(tempDBPath())
		defer mustClose(db)

		queryNames := func(kind string) []string {
			rows, err := db.Query(
				`SELECT name FROM sqlite_master
				 WHERE type = ? AND name NOT LIKE 'sqlite_%' ORDER BY name`, kind)
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

		Expect(queryNames("table")).To(ConsistOf(
			"stations", "monthly_normals", "monthly_normals_extras",
			"daily_observations", "parsing_warnings", "ingest_runs",
			"power_runs", "nasa_power_grid_cells", "station_power_cell",
			"monthly_supplement", "daily_supplement"))
		Expect(queryNames("index")).To(ConsistOf(
			"idx_daily_station_date", "idx_monthly_station_period",
			"idx_extras_station_period", "idx_warnings_severity",
			"idx_stations_source", "idx_supplement_cell_period",
			"idx_station_power_cell_cell", "idx_daily_supplement_date"))
	})

	It("configures the writer PRAGMAs", func() {
		db := mustOpen(tempDBPath())
		defer mustClose(db)

		var journal string
		Expect(db.QueryRow("PRAGMA journal_mode").Scan(&journal)).To(Succeed())
		Expect(strings.ToLower(journal)).To(Equal("wal"))

		for pragma, want := range map[string]int{
			"synchronous":  1, // NORMAL
			"temp_store":   2, // MEMORY
			"cache_size":   -200000,
			"foreign_keys": 0,
		} {
			var got int
			Expect(db.QueryRow("PRAGMA " + pragma).Scan(&got)).To(Succeed())
			Expect(got).To(Equal(want), "PRAGMA %s", pragma)
		}
	})

	It("preserves existing rows across reopen, round-tripping every stations column", func() {
		path := tempDBPath()
		db := mustOpen(path)
		insertStation(db, probeStation)
		mustClose(db)

		db = mustOpen(path)
		defer mustClose(db)
		Expect(selectStation(db, probeStation.source, probeStation.externalID)).
			To(Equal(probeStation))
	})
})

var _ = Describe("CHECK constraints", func() {
	var db *sql.DB

	BeforeEach(func() {
		db = mustOpen(tempDBPath())
		DeferCleanup(func() { mustClose(db) })
	})

	expectCheckViolation := func(err error) {
		GinkgoHelper()
		Expect(err).To(MatchError(ContainSubstring("CHECK")))
	}

	It("rejects an unknown stations.source slug", func() {
		_, err := db.Exec(
			`INSERT INTO stations (source, external_id, name) VALUES (?, ?, ?)`,
			"conagua_typo", "1001", "X")
		expectCheckViolation(err)
	})

	It("accepts exactly the NormalsKinds periods in monthly_normals.period", func() {
		for i, period := range normalsPeriods() {
			_, err := db.Exec(
				`INSERT INTO monthly_normals (station_id, period, month) VALUES (1, ?, ?)`,
				period, i+1)
			Expect(err).NotTo(HaveOccurred(), "period %q", period)
		}
		_, err := db.Exec(
			`INSERT INTO monthly_normals (station_id, period, month) VALUES (1, '1951-1980', 5)`)
		expectCheckViolation(err)
	})

	It("rejects monthly_normals.month outside 1..12", func() {
		for _, month := range []int{1, 12} {
			_, err := db.Exec(
				`INSERT INTO monthly_normals (station_id, period, month) VALUES (1, '1991-2020', ?)`,
				month)
			Expect(err).NotTo(HaveOccurred(), "month %d", month)
		}
		for _, month := range []int{0, 13} {
			_, err := db.Exec(
				`INSERT INTO monthly_normals (station_id, period, month) VALUES (1, '1991-2020', ?)`,
				month)
			expectCheckViolation(err)
		}
	})

	It("rejects a parsing_warnings.severity outside warn/error", func() {
		for _, severity := range []string{"warn", "error"} {
			_, err := db.Exec(
				`INSERT INTO parsing_warnings (severity, issue) VALUES (?, 'x')`, severity)
			Expect(err).NotTo(HaveOccurred(), "severity %q", severity)
		}
		_, err := db.Exec(`INSERT INTO parsing_warnings (severity, issue) VALUES ('fatal', 'x')`)
		expectCheckViolation(err)
	})

	It("rejects an unknown power_runs.status and defaults temporal_mode to monthly", func() {
		insertRun := func(status string) error {
			_, err := db.Exec(
				`INSERT INTO power_runs
				 (started_at, status, endpoint_url, parameters, community,
				  period_start_year, period_end_year, grid_resolution, solar_conversion)
				 VALUES ('2026-07-18T00:00:00Z', ?, 'https://power.example', 'T2M', 'RE',
				         1991, 2020, '0.5x0.625', 11.574)`, status)
			return err
		}
		for _, status := range []string{"running", "complete", "aborted"} {
			Expect(insertRun(status)).To(Succeed(), "status %q", status)
		}
		expectCheckViolation(insertRun("paused"))

		// The DEFAULT clause is load-bearing: an INSERT that omits
		// temporal_mode must land as a monthly run.
		var mode string
		Expect(db.QueryRow(
			`SELECT temporal_mode FROM power_runs LIMIT 1`).Scan(&mode)).To(Succeed())
		Expect(mode).To(Equal("monthly"))
	})

	It("rejects an unknown nasa_power_grid_cells.grid_resolution", func() {
		_, err := db.Exec(
			`INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution)
			 VALUES ('c1', 19.0, -99.0, '0.5x0.625')`)
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(
			`INSERT INTO nasa_power_grid_cells (cell_id, lat, lon, grid_resolution)
			 VALUES ('c2', 19.0, -99.0, '1.0x1.0')`)
		expectCheckViolation(err)
	})

	It("enforces the monthly_supplement.period CHECK", func() {
		_, err := db.Exec(
			`INSERT INTO monthly_supplement (cell_id, period, month) VALUES ('c1', '1981-2010', 1)`)
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(
			`INSERT INTO monthly_supplement (cell_id, period, month) VALUES ('c1', '1951-1980', 2)`)
		expectCheckViolation(err)
	})
})

var _ = Describe("OpenReadOnly", func() {
	It("errors at open time on a nonexistent path", func() {
		db, err := OpenReadOnly(filepath.Join(GinkgoT().TempDir(), "missing.db"))
		Expect(err).To(MatchError(os.ErrNotExist))
		Expect(err.Error()).To(ContainSubstring("missing.db"))
		Expect(db).To(BeNil())
	})

	It("serves reads, refuses writes, and leaves the file bytes untouched", func() {
		path := tempDBPath()
		w := mustOpen(path)
		insertStation(w, probeStation)
		mustClose(w) // clean close checkpoints WAL, so the main file is complete

		before := fileSHA256(path)

		db, err := OpenReadOnly(path)
		Expect(err).NotTo(HaveOccurred())
		defer mustClose(db)

		var queryOnly int
		Expect(db.QueryRow("PRAGMA query_only").Scan(&queryOnly)).To(Succeed())
		Expect(queryOnly).To(Equal(1))

		Expect(selectStation(db, probeStation.source, probeStation.externalID)).
			To(Equal(probeStation))

		_, err = db.Exec(
			`INSERT INTO stations (source, external_id, name) VALUES ('nasa_power', '9', 'X')`)
		Expect(err).To(HaveOccurred())
		_, err = db.Exec(`UPDATE stations SET name = 'MUTATED'`)
		Expect(err).To(HaveOccurred())

		Expect(fileSHA256(path)).To(Equal(before))
	})
})

var _ = Describe("DDL lockstep with the conagua vocabulary", func() {
	It("enumerates exactly the NormalsKinds periods in every period CHECK", func() {
		re := regexp.MustCompile(`CHECK \(period IN \(([^)]+)\)\)`)
		matches := re.FindAllStringSubmatch(ddl, -1)
		Expect(matches).To(HaveLen(3),
			"monthly_normals, monthly_normals_extras, and monthly_supplement each carry a period CHECK")

		want := normalsPeriods()
		for _, m := range matches {
			var got []string
			for lit := range strings.SplitSeq(m[1], ",") {
				got = append(got, strings.Trim(strings.TrimSpace(lit), "'"))
			}
			Expect(got).To(Equal(want))
		}
	})

	It("embeds a NormalsKinds period in each of the 8 wmo_completeness_* columns", func() {
		underscored := make(map[string]bool)
		for _, p := range normalsPeriods() {
			underscored[strings.ReplaceAll(p, "-", "_")] = true
		}

		re := regexp.MustCompile(`wmo_completeness_(bin|cont)_(\d{4}_\d{4})`)
		matches := re.FindAllStringSubmatch(ddl, -1)
		Expect(matches).To(HaveLen(8))

		seen := make(map[string]bool)
		for _, m := range matches {
			Expect(underscored).To(HaveKey(m[2]),
				"wmo_completeness_%s_%s must embed a NormalsKinds period", m[1], m[2])
			seen[m[1]+"_"+m[2]] = true
		}
		// Both scoring systems cover all four periods, no duplicates.
		Expect(seen).To(HaveLen(8))
	})
})

// userVersion reads the header stamp back through whichever handle the
// spec holds, writer or read-only.
func userVersion(db *sql.DB) int {
	GinkgoHelper()
	var v int
	Expect(db.QueryRow("PRAGMA user_version").Scan(&v)).To(Succeed())
	return v
}

var _ = Describe("PRAGMA user_version", func() {
	It("stamps Version into a fresh database", func() {
		db := mustOpen(tempDBPath())
		defer mustClose(db)
		Expect(userVersion(db)).To(Equal(Version))
	})

	It("keeps Version across a second writer open without rewriting the file", func() {
		path := tempDBPath()
		mustClose(mustOpen(path))
		before := fileSHA256(path)

		db := mustOpen(path)
		Expect(userVersion(db)).To(Equal(Version))
		mustClose(db)
		Expect(fileSHA256(path)).To(Equal(before))
	})

	It("reads back through OpenReadOnly, which never writes", func() {
		path := tempDBPath()
		mustClose(mustOpen(path))
		before := fileSHA256(path)

		db, err := OpenReadOnly(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(userVersion(db)).To(Equal(Version))
		mustClose(db)
		Expect(fileSHA256(path)).To(Equal(before))
	})

	It("stamps a database created before the stamp existed on its next writer open", func() {
		// Simulates the production DB: DDL applied by a raw connection,
		// header left at SQLite's default user_version of 0.
		path := tempDBPath()
		raw, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		_, err = raw.Exec(ddl)
		Expect(err).NotTo(HaveOccurred())
		Expect(userVersion(raw)).To(Equal(0))
		mustClose(raw)

		ro, err := OpenReadOnly(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(userVersion(ro)).To(Equal(0), "a read-only open must not stamp")
		mustClose(ro)

		db := mustOpen(path)
		defer mustClose(db)
		Expect(userVersion(db)).To(Equal(Version))
	})

	It("refuses a database stamped by a different schema version, leaving its bytes untouched", func() {
		// A file another schema version wrote: header stamped, no
		// tables of ours — Open must not apply this DDL to it or relabel it.
		path := tempDBPath()
		raw, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		_, err = raw.Exec(fmt.Sprintf("PRAGMA user_version = %d", Version+1))
		Expect(err).NotTo(HaveOccurred())
		_, err = raw.Exec(`CREATE TABLE foreign_shape (id INTEGER PRIMARY KEY)`)
		Expect(err).NotTo(HaveOccurred())
		mustClose(raw)
		before := fileSHA256(path)

		db, err := Open(path)
		Expect(err).To(MatchError(fmt.Sprintf(
			"user_version %d does not match schema version %d (migration required)", Version+1, Version)))
		Expect(db).To(BeNil())
		Expect(fileSHA256(path)).To(Equal(before))

		ro, err := OpenReadOnly(path)
		Expect(err).NotTo(HaveOccurred())
		defer mustClose(ro)
		Expect(userVersion(ro)).To(Equal(Version + 1))
		var stations int
		Expect(ro.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'stations'`).
			Scan(&stations)).To(Succeed())
		Expect(stations).To(BeZero(), "the DDL must not have been applied")
	})
})
