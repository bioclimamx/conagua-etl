package publish_test

// Specs that hold the provenance/ CSVs to the DB column by column
// through the LoadRuns → ProvenanceEntries seam: every DDL-backed cell
// of every row equals the value read back from its power_runs /
// ingest_runs row (integers as digits, NULL as the empty field), on
// rows seeded so that no two columns share a value; the row order is
// the natural key's, never the surrogate id's; and a stored
// unit_conversions text carrying commas, quotes, and newlines is
// RFC-4180 quoted so a reader gets the stored bytes back exactly.

import (
	"context"
	"database/sql"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// prettyConversions is a unit_conversions text in the indented shape a
// hand-edited or future writer might store: embedded newlines, commas,
// and quotes — every character RFC 4180 has to fence.
const prettyConversions = "{\n  \"T2M\": {\"power_unit\": \"C\", \"stored_unit\": \"C\", \"factor\": 1},\n" +
	"  \"WS2M\": {\"power_unit\": \"m/s\", \"stored_unit\": \"m/s\", \"factor\": 1}\n}"

// seedDistinctRuns seeds provenance rows whose every exported column
// holds a distinct value, inserted so that surrogate-id order disagrees
// with natural-key order. Returns the power run ids by label.
func seedDistinctRuns(db *sql.DB) map[string]int64 {
	GinkgoHelper()
	insertIngestRun(db, ingestRunSeed{
		startedAt: "2026-06-09T01:00:00Z", finishedAt: str("2026-06-09T02:00:00Z"),
		snapshotDate: "2026-06-08", sinkKind: "local", gitSHA: str("sha-jun"), status: "complete",
		counters: []*int64{i64(21), i64(22), i64(23), i64(24), i64(25), i64(26), i64(27)},
	})
	// Shadowed by the later complete run of the same snapshot.
	insertIngestRun(db, ingestRunSeed{
		startedAt: "2026-01-16T01:00:00Z", finishedAt: str("2026-01-16T02:00:00Z"),
		snapshotDate: "2026-01-15", sinkKind: "r2", gitSHA: str("sha-jan-old"), status: "complete",
		counters: []*int64{i64(1), i64(2), i64(3), i64(4), i64(5), i64(6), i64(7)},
	})
	insertIngestRun(db, ingestRunSeed{
		startedAt: "2026-01-17T01:00:00Z", finishedAt: str("2026-01-17T02:00:00Z"),
		snapshotDate: "2026-01-15", sinkKind: "s3", gitSHA: str("sha-jan"), status: "complete",
		counters: []*int64{i64(11), i64(12), i64(13), i64(14), i64(15), i64(16), i64(17)},
	})
	insertIngestRun(db, ingestRunSeed{
		startedAt: "2025-12-31T23:00:00Z", finishedAt: nil,
		snapshotDate: "2025-12-31", sinkKind: "local", gitSHA: nil, status: "complete",
		counters: make([]*int64, 7),
	})

	ids := map[string]int64{}
	ids["power-monthly-1991-2020"] = insertPowerRun(db, powerRunSeed{
		startedAt: "2026-07-03T00:00:00Z", finishedAt: str("2026-07-03T06:00:00Z"), status: "complete",
		endpoint: "https://power.larc.nasa.gov/api/temporal/monthly/point?a=1", parameters: "T2M,WS2M",
		community: "RE", startYear: 1991, endYear: 2020, gridResolution: "0.5x0.625",
		unitConversions: str(prettyConversions), temporalMode: "monthly", gitSHA: str("sha-m91"),
		counters: []*int64{i64(31), i64(32), i64(33), i64(34)},
	})
	ids["power-monthly-1981-2010"] = insertPowerRun(db, powerRunSeed{
		startedAt: "2026-07-02T00:00:00Z", finishedAt: str("2026-07-02T06:00:00Z"), status: "complete",
		endpoint: "https://power.larc.nasa.gov/api/temporal/monthly/point", parameters: "T2M,ALLSKY_SFC_SW_DWN",
		community: "AG", startYear: 1981, endYear: 2010, gridResolution: "0.5x0.625",
		unitConversions: str(conversionsText), temporalMode: "monthly", gitSHA: str("sha-m81"),
		counters: []*int64{i64(41), i64(42), i64(43), i64(44)},
	})
	ids["power-daily-1985-2019"] = insertPowerRun(db, powerRunSeed{
		startedAt: "2026-07-01T00:00:00Z", finishedAt: nil, status: "complete",
		endpoint: "https://power.larc.nasa.gov/api/temporal/daily/point", parameters: "T2M",
		community: "SB", startYear: 1985, endYear: 2019, gridResolution: "0.5x0.625",
		unitConversions: nil, temporalMode: "daily",
		startDate: str("1985-03-01"), endDate: str("2019-11-30"), gitSHA: nil,
		counters: []*int64{i64(51), nil, i64(53), i64(54)},
	})
	// Unreferenced: not provenance, whatever its label.
	insertPowerRun(db, powerRunSeed{
		startedAt: "2026-07-04T00:00:00Z", status: "aborted",
		endpoint: "u", parameters: "T2M", community: "AG", startYear: 1981, endYear: 2010,
		gridResolution: "0.5x0.625", temporalMode: "monthly", counters: make([]*int64, 4),
	})

	insertCell(db, "n21.75_w89.375", 21.75, -89.375)
	insertMonthlySupplement(db, "n21.75_w89.375", "1991-2020", 1, ids["power-monthly-1991-2020"])
	insertMonthlySupplement(db, "n21.75_w89.375", "1981-2010", 1, ids["power-monthly-1981-2010"])
	insertDailySupplement(db, "n21.75_w89.375", "2000-01-01", ids["power-daily-1985-2019"])
	return ids
}

// dbCell reads one column of one row back as the CSV must render it:
// the stored text, an integer's digits, or the empty field for NULL.
// table and column come from the FileSpec under test, never from input.
func dbCell(db *sql.DB, table, col string, id int64) string {
	GinkgoHelper()
	var v sql.NullString
	Expect(db.QueryRowContext(context.Background(), "SELECT "+col+" FROM "+table+" WHERE id = ?", id).Scan(&v)).To(Succeed())
	if !v.Valid {
		return ""
	}
	return v.String
}

var _ = Describe("provenance/ against the DB, column by column", func() {
	var (
		db      *sql.DB
		ids     map[string]int64
		entries []archive.Entry
	)

	BeforeEach(func() {
		db = openTempDB()
		ids = seedDistinctRuns(db)
		runs, err := publish.LoadRuns(context.Background(), db)
		Expect(err).NotTo(HaveOccurred())
		entries = publish.ProvenanceEntries(runs)
	})

	It("orders ingest_runs by snapshot_date and power_runs by run_label, never by surrogate id", func() {
		ingestRows := records(render(entryByPath(entries, "provenance/ingest_runs.csv")))
		Expect(column(ingestRows, 0)).To(Equal([]string{"2025-12-31", "2026-01-15", "2026-06-08"}))
		// The later complete run of the shadowed snapshot is the one
		// exported.
		Expect(column(ingestRows, 4)).To(Equal([]string{"", "sha-jan", "sha-jun"}))

		powerRows := records(render(entryByPath(entries, "provenance/power_runs.csv")))
		Expect(column(powerRows, 0)).To(Equal([]string{
			"power-daily-1985-2019", "power-monthly-1981-2010", "power-monthly-1991-2020",
		}))
		Expect(ids["power-monthly-1991-2020"]).To(BeNumerically("<", ids["power-monthly-1981-2010"]))
		Expect(ids["power-monthly-1981-2010"]).To(BeNumerically("<", ids["power-daily-1985-2019"]))
	})

	It("renders every DDL-backed power_runs column of every row as the DB holds it", func() {
		rows := records(render(entryByPath(entries, "provenance/power_runs.csv")))
		Expect(rows[0]).To(Equal(publish.ProvenancePowerRuns.Header()))
		Expect(rows).To(HaveLen(1 + len(ids)))
		for _, row := range rows[1:] {
			id, ok := ids[row[0]]
			Expect(ok).To(BeTrue(), row[0])
			for i, c := range publish.ProvenancePowerRuns.Columns {
				if c.DB == "" {
					continue
				}
				Expect(row[i]).To(Equal(dbCell(db, publish.ProvenancePowerRuns.Table, c.DB, id)), "%s: column %s", row[0], c.Name)
			}
		}
		// The distinct seed makes the check bite: no two populated cells
		// of the monthly row agree (its two date bounds are NULL, as a
		// monthly run's are).
		seen := map[string]bool{}
		for _, cell := range rows[3] {
			if cell == "" {
				continue
			}
			Expect(seen).NotTo(HaveKey(cell), "duplicate cell %q on %s", cell, rows[3][0])
			seen[cell] = true
		}
	})

	It("renders every DDL-backed ingest_runs column of every row as the DB holds it", func() {
		rows := records(render(entryByPath(entries, "provenance/ingest_runs.csv")))
		Expect(rows[0]).To(Equal(publish.ProvenanceIngestRuns.Header()))
		Expect(rows).To(HaveLen(4))
		for _, row := range rows[1:] {
			var id int64
			Expect(db.QueryRowContext(context.Background(), `SELECT id FROM ingest_runs
			  WHERE snapshot_date = ? AND status = 'complete' ORDER BY started_at DESC, id DESC LIMIT 1`, row[0]).
				Scan(&id)).To(Succeed())
			for i, c := range publish.ProvenanceIngestRuns.Columns {
				Expect(row[i]).To(Equal(dbCell(db, publish.ProvenanceIngestRuns.Table, c.DB, id)), "%s: column %s", row[0], c.Name)
			}
		}
		seen := map[string]bool{}
		for _, cell := range rows[2] {
			Expect(seen).NotTo(HaveKey(cell), "duplicate cell %q on %s", cell, rows[2][0])
			seen[cell] = true
		}
	})

	It("quotes unit_conversions per RFC 4180 — commas, quotes, and newlines inside — so a reader gets the stored bytes back exactly", func() {
		data := string(render(entryByPath(entries, "provenance/power_runs.csv")))
		rows := records([]byte(data))
		col := 10
		Expect(publish.ProvenancePowerRuns.Columns[col].Name).To(Equal("unit_conversions"))

		var stored sql.NullString
		Expect(db.QueryRowContext(context.Background(), `SELECT unit_conversions FROM power_runs WHERE id = ?`,
			ids["power-monthly-1991-2020"]).Scan(&stored)).To(Succeed())
		Expect(stored.String).To(Equal(prettyConversions))
		Expect(rows[3][col]).To(Equal(stored.String))
		Expect(rows[2][col]).To(Equal(conversionsText))
		Expect(rows[1][col]).To(BeEmpty())

		// On the wire: the field is fenced in quotes with the inner
		// quotes doubled, the newlines inside it are the text's own, and
		// every record still ends in a bare LF with no CR anywhere.
		Expect(data).To(ContainSubstring(`,"` + strings.ReplaceAll(prettyConversions, `"`, `""`) + `",monthly,`))
		Expect(data).To(ContainSubstring(`,"` + strings.ReplaceAll(conversionsText, `"`, `""`) + `",monthly,`))
		Expect(data).NotTo(ContainSubstring("\r"))
		Expect(data).To(HaveSuffix("\n"))
		Expect(strings.Count(data, "\n")).To(Equal(len(rows) + strings.Count(prettyConversions, "\n")))
	})
})
