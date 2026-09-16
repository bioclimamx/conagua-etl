package cmd_test

// The gate and the docs group on the real binary: every publish run
// evaluates the read-only QA gate right after the snapshot resolves,
// one stderr line per rule in execution order, before anything is
// written. The seeded DB passes — its two stations without coordinates
// are bbox warnings, never blocking — and the docs group writes
// QA-REPORT.md: listed in manifest.json and CHECKSUMS, byte-identical
// across two runs and across group selections, every number in it
// equal to plain SQL over the same DB, no timestamp. A corrupted copy
// of the seed — an impossible coordinate, a power run left running, a
// POWER fill value stored — exits 1 with the findings on stderr, the
// rule and the station named, nothing under --out (no file, no temp
// directory), the gate lines still one per rule with the failing rule
// marked FAIL. The DB's bytes are unchanged by a run that passed and by
// one that was refused.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

var (
	publishGateLineRE = regexp.MustCompile(`(?m)^\[\d{2}:\d{2}:\d{2}\] gate (ok|FAIL) +(\S+) · scanned=(\d+) warn=(\d+) error=(\d+) · elapsed \S+$`)
	publishFileLineRE = regexp.MustCompile(`(?m)^\[\d{2}:\d{2}:\d{2}\] (ok|FAIL) +(\S+) · bytes=(\d+) sha256=(\S+) · elapsed \S+(?: · (.*))?$`)
)

// gateLine is one expected gate line: the rule and its counts.
type gateLine struct {
	rule                string
	scanned, warn, errs int
}

// wantSeededGate is the gate over seedPublishDB, written by hand from
// the seed in the gate's execution order: seven stations, two of them
// (31019, 1010) without coordinates — warnings; no run in flight; two
// ingest runs, one complete; the station references — 8 daily, 18
// normals, 5 extras, 4 cell links, 1 warning — every one resolvable; the
// cell references — 4 monthly, 4 daily, 4 links; the 8 supplement rows
// each with a run; the three referenced runs under distinct labels; no
// fill value, every wind direction in range.
var wantSeededGate = []gateLine{
	{"bbox", 7, 2, 0},
	{"runs-in-flight", 0, 0, 0},
	{"ingest-complete", 2, 0, 0},
	{"station-refs", 36, 0, 0},
	{"cell-refs", 12, 0, 0},
	{"run-refs", 8, 0, 0},
	{"run-label-unique", 3, 0, 0},
	{"fill-leak", 8, 0, 0},
	{"wind-range", 8, 0, 0},
}

// expectGateLines holds stderr's gate lines to want, in order: the
// rule, its counts, and ok or FAIL by its error count.
func expectGateLines(stderr string, want []gateLine) {
	lines := publishGateLineRE.FindAllStringSubmatch(stderr, -1)
	ExpectWithOffset(1, lines).To(HaveLen(len(want)))
	for i, w := range want {
		status := "ok"
		if w.errs > 0 {
			status = "FAIL"
		}
		ExpectWithOffset(1, lines[i][1:]).To(Equal([]string{
			status, w.rule, strconv.Itoa(w.scanned), strconv.Itoa(w.warn), strconv.Itoa(w.errs),
		}), w.rule)
	}
}

// withGateErrors is want with one rule's counts replaced.
func withGateErrors(want []gateLine, rule string, scanned, warn, errs int) []gateLine {
	out := make([]gateLine, len(want))
	copy(out, want)
	for i := range out {
		if out[i].rule == rule {
			out[i] = gateLine{rule, scanned, warn, errs}
		}
	}
	return out
}

// corruptCopy takes a VACUUM INTO copy of the seeded DB — the
// throwaway the corruption goes into, the seed itself untouched — and
// applies the statements through the writer.
func corruptCopy(seeded string, statements ...string) string {
	dir := GinkgoT().TempDir()
	path := filepath.Join(dir, "corrupt.db")
	ExpectWithOffset(1, schema.VacuumInto(context.Background(), seeded, path)).To(Succeed())
	db, err := schema.Open(path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	for _, s := range statements {
		_, err := db.Exec(s)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), s)
	}
	ExpectWithOffset(1, db.Close()).To(Succeed())
	return path
}

// openRO opens a DB through the read-only opener for the spec's own SQL
// and schedules its close.
func openRO(path string) *sql.DB {
	db, err := schema.OpenReadOnly(path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	DeferCleanup(func() error { return db.Close() })
	return db
}

// mdRow renders cells as the report's Markdown table row.
func mdRow(cells ...string) string {
	return "| " + strings.Join(cells, " | ") + " |\n"
}

// pctText is the report's one-decimal share of nonNull over rows.
func pctText(nonNull, rows int) string {
	return strconv.FormatFloat(float64(nonNull)*100/float64(rows), 'f', 1, 64)
}

var _ = Describe("conagua-etl publish behind the gate", func() {
	var dbPath, dbSHA string

	BeforeEach(func() {
		dbPath = seedPublishDB()
		dbSHA = fileSHA(dbPath)
	})

	It("passes the seeded DB — one gate line per rule, warnings never blocking — and writes QA-REPORT.md among the docs group under --only docs: listed in the manifest and CHECKSUMS, every number plain SQL's, byte-identical across runs and group selections", func() {
		out := filepath.Join(GinkgoT().TempDir(), "docs")
		session := runPublishToExit(dbPath, out, "--state", "yuc", "--only", "docs")
		Expect(session.ExitCode()).To(Equal(0))
		Expect(fileSHA(dbPath)).To(Equal(dbSHA))

		stderr := string(session.Err.Contents())
		expectGateLines(stderr, wantSeededGate)
		Expect(publishUnitLineRE.FindAllStringSubmatch(stderr, -1)).To(BeEmpty())
		Expect(publishArtifactLineRE.FindAllStringSubmatch(stderr, -1)).To(BeEmpty())
		files := publishFileLineRE.FindAllStringSubmatch(stderr, -1)
		Expect(files).To(HaveLen(len(wantDocsOrder)))
		qaPath := filepath.Join(out, "QA-REPORT.md")
		Expect(files[6][1:5]).To(Equal([]string{"ok", "QA-REPORT.md", strconv.FormatInt(sizeOf(qaPath), 10), fileSHA(qaPath)[:12]}))

		stdout := string(session.Out.Contents())
		Expect(stdout).To(ContainSubstring("\npublish complete\n"))
		Expect(stdout).To(ContainSubstring("  gate                 : passed · 9 rules · 2 warn · 0 error\n"))
		Expect(stdout).To(ContainSubstring("  groups               : docs\n"))
		Expect(stdout).To(ContainSubstring("  artifacts            : 8 ok / 0 failed / 8 attempted\n"))
		Expect(stdout).To(ContainSubstring("  top-level files      : 10\n"))
		Expect(listDir(out)).To(Equal(wantDocsDir))
		expectNoTempResidue(out)

		sums := readChecksums(out)
		Expect(checksumNames(sums)).To(Equal(wantDocsDir[1:]))
		m, _ := readManifest(out)
		Expect(m.Files).To(HaveLen(len(wantDocsOrder)))
		Expect(m.Files[5]).To(Equal(publish.ManifestFile{Name: "QA-REPORT.md", SHA256: fileSHA(qaPath), Bytes: sizeOf(qaPath)}))
		Expect(m.States).To(Equal([]publish.ManifestState{{Code: "YUC", Name: "Yucatán", Artifacts: []string{}}}))

		// Every number in the report is what plain SQL says over the
		// same DB; the report describes the whole DB, not the one state.
		md := string(readBytes(qaPath))
		db := openRO(dbPath)
		Expect(md).To(HavePrefix("# QA report — " + publish.DatasetTitle + "\n\nVersion 0.1. Snapshot `" + publishSnapshotDate +
			"`, schema version " + strconv.Itoa(schema.Version) + ", ETL git SHA "))
		if binGitSHA == "" {
			Expect(md).To(ContainSubstring("ETL git SHA (none).\n"))
		} else {
			Expect(md).To(ContainSubstring("ETL git SHA `" + binGitSHA + "`.\n"))
		}
		Expect(md).To(ContainSubstring("Snapshot date: `" + publishSnapshotDate + "`.\n"))
		Expect(md).To(ContainSubstring(mdRow("snapshot_date", "status", "sink_kind", "etl_git_sha") + "|---|---|---|---|\n" +
			mdRow(publishSnapshotDate, "complete", "local", "f1r5t")))
		Expect(md).To(ContainSubstring(mdRow("run_label", "temporal_mode", "span", "status", "etl_git_sha") + "|---|---|---|---|---|\n" +
			mdRow("power-daily-1981-2026", "daily", "1981-01-01 to 2026-06-08", "complete", "null") +
			mdRow("power-monthly-1981-2010", "monthly", "1981 to 2010", "complete", "p0w3r") +
			mdRow("power-monthly-1991-2020", "monthly", "1991 to 2020", "complete", "p0w3r")))

		var counts strings.Builder
		counts.WriteString(mdRow("table", "rows") + "|---|---|\n")
		for _, t := range []string{
			"stations", "monthly_normals", "monthly_normals_extras", "daily_observations", "parsing_warnings",
			"ingest_runs", "power_runs", "nasa_power_grid_cells", "station_power_cell", "monthly_supplement", "daily_supplement",
		} {
			counts.WriteString(mdRow(t, strconv.Itoa(queryInt(db, "SELECT COUNT(*) FROM "+t))))
		}
		Expect(md).To(ContainSubstring(counts.String()))

		Expect(md).To(ContainSubstring(mdRow("status", "stations") + "|---|---|\n" +
			mdRow("operating", strconv.Itoa(queryInt(db, `SELECT COUNT(*) FROM stations WHERE status = 'operating'`))) +
			mdRow("suspended", strconv.Itoa(queryInt(db, `SELECT COUNT(*) FROM stations WHERE status = 'suspended'`))) +
			mdRow("null", strconv.Itoa(queryInt(db, `SELECT COUNT(*) FROM stations WHERE status IS NULL`)))))
		Expect(md).To(ContainSubstring(mdRow("code", "state", "stations") + "|---|---|---|\n" +
			mdRow("AGS", "Aguascalientes", strconv.Itoa(queryInt(db, `SELECT COUNT(*) FROM stations WHERE state = 'AGS'`))) +
			mdRow("YUC", "Yucatán", strconv.Itoa(queryInt(db, `SELECT COUNT(*) FROM stations WHERE state = 'YUC'`)))))
		Expect(md).To(ContainSubstring("### Observed daily series (daily_observations)\n\n" +
			"- Rows: " + strconv.Itoa(queryInt(db, `SELECT COUNT(*) FROM daily_observations`)) + "\n" +
			"- Stations with rows: " + strconv.Itoa(queryInt(db, `SELECT COUNT(DISTINCT station_id) FROM daily_observations`)) + "\n" +
			"- First date: " + queryStrings(db, `SELECT MIN(date) FROM daily_observations`)[0] + "\n" +
			"- Last date: " + queryStrings(db, `SELECT MAX(date) FROM daily_observations`)[0] + "\n"))
		Expect(md).To(ContainSubstring("- Grid cells registered: 2\n- Stations linked to a cell: 4\n"))
		Expect(md).To(ContainSubstring(mdRow("period", "cells", "rows") + "|---|---|---|\n" +
			mdRow("1961-1990", "0", "0") + mdRow("1971-2000", "0", "0") + mdRow("1981-2010", "2", "3") + mdRow("1991-2020", "1", "1")))

		rows := queryInt(db, `SELECT COUNT(*) FROM daily_observations`)
		var nulls strings.Builder
		nulls.WriteString("### daily_observations (" + strconv.Itoa(rows) + " rows)\n\n" + mdRow("column", "non-null rows", "%") + "|---|---|---|\n")
		for _, col := range []string{"tmax", "tmin", "precip", "evap"} {
			n := queryInt(db, "SELECT COUNT("+col+") FROM daily_observations")
			nulls.WriteString(mdRow(col, strconv.Itoa(n), pctText(n, rows)))
		}
		Expect(md).To(ContainSubstring(nulls.String()))
		Expect(md).To(ContainSubstring(mdRow("tmax", "7", "87.5")))
		supplementRows := queryInt(db, `SELECT COUNT(*) FROM daily_supplement`)
		for _, col := range wantPower31 {
			n := queryInt(db, "SELECT COUNT("+col+") FROM daily_supplement")
			Expect(md).To(ContainSubstring(mdRow(col, strconv.Itoa(n), pctText(n, supplementRows))), col)
		}

		var gate strings.Builder
		gate.WriteString(mdRow("rule", "name", "scanned", "warn", "error") + "|---|---|---|---|---|\n")
		names := map[string]string{
			"bbox": "Lat/lon plausibility", "runs-in-flight": "Runs in flight / stranded", "ingest-complete": "Complete ingest run",
			"station-refs": "Station references", "cell-refs": "POWER cell references", "run-refs": "POWER run references",
			"run-label-unique": "POWER run_label uniqueness", "fill-leak": "POWER fill-value leak", "wind-range": "Wind direction range",
		}
		for _, w := range wantSeededGate {
			gate.WriteString(mdRow(w.rule, names[w.rule], strconv.Itoa(w.scanned), strconv.Itoa(w.warn), strconv.Itoa(w.errs)))
		}
		// The totals, the gate's own scope note, then the per-rule
		// table — contiguous, in that order: a reader who sees nine
		// clean rules is told, in the same breath, that they are
		// structural rules and that climatological plausibility is the
		// `validate` verb's business.
		Expect(md).To(ContainSubstring("Rules: 9. Error findings: 0. Warn findings: 2.\n\n" +
			"These rules check referential and structural integrity and coordinate\n" +
			"plausibility; none of them assesses climatological plausibility. Within-month\n" +
			"completeness, daily-series sanity and cross-period consistency are the separate\n" +
			"`validate` verb's rules, evaluated there and not part of this gate.\n\n" +
			gate.String()))
		Expect(md).To(ContainSubstring("### bbox\n\nWarnings (2 of 2):\n\n" +
			`- station conagua_conventional/31019 ("PROGRESO") has NULL lat and lon` + "\n" +
			`- station conagua_conventional/1010 ("SAN JOSE DE GRACIA") has NULL lat and lon` + "\n"))

		Expect(md).To(ContainSubstring(mdRow("daily_rows", "8", "daily_observations", strconv.Itoa(rows))))
		Expect(md).To(ContainSubstring(mdRow("run_label", "supplement_rows (stored)", "monthly_supplement rows", "daily_supplement rows") +
			"|---|---|---|---|\n" + mdRow("power-daily-1981-2026", "4", "0", "4") + mdRow("power-monthly-1981-2010", "3", "3", "0") +
			mdRow("power-monthly-1991-2020", "1", "1", "0")))
		Expect(md).To(ContainSubstring("The parsing_warnings rows the database holds (1)"))
		Expect(md).To(ContainSubstring(mdRow("severity", "rows") + "|---|---|\n" + mdRow("warn", "1")))
		Expect(md).To(ContainSubstring(mdRow("source", "warn", "error") + "|---|---|---|\n" + mdRow("daily", "1", "0")))
		// No wall clock, no personal identity.
		Expect(regexp.MustCompile(`\d{4}-\d{2}-\d{2}T`).FindString(md)).To(BeEmpty())
		Expect(strings.ToLower(md)).NotTo(ContainSubstring("orcid"))

		// The same bytes beside an archive, and on a second run.
		again := filepath.Join(GinkgoT().TempDir(), "again")
		Expect(runPublishToExit(dbPath, again, "--state", "ags", "--only", "json,docs").ExitCode()).To(Equal(0))
		Expect(listDir(again)).To(Equal([]string{
			"CHECKSUMS", "CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md",
			"README.md", "ags-json.zip", "manifest.json", "zenodo-metadata.json",
		}))
		Expect(readBytes(filepath.Join(again, "QA-REPORT.md"))).To(Equal(readBytes(qaPath)))
		Expect(checksumNames(readChecksums(again))).To(Equal([]string{
			"CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md",
			"README.md", "ags-json.zip", "manifest.json", "zenodo-metadata.json",
		}))
		m, _ = readManifest(again)
		Expect(m.Files).To(HaveLen(9))
		Expect(m.Files[5].Name).To(Equal("QA-REPORT.md"))
		Expect(m.Files[7].Name).To(Equal("ags-json.zip"))
		Expect(fileSHA(dbPath)).To(Equal(dbSHA))
	})

	It("exits 1 on an impossible coordinate, naming bbox and the station, with nothing under --out", func() {
		corrupt := corruptCopy(dbPath, `UPDATE stations SET lat = 0, lon = 0 WHERE external_id = '31001'`)
		corruptSHA := fileSHA(corrupt)
		out := filepath.Join(GinkgoT().TempDir(), "refused")
		session := runPublishToExit(corrupt, out, "--state", "yuc")
		Expect(session.ExitCode()).To(Equal(1))

		stderr := string(session.Err.Contents())
		expectGateLines(stderr, withGateErrors(wantSeededGate, "bbox", 7, 2, 1))
		Expect(stderr).To(HaveSuffix("error: gate refused: 1 error finding\n" +
			`  bbox — station conagua_conventional/31001 ("MERIDA (OBS)") at impossible lat=0.0000 lon=0.0000 — error blocks publish` + "\n"))
		Expect(publishUnitLineRE.FindAllStringSubmatch(stderr, -1)).To(BeEmpty())
		Expect(publishArtifactLineRE.FindAllStringSubmatch(stderr, -1)).To(BeEmpty())
		Expect(publishFileLineRE.FindAllStringSubmatch(stderr, -1)).To(BeEmpty())
		stdout := string(session.Out.Contents())
		Expect(stdout).To(ContainSubstring("\npublish aborted\n"))
		Expect(stdout).To(ContainSubstring("  gate                 : refused · 9 rules · 2 warn · 1 error\n"))
		Expect(stdout).To(ContainSubstring("  artifacts            : 0 ok / 0 failed / 0 attempted\n"))
		_, err := os.Stat(out)
		Expect(os.IsNotExist(err)).To(BeTrue(), "nothing may be written behind a refusal: no file, no temp directory")
		Expect(fileSHA(corrupt)).To(Equal(corruptSHA))
		Expect(fileSHA(dbPath)).To(Equal(dbSHA))
	})

	It("exits 1 on a power run still marked running, naming runs-in-flight", func() {
		corrupt := corruptCopy(dbPath, `UPDATE power_runs SET status = 'running' WHERE id = 1`)
		out := filepath.Join(GinkgoT().TempDir(), "refused")
		session := runPublishToExit(corrupt, out, "--state", "yuc")
		Expect(session.ExitCode()).To(Equal(1))
		stderr := string(session.Err.Contents())
		expectGateLines(stderr, withGateErrors(wantSeededGate, "runs-in-flight", 1, 0, 1))
		Expect(stderr).To(HaveSuffix("error: gate refused: 1 error finding\n" +
			"  runs-in-flight — power_runs.id=1 started 2026-07-01T00:00:00Z, status='running' — a run in flight or stranded " +
			"cannot be vouched for (finish it, or reconcile through validate)\n"))
		_, err := os.Stat(out)
		Expect(os.IsNotExist(err)).To(BeTrue())
	})

	It("exits 1 on a POWER fill value stored in daily_supplement, naming fill-leak, the column, and the row", func() {
		corrupt := corruptCopy(dbPath, `UPDATE daily_supplement SET rh2m_pct = -999 WHERE cell_id = '`+publishCellYUC+`' AND date = '2020-01-01'`)
		out := filepath.Join(GinkgoT().TempDir(), "refused")
		session := runPublishToExit(corrupt, out, "--state", "yuc", "--only", "docs")
		Expect(session.ExitCode()).To(Equal(1))
		stderr := string(session.Err.Contents())
		expectGateLines(stderr, withGateErrors(wantSeededGate, "fill-leak", 8, 0, 1))
		Expect(stderr).To(HaveSuffix("error: gate refused: 1 error finding\n" +
			"  fill-leak — daily_supplement.rh2m_pct: 1 row holding POWER's -999 fill (" + publishCellYUC + "/2020-01-01)\n"))
		_, err := os.Stat(out)
		Expect(os.IsNotExist(err)).To(BeTrue())
	})

	It("lists every error finding on its own line, in rule order, when several rules fail", func() {
		corrupt := corruptCopy(dbPath,
			`UPDATE stations SET lat = 0, lon = 0 WHERE external_id = '31001'`,
			`UPDATE power_runs SET status = 'running' WHERE id = 1`,
		)
		out := filepath.Join(GinkgoT().TempDir(), "refused")
		session := runPublishToExit(corrupt, out, "--state", "yuc")
		Expect(session.ExitCode()).To(Equal(1))
		stderr := string(session.Err.Contents())
		expectGateLines(stderr, withGateErrors(withGateErrors(wantSeededGate, "bbox", 7, 2, 1), "runs-in-flight", 1, 0, 1))
		Expect(stderr).To(HaveSuffix("error: gate refused: 2 error findings\n" +
			`  bbox — station conagua_conventional/31001 ("MERIDA (OBS)") at impossible lat=0.0000 lon=0.0000 — error blocks publish` + "\n" +
			"  runs-in-flight — power_runs.id=1 started 2026-07-01T00:00:00Z, status='running' — a run in flight or stranded " +
			"cannot be vouched for (finish it, or reconcile through validate)\n"))
		Expect(string(session.Out.Contents())).To(ContainSubstring("  gate                 : refused · 9 rules · 2 warn · 2 error\n"))
		_, err := os.Stat(out)
		Expect(os.IsNotExist(err)).To(BeTrue())
	})

	It("refuses ahead of the out-dir check: a foreign entry in --out is not what a refused run reports", func() {
		corrupt := corruptCopy(dbPath, `UPDATE stations SET lat = 0, lon = 0 WHERE external_id = '31001'`)
		out := filepath.Join(GinkgoT().TempDir(), "occupied")
		Expect(os.MkdirAll(out, 0o755)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(out, "stray"), []byte("x"), 0o600)).To(Succeed())
		session := runPublishToExit(corrupt, out, "--state", "yuc")
		Expect(session.ExitCode()).To(Equal(1))
		Expect(string(session.Err.Contents())).To(ContainSubstring("error: gate refused: 1 error finding\n"))
		Expect(string(session.Err.Contents())).NotTo(ContainSubstring("holds entries this run does not produce"))
		Expect(listDir(out)).To(Equal([]string{"stray"}))
	})

	It("describes the gate and the docs group in --help", func() {
		session := runPublishToExit("", "", "--help")
		Expect(session.ExitCode()).To(Equal(0))
		help := string(session.Out.Contents())
		Expect(help).To(ContainSubstring("then runs\nthe gate: the read-only integrity checks"))
		docsAt := strings.Index(help, "  docs              README.md — the dataset, the artifact set")
		Expect(docsAt).To(BeNumerically(">", 0))
		docsHelp := help[docsAt:]
		docsHelp = docsHelp[:strings.Index(docsHelp, "\nThen manifest.json")]
		for _, name := range wantDocsOrder {
			Expect(docsHelp).To(ContainSubstring(name), name)
		}
		Expect(help).To(ContainSubstring("CITATION.cff's date-released and the stub's\n                    publication_date, the build's UTC date."))
		Expect(help).To(ContainSubstring("the gate refusing the database on an error-severity finding"))
		Expect(help).To(ContainSubstring(fmt.Sprintf("(%s)", "tabular, json, national-csv, national-parquet, national-json, national-sqlite, raw, docs")))
	})
})
