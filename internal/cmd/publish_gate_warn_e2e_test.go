package cmd_test

// The gate's two non-blocking-vs-blocking edges on the real binary: a
// station outside the MX envelope is a bbox warning — exit 0, the
// gate line counting it, the summary "passed", the finding listed
// verbatim in QA-REPORT.md — and an ingest run left 'running' (the
// ledger the other e2e does not strand) is refused with exit 1, the
// ingest ledger named, nothing under --out.

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("conagua-etl publish at the gate's severity edge", func() {
	var dbPath string

	BeforeEach(func() {
		dbPath = seedPublishDB()
	})

	It("exits 0 on a station outside the MX envelope — a bbox warning never blocks — and QA-REPORT.md lists it", func() {
		warned := corruptCopy(dbPath, `UPDATE stations SET lat = 51.5, lon = -0.1 WHERE external_id = '31001'`)
		warnedSHA := fileSHA(warned)
		out := filepath.Join(GinkgoT().TempDir(), "docs")
		session := runPublishToExit(warned, out, "--state", "yuc", "--only", "json,docs")
		Expect(session.ExitCode()).To(Equal(0))

		stderr := string(session.Err.Contents())
		expectGateLines(stderr, withGateErrors(wantSeededGate, "bbox", 7, 3, 0))
		Expect(stderr).NotTo(ContainSubstring("gate refused"))
		stdout := string(session.Out.Contents())
		Expect(stdout).To(ContainSubstring("\npublish complete\n"))
		Expect(stdout).To(ContainSubstring("  gate                 : passed · 9 rules · 3 warn · 0 error\n"))
		Expect(stdout).To(ContainSubstring("  artifacts            : 9 ok / 0 failed / 9 attempted\n"))
		Expect(listDir(out)).To(Equal([]string{
			"CHECKSUMS", "CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md",
			"README.md", "manifest.json", "yuc-json.zip", "zenodo-metadata.json",
		}))
		expectNoTempResidue(out)

		// The three warnings, in station id order, the outside-MX one
		// with the bbox rule's warning text verbatim.
		md := string(readBytes(filepath.Join(out, "QA-REPORT.md")))
		Expect(md).To(ContainSubstring("Rules: 9. Error findings: 0. Warn findings: 3.\n"))
		Expect(md).To(ContainSubstring(mdRow("bbox", "Lat/lon plausibility", "7", "3", "0")))
		Expect(md).To(ContainSubstring("### bbox\n\nWarnings (3 of 3):\n\n" +
			`- station conagua_conventional/31019 ("PROGRESO") has NULL lat and lon` + "\n" +
			`- station conagua_conventional/31001 ("MERIDA (OBS)") at lat=51.5000 lon=-0.1000 outside MX bbox [14.5,32.8]×[-118.5,-86.7] — likely typo` + "\n" +
			`- station conagua_conventional/1010 ("SAN JOSE DE GRACIA") has NULL lat and lon` + "\n\n## 6."))
		Expect(md).NotTo(ContainSubstring("Errors ("))
		Expect(fileSHA(warned)).To(Equal(warnedSHA))
	})

	It("exits 1 on an ingest run still marked running, naming runs-in-flight with the ingest ledger, nothing under --out", func() {
		corrupt := corruptCopy(dbPath, `INSERT INTO ingest_runs (started_at, snapshot_date, sink_kind, status)
		  VALUES ('2026-06-12T01:00:00Z', '`+publishSnapshotDate+`', 'local', 'running')`)
		corruptSHA := fileSHA(corrupt)
		out := filepath.Join(GinkgoT().TempDir(), "refused")
		session := runPublishToExit(corrupt, out, "--state", "yuc", "--only", "docs")
		Expect(session.ExitCode()).To(Equal(1))

		stderr := string(session.Err.Contents())
		expectGateLines(stderr, withGateErrors(withGateErrors(wantSeededGate, "runs-in-flight", 1, 0, 1), "ingest-complete", 3, 0, 0))
		Expect(stderr).To(HaveSuffix("error: gate refused: 1 error finding\n" +
			"  runs-in-flight — ingest_runs.id=3 started 2026-06-12T01:00:00Z, status='running' — a run in flight or stranded " +
			"cannot be vouched for (finish it, or reconcile through validate)\n"))
		Expect(publishFileLineRE.FindAllStringSubmatch(stderr, -1)).To(BeEmpty())
		Expect(string(session.Out.Contents())).To(ContainSubstring("  gate                 : refused · 9 rules · 2 warn · 1 error\n"))
		_, err := os.Stat(out)
		Expect(os.IsNotExist(err)).To(BeTrue(), "nothing may be written behind a refusal")
		// The stranded row is still 'running': the gate reconciled nothing.
		db := openRO(corrupt)
		Expect(queryStrings(db, `SELECT status FROM ingest_runs WHERE id = 3`)).To(Equal([]string{"running"}))
		Expect(fileSHA(corrupt)).To(Equal(corruptSHA))
	})
})
