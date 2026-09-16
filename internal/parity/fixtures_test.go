package parity_test

// Synthetic snapshot fixtures for the parity specs. Bodies are in real
// CONAGUA file format — the daily body is cribbed from
// internal/conagua/testdata/daily/real_01001.txt and the normals body
// is the golden parser fixture itself — so every spec drives the same
// listing → parse → diff → reduce path the production gate uses.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/parity"
)

// obsName is the station name used by dailyBody unless a spec varies it
// to provoke header drift.
const obsName = "AGUASCALIENTES (OBS)"

// dailyBody renders a minimal daily station file in CONAGUA's format:
// banner with an EMISIÓN stamp, the labeled station header, the FECHA
// column and units rows, then the given tab-separated data rows.
func dailyBody(name string, rows ...string) string {
	var b strings.Builder
	b.WriteString("COMISIÓN NACIONAL DEL AGUA\n")
	b.WriteString("REGISTRO DIARIO HISTÓRICO\n")
	b.WriteString("EMISIÓN   : 17/04/2026 \n")
	b.WriteString("\n")
	b.WriteString(" ESTACIÓN  : 1001 \n")
	fmt.Fprintf(&b, " NOMBRE    : %s \n", name)
	b.WriteString(" ESTADO    : AGUASCALIENTES \n")
	b.WriteString(" MUNICIPIO : AGUASCALIENTES \n")
	b.WriteString(" SITUACIÓN : OPERANDO \n")
	b.WriteString(" CVE-OMM   : 76571 \n")
	b.WriteString(" LATITUD   : 21.85027778 ° \n")
	b.WriteString(" LONGITUD  : -102.2908333 ° \n")
	b.WriteString(" ALTITUD   : 1890.8 msnm \n")
	b.WriteString("\n")
	b.WriteString("FECHA\t\tPRECIP\tEVAP\tTMAX\tTMIN\n")
	b.WriteString("\t\t(mm)\t(mm)\t(°C )\t(°C)\n")
	for _, row := range rows {
		b.WriteString(row + "\n")
	}
	return b.String()
}

// threeRows is a small daily data section covering plain values, NULO
// gaps, and a fully-empty row. Returned fresh so specs can mutate it.
func threeRows() []string {
	return []string{
		"1985-01-01\t0\tNULO\t20.2\t9.8",
		"1985-01-02\t1.5\t4.1\t21\t10.5",
		"1985-01-03\tNULO\tNULO\tNULO\tNULO",
	}
}

// normalsFixture loads the golden 1991-2020 normals file shared with
// the parser characterization specs — a body known to exercise every
// section the normals parser fills.
func normalsFixture() string {
	GinkgoHelper()
	body, err := os.ReadFile(filepath.Join("..", "conagua", "testdata", "normals", "real_1991_2020_01001.txt"))
	Expect(err).NotTo(HaveOccurred())
	return string(body)
}

// mustReplace substitutes from with to, asserting from occurs exactly
// once so a fixture edit that would silently change the scenario fails
// loudly instead.
func mustReplace(body, from, to string) string {
	GinkgoHelper()
	Expect(strings.Count(body, from)).To(Equal(1), "fixture token %q must occur exactly once", from)
	return strings.Replace(body, from, to, 1)
}

// writeSnapshot materializes station files under a snapshot date-dir.
// Keys are "<kind>/<station>" (the .txt suffix is appended); values are
// full file bodies.
func writeSnapshot(dir string, files map[string]string) {
	GinkgoHelper()
	for rel, body := range files {
		path := filepath.Join(dir, filepath.FromSlash(rel)+".txt")
		Expect(os.MkdirAll(filepath.Dir(path), 0o755)).To(Succeed())
		Expect(os.WriteFile(path, []byte(body), 0o644)).To(Succeed())
	}
}

// runCompare builds both snapshot dirs in temp space, runs
// CompareSnapshots over them, and asserts the infrastructure-level
// outcome (no error, dirs echoed back) before returning the report.
func runCompare(ctx context.Context, base, updated map[string]string) parity.Report {
	GinkgoHelper()
	baseDir := GinkgoT().TempDir()
	newDir := GinkgoT().TempDir()
	writeSnapshot(baseDir, base)
	writeSnapshot(newDir, updated)
	report, err := parity.CompareSnapshots(ctx, baseDir, newDir)
	Expect(err).NotTo(HaveOccurred())
	Expect(report.BaseDir).To(Equal(baseDir))
	Expect(report.NewDir).To(Equal(newDir))
	return report
}

// kindReportFor extracts the KindReport for kind, failing the spec if
// the report does not carry one.
func kindReportFor(report parity.Report, kind conagua.Kind) parity.KindReport {
	GinkgoHelper()
	for _, kr := range report.Kinds {
		if kr.Kind == kind {
			return kr
		}
	}
	Fail(fmt.Sprintf("no KindReport for kind %s", kind))
	return parity.KindReport{}
}
