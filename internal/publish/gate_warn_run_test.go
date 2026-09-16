package publish_test

// The Run seam under warn-only gate findings and under a refusal into an
// existing directory: a station outside the MX envelope is a bbox
// warning — reported in the Report and in QA-REPORT.md's gate section,
// verbatim — and never blocks the archives; a refusal into a
// pre-existing, empty --out leaves it empty (no file, no run temp
// directory) and touches nothing beside it.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

var _ = Describe("Run under warn-only findings and refusal into an existing out dir", func() {
	var (
		db  *sql.DB
		ids map[string]int64
		out string
		rec *recorder
	)

	BeforeEach(func() {
		db = openTempDB()
		ids = seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		out = filepath.Join(GinkgoT().TempDir(), "deposit", "publish")
		rec = &recorder{}
	})

	run := func(opts publish.Options) (*publish.Report, error) {
		opts.OutDir = out
		opts.Now = fixedClock
		opts.ETLGitSHA = "abc123"
		return publish.Run(context.Background(), db, opts, rec.record)
	}

	It("does not block on a station outside the MX envelope and lists the warning in QA-REPORT.md", func() {
		mustExec(db, `UPDATE stations SET lat = 51.5, lon = -0.1 WHERE id = ?`, ids["conv/31001"])
		outside := `station conagua_conventional/31001 ("Mérida, \"La Plancha\"") at lat=51.5000 lon=-0.1000 ` +
			`outside MX bbox [14.5,32.8]×[-118.5,-86.7] — likely typo`

		report, err := run(publish.Options{States: []string{"yuc"}, Only: []string{"json", "docs"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
		Expect(report.Gate.Errors).To(BeZero())
		bbox := report.Gate.Rules[0]
		Expect(bbox.ID).To(Equal("bbox"))
		Expect(bbox.Errors).To(BeZero())
		// The seed's stations without coordinates, plus this one.
		nullCoords := countSQL(db, `SELECT COUNT(*) FROM stations WHERE lat IS NULL OR lon IS NULL`)
		Expect(nullCoords).To(Equal(int64(3)))
		Expect(bbox.Warnings).To(Equal(int(nullCoords) + 1))
		Expect(report.Gate.Warnings).To(Equal(bbox.Warnings))
		Expect(bbox.Scanned).To(Equal(int(countSQL(db, `SELECT COUNT(*) FROM stations`))))
		var texts []string
		for _, f := range bbox.Findings {
			Expect(f.Severity).To(Equal(validate.SeverityWarn))
			texts = append(texts, f.Issue)
		}
		Expect(texts).To(ContainElement(outside))
		Expect(gateEvents(rec)[0]).To(Equal(publish.ProgressEvent{Rule: "bbox", Scanned: bbox.Scanned, Warnings: bbox.Warnings}))

		// The archive and the report both shipped; the report carries the
		// warning verbatim under the rule, with the counts.
		Expect(dirNames(out)).To(Equal([]string{
			"CHECKSUMS", "CITATION.cff", "DATA-DICTIONARY.json", "DATA-DICTIONARY.md", "LICENSE", "NOTICE", "QA-REPORT.md",
			"README.md", "manifest.json", "yuc-json.zip", "zenodo-metadata.json",
		}))
		Expect(report.Artifacts[0].Name).To(Equal("yuc-json.zip"))
		Expect(report.Artifacts[0].Entries).To(BeNumerically(">", 0))
		data, err := os.ReadFile(filepath.Join(out, "QA-REPORT.md"))
		Expect(err).NotTo(HaveOccurred())
		md := string(data)
		Expect(md).To(ContainSubstring(fmt.Sprintf("Rules: %d. Error findings: 0. Warn findings: %d.\n", len(gateRuleIDs), bbox.Warnings)))
		Expect(md).To(ContainSubstring(fmt.Sprintf("| bbox | Lat/lon plausibility | %d | %d | 0 |\n", bbox.Scanned, bbox.Warnings)))
		Expect(md).To(ContainSubstring(fmt.Sprintf("### bbox\n\nWarnings (%d of %d):\n\n", bbox.Warnings, bbox.Warnings)))
		Expect(md).To(ContainSubstring("\n- " + outside + "\n"))
		Expect(md).NotTo(ContainSubstring("Errors ("))
	})

	It("refuses into a pre-existing empty out dir and leaves it empty — no file, no temp directory — and its parent untouched", func() {
		Expect(os.MkdirAll(out, 0o755)).To(Succeed())
		mustExec(db, `UPDATE stations SET lat = 0, lon = 0 WHERE id = ?`, ids["conv/31001"])

		report, err := run(publish.Options{Only: stateGroups})
		var gateErr *publish.GateError
		Expect(errors.As(err, &gateErr)).To(BeTrue(), err)
		Expect(report.Gate.Errors).To(Equal(1))
		Expect(report.Artifacts).To(BeEmpty())
		Expect(dirNames(out)).To(BeEmpty())
		Expect(dirNames(filepath.Dir(out))).To(Equal([]string{filepath.Base(out)}))
		Expect(rec.events).To(HaveLen(len(gateRuleIDs)), "the gate lines are the only progress")
	})
})
