package validate_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/schema"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// The verb's engine over a real database — a throwaway VACUUM INTO copy
// in practice, never the production file: Run WRITES (its findings into
// parsing_warnings, stranded runs to 'aborted'). Skipped unless
// VALIDATE_RUN_DB names the file. Reports each rule to the Ginkgo
// writer (run with -v to see it); VALIDATE_RUN_PERIOD scopes the run;
// VALIDATE_RUN_DUMP, if set, receives the rows written as TSV
// (station_id, source_file, severity, issue) for the out-of-band
// multiset comparison against a reference run's output,
// and VALIDATE_RUN_REPORT the HTML report. Asserts only what the engine
// controls — every rule ran, and the rows written equal the report's
// totals — since the findings are the DB's to have.
var _ = Describe("Run over VALIDATE_RUN_DB", func() {
	It("runs every rule, writes its findings, and dumps them", func() {
		path := os.Getenv("VALIDATE_RUN_DB")
		if path == "" {
			Skip("VALIDATE_RUN_DB not set")
		}
		db, err := schema.Open(path)
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(db.Close()).To(Succeed()) }()

		report, err := validate.Run(context.Background(), db, validate.Options{
			Period: os.Getenv("VALIDATE_RUN_PERIOD"),
			OnRuleStart: func(id, name string) {
				GinkgoWriter.Printf("[%s] %-24s starting (%s)\n", time.Now().UTC().Format("15:04:05"), id, name)
			},
			OnRuleDone: func(id string, res validate.RuleResult, elapsed time.Duration) {
				GinkgoWriter.Printf("[%s] %-24s done · scanned=%d findings=%d · %s\n",
					time.Now().UTC().Format("15:04:05"), id, res.Scanned, len(res.Findings), elapsed.Round(time.Millisecond))
			},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(report.PerRule).To(HaveLen(len(validate.AllRules(""))))
		for _, rr := range report.PerRule {
			GinkgoWriter.Printf("%-24s scanned=%-10d warn=%-6d error=%-4d %s\n",
				rr.ID, rr.Scanned, rr.Warnings, rr.Errors, rr.Elapsed.Round(time.Millisecond))
		}
		GinkgoWriter.Printf("total warn=%d error=%d elapsed=%s\n",
			report.WarningsTotal, report.ErrorsTotal, report.FinishedAt.Sub(report.StartedAt).Round(time.Millisecond))
		Expect(countWarnings(db, "validate:%")).To(Equal(report.WarningsTotal + report.ErrorsTotal))

		if dump := os.Getenv("VALIDATE_RUN_DUMP"); dump != "" {
			_, err := archive.WriteFileAtomic(dump, func(w io.Writer) error {
				for _, r := range readWarnings(db, "validate:%") {
					sid := ""
					if r.StationID.Valid {
						sid = fmt.Sprint(r.StationID.Int64)
					}
					if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", sid, r.SourceFile.String, r.Severity, r.Issue); err != nil {
						return err
					}
				}
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		}
		if out := os.Getenv("VALIDATE_RUN_REPORT"); out != "" {
			Expect(validate.WriteHTMLReport(out, report)).To(Succeed())
		}
	})
})
