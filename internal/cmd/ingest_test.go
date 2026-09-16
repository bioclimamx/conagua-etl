package cmd

// Direct specs for ingest's cmd-layer pieces the e2e layer can't pin
// deterministically: newestSnapshotDate's resolution and error cases,
// and printIngestReport's exact layout — every Report field routed
// through the printer and read back from the rendered text.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
)

var _ = Describe("newestSnapshotDate", func() {
	It("picks the lexicographically-largest date directory, ignoring plain files", func() {
		root := GinkgoT().TempDir()
		for _, d := range []string{"2026-06-08", "2026-07-18", "2025-12-31"} {
			Expect(os.MkdirAll(filepath.Join(root, "conagua-raw", d), 0o755)).To(Succeed())
		}
		// A stray file must never be mistaken for a snapshot, even when it
		// sorts above every directory.
		Expect(os.WriteFile(filepath.Join(root, "conagua-raw", "9999-note.txt"),
			[]byte("x"), 0o644)).To(Succeed())

		date, err := newestSnapshotDate(root)
		Expect(err).NotTo(HaveOccurred())
		Expect(date).To(Equal("2026-07-18"))
	})

	It("reports a missing conagua-raw directory as holding no snapshots", func() {
		root := GinkgoT().TempDir()
		_, err := newestSnapshotDate(root)
		Expect(err).To(MatchError(
			"no snapshots under " + filepath.Join(root, "conagua-raw")))
	})

	It("errors when conagua-raw holds no snapshot directories", func() {
		root := GinkgoT().TempDir()
		dir := filepath.Join(root, "conagua-raw")
		Expect(os.MkdirAll(dir, 0o755)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("x"), 0o644)).To(Succeed())

		_, err := newestSnapshotDate(root)
		Expect(err).To(MatchError("no snapshots under " + dir))
	})
})

var _ = Describe("printIngestReport", func() {
	var buf bytes.Buffer

	BeforeEach(func() {
		buf.Reset()
	})

	It("renders every field of a full report, failures in full, warnings truncated to 10", func() {
		sid := int64(42)
		warnings := make([]ingest.Warning, 0, 12)
		// The first warning is line-anchored and error-severity; the rest
		// exercise the no-line shape.
		warnings = append(warnings, ingest.Warning{
			StationID:  &sid,
			SourceFile: "f01.txt",
			Line:       42,
			Severity:   ingest.SeverityError,
			Issue:      "issue 1",
		})
		for i := 2; i <= 12; i++ {
			warnings = append(warnings, ingest.Warning{
				SourceFile: fmt.Sprintf("f%02d.txt", i),
				Severity:   ingest.SeverityWarn,
				Issue:      fmt.Sprintf("issue %d", i),
			})
		}
		r := &ingest.Report{
			StationsSeeded:      5,
			DailyRowsInserted:   1234,
			NormalsRowsInserted: 24,
			ExtrasRowsInserted:  12,
			StationsAttempted:   5,
			StationsSucceeded:   4,
			StationsFailed:      1,
			PerStationFailures: []ingest.StationFailure{
				{ExternalID: "1003", Name: "Calvillo (Smn)", Err: errors.New("begin tx: database is locked")},
			},
			StationsWithFirstLast: 4,
			StationsWithWMO:       2,
			IngestRunID:           7,
			CacheHits:             10,
			CacheMisses:           3,
			Warnings:              warnings,
			FilesOpened:           9,
			FilesPullErrored:      1,
			FilesNotInCatalog:     20,
		}

		printIngestReport(&buf, r)

		want := "\n" +
			"ingest complete\n" +
			"  ingest_runs.id        : 7\n" +
			"  stations seeded       : 5\n" +
			"  stations ingested     : 4 ok / 1 failed / 5 attempted\n" +
			"  stations w/ date range: 4\n" +
			"  stations w/ wmo score : 2\n" +
			"  files opened          : 9\n" +
			"  pull-errored          : 1  (data gaps we introduced; see parsing_warnings)\n" +
			"  not in catalog        : 20  (CONAGUA didn't publish; not a gap)\n" +
			"  daily rows inserted   : 1234\n" +
			"  normals rows inserted : 24\n" +
			"  extras rows inserted  : 12\n" +
			"  cache hits/misses     : 10 / 3\n" +
			"  warnings              : 12\n" +
			"  failed stations:\n" +
			"    1003 (Calvillo (Smn)) — begin tx: database is locked\n" +
			"    [error] f01.txt:42  issue 1\n"
		for i := 2; i <= 10; i++ {
			want += fmt.Sprintf("    [warn] f%02d.txt  issue %d\n", i, i)
		}
		want += "    … and 2 more\n"
		Expect(buf.String()).To(Equal(want))
	})

	It("renders the seed-only shape: no run id, no per-station block, no cache line", func() {
		printIngestReport(&buf, &ingest.Report{StationsSeeded: 5})

		Expect(buf.String()).To(Equal("\n" +
			"ingest complete\n" +
			"  stations seeded       : 5\n" +
			"  files opened          : 0\n" +
			"  pull-errored          : 0  (data gaps we introduced; see parsing_warnings)\n" +
			"  not in catalog        : 0  (CONAGUA didn't publish; not a gap)\n" +
			"  daily rows inserted   : 0\n" +
			"  normals rows inserted : 0\n" +
			"  extras rows inserted  : 0\n" +
			"  warnings              : 0\n"))
	})
})
