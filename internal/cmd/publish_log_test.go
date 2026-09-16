package cmd

// Direct specs for the publishLog renderer: the per-rule gate line (ok
// or FAIL on the rule's error count, its scanned / warn / error counts
// and wall time), the per-file unit line (numbered across the archive,
// naming the entry path), the per-archive ok / FAIL line, and the
// docs-file line with no entry count. The wall-clock timestamp is
// matched structurally; everything the ProgressEvent determines is
// pinned exactly.

import (
	"bytes"
	"errors"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
)

var _ = Describe("publishLog", func() {
	var (
		buf bytes.Buffer
		r   *publishLog
	)

	BeforeEach(func() {
		buf.Reset()
		r = newPublishLog(&buf)
	})

	const linePrefix = `^\[\d{2}:\d{2}:\d{2}\] `

	It("renders one gate line per rule: ok with its counts and wall time, FAIL once the rule found an error, warnings never failing it", func() {
		r.event(publish.ProgressEvent{Rule: "bbox", Scanned: 5524, Warnings: 3, Elapsed: 12 * time.Millisecond})
		r.event(publish.ProgressEvent{Rule: "runs-in-flight", Scanned: 1, Errors: 1, Elapsed: 2 * time.Second})
		r.event(publish.ProgressEvent{Rule: "fill-leak", Scanned: 71399000, Elapsed: 4*time.Minute + 3*time.Second})
		lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
		Expect(lines).To(HaveLen(3))
		Expect(lines[0]).To(MatchRegexp(linePrefix + `gate ok   bbox · scanned=5524 warn=3 error=0 · elapsed 0s$`))
		Expect(lines[1]).To(MatchRegexp(linePrefix + `gate FAIL runs-in-flight · scanned=1 warn=0 error=1 · elapsed 2s$`))
		Expect(lines[2]).To(MatchRegexp(linePrefix + `gate ok   fill-leak · scanned=71399000 warn=0 error=0 · elapsed 4m03s$`))
	})

	It("renders a docs file with its size and digest prefix and no entry count, FAIL with the error on failure", func() {
		r.event(publish.ProgressEvent{
			Artifact: "QA-REPORT.md", File: true, Bytes: 8554,
			SHA256:  "2c7cf98f4e01e5cbdec61440dd0191f74f514fbe753b7704354eb579d83a868b",
			Elapsed: 48 * time.Second,
		})
		r.event(publish.ProgressEvent{
			Artifact: "QA-REPORT.md", File: true, Elapsed: time.Second,
			Err: errors.New("write file /out/QA-REPORT.md: load warnings by source: no such column: source_file"),
		})
		lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
		Expect(lines).To(HaveLen(2))
		Expect(lines[0]).To(MatchRegexp(linePrefix + `ok   QA-REPORT\.md · bytes=8554 sha256=2c7cf98f4e01 · elapsed 48s$`))
		Expect(lines[1]).To(MatchRegexp(linePrefix +
			`FAIL QA-REPORT\.md · bytes=0 sha256=- · elapsed 1s · write file /out/QA-REPORT\.md: load warnings by source: no such column: source_file$`))
		Expect(buf.String()).NotTo(ContainSubstring("entries="))
	})

	It("renders one line per unit file with its archive-wide ordinal, the archive, and the entry path", func() {
		r.event(publish.ProgressEvent{
			Artifact: "yuc-tabular.zip", Unit: "combined/combined_daily/yuc/daily-31001.csv", Index: 1, Total: 9,
		})
		r.event(publish.ProgressEvent{
			Artifact: "yuc-tabular.zip", Unit: "conagua/daily_observations/yuc/daily-31001.csv", Index: 5, Total: 9,
		})
		r.event(publish.ProgressEvent{
			Artifact: "yuc-tabular.zip", Unit: "nasa_power/daily/daily-21.0N_89.6250W.csv", Index: 9, Total: 9,
		})
		lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
		Expect(lines).To(HaveLen(3))
		Expect(lines[0]).To(MatchRegexp(linePrefix + `1/9 ok   yuc-tabular\.zip combined/combined_daily/yuc/daily-31001\.csv$`))
		Expect(lines[1]).To(MatchRegexp(linePrefix + `5/9 ok   yuc-tabular\.zip conagua/daily_observations/yuc/daily-31001\.csv$`))
		Expect(lines[2]).To(MatchRegexp(linePrefix + `9/9 ok   yuc-tabular\.zip nasa_power/daily/daily-21\.0N_89\.6250W\.csv$`))
	})

	It("renders a JSON archive's station unit by its profile.json path, the entry that completes the pair", func() {
		r.event(publish.ProgressEvent{
			Artifact: "yuc-json.zip", Unit: "combined/yuc/31001/profile.json", Index: 1, Total: 4,
		})
		r.event(publish.ProgressEvent{
			Artifact: "yuc-json.zip", Unit: "combined/yuc/3101/profile.json", Index: 4, Total: 4,
		})
		lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
		Expect(lines).To(HaveLen(2))
		Expect(lines[0]).To(MatchRegexp(linePrefix + `1/4 ok   yuc-json\.zip combined/yuc/31001/profile\.json$`))
		Expect(lines[1]).To(MatchRegexp(linePrefix + `4/4 ok   yuc-json\.zip combined/yuc/3101/profile\.json$`))
	})

	It("renders a completed archive with its entry count, size, digest prefix, and elapsed time", func() {
		r.event(publish.ProgressEvent{
			Artifact: "yuc-tabular.zip", Bytes: 123456, Entries: 6,
			SHA256:  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Elapsed: 3*time.Minute + 12*time.Second,
		})
		Expect(buf.String()).To(MatchRegexp(linePrefix +
			`ok   yuc-tabular\.zip · entries=6 bytes=123456 sha256=0123456789ab · elapsed 3m12s\n$`))
	})

	It("renders a failed archive as FAIL with the error as a final segment and no digest", func() {
		r.event(publish.ProgressEvent{
			Artifact: "ags-tabular.zip", Elapsed: 4 * time.Second,
			Err: errors.New("ags-tabular.zip: write zip: entry \"conagua/stations.csv\": non-finite value +Inf"),
		})
		Expect(buf.String()).To(MatchRegexp(linePrefix +
			`FAIL ags-tabular\.zip · entries=0 bytes=0 sha256=- · elapsed 4s · ags-tabular\.zip: write zip: entry "conagua/stations\.csv": non-finite value \+Inf\n$`))
	})
})
