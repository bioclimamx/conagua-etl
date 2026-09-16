package cmd

// Direct specs for the ingestLog renderer: the per-station line shape
// (ok and FAIL), the EMA-driven ETA arithmetic, and the fmtDur /
// truncate helpers it formats with. The wall-clock fields (timestamp,
// elapsed-since-start) are matched structurally; everything the
// StationOutcome determines is pinned exactly.

import (
	"bytes"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
)

var _ = Describe("ingestLog", func() {
	var (
		buf bytes.Buffer
		r   *ingestLog
	)

	BeforeEach(func() {
		buf.Reset()
		r = newIngestLog(&buf)
	})

	// linePrefix matches the wall-clock fields shared by every line.
	const linePrefix = `^\[\d{2}:\d{2}:\d{2}\] `

	It("renders one deterministic line per station with an EMA-driven ETA", func() {
		// First station: the EMA seeds at Elapsed (2s), 2 of 3 remain →
		// eta = 2 × 2s = 4s.
		r.stationDone(ingest.StationOutcome{
			Index: 1, Total: 3,
			ExternalID: "1001", Name: "Aguascalientes (Obs)",
			Elapsed:   2 * time.Second,
			DailyRows: 15, NormalsRows: 12, ExtrasRows: 12, Warnings: 3,
		})
		Expect(buf.String()).To(MatchRegexp(linePrefix +
			`1/3 ok   1001 Aguascalientes \(Obs\) · daily=15 normals=12 extras=12 warn=3 · elapsed \S+ · eta 4s\n$`))
		buf.Reset()

		// Second station: ema = 0.8×2s + 0.2×12s = 4s, 1 remains → eta 4s.
		r.stationDone(ingest.StationOutcome{
			Index: 2, Total: 3,
			ExternalID: "1003", Name: "Calvillo (Smn)",
			Elapsed: 12 * time.Second,
		})
		Expect(buf.String()).To(MatchRegexp(linePrefix +
			`2/3 ok   1003 Calvillo \(Smn\) · daily=0 normals=0 extras=0 warn=0 · elapsed \S+ · eta 4s\n$`))
		buf.Reset()

		// Last station fails: FAIL marker, zero remaining → eta 0s, and
		// the error rides the line as a final segment.
		r.stationDone(ingest.StationOutcome{
			Index: 3, Total: 3,
			ExternalID: "1004", Name: "Cañada Honda",
			Elapsed: 4 * time.Second,
			Err:     errors.New("sink.Get: permission denied"),
		})
		Expect(buf.String()).To(MatchRegexp(linePrefix +
			`3/3 FAIL 1004 Cañada Honda · daily=0 normals=0 extras=0 warn=0 · elapsed \S+ · eta 0s · sink\.Get: permission denied\n$`))
	})

	It("truncates a long station name so the line stays aligned", func() {
		r.stationDone(ingest.StationOutcome{
			Index: 1, Total: 1,
			ExternalID: "1025",
			Name:       "SAN FRANCISCO DE LOS ROMO OBSERVATORIO NACIONAL",
		})
		Expect(buf.String()).To(ContainSubstring(
			"1025 " + "SAN FRANCISCO DE LOS ROMO OBSER…" + " · "))
	})

	It("keeps finish a no-op — nothing to terminate in an append-only log", func() {
		r.finish()
		Expect(buf.String()).To(BeEmpty())
	})
})

var _ = Describe("fmtDur", func() {
	It("renders h/m/s pieces without leading zero units", func() {
		Expect(fmtDur(0)).To(Equal("0s"))
		Expect(fmtDur(-5 * time.Second)).To(Equal("0s"))
		Expect(fmtDur(45 * time.Second)).To(Equal("45s"))
		Expect(fmtDur(60 * time.Second)).To(Equal("1m00s"))
		Expect(fmtDur(3*time.Minute + 12*time.Second)).To(Equal("3m12s"))
		Expect(fmtDur(time.Hour)).To(Equal("1h00m00s"))
		Expect(fmtDur(2*time.Hour + 3*time.Minute + 5*time.Second)).To(Equal("2h03m05s"))
	})
})

var _ = Describe("truncate", func() {
	It("caps at n runes with an ellipsis, leaving short strings alone", func() {
		Expect(truncate("short", 32)).To(Equal("short"))
		Expect(truncate("exactly-8", 9)).To(Equal("exactly-8"))
		Expect(truncate("abcdefghij", 5)).To(Equal("abcd…"))
		Expect(truncate("abcdefghij", 1)).To(Equal("a"))
	})

	It("counts runes, never splitting a multi-byte character", func() {
		// "CAÑADA" is 7 bytes but 6 runes; a byte cut at 5 would split Ñ.
		Expect(truncate("CAÑADA HONDA", 5)).To(Equal("CAÑA…"))
		Expect(truncate("CAÑADA", 6)).To(Equal("CAÑADA"))
	})
})
