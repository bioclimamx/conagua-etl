package snapshot

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

func sampleStation() conagua.Station {
	return conagua.Station{
		State:        conagua.Aguascalientes,
		ID:           "01001",
		Name:         "Aguascalientes (Obs)",
		Municipality: "Aguascalientes",
		Status:       conagua.StatusOperating,
		Files: map[conagua.Kind]conagua.FileEntry{
			conagua.KindDaily:   {URL: "https://example/daily"},
			conagua.KindMonthly: {URL: "https://example/monthly"},
		},
	}
}

var _ = Describe("FileOutcome", func() {
	It("pins the on-disk outcome strings", func() {
		// These literals are written verbatim into _progress.json /
		// _index.json — changing one silently breaks resumption against
		// existing snapshots.
		Expect(string(OutcomePending)).To(Equal("pending"))
		Expect(string(OutcomeFetched)).To(Equal("fetched"))
		Expect(string(OutcomeSkipped)).To(Equal("skipped_existing"))
		Expect(string(OutcomeNotFound)).To(Equal("not_found"))
		Expect(string(OutcomeError)).To(Equal("error"))
	})

	DescribeTable("Terminal",
		func(o FileOutcome, want bool) {
			Expect(o.Terminal()).To(Equal(want))
		},
		Entry("pending is not terminal", OutcomePending, false),
		Entry("fetched is terminal", OutcomeFetched, true),
		Entry("skipped_existing is terminal", OutcomeSkipped, true),
		Entry("not_found is terminal", OutcomeNotFound, true),
		Entry("error is terminal (unless --retry-failed)", OutcomeError, true),
	)
})

var _ = Describe("Ledger", func() {
	var path string

	BeforeEach(func() {
		path = filepath.Join(GinkgoT().TempDir(), "_progress.json")
	})

	Describe("UpsertStation and RecordFile", func() {
		var l *Ledger
		s := sampleStation()

		BeforeEach(func() {
			var err error
			l, err = LoadOrInit(path, Progress{
				SnapshotDate: "2026-04-21",
				RNGSeed:      42,
			})
			Expect(err).NotTo(HaveOccurred())
			l.UpsertStation(s)
		})

		It("creates a pending entry with the catalog URL for each file", func() {
			for kind, url := range map[conagua.Kind]string{
				conagua.KindDaily:   "https://example/daily",
				conagua.KindMonthly: "https://example/monthly",
			} {
				fs, ok := l.Lookup(s.ID, kind)
				Expect(ok).To(BeTrue(), "file state missing after upsert: %s", kind)
				Expect(fs.Outcome).To(Equal(OutcomePending))
				Expect(fs.URL).To(Equal(url))
			}
		})

		It("round-trips every FileUpdate field on a successful fetch", func() {
			l.RecordFile(FileUpdate{
				StationID: s.ID,
				Kind:      conagua.KindDaily,
				URL:       "https://example/daily",
				Outcome:   OutcomeFetched,
				HTTPCode:  200,
				Bytes:     1234,
				SHA256:    "deadbeef",
				Attempts:  1,
				Elapsed:   450 * time.Millisecond,
			})

			fs, ok := l.Lookup(s.ID, conagua.KindDaily)
			Expect(ok).To(BeTrue())
			Expect(fs.URL).To(Equal("https://example/daily"))
			Expect(fs.Outcome).To(Equal(OutcomeFetched))
			Expect(fs.HTTPCode).To(Equal(200))
			Expect(fs.Bytes).To(Equal(int64(1234)))
			Expect(fs.SHA256).To(Equal("deadbeef"))
			Expect(fs.Attempts).To(Equal(1))
			Expect(fs.LastError).To(BeEmpty())
			Expect(fs.ElapsedMS).To(Equal(int64(450)))
			Expect(fs.UpdatedAt.IsZero()).To(BeFalse())
		})

		It("records the error path", func() {
			l.RecordFile(FileUpdate{
				StationID: s.ID,
				Kind:      conagua.KindMonthly,
				Outcome:   OutcomeError,
				Err:       errors.New("boom"),
				Attempts:  3,
			})

			fs, ok := l.Lookup(s.ID, conagua.KindMonthly)
			Expect(ok).To(BeTrue())
			Expect(fs.Outcome).To(Equal(OutcomeError))
			Expect(fs.LastError).To(Equal("boom"))
			Expect(fs.Attempts).To(Equal(3))
		})

		It("preserves per-file state on re-upsert while refreshing metadata", func() {
			l.RecordFile(FileUpdate{
				StationID: s.ID, Kind: conagua.KindDaily,
				Outcome: OutcomeFetched, Bytes: 77,
			})

			renamed := s
			renamed.Name = "Aguascalientes (Obs) II"
			l.UpsertStation(renamed)

			fs, ok := l.Lookup(s.ID, conagua.KindDaily)
			Expect(ok).To(BeTrue())
			Expect(fs.Outcome).To(Equal(OutcomeFetched), "re-upsert must not reset terminal state")
			Expect(fs.Bytes).To(Equal(int64(77)))

			snap := l.Snapshot()
			Expect(snap.Stations).To(HaveLen(1))
			Expect(snap.Stations[0].Name).To(Equal("Aguascalientes (Obs) II"))
		})
	})

	Describe("ShouldSkip", func() {
		var l *Ledger
		s := sampleStation()

		record := func(outcome FileOutcome) {
			l.RecordFile(FileUpdate{StationID: s.ID, Kind: conagua.KindDaily, Outcome: outcome})
		}

		BeforeEach(func() {
			var err error
			l, err = LoadOrInit(path, Progress{SnapshotDate: "2026-04-21"})
			Expect(err).NotTo(HaveOccurred())
			l.UpsertStation(s)
		})

		It("does not skip unknown stations or kinds", func() {
			Expect(l.ShouldSkip("ghost", conagua.KindDaily, false)).To(BeFalse())
			Expect(l.ShouldSkip(s.ID, conagua.KindExtremes, false)).To(BeFalse())
		})

		It("does not skip pending files", func() {
			Expect(l.ShouldSkip(s.ID, conagua.KindDaily, false)).To(BeFalse())
		})

		It("skips terminal outcomes", func() {
			for _, o := range []FileOutcome{OutcomeFetched, OutcomeSkipped, OutcomeNotFound} {
				record(o)
				Expect(l.ShouldSkip(s.ID, conagua.KindDaily, false)).To(BeTrue(), "outcome %s", o)
				Expect(l.ShouldSkip(s.ID, conagua.KindDaily, true)).To(BeTrue(), "outcome %s", o)
			}
		})

		It("treats error as terminal unless retryErrors is set", func() {
			record(OutcomeError)
			Expect(l.ShouldSkip(s.ID, conagua.KindDaily, false)).To(BeTrue())
			Expect(l.ShouldSkip(s.ID, conagua.KindDaily, true)).To(BeFalse())
		})
	})

	Describe("Flush and reload", func() {
		It("persists the full Progress document field-for-field", func() {
			l1, err := LoadOrInit(path, Progress{SnapshotDate: "2026-04-21", RNGSeed: 42})
			Expect(err).NotTo(HaveOccurred())
			l1.UpsertStation(sampleStation())
			l1.RecordFile(FileUpdate{
				StationID: "01001", Kind: conagua.KindDaily,
				Outcome: OutcomeFetched, Bytes: 99,
			})
			Expect(l1.Flush()).To(Succeed())
			want := l1.Snapshot() // post-Flush: LastFlush stamped, Counts derived

			// Reload — the whole document should persist verbatim.
			l2, err := LoadOrInit(path, Progress{SnapshotDate: "2026-04-21"})
			Expect(err).NotTo(HaveOccurred())
			Expect(l2.Snapshot()).To(Equal(want))

			// And the load-bearing fields explicitly, so a failure names them.
			fs, ok := l2.Lookup("01001", conagua.KindDaily)
			Expect(ok).To(BeTrue())
			Expect(fs.Outcome).To(Equal(OutcomeFetched))
			Expect(fs.Bytes).To(Equal(int64(99)))
			Expect(l2.Snapshot().RNGSeed).To(Equal(uint64(42)))
		})

		It("refreshes seed and configs from a non-zero init on resume", func() {
			l1, err := LoadOrInit(path, Progress{SnapshotDate: "2026-04-21", RNGSeed: 42})
			Expect(err).NotTo(HaveOccurred())
			Expect(l1.Flush()).To(Succeed())

			l2, err := LoadOrInit(path, Progress{
				RNGSeed:     7,
				RateConfig:  json.RawMessage(`{"target_rps":2}`),
				RetryPolicy: json.RawMessage(`{"max_attempts":5}`),
				ETLGitSHA:   "abc1234",
			})
			Expect(err).NotTo(HaveOccurred())
			p := l2.Snapshot()
			Expect(p.SnapshotDate).To(Equal("2026-04-21"), "existing date is preserved")
			Expect(p.RNGSeed).To(Equal(uint64(7)))
			Expect(p.RateConfig).To(Equal(json.RawMessage(`{"target_rps":2}`)))
			Expect(p.RetryPolicy).To(Equal(json.RawMessage(`{"max_attempts":5}`)))
			Expect(p.ETLGitSHA).To(Equal("abc1234"))
		})
	})

	Describe("Flush atomicity", func() {
		It("leaves no tmp files and writes parseable JSON with the current schema", func() {
			l, err := LoadOrInit(path, Progress{SnapshotDate: "2026-04-21"})
			Expect(err).NotTo(HaveOccurred())
			Expect(l.Flush()).To(Succeed())

			// No leftover tmp files.
			matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), "*.tmp-*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(matches).To(BeEmpty())

			// JSON round-trips, and the on-disk field names are the pinned ones.
			b, err := os.ReadFile(path)
			Expect(err).NotTo(HaveOccurred())
			var p Progress
			Expect(json.Unmarshal(b, &p)).To(Succeed())
			Expect(p.SchemaVersion).To(Equal(ProgressSchemaVersion))
			Expect(string(b)).To(ContainSubstring(`"snapshot_date": "2026-04-21"`))
			Expect(string(b)).To(ContainSubstring(`"schema_version": 1`))
		})
	})

	Describe("LoadOrInit schema guard", func() {
		It("rejects an unknown schema version", func() {
			Expect(os.WriteFile(path, []byte(`{"schema_version": 999}`), 0o644)).To(Succeed())
			_, err := LoadOrInit(path, Progress{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("schema 999"))
		})
	})
})
