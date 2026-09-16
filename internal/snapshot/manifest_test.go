package snapshot

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

var _ = Describe("Index and progress paths", func() {
	It("pins the canonical file locations under a LocalFS root", func() {
		sink := NewLocalFS("/data")
		Expect(IndexPath(sink, "2026-04-21")).To(
			Equal(filepath.Join("/data", "conagua-raw", "2026-04-21", "_index.json")))
		Expect(ProgressPath(sink, "2026-04-21")).To(
			Equal(filepath.Join("/data", "conagua-raw", "2026-04-21", "_progress.json")))
	})
})

var _ = Describe("WriteIndex", func() {
	var dir, path string

	BeforeEach(func() {
		dir = GinkgoT().TempDir()
		path = filepath.Join(dir, "_index.json")
	})

	It("strips LastFlush, preserves CompletedAt, and sorts stations by state then ID", func() {
		// Intentionally out-of-order stations.
		p := Progress{
			SnapshotDate:  "2026-04-21",
			StartedAt:     time.Now().UTC(),
			LastFlush:     time.Now().UTC(), // should be stripped
			CompletedAt:   time.Now().UTC(),
			SchemaVersion: ProgressSchemaVersion,
			Stations: []StationProgress{
				{State: conagua.Jalisco, ID: "14001"},
				{State: conagua.Aguascalientes, ID: "01005"},
				{State: conagua.Aguascalientes, ID: "01001"},
			},
		}
		Expect(WriteIndex(path, p)).To(Succeed())

		got, err := LoadProgressFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(got.LastFlush.IsZero()).To(BeTrue(), "LastFlush should be zeroed in index")
		Expect(got.CompletedAt.IsZero()).To(BeFalse(), "CompletedAt should be preserved")

		ids := make([]string, len(got.Stations))
		for i, s := range got.Stations {
			ids[i] = s.ID
		}
		Expect(ids).To(Equal([]string{"01001", "01005", "14001"}))

		// Atomic write: no tmp residue next to the index.
		matches, err := filepath.Glob(filepath.Join(dir, "*.tmp-*"))
		Expect(err).NotTo(HaveOccurred())
		Expect(matches).To(BeEmpty())
	})

	It("emits exactly the pinned on-disk JSON document", func() {
		// The literal below is the parity surface: every on-disk field name,
		// outcome string, and omitempty/omitzero behaviour is pinned here.
		// A tag rename or a struct change that alters the document shape must
		// fail this spec, not slip through a marshal-vs-marshal comparison.
		p := Progress{
			SnapshotDate:  "2026-04-21",
			StartedAt:     time.Date(2026, 4, 21, 6, 0, 0, 0, time.UTC),
			LastFlush:     time.Date(2026, 4, 21, 7, 0, 0, 0, time.UTC), // stripped by WriteIndex
			CompletedAt:   time.Date(2026, 4, 21, 8, 30, 0, 0, time.UTC),
			RNGSeed:       42,
			RateConfig:    json.RawMessage(`{"target_rps":1}`),
			RetryPolicy:   json.RawMessage(`{"max_attempts":3}`),
			ETLGitSHA:     "abc1234",
			SchemaVersion: ProgressSchemaVersion,
			Catalog:       CatalogSummary{StatesDiscovered: 2, StationsDiscovered: 2},
			Counts:        Counts{FilesExpected: 99}, // garbage in — WriteIndex must re-derive
			Stations: []StationProgress{
				{
					State: conagua.Jalisco, ID: "14001", Name: "Guadalajara (Obs)",
					Municipality: "Guadalajara", Status: conagua.StatusSuspended,
					Files: map[conagua.Kind]FileState{
						conagua.KindDaily: {
							URL:       "https://example/dia14001.txt",
							Outcome:   OutcomeFetched,
							HTTPCode:  200,
							Bytes:     2048,
							SHA256:    "cafe",
							Attempts:  1,
							ElapsedMS: 300,
							UpdatedAt: time.Date(2026, 4, 21, 6, 5, 0, 0, time.UTC),
						},
					},
				},
				{
					State: conagua.Aguascalientes, ID: "01001", Name: "Aguascalientes (Obs)",
					Municipality: "Aguascalientes", Status: conagua.StatusOperating,
					Files: map[conagua.Kind]FileState{
						conagua.KindDaily: {
							URL:       "https://example/dia01001.txt",
							Outcome:   OutcomeNotFound,
							HTTPCode:  404,
							Attempts:  2,
							LastError: "404 not found",
							ElapsedMS: 120,
							UpdatedAt: time.Date(2026, 4, 21, 6, 10, 0, 0, time.UTC),
						},
					},
				},
			},
		}
		Expect(WriteIndex(path, p)).To(Succeed())

		got, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(MatchJSON(`{
			"snapshot_date": "2026-04-21",
			"started_at": "2026-04-21T06:00:00Z",
			"completed_at": "2026-04-21T08:30:00Z",
			"rng_seed": 42,
			"rate_config": {"target_rps": 1},
			"retry_policy": {"max_attempts": 3},
			"etl_git_sha": "abc1234",
			"schema_version": 1,
			"catalog": {
				"states_discovered": 2,
				"stations_discovered": 2
			},
			"counts": {
				"files_expected": 2,
				"files_fetched": 1,
				"files_skipped_existing": 0,
				"files_missing": 1,
				"http_errors": 0,
				"files_pending": 0
			},
			"stations": [
				{
					"state": "ags",
					"id": "01001",
					"name": "Aguascalientes (Obs)",
					"municipality": "Aguascalientes",
					"status": "operating",
					"files": {
						"daily": {
							"url": "https://example/dia01001.txt",
							"outcome": "not_found",
							"http_code": 404,
							"attempts": 2,
							"last_error": "404 not found",
							"elapsed_ms": 120,
							"updated_at": "2026-04-21T06:10:00Z"
						}
					}
				},
				{
					"state": "jal",
					"id": "14001",
					"name": "Guadalajara (Obs)",
					"municipality": "Guadalajara",
					"status": "suspended",
					"files": {
						"daily": {
							"url": "https://example/dia14001.txt",
							"outcome": "fetched",
							"http_code": 200,
							"bytes": 2048,
							"sha256": "cafe",
							"attempts": 1,
							"elapsed_ms": 300,
							"updated_at": "2026-04-21T06:05:00Z"
						}
					}
				}
			]
		}`))
	})
})

var _ = Describe("LoadSnapshot", func() {
	var (
		root *LocalFS
		date string
		dir  string
	)

	BeforeEach(func() {
		root = NewLocalFS(GinkgoT().TempDir())
		date = "2026-04-21"
		dir = filepath.Join(root.Root, "conagua-raw", date)
	})

	It("prefers _index.json over _progress.json", func() {
		Expect(os.MkdirAll(dir, 0o755)).To(Succeed())

		// The raw literal pins the on-disk field names LoadSnapshot must read.
		progPath := filepath.Join(dir, "_progress.json")
		Expect(os.WriteFile(progPath,
			[]byte(`{"schema_version":1,"snapshot_date":"2026-04-21","catalog":{"states_discovered":32,"stations_discovered":5000}}`),
			0o644)).To(Succeed())

		index := Progress{
			SnapshotDate:  date,
			SchemaVersion: ProgressSchemaVersion,
			Catalog:       CatalogSummary{StatesDiscovered: 32, StationsDiscovered: 5412}, // "fresher"
		}
		Expect(WriteIndex(filepath.Join(dir, "_index.json"), index)).To(Succeed())

		got, source, err := LoadSnapshot(root, date)
		Expect(err).NotTo(HaveOccurred())
		Expect(source).To(Equal("index"))
		Expect(got.Catalog.StationsDiscovered).To(Equal(5412))
	})

	It("falls back to _progress.json when there is no index", func() {
		Expect(os.MkdirAll(dir, 0o755)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(dir, "_progress.json"),
			[]byte(`{"schema_version":1,"snapshot_date":"2026-04-21"}`), 0o644)).To(Succeed())

		got, source, err := LoadSnapshot(root, date)
		Expect(err).NotTo(HaveOccurred())
		Expect(source).To(Equal("progress"))
		Expect(got.SnapshotDate).To(Equal("2026-04-21"))
	})

	It("reports a bare snapshot directory as empty", func() {
		Expect(os.MkdirAll(dir, 0o755)).To(Succeed())

		got, source, err := LoadSnapshot(root, date)
		Expect(err).NotTo(HaveOccurred())
		Expect(source).To(Equal("empty"))
		Expect(got).To(Equal(Progress{SnapshotDate: date}))
	})

	It("returns fs.ErrNotExist for a missing snapshot", func() {
		_, _, err := LoadSnapshot(root, "1999-12-31")
		Expect(err).To(MatchError(fs.ErrNotExist))
	})
})

var _ = Describe("LoadProgressFile", func() {
	It("returns fs.ErrNotExist for a missing path", func() {
		_, err := LoadProgressFile(filepath.Join(GinkgoT().TempDir(), "nope.json"))
		Expect(err).To(MatchError(fs.ErrNotExist))
	})
})

var _ = Describe("ListSnapshotDates", func() {
	var root *LocalFS

	BeforeEach(func() {
		root = NewLocalFS(GinkgoT().TempDir())
	})

	It("lists snapshot dates newest first, skipping plain files", func() {
		for _, d := range []string{"2025-12-31", "2026-04-21", "2026-01-15"} {
			Expect(os.MkdirAll(filepath.Join(root.Root, "conagua-raw", d), 0o755)).To(Succeed())
		}
		// A stray file next to the date dirs must not be reported as a snapshot.
		Expect(os.WriteFile(filepath.Join(root.Root, "conagua-raw", "notes.txt"),
			[]byte("x"), 0o644)).To(Succeed())

		dates, err := ListSnapshotDates(root)
		Expect(err).NotTo(HaveOccurred())
		Expect(dates).To(Equal([]string{"2026-04-21", "2026-01-15", "2025-12-31"}))
	})

	It("returns empty for a missing root", func() {
		dates, err := ListSnapshotDates(root)
		Expect(err).NotTo(HaveOccurred())
		Expect(dates).To(BeEmpty())
	})
})
