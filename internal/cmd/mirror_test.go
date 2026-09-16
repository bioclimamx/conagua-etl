package cmd

// Interface-level specs for the mirror core (buildMirrorEntries,
// mirrorOne, loadMirrorMetadata). They run in the same test binary — and
// hence the same Ginkgo suite — as cmd_test's e2e specs; the single
// RunSpecs bootstrap lives in cmd_suite_test.go.
//
// These specs exercise the source≠destination transfer path the e2e
// layer cannot reach: with --sink local the binary's source and
// destination coincide (verify mode), so a genuine sink→root transfer,
// and the Get-call accounting around it, are provable only here, against
// a wrapping Sink over a separate source root.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// mirrorSpecDate pins the snapshot key for the direct specs.
const mirrorSpecDate = "2026-01-15"

// mirrorSpecFixtures maps "<stationID>/<kind>" to the real fixture body
// under testdata/ that backs the entry. These are the four files the
// spec ledger marks fetched.
var mirrorSpecFixtures = map[string]string{
	"01001/daily":             "Diarios/ags/dia01001.txt",
	"01003/daily":             "Diarios/ags/dia01003.txt",
	"01001/normals_1991_2020": "Normales9120/ags/nor9120_01001.txt",
	"01097/normals_1991_2020": "Normales9120/ags/nor9120_01097.txt",
}

func specFixtureBody(rel string) []byte {
	body, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(rel)))
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return body
}

func specSHA256(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// specAddr maps a "<stationID>/<kind>" key to its snapshot Address.
func specAddr(key string) snapshot.Address {
	station, kind, _ := strings.Cut(key, "/")
	return snapshot.Address{Date: mirrorSpecDate, Kind: conagua.Kind(kind), StationID: station}
}

// specTree lists every regular file under root as sorted slash paths
// relative to root — one Equal against it proves both presence of the
// mirrored bodies and absence of .tmp-* residue.
func specTree(root string) []string {
	files := []string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	sort.Strings(files)
	return files
}

// mirrorSpecProgress builds the spec ledger: four fetched entries backed
// by real fixture bodies, plus one each of not_found / error / pending —
// outcomes that have no stored body and must never reach the sink.
// Stations are deliberately out of order to prove the plan sort.
func mirrorSpecProgress() snapshot.Progress {
	mk := func(key string) snapshot.FileState {
		body := specFixtureBody(mirrorSpecFixtures[key])
		return snapshot.FileState{
			Outcome: snapshot.OutcomeFetched,
			SHA256:  specSHA256(body),
			Bytes:   int64(len(body)),
		}
	}
	return snapshot.Progress{
		SnapshotDate:  mirrorSpecDate,
		SchemaVersion: snapshot.ProgressSchemaVersion,
		Stations: []snapshot.StationProgress{
			{
				State: conagua.Aguascalientes, ID: "01097",
				Files: map[conagua.Kind]snapshot.FileState{
					conagua.KindNormals1991_2020: mk("01097/normals_1991_2020"),
				},
			},
			{
				State: conagua.Aguascalientes, ID: "01001",
				Files: map[conagua.Kind]snapshot.FileState{
					conagua.KindNormals1991_2020: mk("01001/normals_1991_2020"),
					conagua.KindDaily:            mk("01001/daily"),
					conagua.KindMonthly:          {Outcome: snapshot.OutcomeNotFound},
					conagua.KindExtremes:         {Outcome: snapshot.OutcomeError, LastError: "unexpected status 403"},
				},
			},
			{
				State: conagua.Aguascalientes, ID: "01003",
				Files: map[conagua.Kind]snapshot.FileState{
					conagua.KindDaily:            mk("01003/daily"),
					conagua.KindNormals1961_1990: {Outcome: snapshot.OutcomePending},
				},
			},
		},
	}
}

// wantMirrorEntries is the exact work list buildMirrorEntries must
// produce from mirrorSpecProgress: fetched entries only, every field
// populated, sorted by (state, station, kind).
func wantMirrorEntries() []mirrorEntry {
	keys := []string{
		"01001/daily",
		"01001/normals_1991_2020",
		"01003/daily",
		"01097/normals_1991_2020",
	}
	out := make([]mirrorEntry, 0, len(keys))
	for _, key := range keys {
		station, kind, _ := strings.Cut(key, "/")
		body := specFixtureBody(mirrorSpecFixtures[key])
		out = append(out, mirrorEntry{
			State:     conagua.Aguascalientes,
			StationID: station,
			Kind:      conagua.Kind(kind),
			SHA256:    specSHA256(body),
			Bytes:     int64(len(body)),
		})
	}
	return out
}

// wantSpecTree is the destination tree after a complete mirror of the
// spec ledger.
var wantSpecTree = []string{
	"conagua-raw/" + mirrorSpecDate + "/daily/01001.txt",
	"conagua-raw/" + mirrorSpecDate + "/daily/01003.txt",
	"conagua-raw/" + mirrorSpecDate + "/normals_1991_2020/01001.txt",
	"conagua-raw/" + mirrorSpecDate + "/normals_1991_2020/01097.txt",
}

// countingSink wraps a Sink and counts Get calls per address, so specs
// can assert exactly which objects a mirror pass requested.
type countingSink struct {
	snapshot.Sink

	mu   sync.Mutex
	gets map[snapshot.Address]int
}

func newCountingSink(s snapshot.Sink) *countingSink {
	return &countingSink{Sink: s, gets: map[snapshot.Address]int{}}
}

func (c *countingSink) Get(ctx context.Context, addr snapshot.Address) (io.ReadCloser, error) {
	c.mu.Lock()
	c.gets[addr]++
	c.mu.Unlock()
	return c.Sink.Get(ctx, addr)
}

func (c *countingSink) getCounts() map[snapshot.Address]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.gets)
}

// fakeIndexSink implements the optional indexFetcher capability over a
// canned payload. The embedded Sink stays nil: loadMirrorMetadata must
// never touch the core Sink methods on the index path, and a nil-panic
// here would surface that regression loudly.
type fakeIndexSink struct {
	snapshot.Sink

	payload []byte
	err     error
	dates   []string
}

func (f *fakeIndexSink) FetchIndex(_ context.Context, date string) ([]byte, error) {
	f.dates = append(f.dates, date)
	if f.err != nil {
		return nil, f.err
	}
	return f.payload, nil
}

var _ = Describe("mirror work list", func() {
	It("flattens exactly the fetched entries, every field populated, in (state, station, kind) order", func() {
		entries := buildMirrorEntries(mirrorSpecProgress())
		// Full-slice Equal round-trips every field of every entry and
		// proves not_found / error / pending never enter the plan.
		Expect(entries).To(Equal(wantMirrorEntries()))
		Expect(countStations(entries)).To(Equal(3))
	})
})

var _ = Describe("mirrorOne", func() {
	var (
		ctx     context.Context
		srcFS   *snapshot.LocalFS
		src     *countingSink
		dst     *snapshot.LocalFS
		entries []mirrorEntry
	)

	// entryFor returns the work-list entry for "<stationID>/<kind>".
	entryFor := func(key string) mirrorEntry {
		station, kind, _ := strings.Cut(key, "/")
		for _, e := range entries {
			if e.StationID == station && string(e.Kind) == kind {
				return e
			}
		}
		Fail("no entry for " + key)
		return mirrorEntry{}
	}

	// plant writes body at the destination path for key, as a prior
	// (interrupted or tampered) mirror would have left it.
	plant := func(key string, body []byte) string {
		final := dst.Path(specAddr(key))
		Expect(os.MkdirAll(filepath.Dir(final), 0o755)).To(Succeed())
		Expect(os.WriteFile(final, body, 0o644)).To(Succeed())
		return final
	}

	BeforeEach(func() {
		ctx = context.Background()
		srcFS = snapshot.NewLocalFS(GinkgoT().TempDir())
		for key, rel := range mirrorSpecFixtures {
			body := specFixtureBody(rel)
			res, err := srcFS.Put(ctx, specAddr(key), bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			Expect(res.SHA256).To(Equal(specSHA256(body)))
		}
		src = newCountingSink(srcFS)
		dst = snapshot.NewLocalFS(GinkgoT().TempDir())
		entries = buildMirrorEntries(mirrorSpecProgress())
	})

	It("materializes every fetched entry into an empty root, byte-equal and requested exactly once", func() {
		for _, e := range entries {
			st, n, err := mirrorOne(ctx, src, dst, mirrorSpecDate, e)
			Expect(err).NotTo(HaveOccurred(), "%s/%s", e.StationID, e.Kind)
			Expect(st).To(Equal(mirrorStatusFetched))
			Expect(n).To(Equal(e.Bytes))
		}

		// Round-trip every body: bytes equal the source fixture, and the
		// landed file re-hashes to the ledger digest.
		for key, rel := range mirrorSpecFixtures {
			got, err := os.ReadFile(dst.Path(specAddr(key)))
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(specFixtureBody(rel)), "byte mismatch for %s", key)
			Expect(specSHA256(got)).To(Equal(entryFor(key).SHA256))
		}

		// The full Get map proves each fetched entry cost one request and
		// the not_found / error / pending addresses were never requested.
		wantGets := map[snapshot.Address]int{}
		for key := range mirrorSpecFixtures {
			wantGets[specAddr(key)] = 1
		}
		Expect(src.getCounts()).To(Equal(wantGets))

		// Exactly the four bodies, no .tmp-* residue.
		Expect(specTree(dst.Root)).To(Equal(wantSpecTree))
	})

	It("verifies a pre-existing matching file without touching the sink", func() {
		const key = "01001/daily"
		body := specFixtureBody(mirrorSpecFixtures[key])
		final := plant(key, body)

		st, n, err := mirrorOne(ctx, src, dst, mirrorSpecDate, entryFor(key))
		Expect(err).NotTo(HaveOccurred())
		Expect(st).To(Equal(mirrorStatusVerified))
		Expect(n).To(Equal(int64(len(body))))
		Expect(src.getCounts()).To(BeEmpty())

		got, err := os.ReadFile(final)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(body))
	})

	It("reports a mismatched local file as a finding and never overwrites it", func() {
		const key = "01003/daily"
		e := entryFor(key)
		tampered := []byte("TAMPERED — not the body the ledger recorded\n")
		final := plant(key, tampered)

		st, n, err := mirrorOne(ctx, src, dst, mirrorSpecDate, e)
		Expect(st).To(Equal(mirrorStatusMismatch))
		Expect(n).To(Equal(int64(len(tampered))))
		Expect(err).To(MatchError(ContainSubstring("kept, not overwritten")))
		Expect(err).To(MatchError(ContainSubstring("local sha256=" + specSHA256(tampered)[:12])))
		Expect(err).To(MatchError(ContainSubstring("index sha256=" + e.SHA256[:12])))

		// The evidence survives byte-identical, and the sink was never asked.
		got, readErr := os.ReadFile(final)
		Expect(readErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(tampered))
		Expect(src.getCounts()).To(BeEmpty())
	})

	It("rejects a corrupt source body: per-file error, no final file, no temp residue", func() {
		const key = "01001/normals_1991_2020"
		e := entryFor(key)
		corrupt := []byte("corrupted in transit\n")
		Expect(os.WriteFile(srcFS.Path(specAddr(key)), corrupt, 0o644)).To(Succeed())

		st, _, err := mirrorOne(ctx, src, dst, mirrorSpecDate, e)
		Expect(st).To(Equal(mirrorStatusError))
		Expect(err).To(MatchError(ContainSubstring("sink sha256=" + specSHA256(corrupt)[:12])))
		Expect(err).To(MatchError(ContainSubstring("index sha256=" + e.SHA256[:12])))

		// The transfer was attempted once, but nothing landed: no file at
		// the final path and no .tmp-* sibling anywhere under the root.
		Expect(src.getCounts()).To(Equal(map[snapshot.Address]int{specAddr(key): 1}))
		Expect(dst.Path(specAddr(key))).NotTo(BeAnExistingFile())
		Expect(specTree(dst.Root)).To(BeEmpty())
	})

	It("errors on a fetched entry whose object is missing from the source", func() {
		const key = "01097/normals_1991_2020"
		Expect(os.Remove(srcFS.Path(specAddr(key)))).To(Succeed())

		st, _, err := mirrorOne(ctx, src, dst, mirrorSpecDate, entryFor(key))
		Expect(st).To(Equal(mirrorStatusError))
		Expect(err).To(MatchError(ContainSubstring("marked fetched in the index but missing from the sink")))
		Expect(specTree(dst.Root)).To(BeEmpty())
	})

	It("refuses an entry that carries no sha256 to verify against", func() {
		e := entryFor("01001/daily")
		e.SHA256 = ""

		st, _, err := mirrorOne(ctx, src, dst, mirrorSpecDate, e)
		Expect(st).To(Equal(mirrorStatusError))
		Expect(err).To(MatchError(ContainSubstring("no sha256 to verify against")))
		// An unverifiable entry must not cost a transfer.
		Expect(src.getCounts()).To(BeEmpty())
		Expect(specTree(dst.Root)).To(BeEmpty())
	})

	It("aborts under a cancelled context without landing anything", func() {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		st, _, err := mirrorOne(cancelled, src, dst, mirrorSpecDate, entryFor("01001/daily"))
		Expect(st).To(Equal(mirrorStatusError))
		Expect(err).To(MatchError(context.Canceled))
		Expect(specTree(dst.Root)).To(BeEmpty())
	})

	It("resumes a partial destination by transferring only the missing files, then converges", func() {
		// Simulate an interrupted mirror: two of four bodies already landed.
		preexisting := map[string]bool{
			"01001/daily":             true,
			"01097/normals_1991_2020": true,
		}
		for key := range preexisting {
			plant(key, specFixtureBody(mirrorSpecFixtures[key]))
		}

		// First pass: present files verify, missing files transfer.
		for _, e := range entries {
			key := e.StationID + "/" + string(e.Kind)
			st, n, err := mirrorOne(ctx, src, dst, mirrorSpecDate, e)
			Expect(err).NotTo(HaveOccurred(), key)
			Expect(n).To(Equal(e.Bytes), key)
			if preexisting[key] {
				Expect(st).To(Equal(mirrorStatusVerified), key)
			} else {
				Expect(st).To(Equal(mirrorStatusFetched), key)
			}
		}
		wantGets := map[snapshot.Address]int{}
		for key := range mirrorSpecFixtures {
			if !preexisting[key] {
				wantGets[specAddr(key)] = 1
			}
		}
		Expect(src.getCounts()).To(Equal(wantGets))

		// Converged: the complete tree, every body byte-equal.
		Expect(specTree(dst.Root)).To(Equal(wantSpecTree))
		for key, rel := range mirrorSpecFixtures {
			got, err := os.ReadFile(dst.Path(specAddr(key)))
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(specFixtureBody(rel)), "byte mismatch for %s", key)
		}

		// Second pass: pure verification, zero further sink reads.
		for _, e := range entries {
			st, _, err := mirrorOne(ctx, src, dst, mirrorSpecDate, e)
			Expect(err).NotTo(HaveOccurred())
			Expect(st).To(Equal(mirrorStatusVerified))
		}
		Expect(src.getCounts()).To(Equal(wantGets))
	})
})

var _ = Describe("loadMirrorMetadata", func() {
	var (
		ctx  context.Context
		root *snapshot.LocalFS
	)

	// metadataProgress builds a fully-populated Progress whose every field
	// must survive the JSON round trip through loadMirrorMetadata. The seed
	// doubles as a marker distinguishing competing metadata sources.
	metadataProgress := func(seed uint64) snapshot.Progress {
		return snapshot.Progress{
			SnapshotDate:  mirrorSpecDate,
			StartedAt:     time.Date(2026, 1, 15, 8, 0, 0, 0, time.UTC),
			CompletedAt:   time.Date(2026, 1, 15, 9, 30, 0, 0, time.UTC),
			RNGSeed:       seed,
			SchemaVersion: snapshot.ProgressSchemaVersion,
			Catalog:       snapshot.CatalogSummary{StatesDiscovered: 1, StationsDiscovered: 5},
			Counts:        snapshot.Counts{FilesExpected: 1, FilesFetched: 1},
			Stations: []snapshot.StationProgress{{
				State:        conagua.Aguascalientes,
				ID:           "01001",
				Name:         "Aguascalientes (Obs)",
				Municipality: "Aguascalientes",
				Status:       conagua.StatusOperating,
				Files: map[conagua.Kind]snapshot.FileState{
					conagua.KindDaily: {
						URL:       "https://smn.example/Diarios/ags/dia01001.txt",
						Outcome:   snapshot.OutcomeFetched,
						HTTPCode:  200,
						Bytes:     1016,
						SHA256:    specSHA256(specFixtureBody("Diarios/ags/dia01001.txt")),
						Attempts:  1,
						ElapsedMS: 42,
						UpdatedAt: time.Date(2026, 1, 15, 8, 5, 0, 0, time.UTC),
					},
				},
			}},
		}
	}

	marshal := func(p snapshot.Progress) []byte {
		data, err := json.MarshalIndent(p, "", "  ")
		Expect(err).NotTo(HaveOccurred())
		return data
	}

	writeMeta := func(name string, p snapshot.Progress) {
		dir := filepath.Join(root.Root, "conagua-raw", mirrorSpecDate)
		Expect(os.MkdirAll(dir, 0o755)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(dir, name), marshal(p), 0o644)).To(Succeed())
	}

	BeforeEach(func() {
		ctx = context.Background()
		root = snapshot.NewLocalFS(GinkgoT().TempDir())
	})

	It("prefers the local _index.json over _progress.json and round-trips every field", func() {
		writeMeta("_index.json", metadataProgress(111))
		writeMeta("_progress.json", metadataProgress(222))

		got, source, err := loadMirrorMetadata(ctx, root, snapshot.NewLocalFS(GinkgoT().TempDir()), mirrorSpecDate)
		Expect(err).NotTo(HaveOccurred())
		Expect(source).To(Equal("local _index.json"))
		Expect(got).To(Equal(metadataProgress(111)))
	})

	It("falls back to the local _progress.json when no index exists", func() {
		writeMeta("_progress.json", metadataProgress(222))

		got, source, err := loadMirrorMetadata(ctx, root, snapshot.NewLocalFS(GinkgoT().TempDir()), mirrorSpecDate)
		Expect(err).NotTo(HaveOccurred())
		Expect(source).To(Equal("local _progress.json"))
		Expect(got).To(Equal(metadataProgress(222)))
	})

	It("fetches the sink's index when the local root has no metadata", func() {
		want := metadataProgress(333)
		src := &fakeIndexSink{payload: marshal(want)}

		got, source, err := loadMirrorMetadata(ctx, root, src, mirrorSpecDate)
		Expect(err).NotTo(HaveOccurred())
		Expect(source).To(Equal("sink _index.json"))
		Expect(got).To(Equal(want))
		Expect(src.dates).To(Equal([]string{mirrorSpecDate}))
	})

	It("treats a metadata-less snapshot directory as absent and still consults the sink", func() {
		Expect(os.MkdirAll(filepath.Join(root.Root, "conagua-raw", mirrorSpecDate), 0o755)).To(Succeed())
		want := metadataProgress(444)
		src := &fakeIndexSink{payload: marshal(want)}

		got, source, err := loadMirrorMetadata(ctx, root, src, mirrorSpecDate)
		Expect(err).NotTo(HaveOccurred())
		Expect(source).To(Equal("sink _index.json"))
		Expect(got).To(Equal(want))
	})

	It("errors when nothing is local and the sink cannot serve an index", func() {
		_, _, err := loadMirrorMetadata(ctx, root, snapshot.NewLocalFS(GinkgoT().TempDir()), mirrorSpecDate)
		Expect(err).To(MatchError(ContainSubstring(
			"no snapshot metadata for " + mirrorSpecDate + " under " + root.Root)))
		Expect(err).To(MatchError(ContainSubstring("the selected sink cannot serve an index (--sink r2 can)")))
	})

	It("errors when neither the root nor the sink has the snapshot", func() {
		src := &fakeIndexSink{err: fs.ErrNotExist}

		_, _, err := loadMirrorMetadata(ctx, root, src, mirrorSpecDate)
		Expect(err).To(MatchError(ContainSubstring(
			"no snapshot metadata for " + mirrorSpecDate + " locally or in the sink")))
	})

	It("propagates a sink transport failure", func() {
		src := &fakeIndexSink{err: errors.New("r2 get: connection reset")}

		_, _, err := loadMirrorMetadata(ctx, root, src, mirrorSpecDate)
		Expect(err).To(MatchError(ContainSubstring("fetch index from sink: r2 get: connection reset")))
	})

	It("rejects a malformed sink index", func() {
		src := &fakeIndexSink{payload: []byte("{not json")}

		_, _, err := loadMirrorMetadata(ctx, root, src, mirrorSpecDate)
		Expect(err).To(MatchError(ContainSubstring("parse sink _index.json")))
	})
})
