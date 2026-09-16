package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/envfile"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

var (
	mirrorSnapshotDate string
	mirrorSinkFlag     string
	mirrorConcurrency  int
)

// Concurrency bounds for the transfer pool. Unlike pull, mirror talks to
// our own sink (R2 or local disk), never to CONAGUA, so the politeness
// doctrine does not apply — parallelism is bounded only by this clamp.
const (
	mirrorMinConcurrency = 1
	mirrorMaxConcurrency = 32
)

var mirrorCmd = &cobra.Command{
	Use:   "mirror",
	Short: "Materialize a snapshot's file bodies from the sink to the local root",
	Long: "Reads the snapshot metadata for --snapshot-date (_index.json preferred,\n" +
		"_progress.json otherwise; fetched from the sink when the local root has\n" +
		"neither and the sink can serve one) and downloads every file the ledger\n" +
		"marks as fetched into\n" +
		"  <root>/conagua-raw/<snapshot-date>/<kind>/<station_id>.txt\n" +
		"verifying each body against the recorded sha256.\n\n" +
		"A file already present locally is hashed instead of transferred: a match\n" +
		"is skipped (so an interrupted mirror resumes cleanly on re-run), a\n" +
		"mismatch is reported as an error and left untouched — the local file may\n" +
		"be corruption evidence and is never overwritten.\n\n" +
		"With --sink local the source and destination coincide and mirror\n" +
		"degenerates into an in-place integrity audit: every fetched entry is\n" +
		"hashed against the index, and a missing or mismatched file is an error.\n\n" +
		"Snapshot metadata is never modified; exits 0 only when every file\n" +
		"mirrored or verified cleanly.",
	RunE: runMirror,
}

func init() {
	f := mirrorCmd.Flags()
	f.StringVar(&mirrorSnapshotDate, "snapshot-date", "",
		"Snapshot date key (YYYY-MM-DD) to materialize. Required.")
	f.StringVar(&mirrorSinkFlag, "sink", "local",
		"Source to read file bodies from: 'local' (under --root; an in-place verify pass) or 'r2' (needs R2_* env vars).")
	f.IntVar(&mirrorConcurrency, "concurrency", 8,
		fmt.Sprintf("Parallel sink transfers (clamped to %d..%d).", mirrorMinConcurrency, mirrorMaxConcurrency))
	if err := mirrorCmd.MarkFlagRequired("snapshot-date"); err != nil {
		panic(err) // the flag is registered just above; failure is a programming error
	}
}

func runMirror(cmd *cobra.Command, _ []string) error {
	// Best-effort: pick up R2_* (and anything else) from ./.env. Real env
	// always wins — Load doesn't overwrite.
	if err := envfile.Load(".env"); err != nil {
		fprintf(cmd.ErrOrStderr(), "warning: .env load: %v\n", err)
	}

	conc := min(max(mirrorConcurrency, mirrorMinConcurrency), mirrorMaxConcurrency)

	src, srcLabel, err := buildSink(mirrorSinkFlag, snapshotRootFlag)
	if err != nil {
		return err
	}

	// Graceful cancellation is the only runtime intervention: SIGINT or
	// SIGTERM cancels ctx, in-flight transfers wind down at the next IO
	// point, and a re-run resumes via the hash-verified skip path.
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stderr := cmd.ErrOrStderr()
	stdout := cmd.OutOrStdout()

	fprintf(stderr,
		"snapshot-date=%s  root=%s  concurrency=%d\nsource: %s\n",
		mirrorSnapshotDate, snapshotRootFlag, conc, srcLabel)

	dst := snapshot.NewLocalFS(snapshotRootFlag)
	prog, metaSource, err := loadMirrorMetadata(ctx, dst, src, mirrorSnapshotDate)
	if err != nil {
		return err
	}
	fprintf(stderr, "metadata: %s\n", metaSource)

	entries := buildMirrorEntries(prog)
	if len(entries) == 0 {
		fprintln(stderr, "nothing to mirror — the snapshot metadata records no fetched files.")
		return nil
	}

	renderer := newMirrorLog(stdout, stderr)
	renderer.plan(len(entries), countStations(entries))

	start := time.Now()

	// A bounded worker pool over the sink — deliberately NOT the polite
	// sequential fetcher: the sink is our own infrastructure, not CONAGUA,
	// so no rate limiter applies and the only bound is --concurrency.
	work := make(chan mirrorEntry)
	var wg sync.WaitGroup
	for range conc {
		wg.Go(func() {
			for e := range work {
				if ctx.Err() != nil {
					continue // cancelled — drain the channel without working
				}
				st, n, err := mirrorOne(ctx, src, dst, mirrorSnapshotDate, e)
				// A transfer cut down by cancellation is a casualty, not an
				// error: it landed nothing, and the re-run's verify-skip
				// pass picks it up. Completed work (a verify, a mismatch
				// finding) still records even under a dying ctx.
				if st == mirrorStatusError && ctx.Err() != nil {
					continue
				}
				renderer.result(e, st, n, err)
			}
		})
	}
feed:
	for _, e := range entries {
		select {
		case work <- e:
		case <-ctx.Done():
			break feed
		}
	}
	close(work)
	wg.Wait()

	mirrored, skipped, errored, mismatches := renderer.totals()
	fprintf(stderr, "\nDone in %v — mirrored=%d skipped=%d errors=%d\n",
		time.Since(start).Round(time.Millisecond), mirrored, skipped, errored)
	noteMismatches(stderr, mismatches)
	if ctx.Err() != nil {
		fprintln(stderr, "interrupted — re-run with the same parameters to resume")
		return ctx.Err()
	}
	if errored > 0 {
		return fmt.Errorf("%d of %d files failed", errored, len(entries))
	}
	return nil
}

// indexFetcher is the optional Sink capability of serving a snapshot's
// terminal _index.json — a named optional sub-interface discovered by
// type assertion, not a widened core Sink.
// R2 implements it; LocalFS doesn't need to — a local index, when it
// exists at all, is already under the root.
type indexFetcher interface {
	FetchIndex(ctx context.Context, date string) ([]byte, error)
}

// Pin R2's capability at compile time: a FetchIndex signature drift would
// otherwise fail the type assertion below and degrade into a misleading
// "sink cannot serve an index" runtime error.
var _ indexFetcher = (*snapshot.R2Sink)(nil)

// loadMirrorMetadata resolves the snapshot metadata mirror works from: the
// local _index.json/_progress.json when present, else the sink's
// _index.json when the sink can serve one. A sink-fetched index is used in
// memory only — mirror materializes file bodies, never metadata.
func loadMirrorMetadata(
	ctx context.Context,
	root *snapshot.LocalFS,
	src snapshot.Sink,
	date string,
) (snapshot.Progress, string, error) {
	p, source, err := snapshot.LoadSnapshot(root, date)
	switch {
	case err == nil && source != "empty":
		return p, fmt.Sprintf("local _%s.json", source), nil
	case err != nil && !errors.Is(err, fs.ErrNotExist):
		return snapshot.Progress{}, "", fmt.Errorf("load snapshot %s: %w", date, err)
	}

	f, ok := src.(indexFetcher)
	if !ok {
		return snapshot.Progress{}, "", fmt.Errorf(
			"no snapshot metadata for %s under %s, and the selected sink cannot serve an index (--sink r2 can)",
			date, root.Root)
	}
	data, err := f.FetchIndex(ctx, date)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return snapshot.Progress{}, "", fmt.Errorf(
				"no snapshot metadata for %s locally or in the sink", date)
		}
		return snapshot.Progress{}, "", fmt.Errorf("fetch index from sink: %w", err)
	}
	var fetched snapshot.Progress
	if err := json.Unmarshal(data, &fetched); err != nil {
		return snapshot.Progress{}, "", fmt.Errorf("parse sink _index.json: %w", err)
	}
	return fetched, "sink _index.json", nil
}

// mirrorEntry is one unit of mirror work: a file the ledger marks fetched,
// with the digest and size the local copy must reproduce.
type mirrorEntry struct {
	State     conagua.StateCode
	StationID string
	Kind      conagua.Kind
	SHA256    string
	Bytes     int64
}

// buildMirrorEntries flattens the ledger into the work list: every file
// with outcome fetched. not_found/error/pending entries have no stored
// body to materialize and are excluded. Sorted (state, station, kind) so
// the plan is deterministic run to run.
func buildMirrorEntries(p snapshot.Progress) []mirrorEntry {
	var out []mirrorEntry
	for _, s := range p.Stations {
		for kind, f := range s.Files {
			if f.Outcome != snapshot.OutcomeFetched {
				continue
			}
			out = append(out, mirrorEntry{
				State:     s.State,
				StationID: s.ID,
				Kind:      kind,
				SHA256:    f.SHA256,
				Bytes:     f.Bytes,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].State != out[j].State {
			return out[i].State < out[j].State
		}
		if out[i].StationID != out[j].StationID {
			return out[i].StationID < out[j].StationID
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

// countStations counts distinct stations in the sorted work list (a
// station's entries are adjacent after buildMirrorEntries' sort).
func countStations(entries []mirrorEntry) int {
	n := 0
	for i, e := range entries {
		if i == 0 || e.StationID != entries[i-1].StationID {
			n++
		}
	}
	return n
}

// mirrorStatus is a file's disposition after one mirror attempt.
type mirrorStatus int

const (
	// mirrorStatusFetched — the body was transferred, verified, and renamed
	// into place.
	mirrorStatusFetched mirrorStatus = iota
	// mirrorStatusVerified — a local file already present with a matching
	// hash; nothing transferred (the resumable skip).
	mirrorStatusVerified
	// mirrorStatusMismatch — a local file present whose hash contradicts
	// the index: left untouched (it may be corruption evidence) and counted
	// as an error for the exit code.
	mirrorStatusMismatch
	// mirrorStatusError — the transfer or verification failed; nothing
	// landed at the final path.
	mirrorStatusError
)

// mirrorOne materializes (or verifies) a single file. It never overwrites
// an existing local file: a hash mismatch is evidence the operator must
// inspect, not a condition to silently repair.
func mirrorOne(
	ctx context.Context,
	src snapshot.Sink,
	dst *snapshot.LocalFS,
	date string,
	e mirrorEntry,
) (st mirrorStatus, n int64, err error) {
	// Without an expected digest there is nothing to verify against, and
	// an unverifiable transfer would violate the integrity contract.
	if e.SHA256 == "" {
		return mirrorStatusError, 0, errors.New("index entry carries no sha256 to verify against")
	}
	addr := snapshot.Address{Date: date, Kind: e.Kind, StationID: e.StationID}
	final := dst.Path(addr)

	got, n, hashErr := hashLocalFile(ctx, final)
	switch {
	case hashErr == nil && got == e.SHA256:
		return mirrorStatusVerified, n, nil
	case hashErr == nil:
		return mirrorStatusMismatch, n, fmt.Errorf(
			"local sha256=%.12s… != index sha256=%.12s… (%d B local, %d B expected) — kept, not overwritten",
			got, e.SHA256, n, e.Bytes)
	case !errors.Is(hashErr, fs.ErrNotExist):
		return mirrorStatusError, 0, fmt.Errorf("hash local file: %w", hashErr)
	}

	// Not present locally — pull the body from the sink.
	rc, err := src.Get(ctx, addr)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return mirrorStatusError, 0, errors.New("marked fetched in the index but missing from the sink")
		}
		return mirrorStatusError, 0, fmt.Errorf("sink get: %w", err)
	}
	defer rc.Close() //nolint:errcheck // read-side close; no recovery possible

	// Same write discipline as LocalFS.Put — stream to a TempSuffix sibling
	// and rename only once the body is complete AND verified, so a failed
	// or corrupt transfer never leaves a file at the final path.
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return mirrorStatusError, 0, fmt.Errorf("mkdir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(final), filepath.Base(final)+snapshot.TempSuffix+"*")
	if err != nil {
		return mirrorStatusError, 0, fmt.Errorf("create tmp: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }

	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(tmp, h), ctxReader{ctx: ctx, r: rc})
	closeErr := tmp.Close()
	if copyErr != nil {
		cleanup()
		return mirrorStatusError, 0, fmt.Errorf("copy: %w", copyErr)
	}
	if closeErr != nil {
		cleanup()
		return mirrorStatusError, 0, fmt.Errorf("close tmp: %w", closeErr)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != e.SHA256 {
		cleanup()
		return mirrorStatusError, 0, fmt.Errorf(
			"sink sha256=%.12s… != index sha256=%.12s… (%d B received, %d B expected)",
			got, e.SHA256, n, e.Bytes)
	}
	if err := os.Rename(tmpPath, final); err != nil {
		cleanup()
		return mirrorStatusError, 0, fmt.Errorf("rename: %w", err)
	}
	return mirrorStatusFetched, n, nil
}

// hashLocalFile returns the sha256 hex digest and size of the file at
// path. A missing file surfaces as fs.ErrNotExist.
func hashLocalFile(ctx context.Context, path string) (sha string, n int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close() //nolint:errcheck // read-side close; no recovery possible
	h := sha256.New()
	n, err = io.Copy(h, ctxReader{ctx: ctx, r: f})
	if err != nil {
		return "", 0, fmt.Errorf("read %s: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// noteMismatches warns that local files contradict the index hash. They
// were deliberately left in place — overwriting would destroy the evidence
// an operator needs to judge the corruption — so the recipe is manual.
func noteMismatches(stderr io.Writer, n int) {
	if n <= 0 {
		return
	}
	phrase := "file mismatched the index hash and was"
	if n != 1 {
		phrase = "files mismatched the index hash and were"
	}
	fprintf(stderr,
		"note: %d local %s kept untouched — inspect, then delete to re-mirror\n",
		n, phrase)
}

// ctxReader makes an io.Copy honour cancellation: without it, an in-flight
// sink stream would keep draining after Ctrl-C. (snapshot has the same
// wrapper unexported; duplicating two lines beats widening its API.)
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (cr ctxReader) Read(p []byte) (int, error) {
	if err := cr.ctx.Err(); err != nil {
		return 0, err
	}
	return cr.r.Read(p)
}
