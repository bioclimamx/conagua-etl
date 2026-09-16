package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// LocalFS is a Sink backed by the local filesystem. File layout mirrors the
// R2 prefix:
//
//	<Root>/conagua-raw/<Date>/<Kind>/<StationID>.txt
type LocalFS struct {
	// Root is the directory under which snapshots live. It is created
	// lazily on first Put.
	Root string
}

// rawPrefix is the directory under Root that holds every snapshot, one
// date-named directory each — the same prefix the R2 keys carry.
const rawPrefix = "conagua-raw"

// TempSuffix tags every temp file a snapshot writer stages before its
// atomic rename: <final name>.tmp-<random>, a sibling of the final path
// (LocalFS.Put, WriteIndex, Ledger.Flush, and the mirror and ingest
// writers that follow the same discipline). It is the one spelling of
// the convention, so a reader that must recognise a crashed writer's
// residue (IsTempResidue) cannot drift from the writers.
const TempSuffix = ".tmp-"

// IsTempResidue reports whether name is a snapshot writer's temp file —
// a partial body a crashed run left beside the file it was staging.
// Such a file is never snapshot content: a reader that lists a snapshot
// tree skips or refuses it rather than treating it as a station file.
func IsTempResidue(name string) bool {
	return strings.Contains(name, TempSuffix)
}

// SnapshotDir returns the directory of one snapshot date under Root: the
// pull output's home — the kind directories, _progress.json, and
// _index.json — and what the raw release artifact ships whole.
func (l *LocalFS) SnapshotDir(date string) string {
	return filepath.Join(l.Root, rawPrefix, date)
}

var _ Sink = (*LocalFS)(nil)

// NewLocalFS returns a LocalFS rooted at root.
func NewLocalFS(root string) *LocalFS {
	return &LocalFS{Root: root}
}

// Path returns the filesystem path LocalFS uses for addr. Exported so the
// `snapshot list`/`snapshot status` commands and tests can inspect it.
func (l *LocalFS) Path(addr Address) string {
	return filepath.Join(l.SnapshotDir(addr.Date), string(addr.Kind), addr.StationID+".txt")
}

// Exists implements Sink.
func (l *LocalFS) Exists(_ context.Context, addr Address) (bool, error) {
	_, err := os.Stat(l.Path(addr))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// Get implements Sink. Returns an *os.File, whose "not found" error is
// already fs.ErrNotExist-compatible, so ingest-side code can match it
// uniformly across LocalFS and R2.
func (l *LocalFS) Get(ctx context.Context, addr Address) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(l.Path(addr))
	if err != nil {
		return nil, err
	}
	return f, nil
}

// Put implements Sink. The write is atomic: contents are staged to a
// sibling TempSuffix file and renamed into place only after the reader
// drains cleanly.
func (l *LocalFS) Put(ctx context.Context, addr Address, r io.Reader) (PutResult, error) {
	final := l.Path(addr)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return PutResult{}, fmt.Errorf("mkdir: %w", err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(final), filepath.Base(final)+TempSuffix+"*")
	if err != nil {
		return PutResult{}, fmt.Errorf("create tmp: %w", err)
	}
	tmpPath := tmp.Name()
	// On any error below, drop the tmp file.
	cleanup := func() { _ = os.Remove(tmpPath) }

	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(tmp, h), contextReader{ctx: ctx, r: r})
	closeErr := tmp.Close()
	if copyErr != nil {
		cleanup()
		return PutResult{}, fmt.Errorf("copy: %w", copyErr)
	}
	if closeErr != nil {
		cleanup()
		return PutResult{}, fmt.Errorf("close tmp: %w", closeErr)
	}
	if err := os.Rename(tmpPath, final); err != nil {
		cleanup()
		return PutResult{}, fmt.Errorf("rename: %w", err)
	}
	return PutResult{
		Bytes:  n,
		SHA256: hex.EncodeToString(h.Sum(nil)),
	}, nil
}

// contextReader wraps r with context cancellation. Needed because io.Copy
// doesn't natively honour context — without this, an in-flight stream could
// keep draining after the user hits Ctrl-C.
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (cr contextReader) Read(p []byte) (int, error) {
	if err := cr.ctx.Err(); err != nil {
		return 0, err
	}
	return cr.r.Read(p)
}
