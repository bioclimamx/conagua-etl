package snapshot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"sync/atomic"
)

// CachingSink wraps a primary Sink with a LocalFS mirror. Every
// successful primary.Get() is tee'd into the cache, so subsequent
// Get()s for the same Address serve from local disk without a network
// round-trip. The typical wiring is
//
//	primary = *R2 (remote, expensive)
//	cache   = *LocalFS (under --root, basically free)
//
// Put() and Exists() delegate to the primary unchanged: the cache is
// read-through only. A cache write failure is intentionally NOT fatal
// — the primary bytes still flow back to the caller and the ingest
// keeps running. The failure is logged so the operator can address it.
//
// The sink exposes atomic hit/miss counters so the ingest orchestrator
// can report cache efficiency at the end of a run.
type CachingSink struct {
	primary Sink
	cache   *LocalFS

	// logger receives cache-write failures. Defaults to log.Default(),
	// so test code can inject a quieter sink.
	logger *log.Logger

	hits   atomic.Int64
	misses atomic.Int64
}

var _ Sink = (*CachingSink)(nil)

// NewCachingSink returns a CachingSink with the given primary and an
// on-disk cache rooted at the LocalFS. The logger is optional; if nil
// the standard logger is used.
func NewCachingSink(primary Sink, cache *LocalFS, logger *log.Logger) *CachingSink {
	if logger == nil {
		logger = log.Default()
	}
	return &CachingSink{primary: primary, cache: cache, logger: logger}
}

// Exists forwards to the primary. The cache is read-through only, so
// the authoritative existence answer always comes from upstream — an
// object deleted on R2 after being cached locally must still report
// false here so the ingest doesn't operate on a stale mirror.
func (c *CachingSink) Exists(ctx context.Context, addr Address) (bool, error) {
	return c.primary.Exists(ctx, addr)
}

// Put forwards to the primary. Ingest never writes, so this is mostly
// defensive for composability with snapshot-pull if it ever runs
// through a caching sink.
func (c *CachingSink) Put(ctx context.Context, addr Address, r io.Reader) (PutResult, error) {
	return c.primary.Put(ctx, addr, r)
}

// Get tries the cache first. On a hit, returns the local reader
// directly. On a miss, fetches from the primary, reads the full body
// into memory (CONAGUA files are kB-scale), writes it through to the
// cache atomically, and returns a reader over the in-memory copy.
//
// fs.ErrNotExist from the primary propagates unchanged so callers can
// match on it (ingest distinguishes "pull ledger said fetched, sink
// has nothing" from other errors).
func (c *CachingSink) Get(ctx context.Context, addr Address) (io.ReadCloser, error) {
	if rc, err := c.cache.Get(ctx, addr); err == nil {
		c.hits.Add(1)
		return rc, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		// Unexpected cache read error (permission, IO, etc.). Log and
		// fall through — the primary is still trustworthy.
		c.logger.Printf("cache read %s: %v; falling through to primary", c.cache.Path(addr), err)
	}

	// Miss: fetch, buffer, tee-into-cache.
	rc, err := c.primary.Get(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer rc.Close() //nolint:errcheck // read-side close; no recovery possible

	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read primary body %s: %w", c.cache.Path(addr), err)
	}
	c.misses.Add(1)

	if _, err := c.cache.Put(ctx, addr, bytes.NewReader(body)); err != nil {
		c.logger.Printf("cache write %s: %v; ingest continues", c.cache.Path(addr), err)
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

// Stats returns cumulative hit and miss counts since the sink was
// constructed. Safe to call at any time; counters are atomic.
func (c *CachingSink) Stats() (hits, misses int64) {
	return c.hits.Load(), c.misses.Load()
}

// FetchIndex forwards to the primary if the primary implements it.
// The cache is left alone: _index.json is not a per-file Address
// blob, so caching it is the orchestrator's responsibility (it knows
// where the metadata root is). If the primary doesn't support
// FetchIndex, returns an error so the caller knows to fall through.
func (c *CachingSink) FetchIndex(ctx context.Context, date string) ([]byte, error) {
	fetcher, ok := c.primary.(interface {
		FetchIndex(ctx context.Context, date string) ([]byte, error)
	})
	if !ok {
		return nil, errors.New("primary sink does not implement FetchIndex")
	}
	return fetcher.FetchIndex(ctx, date)
}
