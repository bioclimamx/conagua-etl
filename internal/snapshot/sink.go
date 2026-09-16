// Package snapshot persists raw CONAGUA files and run-time state for a
// snapshot. The Sink interface abstracts the storage backend so the same
// orchestrator/fetcher code can target a local filesystem or Cloudflare
// R2 interchangeably.
package snapshot

import (
	"context"
	"io"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

// Address is the logical location of one raw CONAGUA file inside a snapshot.
// Sinks map it to a concrete backend path (e.g. a filesystem path for
// LocalFS, an object key for R2).
type Address struct {
	// Date is the snapshot date in "YYYY-MM-DD" form. It groups all files
	// pulled in one run together and keys the immutable snapshot prefix on
	// R2.
	Date string

	// Kind is which of the 7 CONAGUA file types this is.
	Kind conagua.Kind

	// StationID is the 5-digit zero-padded station ID from the URL.
	StationID string
}

// PutResult reports the durable state of a file after it was written.
type PutResult struct {
	// Bytes is the total number of bytes written to the sink.
	Bytes int64

	// SHA256 is the lowercase hex digest of the bytes written. Computed
	// during Put so we don't re-read the file.
	SHA256 string
}

// Sink persists raw CONAGUA files for one or more snapshot dates. All
// methods are safe for concurrent callers unless a specific implementation
// documents otherwise.
type Sink interface {
	// Exists reports whether addr is already stored. The fetcher uses this
	// for cheap resumption: on a re-run, a file that's already on disk is
	// skipped without re-fetching.
	Exists(ctx context.Context, addr Address) (bool, error)

	// Put stores the contents of r at addr. The returned PutResult
	// captures the final size and SHA-256 digest.
	//
	// Implementations should write atomically: a Put that fails mid-stream
	// must not leave a partially-written blob at addr (so Exists stays
	// honest across interrupted runs).
	Put(ctx context.Context, addr Address, r io.Reader) (PutResult, error)

	// Get returns a reader over the bytes stored at addr. Callers must
	// Close the reader when done.
	//
	// If addr does not exist, implementations return an error that
	// errors.Is(err, fs.ErrNotExist) reports true, so the ingest code can
	// treat local-filesystem and R2 misses uniformly.
	Get(ctx context.Context, addr Address) (io.ReadCloser, error)
}
