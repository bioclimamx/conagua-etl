// Package archive is the deterministic, atomic artifact-writing seam the
// publish verb builds on. It knows nothing about the database or the
// artifact set: callers stream content into zip entries or plain files,
// and the package guarantees the two properties a release depends on.
//
// Every durable file is written to a temp sibling and renamed into place,
// so a crash or an error mid-write never leaves a partial file at the
// final path. And a zip built from the same entries and options is
// byte-identical on every run,
// because every timestamp and layout choice that would otherwise drift —
// entry modification times, compression level, comments — is pinned
// here rather than left to the library or the wall clock.
package archive

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Entry is one file inside an archive. Path is the in-archive name,
// forward-slash separated and relative. Write streams the entry's content
// to w; it is called exactly once, in the order entries are passed, and
// must not retain w past its return.
type Entry struct {
	Path  string
	Write func(w io.Writer) error
}

// Result describes a written artifact: its size, the sha256 of its bytes
// as they reached disk, and — for archives — the number of entries. Bytes
// and SHA256 are computed from the same stream the file was written from,
// so filling CHECKSUMS needs no second pass over the file.
type Result struct {
	Bytes   int64
	SHA256  string
	Entries int
}

// WriteFileAtomic writes a single file at path by streaming write into a
// temp sibling and renaming it into place, so a failure mid-write never
// leaves a partial file at path. The parent directory is created if
// missing; an existing file at path is replaced. Result.Entries is zero
// for a plain file.
func WriteFileAtomic(path string, write func(w io.Writer) error) (Result, error) {
	if write == nil {
		return Result{}, fmt.Errorf("write file %s: nil write function", path)
	}
	res, err := writeAtomic(path, write)
	if err != nil {
		return Result{}, fmt.Errorf("write file %s: %w", path, err)
	}
	return res, nil
}

// SHA256File hashes the file at path, returning the lowercase hex digest
// and the byte count — the two values a CHECKSUMS line and the manifest
// need for an artifact produced outside this package.
func SHA256File(path string) (hexsum string, bytes int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, fmt.Errorf("sha256 %s: %w", path, err)
	}
	defer f.Close() //nolint:errcheck // read-side close; no recovery possible

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, fmt.Errorf("sha256 %s: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// writeAtomic is the temp-sibling + rename discipline shared by WriteZip
// and WriteFileAtomic. The temp file lives in path's own directory (a
// rename across filesystems is a copy, not an atomic swap) under a
// dot-prefixed name, so a leftover from a crashed run never surfaces in a
// plain glob of the deposit directory. The bytes are hashed and counted
// as they are written. On any failure the temp file is removed and path
// is left exactly as it was.
//
// The error from write is returned unwrapped; callers add the operation
// and path, and write's own errors already name the failing unit.
func writeAtomic(path string, write func(w io.Writer) error) (Result, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Result{}, fmt.Errorf("mkdir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return Result{}, fmt.Errorf("create tmp: %w", err)
	}
	tmpPath := tmp.Name()
	discard := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}

	h := sha256.New()
	cw := &countingWriter{w: io.MultiWriter(tmp, h)}
	if err := write(cw); err != nil {
		discard()
		return Result{}, err
	}
	// Flush to stable storage before the rename: without it a crash after
	// the rename can leave a zero-length file at the final path on some
	// filesystems — exactly the partial file the rename is meant to
	// rule out. The cost is one fsync per artifact, not per entry.
	if err := tmp.Sync(); err != nil {
		discard()
		return Result{}, fmt.Errorf("sync tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return Result{}, fmt.Errorf("close tmp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return Result{}, fmt.Errorf("rename: %w", err)
	}
	return Result{Bytes: cw.n, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

// countingWriter tallies the bytes that reach the underlying writer so
// Result.Bytes reflects what is on disk, not what the caller offered.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	if err != nil {
		return n, err
	}
	if n < len(p) {
		return n, io.ErrShortWrite
	}
	return n, nil
}
