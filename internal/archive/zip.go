package archive

import (
	"archive/zip"
	"compress/flate"
	"context"
	"fmt"
	"io"
	"strings"
	"time"
)

// ZipOptions pins the bits of a zip that would otherwise make its bytes
// drift between runs. Modified is stamped on every generated entry —
// archive/zip derives both the DOS timestamp and the extended-timestamp
// extra field from it — and is required, so a caller cannot leak the
// wall clock into an artifact by omission. A copied entry keeps its
// source's timestamp (MixedEntry).
type ZipOptions struct {
	Modified time.Time
}

// deflateLevel is the compression level every generated entry is written
// with. It is registered explicitly rather than left to the library
// default so the size/speed trade-off is visible here and an upstream
// default change cannot silently alter the bytes.
const deflateLevel = flate.DefaultCompression

// MixedEntry is one file of an archive WriteZipMixed writes, in one of
// two forms. Generated: Write streams the content and the writer
// Deflate-compresses it under the archive's pinned timestamp, exactly as
// WriteZip does. Copied: From is an entry of an opened source archive
// whose raw form — the compressed bytes, CRC-32, sizes, flags, method,
// timestamps, and extra fields — is transferred as is, bypassing
// decompression and recompression, so the copy's bytes, CRC, and
// compressed size are the source's and its Modified stays the source's.
// Exactly one of Write and From is set. Path is the in-archive name in
// both forms; for a copy it must equal From.Name, since a raw copy
// carries the source header and cannot rename. The source archive must
// stay open until the write completes: the copy reads it then. Written,
// if set, is called once the entry's content is in the archive,
// whichever the form — the hook a caller counts progress with for a
// copy, whose write path runs no caller code.
type MixedEntry struct {
	Path    string
	Write   func(w io.Writer) error
	From    *zip.File
	Written func()
}

// Generated is e as a MixedEntry.
func Generated(e Entry) MixedEntry {
	return MixedEntry{Path: e.Path, Write: e.Write}
}

// Copied is the MixedEntry that copies f raw, under f's own name.
func Copied(f *zip.File) MixedEntry {
	return MixedEntry{Path: f.Name, From: f}
}

// WriteZip writes entries to path atomically, in the order given (callers
// pass Path-sorted entries — the order is part of the byte contract),
// each Deflate-compressed with opts.Modified as its timestamp and no
// per-entry or archive comment. ctx is checked before each entry; a
// cancellation, a failing Entry.Write, or an invalid entry aborts the
// write, removes the temp file, and leaves path untouched. The returned
// Bytes and SHA256 cover the finished zip. It is WriteZipMixed over
// generated entries only.
func WriteZip(ctx context.Context, path string, entries []Entry, opts ZipOptions) (Result, error) {
	if err := validateEntries(entries); err != nil {
		return Result{}, fmt.Errorf("write zip %s: %w", path, err)
	}
	mixed := make([]MixedEntry, len(entries))
	for i, e := range entries {
		mixed[i] = Generated(e)
	}
	return writeZip(ctx, path, mixed, opts)
}

// WriteZipMixed writes entries to path atomically, in the order given
// (Path-sorted by the caller), each generated entry Deflate-compressed
// under opts.Modified and each copied entry transferred raw from its
// source archive, with no per-entry or archive comment. ctx is checked
// before each entry; a cancellation, a failing Write or copy, or an
// invalid entry — a duplicate path across either form included — aborts
// the write, removes the temp file, and leaves path untouched. The
// source archives are only ever read. The returned Bytes and SHA256
// cover the finished zip.
func WriteZipMixed(ctx context.Context, path string, entries []MixedEntry, opts ZipOptions) (Result, error) {
	if err := validateMixed(entries); err != nil {
		return Result{}, fmt.Errorf("write zip %s: %w", path, err)
	}
	return writeZip(ctx, path, entries, opts)
}

// writeZip is the write both surfaces share, over an entry list already
// validated for its form.
func writeZip(ctx context.Context, path string, entries []MixedEntry, opts ZipOptions) (Result, error) {
	if opts.Modified.IsZero() {
		return Result{}, fmt.Errorf("write zip %s: ZipOptions.Modified is required", path)
	}
	if err := ctx.Err(); err != nil {
		return Result{}, fmt.Errorf("write zip %s: %w", path, err)
	}
	modified := opts.Modified.UTC()

	res, err := writeAtomic(path, func(w io.Writer) error {
		zw := zip.NewWriter(w)
		zw.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
			return flate.NewWriter(out, deflateLevel)
		})
		for _, e := range entries {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("entry %q: %w", e.Path, err)
			}
			if err := writeEntry(zw, e, modified); err != nil {
				return fmt.Errorf("entry %q: %w", e.Path, err)
			}
			if e.Written != nil {
				e.Written()
			}
		}
		if err := zw.Close(); err != nil {
			return fmt.Errorf("close zip: %w", err)
		}
		return nil
	})
	if err != nil {
		return Result{}, fmt.Errorf("write zip %s: %w", path, err)
	}
	res.Entries = len(entries)
	return res, nil
}

// writeEntry puts one entry into zw: a copy transfers the source's raw
// record, a generated entry streams through a fresh Deflate header
// stamped with the archive's timestamp.
func writeEntry(zw *zip.Writer, e MixedEntry, modified time.Time) error {
	if e.From != nil {
		if err := zw.Copy(e.From); err != nil {
			return fmt.Errorf("copy: %w", err)
		}
		return nil
	}
	ew, err := zw.CreateHeader(&zip.FileHeader{
		Name:     e.Path,
		Method:   zip.Deflate,
		Modified: modified,
	})
	if err != nil {
		return fmt.Errorf("create header: %w", err)
	}
	return e.Write(ew)
}

// validateEntries rejects, before any byte is written, a generated entry
// set a release artifact must never carry: an invalid path (validatePath)
// or an entry with nothing to write.
func validateEntries(entries []Entry) error {
	seen := make(map[string]struct{}, len(entries))
	for i, e := range entries {
		if err := validatePath(i, e.Path, seen); err != nil {
			return err
		}
		if e.Write == nil {
			return fmt.Errorf("entry %q: nil Write", e.Path)
		}
	}
	return nil
}

// validateMixed is validateEntries for a mixed set: the same path rules
// over both forms — so a copy and a generated entry cannot share a name
// — plus the form rules: exactly one of Write and From, and a copy
// named as its source is.
func validateMixed(entries []MixedEntry) error {
	seen := make(map[string]struct{}, len(entries))
	for i, e := range entries {
		if err := validatePath(i, e.Path, seen); err != nil {
			return err
		}
		switch {
		case e.Write == nil && e.From == nil:
			return fmt.Errorf("entry %q: neither Write nor From", e.Path)
		case e.Write != nil && e.From != nil:
			return fmt.Errorf("entry %q: both Write and From", e.Path)
		case e.From != nil && e.From.Name != e.Path:
			return fmt.Errorf("entry %q: copied from %q (a raw copy keeps the source name)", e.Path, e.From.Name)
		}
	}
	return nil
}

// validatePath rejects the names archive/zip would accept but a release
// artifact must never carry: an absolute or parent-escaping path (a
// zip-slip hazard for whoever extracts it), a backslash (not a zip
// separator; ambiguous on Windows), a directory entry (this seam models
// files only), and a duplicate (a second entry with the same name
// shadows the first in most extractors). seen accumulates the names
// accepted so far.
func validatePath(i int, path string, seen map[string]struct{}) error {
	switch {
	case path == "":
		return fmt.Errorf("entry %d: empty path", i)
	case strings.HasPrefix(path, "/"):
		return fmt.Errorf("entry %q: leading slash", path)
	case strings.HasSuffix(path, "/"):
		return fmt.Errorf("entry %q: trailing slash (directory entries are not supported)", path)
	case strings.Contains(path, `\`):
		return fmt.Errorf("entry %q: backslash in path", path)
	}
	for _, seg := range strings.Split(path, "/") {
		if seg == ".." {
			return fmt.Errorf("entry %q: parent segment", path)
		}
	}
	if _, dup := seen[path]; dup {
		return fmt.Errorf("entry %q: duplicate path", path)
	}
	seen[path] = struct{}{}
	return nil
}
