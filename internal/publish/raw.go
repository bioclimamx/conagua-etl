package publish

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// RawArchiveName is the raw snapshot artifact's top-level name,
// conagua-raw-<snapshot_date>.zip.
func RawArchiveName(snapshotDate string) string {
	return "conagua-raw-" + snapshotDate + ".zip"
}

// RawSnapshotDir is the local snapshot directory the raw artifact ships:
// <root>/conagua-raw/<snapshotDate>/, as pull lays it out, from
// the one definition of that layout.
func RawSnapshotDir(root, snapshotDate string) string {
	return snapshot.NewLocalFS(root).SnapshotDir(snapshotDate)
}

// CheckRawSnapshot refuses a raw group whose snapshot directory is
// missing — naming the path and the flag that locates it — or is a
// symlink, or whose date is not a snapshot date. A run that includes
// the raw group calls it before anything is written; RawSnapshotEntries
// repeats it. The directory is examined without following a symlink,
// as the walk examines it: a symlinked snapshot directory would pass a
// following stat here and fail at the walk, after every state archive
// was built, and the pre-flight exists to refuse before that.
func CheckRawSnapshot(root, snapshotDate string) error {
	if _, err := time.Parse(snapshotDateLayout, snapshotDate); err != nil {
		return fmt.Errorf("snapshot date %q is not YYYY-MM-DD: %w", snapshotDate, err)
	}
	dir := RawSnapshotDir(root, snapshotDate)
	info, err := os.Lstat(dir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("snapshot directory %s does not exist: the raw artifact ships the local pull output "+
			"of snapshot %s (--root points at the local snapshot root)", dir, snapshotDate)
	case err != nil:
		return fmt.Errorf("snapshot directory %s: %w", dir, err)
	case info.Mode()&fs.ModeSymlink != 0:
		return fmt.Errorf("snapshot directory %s is a symlink, not a directory (point --root at the snapshot root it resolves under)", dir)
	case !info.IsDir():
		return fmt.Errorf("snapshot directory %s is not a directory", dir)
	}
	return nil
}

// RawSnapshotEntries lists the local snapshot directory of snapshotDate
// under root as archive entries, Path-sorted: every station file of
// every kind directory — all fetched kinds, the unparsed monthly/ and
// extremes/ included — and the two metadata files, _index.json and
// _progress.json, each under its path relative to the snapshot
// directory with forward slashes, streamed verbatim when written: the
// honest "txt" artifact is the pull output as pulled, CONAGUA-only, at
// the snapshot the shipped DB was ingested from. The walk is held to
// pull's layout: at the top level a
// kind directory (conagua.AllKinds) or one of the two metadata files,
// inside a kind directory a regular <station_id>.txt with no nesting.
// An entry the layout does not account for — a database parked beside
// _index.json, a directory of notes, a dotfile, a symlink or any other
// non-regular file, a writer's temp residue (snapshot.IsTempResidue: a
// partial body a crashed pull left) — is refused naming its path rather
// than skipped or shipped: the artifact claims to be CONAGUA's pull
// output and nothing else, a silently skipped file would be a lost one,
// and a silently shipped one a foreign file under CONAGUA's name. A
// kind directory that is missing or empty contributes nothing: the
// ledger records what was pulled, and presence is not the walker's to
// assert. A missing directory is refused as CheckRawSnapshot refuses
// it; a directory with no file at all is refused, since there is
// nothing to ship. ctx is checked at every directory, so an operator's
// interrupt lands within one directory listing.
func RawSnapshotEntries(ctx context.Context, root, snapshotDate string) ([]archive.Entry, error) {
	if err := CheckRawSnapshot(root, snapshotDate); err != nil {
		return nil, err
	}
	dir := RawSnapshotDir(root, snapshotDate)
	var entries []archive.Entry
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == dir {
			return ctx.Err()
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if d.IsDir() {
			if err := ctx.Err(); err != nil {
				return err
			}
			return checkSnapshotDir(name)
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("%s is %s, not a regular file", name, describeMode(d.Type()))
		}
		if snapshot.IsTempResidue(d.Name()) {
			return fmt.Errorf("%s is a writer's temp residue (a crashed pull; re-run pull to convergence or remove it)", name)
		}
		if err := checkSnapshotFile(name); err != nil {
			return err
		}
		entries = append(entries, rawFileEntry(name, p))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk snapshot directory %s: %w", dir, err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("snapshot directory %s holds no files", dir)
	}
	slices.SortFunc(entries, func(a, b archive.Entry) int { return strings.Compare(a.Path, b.Path) })
	return entries, nil
}

// checkSnapshotDir holds a directory below the snapshot directory to
// the layout: an immediate child named for a kind, nothing deeper.
func checkSnapshotDir(name string) error {
	if strings.Contains(name, "/") {
		return fmt.Errorf("%s is a directory inside a kind directory (the layout holds only <station_id>.txt files there)", name)
	}
	if !slices.Contains(conagua.AllKinds, conagua.Kind(name)) {
		return fmt.Errorf("%s is not a kind directory of the snapshot layout (%s)", name, kindNames())
	}
	return nil
}

// checkSnapshotFile holds a regular file below the snapshot directory
// to the layout: at the top level one of the two metadata files, inside
// a kind directory (already admitted by checkSnapshotDir) a
// <station_id>.txt — the station id opaque, non-empty, and never
// dot-prefixed, since no pull writer names a file so and a hidden file
// is an editor's or a system's, not CONAGUA's.
func checkSnapshotFile(name string) error {
	kind, base, inKind := strings.Cut(name, "/")
	if !inKind {
		if name == snapshot.IndexFile || name == snapshot.ProgressFile {
			return nil
		}
		return fmt.Errorf("%s is not part of the snapshot layout (a kind directory, %s, or %s)",
			name, snapshot.IndexFile, snapshot.ProgressFile)
	}
	if id, ok := strings.CutSuffix(base, ".txt"); !ok || id == "" || strings.HasPrefix(id, ".") {
		return fmt.Errorf("%s is not a station file of the %s kind (<station_id>.txt)", name, kind)
	}
	return nil
}

// kindNames lists the kind directory names for a refusal.
func kindNames() string {
	names := make([]string, len(conagua.AllKinds))
	for i, k := range conagua.AllKinds {
		names[i] = string(k)
	}
	return strings.Join(names, ", ")
}

// rawUnits marks the raw archive's progress units on entries — the
// Path-sorted list RawSnapshotEntries returns — and reports how many
// there are: one per immediate child of the snapshot directory, a kind
// directory (labelled "daily/", every station file of the kind) or a
// root file ("_index.json"), fired after the last entry under it in
// write order. Sorted paths keep a directory's entries contiguous, so
// the last entry under each child is the unit's completion. The twenty
// thousand station files earn no line each; the nine children are what
// an operator waits on. onUnit nil marks nothing.
func rawUnits(entries []archive.Entry, onUnit UnitFunc) int {
	last := map[string]int{}
	var order []string
	for i, e := range entries {
		label := e.Path
		if dir, _, found := strings.Cut(e.Path, "/"); found {
			label = dir + "/"
		}
		if _, seen := last[label]; !seen {
			order = append(order, label)
		}
		last[label] = i
	}
	if onUnit == nil {
		return len(order)
	}
	for _, label := range order {
		i := last[label]
		write := entries[i].Write
		entries[i].Write = func(w io.Writer) error {
			if err := write(w); err != nil {
				return err
			}
			onUnit(label)
			return nil
		}
	}
	return len(order)
}

// describeMode names a non-regular file's kind for the refusal.
func describeMode(m fs.FileMode) string {
	if m&fs.ModeSymlink != 0 {
		return "a symlink"
	}
	return "of mode " + m.Type().String()
}

// RawHTMLPages lists, Path-sorted, the station files of the snapshot
// directory whose body is an HTML page rather than CONAGUA's text. The
// SMN server answers some station requests with an error page and a
// success status, so pull records them as fetched and the raw artifact
// ships them as pulled; the README names them, and this
// is where that list comes from — read from the files at build time,
// because a re-pull may catch a different set. A station file is an
// HTML page when its first non-blank byte, after an optional UTF-8
// BOM, is '<': CONAGUA's text begins with its institutional banner.
// The two metadata files are not station files and are not examined.
func RawHTMLPages(ctx context.Context, root, snapshotDate string) ([]string, error) {
	entries, err := RawSnapshotEntries(ctx, root, snapshotDate)
	if err != nil {
		return nil, err
	}
	dir := RawSnapshotDir(root, snapshotDate)
	var pages []string
	for _, e := range entries {
		if !strings.Contains(e.Path, "/") {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		html, err := isHTMLPage(filepath.Join(dir, filepath.FromSlash(e.Path)))
		if err != nil {
			return nil, err
		}
		if html {
			pages = append(pages, e.Path)
		}
	}
	return pages, nil
}

// htmlSniffBytes bounds how much of a station file isHTMLPage reads:
// enough to pass any leading BOM and blank lines.
const htmlSniffBytes = 512

// isHTMLPage reports whether the file at path begins, past an optional
// UTF-8 BOM and blank space, with '<'.
func isHTMLPage(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, htmlSniffBytes)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return false, fmt.Errorf("read %s: %w", path, err)
	}
	head := bytes.TrimLeft(bytes.TrimPrefix(buf[:n], []byte("\xef\xbb\xbf")), " \t\r\n")
	return len(head) > 0 && head[0] == '<', nil
}

// ingestedKind reports whether ingest reads the files of kind k — the
// daily series and the four normals periods; monthly/ and extremes/ are
// pulled and shipped but feed no table.
func ingestedKind(k conagua.Kind) bool {
	return k == conagua.KindDaily || slices.Contains(conagua.NormalsKinds, k)
}

// rawFileEntry is one snapshot file, streamed verbatim when written.
func rawFileEntry(name, path string) archive.Entry {
	return archive.Entry{
		Path: name,
		Write: func(w io.Writer) error {
			return streamFile(w, path)
		},
	}
}
