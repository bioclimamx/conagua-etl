package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/minio/minio-go/v7"
)

// IndexFile is the terminal manifest's file name at the root of a
// snapshot directory; ProgressFile the live ledger's. With the kind
// directories they are the whole top level of a snapshot, and what the
// raw release artifact ships beside the station files.
const (
	IndexFile    = "_index.json"
	ProgressFile = "_progress.json"
)

// IndexPath returns the canonical _index.json path for a snapshot date
// under a LocalFS sink's root.
func IndexPath(sink *LocalFS, date string) string {
	return filepath.Join(sink.SnapshotDir(date), IndexFile)
}

// WriteIndex atomically writes a terminal snapshot manifest.
//
// "Terminal" means the live-run field LastFlush is stripped, Counts are
// recomputed from stations, and the station list is sorted deterministically
// (by state then ID) so diffs between successive releases are meaningful.
//
// The write uses a TempSuffix sibling + os.Rename to stay atomic against
// concurrent readers (snapshot status).
func WriteIndex(path string, p Progress) error {
	p.LastFlush = time.Time{}
	p.Counts = DeriveCounts(p)
	sort.Slice(p.Stations, func(i, j int) bool {
		if p.Stations[i].State != p.Stations[j].State {
			return p.Stations[i].State < p.Stations[j].State
		}
		return p.Stations[i].ID < p.Stations[j].ID
	})

	payload, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("write index %s: %w", path, err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("write index %s: %w", path, err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+TempSuffix+"*")
	if err != nil {
		return fmt.Errorf("write index %s: %w", path, err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write index %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write index %s: %w", path, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write index %s: %w", path, err)
	}
	return nil
}

// MirrorIndexToSink uploads a terminal _index.json to the Sink so that
// the bucket carries the manifest alongside the raw files. Without
// this, an R2-only consumer (e.g. a fresh clone that wants to ingest)
// has bytes but no catalogue of what's in them.
//
// LocalFS doesn't need a mirror: snapshot_pull already wrote the file
// to the LocalFS root via WriteIndex. For R2 we PUT the same payload
// (applying the same Progress normalisation WriteIndex does, so the
// local and remote copies are byte-identical) to the key
//
//	conagua-raw/<date>/_index.json
//
// with Cache-Control off — the object is immutable per date, but
// re-runs of a pull can overwrite it (new stations, retried errors),
// so we don't want aggressive edge caching while the snapshot layer
// is still iterating. A future publish step would emit a separate,
// immutable data-release manifest.
func MirrorIndexToSink(ctx context.Context, s Sink, date string, p Progress) error {
	r2, ok := s.(*R2Sink)
	if !ok {
		return nil
	}

	// Same normalisation WriteIndex does — strip live-only fields,
	// recompute counts, sort stations — so the local and remote copies
	// can be compared cheaply if we ever need to audit drift.
	p.LastFlush = time.Time{}
	p.Counts = DeriveCounts(p)
	sort.Slice(p.Stations, func(i, j int) bool {
		if p.Stations[i].State != p.Stations[j].State {
			return p.Stations[i].State < p.Stations[j].State
		}
		return p.Stations[i].ID < p.Stations[j].ID
	})

	payload, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal index: %w", err)
	}

	key := fmt.Sprintf("conagua-raw/%s/_index.json", date)
	// bytes.Reader so minio-go can Seek(0) on an internal retry — same
	// fix that went into R2Sink.Put for station files.
	_, err = r2.Client().PutObject(ctx, r2.Bucket(), key,
		bytes.NewReader(payload), int64(len(payload)),
		minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return fmt.Errorf("r2 put %s: %w", key, err)
	}
	return nil
}

// LoadProgressFile reads a Progress document from path. Used by both
// `snapshot list` (over _progress.json or _index.json) and `snapshot
// status`. Returns fs.ErrNotExist-wrapped error when path is missing.
func LoadProgressFile(path string) (Progress, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Progress{}, err
	}
	var p Progress
	if err := json.Unmarshal(data, &p); err != nil {
		return Progress{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return p, nil
}

// LoadSnapshot reads either _index.json (preferred) or _progress.json for
// the given date under root. Returns the Progress, which file it came
// from ("index" or "progress"), and an error.
//
// If neither file exists but the directory itself does, returns an empty
// Progress with source="empty" and nil error — this is how list can still
// show a half-created snapshot dir.
func LoadSnapshot(root *LocalFS, date string) (p Progress, source string, err error) {
	dir := root.SnapshotDir(date)

	indexPath := filepath.Join(dir, IndexFile)
	if _, statErr := os.Stat(indexPath); statErr == nil {
		p, err = LoadProgressFile(indexPath)
		return p, "index", err
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return Progress{}, "", statErr
	}

	progressPath := filepath.Join(dir, ProgressFile)
	if _, statErr := os.Stat(progressPath); statErr == nil {
		p, err = LoadProgressFile(progressPath)
		return p, "progress", err
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return Progress{}, "", statErr
	}

	if _, statErr := os.Stat(dir); statErr == nil {
		return Progress{SnapshotDate: date}, "empty", nil
	}
	return Progress{}, "", fs.ErrNotExist
}

// ListSnapshotDates returns the snapshot date strings under root, newest
// first. A "snapshot" is any immediate subdirectory of root/conagua-raw/.
func ListSnapshotDates(root *LocalFS) ([]string, error) {
	dir := filepath.Join(root.Root, rawPrefix)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		out = append(out, e.Name())
	}
	sort.Sort(sort.Reverse(sort.StringSlice(out)))
	return out, nil
}
