package snapshot

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

// FileOutcome is a file's terminal state in the ledger. It mirrors
// fetcher.Outcome but is defined here so the snapshot package doesn't have
// to import fetcher (which would create a cycle).
type FileOutcome string

// The file outcomes. Pending is the only non-terminal state; Error is
// terminal for normal resumption but re-attemptable under --retry-failed
// (see Terminal and ShouldSkip). The string values are load-bearing: they
// are written verbatim into _progress.json / _index.json.
const (
	OutcomePending  FileOutcome = "pending"
	OutcomeFetched  FileOutcome = "fetched"
	OutcomeSkipped  FileOutcome = "skipped_existing"
	OutcomeNotFound FileOutcome = "not_found"
	OutcomeError    FileOutcome = "error"
)

// Terminal reports whether o is a "done" state (no more attempts should be
// made under normal resumption).
func (o FileOutcome) Terminal() bool {
	switch o {
	case OutcomeFetched, OutcomeSkipped, OutcomeNotFound:
		return true
	case OutcomeError:
		return true // terminal unless --retry-failed
	default:
		return false
	}
}

// FileState is the ledger entry for one (station, kind) pair.
type FileState struct {
	URL       string      `json:"url"`
	Outcome   FileOutcome `json:"outcome"`
	HTTPCode  int         `json:"http_code,omitempty"`
	Bytes     int64       `json:"bytes,omitempty"`
	SHA256    string      `json:"sha256,omitempty"`
	Attempts  int         `json:"attempts,omitempty"`
	LastError string      `json:"last_error,omitempty"`
	ElapsedMS int64       `json:"elapsed_ms,omitempty"`
	UpdatedAt time.Time   `json:"updated_at,omitzero"`
}

// StationProgress is the ledger entry for one station.
type StationProgress struct {
	State        conagua.StateCode          `json:"state"`
	ID           string                     `json:"id"`
	Name         string                     `json:"name"`
	Municipality string                     `json:"municipality"`
	Status       conagua.Status             `json:"status"`
	Files        map[conagua.Kind]FileState `json:"files"`
}

// CatalogSummary captures what discovery found before the fetch loop ran.
type CatalogSummary struct {
	StatesDiscovered   int `json:"states_discovered"`
	StationsDiscovered int `json:"stations_discovered"`
}

// Counts is the running tally of file-level outcomes.
type Counts struct {
	FilesExpected int `json:"files_expected"`
	FilesFetched  int `json:"files_fetched"`
	FilesSkipped  int `json:"files_skipped_existing"`
	FilesNotFound int `json:"files_missing"`
	FilesErrored  int `json:"http_errors"`
	FilesPending  int `json:"files_pending"`
}

// Progress is the full ledger document. Its shape is a superset of the
// eventual `_index.json`: at end-of-run a terminal Progress becomes an
// index file by dropping the live-run fields.
type Progress struct {
	SnapshotDate string    `json:"snapshot_date"`
	StartedAt    time.Time `json:"started_at"`
	LastFlush    time.Time `json:"last_flush,omitzero"`
	CompletedAt  time.Time `json:"completed_at,omitzero"`

	RNGSeed       uint64          `json:"rng_seed"`
	RateConfig    json.RawMessage `json:"rate_config,omitempty"`
	RetryPolicy   json.RawMessage `json:"retry_policy,omitempty"`
	ETLGitSHA     string          `json:"etl_git_sha,omitempty"`
	SchemaVersion int             `json:"schema_version"`

	Catalog  CatalogSummary    `json:"catalog"`
	Counts   Counts            `json:"counts"`
	Stations []StationProgress `json:"stations"`
}

// ProgressSchemaVersion bumps whenever the Progress JSON layout changes in
// a way that existing files wouldn't round-trip.
const ProgressSchemaVersion = 1

// FileUpdate is the input to Ledger.RecordFile. Callers build this from
// their own result types (e.g. fetcher.Result).
type FileUpdate struct {
	StationID string
	Kind      conagua.Kind
	URL       string
	Outcome   FileOutcome
	HTTPCode  int
	Bytes     int64
	SHA256    string
	Attempts  int
	Err       error
	Elapsed   time.Duration
}

// Ledger is the mutable, periodically-flushed Progress document for one
// snapshot run. Safe for concurrent use.
type Ledger struct {
	path string

	mu  sync.Mutex
	p   Progress
	idx map[string]int // station ID → index in p.Stations
}

// LoadOrInit opens an existing ledger at path or initialises a fresh one
// seeded from init (snapshot date, seed, configs). init.Stations is ignored
// if the file already exists; the on-disk station list is preserved so
// resumption keeps prior terminal states.
func LoadOrInit(path string, init Progress) (*Ledger, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			init.SchemaVersion = ProgressSchemaVersion
			if init.StartedAt.IsZero() {
				init.StartedAt = time.Now().UTC()
			}
			if init.Stations == nil {
				init.Stations = []StationProgress{}
			}
			l := newLedgerFromProgress(path, init)
			return l, nil
		}
		return nil, fmt.Errorf("open ledger: %w", err)
	}
	defer f.Close() //nolint:errcheck // read-side close; no recovery possible

	var existing Progress
	if err := json.NewDecoder(f).Decode(&existing); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	if existing.SchemaVersion != ProgressSchemaVersion {
		return nil, fmt.Errorf("schema %d in %s; expected %d",
			existing.SchemaVersion, path, ProgressSchemaVersion)
	}
	// Resume: keep station state, refresh configs/seed from init if present
	// (caller can pass zero init to mean "use existing").
	if init.RNGSeed != 0 {
		existing.RNGSeed = init.RNGSeed
	}
	if len(init.RateConfig) > 0 {
		existing.RateConfig = init.RateConfig
	}
	if len(init.RetryPolicy) > 0 {
		existing.RetryPolicy = init.RetryPolicy
	}
	if init.ETLGitSHA != "" {
		existing.ETLGitSHA = init.ETLGitSHA
	}
	return newLedgerFromProgress(path, existing), nil
}

func newLedgerFromProgress(path string, p Progress) *Ledger {
	l := &Ledger{
		path: path,
		p:    p,
		idx:  make(map[string]int, len(p.Stations)),
	}
	for i, s := range p.Stations {
		l.idx[s.ID] = i
	}
	return l
}

// UpsertStation ensures a station entry exists and its metadata and URL
// list match the current catalog. Files that are already present retain
// their state; newly-discovered kinds are added as pending.
func (l *Ledger) UpsertStation(s conagua.Station) {
	l.mu.Lock()
	defer l.mu.Unlock()

	i, ok := l.idx[s.ID]
	if !ok {
		sp := StationProgress{
			State:        s.State,
			ID:           s.ID,
			Name:         s.Name,
			Municipality: s.Municipality,
			Status:       s.Status,
			Files:        make(map[conagua.Kind]FileState, len(s.Files)),
		}
		for k, f := range s.Files {
			sp.Files[k] = FileState{URL: f.URL, Outcome: OutcomePending}
		}
		l.p.Stations = append(l.p.Stations, sp)
		l.idx[s.ID] = len(l.p.Stations) - 1
		return
	}
	// Existing station — refresh metadata but preserve per-file state.
	sp := &l.p.Stations[i]
	sp.State = s.State
	sp.Name = s.Name
	sp.Municipality = s.Municipality
	sp.Status = s.Status
	if sp.Files == nil {
		sp.Files = make(map[conagua.Kind]FileState, len(s.Files))
	}
	for k, f := range s.Files {
		if _, ok := sp.Files[k]; !ok {
			sp.Files[k] = FileState{URL: f.URL, Outcome: OutcomePending}
		} else {
			st := sp.Files[k]
			if st.URL == "" {
				st.URL = f.URL
			}
			sp.Files[k] = st
		}
	}
}

// Lookup returns the FileState for (stationID, kind).
func (l *Ledger) Lookup(stationID string, kind conagua.Kind) (FileState, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	i, ok := l.idx[stationID]
	if !ok {
		return FileState{}, false
	}
	st, ok := l.p.Stations[i].Files[kind]
	return st, ok
}

// RecordFile updates the FileState for one file.
func (l *Ledger) RecordFile(u FileUpdate) {
	l.mu.Lock()
	defer l.mu.Unlock()
	i, ok := l.idx[u.StationID]
	if !ok {
		return
	}
	sp := &l.p.Stations[i]
	if sp.Files == nil {
		sp.Files = make(map[conagua.Kind]FileState)
	}
	st := sp.Files[u.Kind]
	if u.URL != "" {
		st.URL = u.URL
	}
	st.Outcome = u.Outcome
	if u.HTTPCode != 0 {
		st.HTTPCode = u.HTTPCode
	}
	if u.Bytes != 0 {
		st.Bytes = u.Bytes
	}
	if u.SHA256 != "" {
		st.SHA256 = u.SHA256
	}
	if u.Attempts != 0 {
		st.Attempts = u.Attempts
	}
	if u.Err != nil {
		st.LastError = u.Err.Error()
	} else if u.Outcome != OutcomeError {
		st.LastError = ""
	}
	st.ElapsedMS = u.Elapsed.Milliseconds()
	st.UpdatedAt = time.Now().UTC()
	sp.Files[u.Kind] = st
}

// SetCatalogSummary records the discovery totals.
func (l *Ledger) SetCatalogSummary(cs CatalogSummary) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.p.Catalog = cs
}

// MarkCompleted stamps CompletedAt.
func (l *Ledger) MarkCompleted() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.p.CompletedAt = time.Now().UTC()
}

// Snapshot returns a defensive copy of the current Progress for read-only
// consumption (status command, final index emission, tests).
func (l *Ledger) Snapshot() Progress {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := l.p
	cp.Stations = make([]StationProgress, len(l.p.Stations))
	for i, s := range l.p.Stations {
		cp.Stations[i] = s
		cp.Stations[i].Files = make(map[conagua.Kind]FileState, len(s.Files))
		maps.Copy(cp.Stations[i].Files, s.Files)
	}
	return cp
}

// Flush atomically writes the ledger to path. The write is always performed
// even if no changes happened — callers can trust that after Flush returns
// nil, the on-disk file is current.
func (l *Ledger) Flush() error {
	l.mu.Lock()
	l.p.LastFlush = time.Now().UTC()
	l.p.Counts = DeriveCounts(l.p)
	payload, err := json.MarshalIndent(l.p, "", "  ")
	l.mu.Unlock()
	if err != nil {
		return fmt.Errorf("flush %s: %w", l.path, err)
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return fmt.Errorf("flush %s: %w", l.path, err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(l.path), filepath.Base(l.path)+TempSuffix+"*")
	if err != nil {
		return fmt.Errorf("flush %s: %w", l.path, err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("flush %s: %w", l.path, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("flush %s: %w", l.path, err)
	}
	if err := os.Rename(tmpPath, l.path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("flush %s: %w", l.path, err)
	}
	return nil
}

// Path returns the on-disk ledger path.
func (l *Ledger) Path() string { return l.path }

// ProgressPath returns the canonical _progress.json path for a LocalFS
// sink and a snapshot date.
func ProgressPath(sink *LocalFS, date string) string {
	return filepath.Join(sink.SnapshotDir(date), ProgressFile)
}

// ShouldSkip returns true when the ledger records a terminal outcome for
// (stationID, kind). retryErrors lets the caller treat OutcomeError as
// non-terminal (for --retry-failed).
func (l *Ledger) ShouldSkip(stationID string, kind conagua.Kind, retryErrors bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	i, ok := l.idx[stationID]
	if !ok {
		return false
	}
	st, ok := l.p.Stations[i].Files[kind]
	if !ok {
		return false
	}
	if st.Outcome == OutcomeError {
		return !retryErrors
	}
	return st.Outcome.Terminal()
}

// DeriveCounts walks stations and computes aggregate counts. Flush calls
// this automatically so the on-disk counts stay current.
func DeriveCounts(p Progress) Counts {
	var c Counts
	for _, s := range p.Stations {
		for _, f := range s.Files {
			c.FilesExpected++
			switch f.Outcome {
			case OutcomeFetched:
				c.FilesFetched++
			case OutcomeSkipped:
				c.FilesSkipped++
			case OutcomeNotFound:
				c.FilesNotFound++
			case OutcomeError:
				c.FilesErrored++
			default:
				c.FilesPending++
			}
		}
	}
	return c
}
