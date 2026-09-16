// Package parity implements the value-level cross-snapshot comparison
// between two snapshots of the CONAGUA archive. Two snapshot date-dirs in the standard layout
// (<dir>/<kind>/<station>.txt) are compared through the
// internal/conagua parsers, field by field — never by raw bytes or
// sha256, because CONAGUA re-emits its archive under fresh EMISIÓN
// header stamps that the parsers exclude by construction.
//
// Kinds with a parser (daily and the four normals periods) are compared
// at parsed-value level; monthly and extremes have no parser in either
// build, so their files are counted for presence only and the report
// says so explicitly.
package parity

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// maxWorkers bounds the station-comparison pool. Comparison is local
// disk IO plus parsing; a small pool saturates it without swamping the
// machine.
const maxWorkers = 8

// valueKinds are the kinds compared at parsed-value level — exactly the
// kinds internal/conagua can parse.
var valueKinds = map[conagua.Kind]bool{
	conagua.KindDaily:            true,
	conagua.KindNormals1961_1990: true,
	conagua.KindNormals1971_2000: true,
	conagua.KindNormals1981_2010: true,
	conagua.KindNormals1991_2020: true,
}

// CompareSnapshots compares the snapshot at baseDir against the one at
// newDir and returns the full per-kind report. Both arguments are
// snapshot date-dirs (e.g. .../conagua-raw/2026-06-08). A file that
// fails to parse on either side is recorded as a finding in the report,
// not returned as an error; the error return is reserved for
// infrastructure failures (unreadable dirs, context cancellation) and
// for inputs that do not look like snapshot dirs at all.
func CompareSnapshots(ctx context.Context, baseDir, newDir string) (Report, error) {
	for _, dir := range []string{baseDir, newDir} {
		info, err := os.Stat(dir)
		if err != nil {
			return Report{}, fmt.Errorf("snapshot dir: %w", err)
		}
		if !info.IsDir() {
			return Report{}, fmt.Errorf("snapshot dir %s: not a directory", dir)
		}
	}

	report := Report{BaseDir: baseDir, NewDir: newDir}
	totalFiles := 0
	for _, kind := range conagua.AllKinds {
		if err := ctx.Err(); err != nil {
			return Report{}, err
		}
		kr, err := compareKind(ctx, kind, baseDir, newDir)
		if err != nil {
			return Report{}, fmt.Errorf("compare kind %s: %w", kind, err)
		}
		totalFiles += kr.BaseFiles + kr.NewFiles
		report.Kinds = append(report.Kinds, kr)
	}
	if totalFiles == 0 {
		return Report{}, fmt.Errorf("no station files under %s or %s (expected <dir>/<kind>/<station>.txt)", baseDir, newDir)
	}
	return report, nil
}

// compareKind builds the KindReport for one file kind. Presence-only
// kinds get universe accounting only; value kinds additionally parse
// and compare every station present on both sides.
func compareKind(ctx context.Context, kind conagua.Kind, baseDir, newDir string) (KindReport, error) {
	baseSet, err := listStations(filepath.Join(baseDir, string(kind)))
	if err != nil {
		return KindReport{}, err
	}
	newSet, err := listStations(filepath.Join(newDir, string(kind)))
	if err != nil {
		return KindReport{}, err
	}

	kr := KindReport{
		Kind:          kind,
		ValueCompared: valueKinds[kind],
		BaseFiles:     len(baseSet),
		NewFiles:      len(newSet),
	}
	union := sortedUnionKeys(baseSet, newSet)

	if !kr.ValueCompared {
		for _, st := range union {
			_, inBase := baseSet[st]
			_, inNew := newSet[st]
			switch {
			case inBase && inNew:
				kr.StationsBoth++
			case inBase:
				kr.BaseOnly.Add(st)
			default:
				kr.NewOnly.Add(st)
			}
		}
		return kr, nil
	}

	outcomes, err := mapStations(ctx, union, func(st string) stationOutcome {
		_, inBase := baseSet[st]
		_, inNew := newSet[st]
		return compareStation(kind, st, baseDir, newDir, inBase, inNew)
	})
	if err != nil {
		return KindReport{}, err
	}

	// Reduce sequentially in sorted station order so counts and capped
	// example lists are deterministic across runs.
	for i, st := range union {
		o := &outcomes[i]
		switch {
		case o.baseOnly:
			kr.BaseOnly.Add(st)
		case o.newOnly:
			kr.NewOnly.Add(st)
		default:
			kr.StationsBoth++
		}
		for _, pe := range o.parseErrors {
			kr.ParseErrors.Add(pe)
		}
		if !o.compared {
			continue
		}
		if o.identical {
			kr.StationsIdentical++
		} else {
			kr.StationsRevised++
		}
		kr.RowsIdentical += o.rowsIdentical
		kr.RowsRevised += o.rowsRevised
		kr.RowsAppended.Merge(o.appended)
		kr.RowsRemoved.Merge(o.removed)
		kr.Revisions.Merge(o.revisions)
		kr.HeaderDiffs.Merge(o.headerDiffs)
		if o.warningDiff != nil {
			kr.WarningDiffs.Add(*o.warningDiff)
		}
	}
	return kr, nil
}

// mapStations runs fn over stations with a bounded worker pool and
// returns the outcomes in input order. On context cancellation the
// remaining work is abandoned and the context's error is returned.
func mapStations(ctx context.Context, stations []string, fn func(string) stationOutcome) ([]stationOutcome, error) {
	workers := min(maxWorkers, runtime.GOMAXPROCS(0))

	outcomes := make([]stationOutcome, len(stations))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for i := range jobs {
				// Drain without working once cancelled, so the feeder
				// never blocks and Wait returns promptly.
				if ctx.Err() != nil {
					continue
				}
				outcomes[i] = fn(stations[i])
			}
		})
	}

feed:
	for i := range stations {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return outcomes, nil
}

// listStations enumerates the station IDs stored under one kind dir
// (the *.txt basenames). A missing dir is an empty set, not an error —
// absence is a legitimate universe finding (a snapshot may carry no
// monthly/extremes dirs at all). Stray
// entries (subdirs, a snapshot writer's temp residue, metadata files)
// are ignored.
func listStations(dir string) (map[string]struct{}, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("list %s: %w", dir, err)
	}
	stations := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".txt") || snapshot.IsTempResidue(name) {
			continue
		}
		stations[strings.TrimSuffix(name, ".txt")] = struct{}{}
	}
	return stations, nil
}

// sortedUnionKeys returns the sorted union of both maps' keys.
func sortedUnionKeys[V1, V2 any](a map[string]V1, b map[string]V2) []string {
	keys := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		keys[k] = struct{}{}
	}
	for k := range b {
		keys[k] = struct{}{}
	}
	return slices.Sorted(maps.Keys(keys))
}
