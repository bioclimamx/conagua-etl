package cmd

import (
	"fmt"
	"io"
	"sync"
)

// mirrorLog is mirror's plain-log renderer: one line per file on out
// (stdout), meta lines (plan summary) on err (stderr). Line shapes follow
// pull's renderer with a 5-wide index (a national snapshot runs to ~23k
// files) and no elapsed column — sink transfers are uniform enough that
// per-file timing is noise. Unlike pull's synchronous hooks, results
// arrive from a worker pool, so the renderer serialises internally.
type mirrorLog struct {
	out io.Writer
	err io.Writer

	mu         sync.Mutex
	total      int
	done       int
	mirrored   int
	skipped    int
	errored    int
	mismatches int // subset of errored: local files contradicting the index
}

func newMirrorLog(out, err io.Writer) *mirrorLog {
	return &mirrorLog{out: out, err: err}
}

// plan prints the work-list summary and pins the [idx/total] denominator
// used by every subsequent result line.
func (r *mirrorLog) plan(files, stations int) {
	r.total = files
	fprintf(r.err, "planned %d fetched files across %d stations\n\n", files, stations)
}

// result prints one line for a file's disposition and advances the tally.
func (r *mirrorLog) result(e mirrorEntry, st mirrorStatus, bytes int64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done++
	prefix := fmt.Sprintf("[%5d/%5d]", r.done, r.total)
	loc := fmt.Sprintf("%s/%s %-17s", e.State, e.StationID, e.Kind)

	switch st {
	case mirrorStatusFetched:
		r.mirrored++
		fprintf(r.out, "%s %s OK   %7d B\n", prefix, loc, bytes)
	case mirrorStatusVerified:
		r.skipped++
		fprintf(r.out, "%s %s SKIP %7d B  (verified)\n", prefix, loc, bytes)
	case mirrorStatusMismatch:
		r.errored++
		r.mismatches++
		fprintf(r.out, "%s %s ERR  %s\n", prefix, loc, errText(err))
	case mirrorStatusError:
		r.errored++
		fprintf(r.out, "%s %s ERR  %s\n", prefix, loc, errText(err))
	}
}

// totals returns the final tallies for the end-of-run summary.
func (r *mirrorLog) totals() (mirrored, skipped, errored, mismatches int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mirrored, r.skipped, r.errored, r.mismatches
}

// errText renders an error for a result line; per-file statuses carry the
// message in err, which is nil for the success shapes.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
