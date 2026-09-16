package cmd

import (
	"fmt"
	"io"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/fetcher"
)

// pullLog is pull's plain-log renderer: one line per Result on out
// (stdout), meta lines (plan summary, rest notices) on err (stderr).
// Result and plan-summary line formats are fixed verbatim so operator
// habits and any log-scraping keep working; the [rest …] notice makes
// the limiter's periodic rests visible on stderr.
type pullLog struct {
	out io.Writer
	err io.Writer

	total  int
	counts map[fetcher.Outcome]int

	// lastSinceRest holds the previous tick's WaitInfo.RequestsSinceRest —
	// the limiter's authoritative count of requests since the last rest.
	// The rest notice reports how much work preceded the pause, and the
	// rest-firing tick's own WaitInfo has already reset the count to 1, so
	// the renderer keeps the one value it needs from the tick before.
	lastSinceRest int
}

func newPullLog(out, err io.Writer) *pullLog {
	return &pullLog{out: out, err: err, counts: make(map[fetcher.Outcome]int)}
}

// plan prints the task-plan summary and pins the [idx/total] denominator
// used by every subsequent result line.
func (r *pullLog) plan(filesTotal, stations, alreadyDone int) {
	r.total = filesTotal
	fprintf(r.err,
		"planned %d files across %d stations (%d already terminal in ledger)\n\n",
		filesTotal, stations, alreadyDone)
}

// wait prints a one-line notice when a limiter rest fired. Ordinary
// inter-request waits are silent — one line per wait would flood the log.
func (r *pullLog) wait(info fetcher.WaitInfo) {
	if info.RestFired {
		fprintf(r.err, "[rest %s after %d requests]\n",
			info.Rest.Round(time.Second), r.lastSinceRest)
	}
	r.lastSinceRest = info.RequestsSinceRest
}

// result prints one line for a Task's final disposition and advances the
// running outcome tally.
func (r *pullLog) result(e fetcher.Result) {
	r.counts[e.Outcome]++
	idx := r.counts[fetcher.OutcomeFetched] +
		r.counts[fetcher.OutcomeSkipped] +
		r.counts[fetcher.OutcomeNotFound] +
		r.counts[fetcher.OutcomeError]
	prefix := fmt.Sprintf("[%4d/%4d]", idx, r.total)
	loc := fmt.Sprintf("%s/%s %-17s", e.Task.Station.State, e.Task.Station.ID, e.Task.Kind)

	retryTag := ""
	if e.Attempts > 1 {
		retryTag = fmt.Sprintf(" (attempts=%d)", e.Attempts)
	}
	elapsed := e.Elapsed.Round(time.Millisecond)

	switch e.Outcome {
	case fetcher.OutcomeFetched:
		sha := e.SHA256
		if len(sha) > 16 {
			sha = sha[:16]
		}
		fprintf(r.out, "%s %s OK  %7d B  %8s  sha256=%s%s\n",
			prefix, loc, e.Bytes, elapsed, sha, retryTag)
	case fetcher.OutcomeSkipped:
		fprintf(r.out, "%s %s SKIP (already present)\n", prefix, loc)
	case fetcher.OutcomeNotFound:
		fprintf(r.out, "%s %s 404 %8s%s\n", prefix, loc, elapsed, retryTag)
	case fetcher.OutcomeError:
		errMsg := ""
		if e.Err != nil {
			errMsg = e.Err.Error()
		}
		fprintf(r.out, "%s %s ERR %8s  %s%s\n",
			prefix, loc, elapsed, errMsg, retryTag)
	}
}
