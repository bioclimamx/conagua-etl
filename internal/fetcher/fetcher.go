// Package fetcher pulls CONAGUA station files sequentially into a Sink.
//
// The fetcher consumes a flat list of Tasks and processes them one at a
// time on the calling goroutine — there is no worker pool. Politeness to
// CONAGUA's SMN is the binding constraint, so a single sequential stream
// is the only request source and the rate Limiter is the sole arbiter of
// pacing. Progress surfaces through nil-safe Hooks invoked synchronously
// from the dispatch loop; context cancellation is the only stop mechanism.
//
// Resumption is not the fetcher's job: the Sink.Exists short-circuit skips
// already-stored files, and the caller pre-filters the task list through
// the snapshot ledger, so a re-run simply produces a shorter task list.
package fetcher

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

// Task is one unit of work: fetch a specific file for a specific station
// and persist it at the derived sink address.
type Task struct {
	Station conagua.Station
	Kind    conagua.Kind
	URL     string
}

// Address returns the sink address this Task will write to on the given
// snapshot date.
func (t Task) Address(date string) snapshot.Address {
	return snapshot.Address{
		Date:      date,
		Kind:      t.Kind,
		StationID: t.Station.ID,
	}
}

// Outcome categorises the final state of a Task after the fetcher handled it.
// The string values match snapshot.FileOutcome, so the translation into a
// ledger record is a plain cast.
type Outcome string

// The four terminal states. Fetched and Skipped mean the file is in the
// sink; NotFound is success-shaped (the pair doesn't exist upstream);
// Error means retries were exhausted or the failure was terminal.
const (
	OutcomeFetched  Outcome = "fetched"
	OutcomeSkipped  Outcome = "skipped_existing"
	OutcomeNotFound Outcome = "not_found"
	OutcomeError    Outcome = "error"
)

// Result is the final disposition of one Task after all retries —
// everything a ledger record and a log line need.
type Result struct {
	Task        Task
	Outcome     Outcome
	Status      int // last HTTP status observed; 0 if no response arrived
	Attempts    int
	Bytes       int64
	SHA256      string
	Err         error
	Elapsed     time.Duration // total wall time (limiter + retry + HTTP + sink)
	HTTPElapsed time.Duration // last attempt's HTTP round-trip only (server-health signal)
}

// Hooks are the fetcher's progress surface: optional callbacks invoked
// synchronously from the sequential dispatch goroutine. Either field may be
// nil. A hook must return promptly — it runs between requests, so a slow
// hook stretches the effective request interval (it can never shrink it;
// the Limiter's floor still applies).
type Hooks struct {
	// OnResult fires exactly once per Task that reaches a verdict. A task
	// interrupted by outer-ctx cancellation is a cancellation casualty and
	// produces no Result at all (see Run), so the caller records nothing
	// and the task's ledger state stays pending.
	OnResult func(Result)

	// OnWait fires each time the limiter sleeps before an attempt. The
	// WaitInfo carries whether a rest fired, its duration, and the
	// rest-cadence counters — enough for a renderer to print rest notices.
	OnWait func(WaitInfo)
}

// Fetcher runs a list of Tasks against a Sink through the Limiter.
type Fetcher struct {
	Client  *conagua.Client
	Sink    snapshot.Sink
	Date    string
	Limiter *Limiter // nil disables pacing (tests only — never against live CONAGUA)
	Retry   RetryPolicy
	Hooks   Hooks
}

// Run executes tasks sequentially on the calling goroutine.
//
// Per-task failures do NOT abort the run — they surface via Hooks.OnResult
// with Outcome=OutcomeError, so one bad station doesn't stop the rest. The
// only error Run returns is the loop-level fatal: ctx cancellation, checked
// before every task (and honoured inside every wait and IO point).
//
// A task cut down mid-flight by outer-ctx cancellation is a cancellation
// casualty, not an error: operator cancellation is not a server answer, so
// the task reaches no verdict, no OnResult fires, and its ledger state stays
// pending — a plain re-run re-attempts it, and resumption converges. The one
// exception is completed work: a success-shaped outcome (body stored, skip,
// stable 404) keeps its Result even under a dying ctx — finished work is
// never discarded.
func (f *Fetcher) Run(ctx context.Context, tasks []Task) error {
	for _, t := range tasks {
		if err := ctx.Err(); err != nil {
			return err
		}
		res := f.handle(ctx, t)
		// The outer ctx state — never error identity — is the authoritative
		// casualty test: a server-side failure that merely wraps a
		// cancellation string must not be misclassified, and any error
		// verdict reached under a dead outer ctx is an artifact of the
		// shutdown, not a server answer.
		if res.Outcome == OutcomeError && ctx.Err() != nil {
			return ctx.Err()
		}
		if f.Hooks.OnResult != nil {
			f.Hooks.OnResult(res)
		}
	}
	return nil
}

// handle executes one Task with retry. Named return so the deferred Elapsed
// write reaches the caller (a value return copies before defers run).
func (f *Fetcher) handle(ctx context.Context, t Task) (res Result) {
	start := time.Now()
	res = Result{Task: t}
	defer func() { res.Elapsed = time.Since(start) }()

	addr := t.Address(f.Date)

	exists, err := f.Sink.Exists(ctx, addr)
	if err != nil {
		res.Outcome = OutcomeError
		res.Err = fmt.Errorf("sink exists: %w", err)
		return
	}
	if exists {
		res.Outcome = OutcomeSkipped
		return
	}

	maxAttempts := max(1, f.Retry.MaxAttempts)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			d := f.Retry.Backoff(attempt)
			if d > 0 {
				timer := time.NewTimer(d)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					res.Outcome = OutcomeError
					res.Err = ctx.Err()
					return
				}
			}
		}
		if f.Limiter != nil {
			info, err := f.Limiter.Wait(ctx)
			if err != nil {
				res.Outcome = OutcomeError
				res.Err = err
				return
			}
			if f.Hooks.OnWait != nil {
				f.Hooks.OnWait(info)
			}
		}
		res.Attempts = attempt

		if done := f.attempt(ctx, t, addr, &res); done {
			return
		}
	}
	res.Outcome = OutcomeError
	if res.Err == nil {
		res.Err = fmt.Errorf("exhausted %d attempts", maxAttempts)
	}
	return
}

// attempt performs a single fetch+write cycle and records its result on res.
//
// res.HTTPElapsed is set to the wall time from request send to body close —
// server-responsiveness signal, independent of limiter/retry wait. It's
// overwritten on every attempt, so successful runs show the final (good)
// round-trip and failed runs show the last (bad) one.
func (f *Fetcher) attempt(ctx context.Context, t Task, addr snapshot.Address, res *Result) (done bool) {
	httpStart := time.Now()
	body, status, err := f.Client.FetchFile(ctx, t.URL)
	res.Status = status
	if err != nil {
		res.HTTPElapsed = time.Since(httpStart)
		res.Err = err
		if !isRetryable(ctx, status, err) {
			res.Outcome = OutcomeError
			return true
		}
		return false
	}
	defer func() {
		_ = body.Close()
		res.HTTPElapsed = time.Since(httpStart)
	}()

	switch status {
	case http.StatusOK:
		put, putErr := f.Sink.Put(ctx, addr, body)
		if putErr != nil {
			// Sink-side failures (transient object-store transport error,
			// "connection broken" mid-upload, etc.) flow through the same
			// retry classification as HTTP-side failures. The cost of a
			// re-fetch from CONAGUA is amortized against the rarity of
			// sink failures (<1% in practice).
			res.Err = fmt.Errorf("sink put: %w", putErr)
			if !isRetryable(ctx, status, putErr) {
				res.Outcome = OutcomeError
				return true
			}
			return false
		}
		res.Outcome = OutcomeFetched
		res.Bytes = put.Bytes
		res.SHA256 = put.SHA256
		res.Err = nil
		return true
	case http.StatusNotFound:
		// A 404 is a stable, success-shaped answer: many (station, kind)
		// pairs simply don't exist upstream. Drain the body so the
		// connection can be reused, and never retry.
		_, _ = io.Copy(io.Discard, body)
		res.Outcome = OutcomeNotFound
		res.Err = nil
		return true
	default:
		_, _ = io.Copy(io.Discard, body)
		res.Err = fmt.Errorf("unexpected status %d", status)
		if !isRetryable(ctx, status, nil) {
			res.Outcome = OutcomeError
			return true
		}
		return false
	}
}

// BuildTasks flattens a slice of stations into individual Tasks, one per
// (station, kind) pair that has a URL in the catalog, iterating
// conagua.AllKinds in canonical order. Only kinds in wantKinds are emitted;
// pass nil to include all kinds.
func BuildTasks(stations []conagua.Station, wantKinds []conagua.Kind) []Task {
	kindFilter := map[conagua.Kind]bool{}
	for _, k := range wantKinds {
		kindFilter[k] = true
	}
	includeAll := len(kindFilter) == 0

	tasks := make([]Task, 0, len(stations)*len(conagua.AllKinds))
	for _, s := range stations {
		for _, k := range conagua.AllKinds {
			if !includeAll && !kindFilter[k] {
				continue
			}
			f, ok := s.Files[k]
			if !ok {
				continue
			}
			tasks = append(tasks, Task{Station: s, Kind: k, URL: f.URL})
		}
	}
	return tasks
}
