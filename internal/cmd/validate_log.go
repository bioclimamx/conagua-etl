package cmd

import (
	"io"
	"sync"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// validateLog is validate's plain-log renderer: one timestamped line per
// rule as it starts and as it finishes, on err (stderr), plus a
// heartbeat while a rule is in flight. No terminal rewrites — the lines
// interleave cleanly, and redirecting stderr to a logfile produces a
// greppable append-only record.
type validateLog struct {
	err  io.Writer
	beat *heartbeat
}

func newValidateLog(err io.Writer) *validateLog {
	return &validateLog{err: err, beat: newHeartbeat(err, heartbeatInterval)}
}

// ruleStart is the validate.Options.OnRuleStart callback: the rule's
// id and name, then the heartbeat starts for it.
func (r *validateLog) ruleStart(id, name string) {
	fprintf(r.err, "[%s] %-24s starting (%s)\n", time.Now().Format("15:04:05"), id, name)
	r.beat.start(id)
}

// ruleDone is the validate.Options.OnRuleDone callback: the heartbeat
// stops, then the rule's rows scanned, its warn and error counts, and
// its wall time.
func (r *validateLog) ruleDone(id string, res validate.RuleResult, elapsed time.Duration) {
	r.beat.stop()
	warns, errs := countSeverities(res.Findings)
	fprintf(r.err, "[%s] %-24s done · scanned=%d warn=%d error=%d · %s\n",
		time.Now().Format("15:04:05"), id, res.Scanned, warns, errs, elapsed.Round(time.Millisecond))
}

// finish stops a heartbeat left running by a rule that never reached
// ruleDone — the rule's own error, or cancellation, ends the run.
func (r *validateLog) finish() { r.beat.stop() }

// countSeverities tallies a rule's findings by severity.
func countSeverities(findings []validate.Finding) (warns, errs int) {
	for _, f := range findings {
		switch f.Severity {
		case validate.SeverityWarn:
			warns++
		case validate.SeverityError:
			errs++
		}
	}
	return warns, errs
}

// heartbeatInterval is how often a still-running line prints while a
// rule is in flight. The slowest rule (daily-sanity over ~71M rows)
// runs for minutes; without the line an operator cannot tell progress
// from a hang.
const heartbeatInterval = 30 * time.Second

// heartbeat prints "still running" lines every interval while a rule is
// in flight. start and stop pair per rule; stop blocks until the ticker
// goroutine has exited so no line prints after a rule's done line.
type heartbeat struct {
	w        io.Writer
	interval time.Duration

	mu      sync.Mutex
	stopCh  chan struct{}
	doneCh  chan struct{}
	started time.Time
}

func newHeartbeat(w io.Writer, interval time.Duration) *heartbeat {
	return &heartbeat{w: w, interval: interval}
}

// start begins the heartbeat for ruleID. A heartbeat already running is
// stopped first, so a missed stop never leaks a goroutine.
func (h *heartbeat) start(ruleID string) {
	h.stop()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.started = time.Now()
	h.stopCh = make(chan struct{})
	h.doneCh = make(chan struct{})
	go h.loop(h.stopCh, h.doneCh, ruleID)
}

// stop ends the running heartbeat, if any, and waits for its goroutine.
func (h *heartbeat) stop() {
	h.mu.Lock()
	stopCh, doneCh := h.stopCh, h.doneCh
	h.stopCh = nil
	h.doneCh = nil
	h.mu.Unlock()
	if stopCh != nil {
		close(stopCh)
		<-doneCh
	}
}

func (h *heartbeat) loop(stop <-chan struct{}, done chan<- struct{}, ruleID string) {
	defer close(done)
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			h.mu.Lock()
			elapsed := time.Since(h.started).Round(time.Second)
			h.mu.Unlock()
			fprintf(h.w, "[%s] %-24s still running · %s\n",
				time.Now().Format("15:04:05"), ruleID, elapsed)
		}
	}
}
