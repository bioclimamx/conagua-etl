package cmd

import (
	"io"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/power"
)

// powerLog is power's plain-log renderer: one timestamped line per cell on
// err (stderr). No terminal rewrites — FAIL lines interleave without
// corrupting the display, and redirecting stderr to a logfile produces a
// greppable append-only record.
//
// power.ProgressEvent carries no per-cell duration, so the renderer times
// cells itself: the orchestrator is strictly sequential, so the wall-clock
// gap between consecutive events *is* one cell's cost (fetch + limiter
// waits + DB write — exactly what an ETA should price in). The gaps feed
// an exponential moving average (α=0.2) that drives the ETA; it is a rough
// estimate, not a guarantee.
type powerLog struct {
	err        io.Writer
	startTime  time.Time
	lastEvent  time.Time
	emaElapsed time.Duration
}

func newPowerLog(err io.Writer) *powerLog {
	now := time.Now()
	return &powerLog{err: err, startTime: now, lastEvent: now}
}

// cellDone is the power.ProgressFunc callback: one line per cell, ok and
// FAIL alike. A FAIL line carries the cell's error as a final segment —
// this renderer is the run's only live failure surface (the library does
// not print failures itself).
func (r *powerLog) cellDone(ev power.ProgressEvent) {
	const alpha = 0.2
	now := time.Now()
	cellElapsed := now.Sub(r.lastEvent)
	r.lastEvent = now
	if r.emaElapsed == 0 {
		r.emaElapsed = cellElapsed
	} else {
		r.emaElapsed = time.Duration(float64(r.emaElapsed)*(1-alpha) + float64(cellElapsed)*alpha)
	}
	remaining := ev.Total - ev.Index
	eta := time.Duration(remaining) * r.emaElapsed

	status := "ok"
	errSuffix := ""
	if !ev.OK {
		status = "FAIL"
		if ev.Err != nil {
			errSuffix = " · " + ev.Err.Error()
		}
	}

	fprintf(r.err,
		"[%s] %d/%d %-4s %-22s · stations=%d · elapsed %s · eta %s%s\n",
		now.Format("15:04:05"),
		ev.Index, ev.Total,
		status,
		ev.CellID, ev.StationCount,
		fmtDur(now.Sub(r.startTime)), fmtDur(eta), errSuffix)
}
