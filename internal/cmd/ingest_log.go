package cmd

import (
	"fmt"
	"io"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/ingest"
)

// ingestLog is ingest's plain-log renderer: one timestamped line per
// station on err (stderr). No terminal rewrites — per-station failure
// messages interleave without corrupting the display, and redirecting
// stderr to a logfile produces a greppable append-only record.
//
// The per-station wall time feeds an exponential moving average (α=0.2)
// that drives the ETA. The ETA is a rough estimate, not a guarantee —
// slow and fast station clusters bias the average in the short term.
type ingestLog struct {
	err        io.Writer
	startTime  time.Time
	emaElapsed time.Duration
}

func newIngestLog(err io.Writer) *ingestLog {
	return &ingestLog{err: err, startTime: time.Now()}
}

// stationDone is the ingest.Options.OnStationDone callback: one line per
// station, ok and FAIL alike. A FAIL line carries the station's error as
// a final segment — this renderer is the run's only live failure
// surface (the library does not print failures itself).
func (r *ingestLog) stationDone(s ingest.StationOutcome) {
	const alpha = 0.2
	if r.emaElapsed == 0 {
		r.emaElapsed = s.Elapsed
	} else {
		r.emaElapsed = time.Duration(float64(r.emaElapsed)*(1-alpha) + float64(s.Elapsed)*alpha)
	}
	remaining := s.Total - s.Index
	eta := time.Duration(remaining) * r.emaElapsed
	elapsed := time.Since(r.startTime)

	status := "ok"
	errSuffix := ""
	if s.Err != nil {
		status = "FAIL"
		errSuffix = " · " + s.Err.Error()
	}

	fprintf(r.err,
		"[%s] %d/%d %-4s %s %s · daily=%d normals=%d extras=%d warn=%d · elapsed %s · eta %s%s\n",
		time.Now().Format("15:04:05"),
		s.Index, s.Total,
		status,
		s.ExternalID, truncate(s.Name, 32),
		s.DailyRows, s.NormalsRows, s.ExtrasRows, s.Warnings,
		fmtDur(elapsed), fmtDur(eta), errSuffix)
}

// finish is a no-op — there is no in-flight carriage-return line to
// terminate. Kept so the CLI wiring stays unchanged if a richer renderer
// returns.
func (r *ingestLog) finish() {}

// fmtDur renders a Duration as h/m/s pieces, dropping leading zeros so a
// 3m12s duration doesn't look like 0h3m12s.
func fmtDur(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	h := int(d / time.Hour)
	m := int(d%time.Hour) / int(time.Minute)
	s := int(d%time.Minute) / int(time.Second)
	switch {
	case h > 0:
		return fmt.Sprintf("%dh%02dm%02ds", h, m, s)
	case m > 0:
		return fmt.Sprintf("%dm%02ds", m, s)
	default:
		return fmt.Sprintf("%ds", s)
	}
}

// truncate caps s at n runes with an ellipsis so longer station names
// don't misalign the log. The cut is rune-based because station names
// carry Ñ and accents — a byte cut could split a UTF-8 sequence.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	if n <= 1 {
		return string(r[:n])
	}
	return string(r[:n-1]) + "…"
}
