package cmd

import (
	"io"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// publishLog is publish's plain-log renderer: one timestamped line per
// gate rule as the gate evaluates it, ahead of any write; one per unit
// as it lands in an archive — a per-station or per-cell daily file or
// the {state}.db of a tabular archive, a station's profile + daily.json
// pair of a JSON archive, labelled by its profile.json, a state archive
// consumed by a national copy, the national bioclima.db, a kind
// directory or root file of the raw snapshot — and one per archive or
// docs file on completion or failure, on err (stderr). No terminal
// rewrites — FAIL lines interleave without corrupting the display, and
// redirecting stderr to a logfile produces a greppable append-only
// record.
type publishLog struct {
	err io.Writer
}

func newPublishLog(err io.Writer) *publishLog {
	return &publishLog{err: err}
}

// shortSHALen is how much of an archive's sha256 the artifact line
// shows — enough to eyeball against CHECKSUMS, short enough to stay on
// one line.
const shortSHALen = 12

// event is the publish.ProgressFunc callback. A gate line carries the
// rule's id, the rows it examined, its warn and error counts, and its
// wall time — FAIL when it found an error, the refusal that follows. A
// unit line carries the unit's ordinal across the archive and the
// in-archive path of the file whose write completed the unit — the
// path, not the bare id, since the same station has a conagua/ and a
// combined/ daily file and, in the JSON archive, a profile and a daily
// file; an artifact line carries the archive's entry count (a docs file
// has none, and its line omits it), size, digest prefix, and wall time,
// with the error as a final segment on FAIL — this renderer is the
// run's only live failure surface (the library does not print failures
// itself).
func (r *publishLog) event(ev publish.ProgressEvent) {
	ts := time.Now().Format("15:04:05")
	if ev.Rule != "" {
		status := "ok"
		if ev.Errors > 0 {
			status = "FAIL"
		}
		fprintf(r.err, "[%s] gate %-4s %s · scanned=%d warn=%d error=%d · elapsed %s\n",
			ts, status, ev.Rule, ev.Scanned, ev.Warnings, ev.Errors, fmtDur(ev.Elapsed))
		return
	}
	if ev.Unit != "" {
		fprintf(r.err, "[%s] %d/%d %-4s %s %s\n", ts, ev.Index, ev.Total, "ok", ev.Artifact, ev.Unit)
		return
	}

	status := "ok"
	errSuffix := ""
	if ev.Err != nil {
		status = "FAIL"
		errSuffix = " · " + ev.Err.Error()
	}
	sha := "-"
	if len(ev.SHA256) >= shortSHALen {
		sha = ev.SHA256[:shortSHALen]
	}
	if ev.File {
		fprintf(r.err, "[%s] %-4s %s · bytes=%d sha256=%s · elapsed %s%s\n",
			ts, status, ev.Artifact, ev.Bytes, sha, fmtDur(ev.Elapsed), errSuffix)
		return
	}
	fprintf(r.err, "[%s] %-4s %s · entries=%d bytes=%d sha256=%s · elapsed %s%s\n",
		ts, status, ev.Artifact, ev.Entries, ev.Bytes, sha, fmtDur(ev.Elapsed), errSuffix)
}
