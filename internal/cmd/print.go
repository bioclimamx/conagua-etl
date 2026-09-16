package cmd

import (
	"fmt"
	"io"
)

// CLI progress and error output is best-effort: a write error on a
// terminal/pipe is unactionable (a broken pipe kills the process via
// SIGPIPE before we could react), so these helpers discard it. Routing
// every print through them keeps that policy in one place instead of a
// `_, _ =` at each call site.

func fprintf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func fprintln(w io.Writer, args ...any) {
	_, _ = fmt.Fprintln(w, args...)
}
