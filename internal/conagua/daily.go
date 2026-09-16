package conagua

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// DailyRow is one parsed row from a CONAGUA daily observations file.
// Date is ISO-8601 ("YYYY-MM-DD"). Value fields are nil when the source
// row had a missing token (NULO / -999 / empty).
type DailyRow struct {
	Date   string
	Tmax   *float64
	Tmin   *float64
	Precip *float64
	Evap   *float64
}

// isoDate matches the one CONAGUA format we've seen: 4-digit year,
// 2-digit month, 2-digit day, hyphen-separated. Used to tell data rows
// apart from column headers, blank lines, and stray trailer text.
var isoDate = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

// ParseDaily reads tab-separated daily observations from br and emits
// them via rowFn. br is expected to be positioned where ParseHeader
// left off — ParseDaily tolerates the blank lines and column-header
// rows that separate the station metadata from the data proper.
//
// Each non-fatal parse issue is emitted via warnFn; lineOffset is added
// to the local line counter so warnings are keyed to positions in the
// full source file (ingest hands in "how many lines ParseHeader
// consumed" here).
//
// ParseDaily never returns a parse error for a single bad row — that's
// what warnings are for. An error is returned only on a read failure
// from br (which, for a *bufio.Reader over a buffered source, is
// practically impossible after construction).
func ParseDaily(br *bufio.Reader, lineOffset int, rowFn func(DailyRow), warnFn func(Warning)) error {
	localLine := 0
	for {
		raw, err := br.ReadString('\n')
		localLine++
		lineNo := lineOffset + localLine
		line := strings.TrimRight(raw, "\r\n")

		switch {
		case strings.TrimSpace(line) == "":
			// blank separator — skip
		case !isoDate.MatchString(firstField(line)):
			// column-header row, unit row, trailer, etc.
		default:
			row, rowWarnings := parseDailyLine(line, lineNo)
			for _, w := range rowWarnings {
				warnFn(w)
			}
			if row != nil {
				rowFn(*row)
			}
		}

		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read line %d: %w", lineNo, err)
		}
	}
}

// parseDailyLine splits one tab-separated data line and returns the
// DailyRow it represents, plus any Warnings emitted along the way.
// A return of (nil, warnings) means the line was structurally too short
// to be a data row (<5 fields); one Warning describing that is the
// whole return value.
func parseDailyLine(line string, lineNo int) (*DailyRow, []Warning) {
	fields := strings.Split(line, "\t")
	if len(fields) < 5 {
		return nil, []Warning{{
			Line:    lineNo,
			Message: fmt.Sprintf("daily row has %d fields, expected 5: %q", len(fields), line),
		}}
	}

	var warnings []Warning
	row := &DailyRow{Date: strings.TrimSpace(fields[0])}
	row.Precip = parseDailyValue(fields[1], lineNo, "PRECIP", &warnings)
	row.Evap = parseDailyValue(fields[2], lineNo, "EVAP", &warnings)
	row.Tmax = parseDailyValue(fields[3], lineNo, "TMAX", &warnings)
	row.Tmin = parseDailyValue(fields[4], lineNo, "TMIN", &warnings)
	return row, warnings
}

// parseDailyValue applies the CONAGUA missing-token rules, then
// ParseFloat. Anything else becomes a warning and a nil value — we
// never silently coerce a malformed value to zero.
func parseDailyValue(raw string, lineNo int, field string, warnings *[]Warning) *float64 {
	s := strings.TrimSpace(raw)
	if isMissingToken(s) {
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		*warnings = append(*warnings, Warning{
			Line:    lineNo,
			Message: fmt.Sprintf("malformed %s %q: %v", field, raw, err),
		})
		return nil
	}
	return &f
}

// firstField returns what comes before the first tab in line, without
// allocating a full slice for the whole split.
func firstField(line string) string {
	if i := strings.IndexByte(line, '\t'); i >= 0 {
		return strings.TrimSpace(line[:i])
	}
	return strings.TrimSpace(line)
}
