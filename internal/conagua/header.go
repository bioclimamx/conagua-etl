package conagua

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// Header is the 9 labeled station-metadata lines that open every
// CONAGUA file kind (daily, monthly, extremes, each normals period).
//
// Lat/Lon/AltitudeM are pointers because CONAGUA can legitimately omit
// them (typically as "NULO") and we must not fabricate a zero value
// that downstream code would read as "at the equator, sea level".
type Header struct {
	ExternalID   string // as printed in the file header ("1001", not the 5-digit URL form)
	Name         string
	State        string // Spanish uppercase name as CONAGUA prints it (e.g. "AGUASCALIENTES")
	Municipality string
	Status       Status
	CVEOMM       string // WMO identifier; not all stations have one
	Lat          *float64
	Lon          *float64
	AltitudeM    *float64
}

// Warning is a non-fatal parse issue. ingest batches these into
// parsing_warnings rows (severity='warn').
type Warning struct {
	Line    int
	Message string
}

// keyLine matches one labeled field, e.g. " LATITUD   : 21.85027778 °".
// The key is everything before the first colon, trimmed; the value is
// everything after, trimmed.
var keyLine = regexp.MustCompile(`^\s*([^:]+?)\s*:\s*(.*?)\s*$`)

// bom is the UTF-8 encoding of U+FEFF. CONAGUA has historically added
// and removed a BOM across format changes; strip it if present on the
// very first read.
const bom = "\ufeff"

// ParseHeader reads lines from br until the station-metadata block ends
// (blank line following at least one labeled field). After return, br
// is positioned on the first byte of the data section.
//
// Missing optional fields leave the Header zero-valued — callers decide
// whether that's fatal for a given row. Malformed numeric values (bad
// LATITUD/LONGITUD/ALTITUD) are reported as Warnings and the field is
// left nil; the rest of the header still parses. An error is returned
// only if no header was found at all (empty file, wrong format, EOF
// before any labeled line).
//
// A numeric value CONAGUA wrapped across two physical lines is rejoined
// (see rejoinWrappedValue) so the fields *below* the wrap are still
// read, and the wrap is reported as a Warning. Any other line that
// interrupts the header block still ends the parse — the parser never
// goes hunting for labeled fields further down the file — and that end
// is reported as a Warning, never silent, because a header line we
// cannot interpret is exactly how published values go missing.
func ParseHeader(br *bufio.Reader) (Header, []Warning, error) {
	var h Header
	var warnings []Warning
	started := false
	lineNo := 0
	// The numeric field carried by the line just read, if any. Cleared
	// by every other line, so only an *immediately* following fragment
	// can be rejoined onto it.
	var wrapKey, wrapVal string

	for {
		line, err := br.ReadString('\n')
		lineNo++
		if lineNo == 1 {
			line = strings.TrimPrefix(line, bom)
		}
		// ReadString returns the last partial line along with io.EOF.
		// Handle both the "got a line, then EOF" and "just EOF" cases.
		trimmed := strings.TrimRight(line, "\r\n")
		// Normalize CONAGUA's byte/codepoint escapes back to real
		// UTF-8 before regex matching, so keys like "ESTACIÓN" match
		// whether they came through clean or mangled.
		trimmed = decodeConaguaMojibake(trimmed)

		// Blank line: ends the header block once we've seen content.
		if strings.TrimSpace(trimmed) == "" {
			if started {
				return h, warnings, nil
			}
			if err == io.EOF {
				return h, warnings, fmt.Errorf("no station header found (EOF at line %d)", lineNo)
			}
			continue
		}

		m := keyLine.FindStringSubmatch(trimmed)
		if m == nil {
			if !started {
				// Non-matching content before the header block (file
				// banner): skip silently.
				if err == io.EOF {
					return h, warnings, fmt.Errorf("no station header found (EOF at line %d)", lineNo)
				}
				continue
			}
			if joined, ok := rejoinWrappedValue(wrapKey, wrapVal, trimmed); ok {
				warnings = append(warnings, Warning{
					Line:    lineNo,
					Message: fmt.Sprintf("wrapped %s value continued on this line; rejoined as %q", wrapKey, joined),
				})
				applyHeaderField(&h, wrapKey, joined, lineNo, &warnings)
				// Keep the rejoined value current so a value wrapped
				// twice can keep accreting; a fragment that no longer
				// parses is rejected by rejoinWrappedValue, which bounds
				// the accretion without a separate counter.
				wrapVal = joined
				if err == io.EOF {
					return h, warnings, nil
				}
				continue
			}
			// Not a labeled field, and not attributable to the line
			// above: the header block ended without its usual blank
			// separator. Stop — scanning on for stray keys would let the
			// data section masquerade as metadata — but never silently.
			warnings = append(warnings, Warning{
				Line:    lineNo,
				Message: fmt.Sprintf("header block ended at unrecognized line %q (line skipped)", truncateForWarning(trimmed)),
			})
			return h, warnings, nil
		}

		key := strings.ToUpper(m[1])
		val := strings.TrimSpace(m[2])
		known := applyHeaderField(&h, key, val, lineNo, &warnings)
		if known {
			started = true
		}
		wrapKey, wrapVal = "", ""
		if _, numeric := numericHeaderTrims[key]; known && numeric {
			wrapKey, wrapVal = key, val
		}
		// An empty value on a known key still counts as "we've seen the
		// header block" — ingest's decision whether the missing field
		// is fatal belongs downstream.
		if err == io.EOF {
			if started {
				return h, warnings, nil
			}
			return h, warnings, fmt.Errorf("no station header found (EOF at line %d)", lineNo)
		}
	}
}

// wrappedValueTail matches the only stranded fragments CONAGUA has been
// observed to emit: the unit decoration that trails a coordinate or an
// altitude, pushed onto its own physical line by a hard wrap (station
// 25171's LONGITUD wraps before its degree sign in all three of its
// files; without the rejoin, its published ALTITUD below the wrap is
// never read). Deliberately narrow — the whitelist, not the parse
// check below, is what stops a bare numeric data row from being absorbed
// into a coordinate. A wrap shaped differently ends the header and is
// reported, so the next upstream surprise surfaces as a warning rather
// than as a silent drop.
var wrappedValueTail = regexp.MustCompile(`^(?i:°|msnm)$`)

// rejoinWrappedValue reports whether frag is the tail of the numeric
// header value "key : val" that CONAGUA wrapped onto the next physical
// line, and if so returns the rejoined raw value. A hard wrap inserts no
// character, so the tail is concatenated directly; the join is accepted
// only if the result still parses as that field's number, which keeps
// the recovery self-checking rather than speculative.
func rejoinWrappedValue(key, val, frag string) (string, bool) {
	trim, numeric := numericHeaderTrims[key]
	if !numeric {
		return "", false
	}
	if !wrappedValueTail.MatchString(strings.TrimSpace(frag)) {
		return "", false
	}
	joined := val + strings.TrimSpace(frag)
	if _, err := strconv.ParseFloat(trim(joined), 64); err != nil {
		return "", false
	}
	return joined, true
}

// truncateForWarning bounds a source line quoted inside a warning
// message: parsing_warnings rows are read by humans, and the line that
// ends a header block is typically a data row hundreds of columns wide.
func truncateForWarning(s string) string {
	const maxRunes = 60
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return string(r[:maxRunes]) + "…"
}

// applyHeaderField stores val into the matching Header field. Returns
// true if key was recognized (known CONAGUA header key), false if the
// line was a stray labeled field we don't care about. Unknown keys are
// silently ignored so future CONAGUA additions don't break the parser.
func applyHeaderField(h *Header, key, val string, lineNo int, warnings *[]Warning) bool {
	// The numeric fields go through the one trimmer table, looked up
	// once: a key the table lacks can never reach parseHeaderFloat with
	// a nil trimmer.
	var num *float64
	if trim, ok := numericHeaderTrims[key]; ok {
		num = parseHeaderFloat(val, trim, lineNo, key, warnings)
	}
	switch key {
	case "ESTACIÓN", "ESTACION":
		h.ExternalID = strings.TrimSpace(val)
	case "NOMBRE":
		h.Name = val
	case "ESTADO":
		h.State = val
	case "MUNICIPIO":
		h.Municipality = val
	case "SITUACIÓN", "SITUACION":
		h.Status = parseStatus(val)
	case "CVE-OMM", "CVE_OMM", "CVEOMM":
		h.CVEOMM = val
	case "LATITUD":
		h.Lat = num
	case "LONGITUD":
		h.Lon = num
	case "ALTITUD":
		h.AltitudeM = num
	default:
		return false
	}
	return true
}

// numericHeaderTrims lists the header keys whose value is a number,
// each mapped to the trimmer that strips that field's unit decoration.
// One table, two readers: applyHeaderField parses the printed value
// through it, and rejoinWrappedValue re-parses a rejoined one through
// exactly the same trimmer — so a recovered value can never be accepted
// on looser terms than a normal one.
var numericHeaderTrims = map[string]func(string) string{
	"LATITUD":  trimLatLon,
	"LONGITUD": trimLatLon,
	"ALTITUD":  trimAltitude,
}

// trimLatLon strips the degree symbol and any trailing whitespace from
// a "21.85027778 °" value. Uses a substring cut rather than a regex so
// unexpected suffixes (older files with "N" or "W" markers, for
// instance) become a parse warning via the caller's ParseFloat failure
// rather than being silently dropped.
func trimLatLon(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, "°")
	return strings.TrimSpace(s)
}

// trimAltitude strips "msnm" (metros sobre el nivel del mar) and
// whitespace from "1890.8 msnm".
func trimAltitude(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, "msnm")
	s = strings.TrimSuffix(s, "MSNM")
	return strings.TrimSpace(s)
}

// parseHeaderFloat trims the value, treats the standard CONAGUA missing
// tokens (NULO, -999, empty) as "not present" (returns nil with no
// warning), and returns a warning + nil for anything else that fails
// to parse — the rest of the header parse continues.
func parseHeaderFloat(raw string, trim func(string) string, lineNo int, field string, warnings *[]Warning) *float64 {
	s := trim(raw)
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

// mojibakeByte matches a hex-byte escape like <c3>. Some CONAGUA
// files have non-ASCII characters escaped as their raw UTF-8 bytes
// printed in text (so `Ó` shows up as the literal string "<c3><93>"
// instead of the two UTF-8 bytes 0xC3 0x93). Restoring the raw bytes
// and letting the rest of the pipeline treat the result as UTF-8
// recovers the original character.
var mojibakeByte = regexp.MustCompile(`<([0-9a-fA-F]{2})>`)

// mojibakeCodepoint matches a Unicode-codepoint escape like <U+00D1>.
// Different CONAGUA publication pipeline, same root cause as
// mojibakeByte — some files escape codepoints instead of bytes, and
// both escape forms appear in the same file.
var mojibakeCodepoint = regexp.MustCompile(`<U\+([0-9A-Fa-f]{4,6})>`)

// decodeConaguaMojibake normalizes both CONAGUA escape conventions
// back to canonical UTF-8, so the header parser sees ESTACIÓN as
// "ESTACIÓN" regardless of whether the upstream file kept the char
// clean, escaped it as <c3><93>, or escaped it as <U+00D3>.
//
// Safe to call on clean input: the patterns don't match well-formed
// UTF-8 text, and real CONAGUA content doesn't legitimately contain
// strings that look like these escapes.
//
// Applied at line level inside ParseHeader so normalization is scoped
// to the header block — the data section (tab-separated dates and
// numbers) neither needs nor benefits from it.
func decodeConaguaMojibake(line string) string {
	line = mojibakeCodepoint.ReplaceAllStringFunc(line, func(m string) string {
		hex := m[3 : len(m)-1] // strip "<U+" and ">"
		cp, err := strconv.ParseInt(hex, 16, 32)
		if err != nil {
			return m
		}
		return string(rune(cp))
	})
	line = mojibakeByte.ReplaceAllStringFunc(line, func(m string) string {
		hex := m[1 : len(m)-1] // strip "<" and ">"
		b, err := strconv.ParseUint(hex, 16, 8)
		if err != nil {
			return m
		}
		return string([]byte{byte(b)})
	})
	return line
}

// isMissingToken reports whether s is one of CONAGUA's conventions for
// "no value": empty, NULO (upper or lower), or the legacy -999 sentinel.
func isMissingToken(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return true
	}
	switch strings.ToUpper(s) {
	case "NULO":
		return true
	}
	if s == "-999" || s == "-9999" {
		return true
	}
	return false
}
