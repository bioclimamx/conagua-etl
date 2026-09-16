package conagua

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// MonthlyNormalsRow is one month's worth of climatological means for a
// single (station, period). ParseNormalsFile emits 12 of these per call
// (one for each calendar month). Value pointers are nil when the
// section was absent in the source file, or when that month's cell
// carried a missing token.
type MonthlyNormalsRow struct {
	Month  int // 1..12
	Tmax   *float64
	Tmin   *float64
	Tmean  *float64
	Precip *float64
	Evap   *float64
}

// MonthlyNormalsExtrasRow carries the per-(station, period, month)
// data beyond NORMAL: extremes, the years they occurred, and the
// AÑOS CON DATOS counts used for WMO completeness scoring.
//
// Fields are all pointer types — any given one may be absent in the
// source file, and the ingest layer persists them as SQL NULL. Dates
// are ISO ('YYYY-MM-DD'); the file's 'DD/YYYY' form is expanded using
// the section's calendar month as the month component.
type MonthlyNormalsExtrasRow struct {
	Month int // 1..12

	TmaxMonthlyExtreme       *float64
	TmaxMonthlyExtremeYear   *int
	TmaxDailyExtreme         *float64
	TmaxDailyExtremeDate     *string
	TminMonthlyExtreme       *float64
	TminMonthlyExtremeYear   *int
	TminDailyExtreme         *float64
	TminDailyExtremeDate     *string
	PrecipMonthlyExtreme     *float64
	PrecipMonthlyExtremeYear *int
	PrecipDailyExtreme       *float64
	PrecipDailyExtremeDate   *string

	TmaxYearsWithData   *int
	TminYearsWithData   *int
	TmeanYearsWithData  *int
	PrecipYearsWithData *int
	EvapYearsWithData   *int

	RainDays              *float64
	RainDaysYearsWithData *int
}

// PeriodForKind returns the period string stored in monthly_normals.period
// for a Normals Kind. Returns ("", false) for non-normals kinds.
func PeriodForKind(k Kind) (string, bool) {
	switch k {
	case KindNormals1961_1990:
		return "1961-1990", true
	case KindNormals1971_2000:
		return "1971-2000", true
	case KindNormals1981_2010:
		return "1981-2010", true
	case KindNormals1991_2020:
		return "1991-2020", true
	default:
		return "", false
	}
}

// periodLine matches "NORMAL CLIMATOLÓGICA 1991-2020" in the file banner.
// Used as the drift detector between Kind (derived from the sink path)
// and the period label CONAGUA stamped on the file body.
var periodLine = regexp.MustCompile(`NORMAL\s+CLIMATOL[ÓO]GICA\s+(\d{4}-\d{4})`)

// sectionKind identifies one of the section tables we parse, plus
// sectionIgnored for titles we recognize without parsing.
type sectionKind int

const (
	sectionNone sectionKind = iota
	sectionIgnored
	sectionTmax
	sectionTmin
	sectionTmean
	sectionPrecip
	sectionEvap
	sectionRainDays
)

// sectionByTitle maps the exact Spanish title text CONAGUA prints at
// the top of each section (trimmed, case-preserved) to the sectionKind
// we fill from that section's rows.
//
// "HUMEDAD RELATIVA" is recognized but ignored: CONAGUA's conventional
// archive publishes no observed humidity (the section appears in no
// snapshot; RH reaches the dataset only as POWER reanalysis in the
// supplement tables). The title MUST stay in this map regardless — if
// it went unrecognized, an RH section reappearing upstream would leave
// `section` pointing at the preceding table (TEMPERATURA MEDIA), and
// RH's NORMAL / AÑOS CON DATOS rows would silently overwrite that
// section's values.
var sectionByTitle = map[string]sectionKind{
	"TEMPERATURA MÁXIMA":        sectionTmax,
	"TEMPERATURA MÍNIMA":        sectionTmin,
	"TEMPERATURA MEDIA":         sectionTmean,
	"HUMEDAD RELATIVA":          sectionIgnored,
	"PRECIPITACIÓN":             sectionPrecip,
	"EVAPORACIÓN":               sectionEvap,
	"NÚMERO DE DÍAS CON LLUVIA": sectionRainDays,
}

// rowKind identifies a sub-row within a section. Labels are matched on
// the trimmed first tab-field of each line.
type rowKind int

const (
	rowUnknown            rowKind = iota
	rowNormal                     // "NORMAL"
	rowMonthlyExtreme             // "MÁXIMA MENSUAL" / "MÍNIMA MENSUAL"
	rowMonthlyExtremeYear         // "AÑO DE MÁXIMA" / "AÑO DE MÍNIMA"
	rowDailyExtreme               // "MÁXIMA DIARIA" / "MÍNIMA DIARIA"
	rowDailyExtremeDate           // "FECHA MÁX DIARIA" / "FECHA MÍN DIARIA"
	rowYearsWithData              // "AÑOS CON DATOS"
)

// rowKindByLabel maps the first tab-field text of a section row to
// a rowKind. Unknown labels (MESES header, annual-only rows) return
// rowUnknown via the zero value.
var rowKindByLabel = map[string]rowKind{
	"NORMAL":           rowNormal,
	"MÁXIMA MENSUAL":   rowMonthlyExtreme,
	"MÍNIMA MENSUAL":   rowMonthlyExtreme,
	"AÑO DE MÁXIMA":    rowMonthlyExtremeYear,
	"AÑO DE MÍNIMA":    rowMonthlyExtremeYear,
	"MÁXIMA DIARIA":    rowDailyExtreme,
	"MÍNIMA DIARIA":    rowDailyExtreme,
	"FECHA MÁX DIARIA": rowDailyExtremeDate,
	"FECHA MÍN DIARIA": rowDailyExtremeDate,
	"AÑOS CON DATOS":   rowYearsWithData,
}

// ParseNormalsFile parses a whole CONAGUA normals file: banner,
// station header, and the section tables we care about.
//
// Returns the station Header, 12 MonthlyNormalsRow (the NORMAL values),
// 12 MonthlyNormalsExtrasRow (extremes + years_with_data + rain_days),
// and any non-fatal warnings. expectedPeriod is cross-checked against
// the file-body "NORMAL CLIMATOLÓGICA <period>" line — mismatch is a
// file-level error so the caller can emit a severity='error' warning
// and skip the file.
func ParseNormalsFile(r io.Reader, expectedPeriod string) (Header, []MonthlyNormalsRow, []MonthlyNormalsExtrasRow, []Warning, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return Header{}, nil, nil, nil, fmt.Errorf("read normals: %w", err)
	}

	filePeriod, ok := scanPeriod(data)
	if !ok {
		return Header{}, nil, nil, nil, errors.New("no 'NORMAL CLIMATOLÓGICA YYYY-YYYY' line found")
	}
	if filePeriod != expectedPeriod {
		return Header{}, nil, nil, nil, fmt.Errorf("period mismatch: file says %q, expected %q", filePeriod, expectedPeriod)
	}

	br := bufio.NewReader(bytes.NewReader(data))
	h, headerWarnings, err := ParseHeader(br)
	if err != nil {
		return Header{}, nil, nil, headerWarnings, fmt.Errorf("parse header: %w", err)
	}

	rows, extras, sectionWarnings := parseNormalsSections(br)

	warnings := append([]Warning(nil), headerWarnings...)
	warnings = append(warnings, sectionWarnings...)
	return h, rows, extras, warnings, nil
}

// scanPeriod extracts "NORMAL CLIMATOLÓGICA YYYY-YYYY" from the file
// banner without consuming the reader's position. One regex pass over
// the full bytes is fine — normals files are < 10 KiB.
func scanPeriod(data []byte) (string, bool) {
	m := periodLine.FindSubmatch(data)
	if len(m) < 2 {
		return "", false
	}
	return string(m[1]), true
}

// sectionAccumulator holds in-flight per-section values while the
// state machine walks the file body. Each array has one slot per
// calendar month (1-indexed via month-1).
type sectionAccumulator struct {
	// NORMAL values (go into MonthlyNormalsRow).
	tmaxNormal   [12]*float64
	tminNormal   [12]*float64
	tmeanNormal  [12]*float64
	precipNormal [12]*float64
	evapNormal   [12]*float64

	// Extremes (go into MonthlyNormalsExtrasRow).
	tmaxMonthlyExtreme       [12]*float64
	tmaxMonthlyExtremeYear   [12]*int
	tmaxDailyExtreme         [12]*float64
	tmaxDailyExtremeDate     [12]*string
	tminMonthlyExtreme       [12]*float64
	tminMonthlyExtremeYear   [12]*int
	tminDailyExtreme         [12]*float64
	tminDailyExtremeDate     [12]*string
	precipMonthlyExtreme     [12]*float64
	precipMonthlyExtremeYear [12]*int
	precipDailyExtreme       [12]*float64
	precipDailyExtremeDate   [12]*string

	// Years-with-data per field.
	tmaxYearsWithData   [12]*int
	tminYearsWithData   [12]*int
	tmeanYearsWithData  [12]*int
	precipYearsWithData [12]*int
	evapYearsWithData   [12]*int

	// Rain-days section.
	rainDaysNormal        [12]*float64
	rainDaysYearsWithData [12]*int
}

// parseNormalsSections walks the data section of a normals file from
// the reader's current position (after ParseHeader) to EOF. Returns
// the 12-month NORMAL rows, the 12-month extras rows, and warnings.
func parseNormalsSections(br *bufio.Reader) ([]MonthlyNormalsRow, []MonthlyNormalsExtrasRow, []Warning) {
	var acc sectionAccumulator
	var warnings []Warning

	section := sectionNone
	lineNo := 0

	for {
		raw, err := br.ReadString('\n')
		lineNo++
		line := strings.TrimRight(raw, "\r\n")
		trimmed := strings.TrimSpace(line)

		if s, ok := sectionByTitle[trimmed]; ok {
			section = s
		} else if section != sectionNone && section != sectionIgnored {
			handleSectionRow(&acc, section, line, lineNo, &warnings)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			warnings = append(warnings, Warning{
				Line:    lineNo,
				Message: fmt.Sprintf("read error in normals body: %v", err),
			})
			break
		}
	}

	rows := make([]MonthlyNormalsRow, 12)
	extras := make([]MonthlyNormalsExtrasRow, 12)
	for i := 0; i < 12; i++ {
		rows[i] = MonthlyNormalsRow{
			Month:  i + 1,
			Tmax:   acc.tmaxNormal[i],
			Tmin:   acc.tminNormal[i],
			Tmean:  acc.tmeanNormal[i],
			Precip: acc.precipNormal[i],
			Evap:   acc.evapNormal[i],
		}
		extras[i] = MonthlyNormalsExtrasRow{
			Month: i + 1,

			TmaxMonthlyExtreme:       acc.tmaxMonthlyExtreme[i],
			TmaxMonthlyExtremeYear:   acc.tmaxMonthlyExtremeYear[i],
			TmaxDailyExtreme:         acc.tmaxDailyExtreme[i],
			TmaxDailyExtremeDate:     acc.tmaxDailyExtremeDate[i],
			TminMonthlyExtreme:       acc.tminMonthlyExtreme[i],
			TminMonthlyExtremeYear:   acc.tminMonthlyExtremeYear[i],
			TminDailyExtreme:         acc.tminDailyExtreme[i],
			TminDailyExtremeDate:     acc.tminDailyExtremeDate[i],
			PrecipMonthlyExtreme:     acc.precipMonthlyExtreme[i],
			PrecipMonthlyExtremeYear: acc.precipMonthlyExtremeYear[i],
			PrecipDailyExtreme:       acc.precipDailyExtreme[i],
			PrecipDailyExtremeDate:   acc.precipDailyExtremeDate[i],

			TmaxYearsWithData:   acc.tmaxYearsWithData[i],
			TminYearsWithData:   acc.tminYearsWithData[i],
			TmeanYearsWithData:  acc.tmeanYearsWithData[i],
			PrecipYearsWithData: acc.precipYearsWithData[i],
			EvapYearsWithData:   acc.evapYearsWithData[i],

			RainDays:              acc.rainDaysNormal[i],
			RainDaysYearsWithData: acc.rainDaysYearsWithData[i],
		}
	}
	return rows, extras, warnings
}

// handleSectionRow dispatches a non-title line to the appropriate
// accumulator bucket based on (section, row-label). Unknown rows
// (MESES column-header, blanks, stray labels) are silently skipped.
func handleSectionRow(acc *sectionAccumulator, section sectionKind, line string, lineNo int, warnings *[]Warning) {
	label := firstField(line)
	rk, ok := rowKindByLabel[label]
	if !ok {
		return
	}

	switch rk {
	case rowNormal:
		floats := parseMonthlyFloats(line, lineNo, warnings)
		switch section {
		case sectionTmax:
			acc.tmaxNormal = floats
		case sectionTmin:
			acc.tminNormal = floats
		case sectionTmean:
			acc.tmeanNormal = floats
		case sectionPrecip:
			acc.precipNormal = floats
		case sectionEvap:
			acc.evapNormal = floats
		case sectionRainDays:
			acc.rainDaysNormal = floats
		}

	case rowMonthlyExtreme:
		floats := parseMonthlyFloats(line, lineNo, warnings)
		switch section {
		case sectionTmax:
			acc.tmaxMonthlyExtreme = floats
		case sectionTmin:
			acc.tminMonthlyExtreme = floats
		case sectionPrecip:
			acc.precipMonthlyExtreme = floats
		}

	case rowMonthlyExtremeYear:
		ints := parseMonthlyInts(line, lineNo, warnings)
		switch section {
		case sectionTmax:
			acc.tmaxMonthlyExtremeYear = ints
		case sectionTmin:
			acc.tminMonthlyExtremeYear = ints
		case sectionPrecip:
			acc.precipMonthlyExtremeYear = ints
		}

	case rowDailyExtreme:
		floats := parseMonthlyFloats(line, lineNo, warnings)
		switch section {
		case sectionTmax:
			acc.tmaxDailyExtreme = floats
		case sectionTmin:
			acc.tminDailyExtreme = floats
		case sectionPrecip:
			acc.precipDailyExtreme = floats
		}

	case rowDailyExtremeDate:
		dates := parseMonthlyISODates(line, lineNo, warnings)
		switch section {
		case sectionTmax:
			acc.tmaxDailyExtremeDate = dates
		case sectionTmin:
			acc.tminDailyExtremeDate = dates
		case sectionPrecip:
			acc.precipDailyExtremeDate = dates
		}

	case rowYearsWithData:
		ints := parseMonthlyInts(line, lineNo, warnings)
		switch section {
		case sectionTmax:
			acc.tmaxYearsWithData = ints
		case sectionTmin:
			acc.tminYearsWithData = ints
		case sectionTmean:
			acc.tmeanYearsWithData = ints
		case sectionPrecip:
			acc.precipYearsWithData = ints
		case sectionEvap:
			acc.evapYearsWithData = ints
		case sectionRainDays:
			acc.rainDaysYearsWithData = ints
		}
	}
}

// monthlyTokens pulls the 12 per-month tokens out of a NORMAL / MÁXIMA
// MENSUAL / ... row. It skips the label itself, any padding empty
// fields, and the trailing annual column. Skipping empties is safe
// because missing cells arrive as NULO or -999-style sentinels, never
// as empty fields — a genuinely empty interior cell would shift every
// month after it. Rows with fewer than 12 data fields leave the
// trailing tokens empty.
func monthlyTokens(line string) [12]string {
	var out [12]string
	fields := strings.Split(line, "\t")
	i := 0
	sawLabel := false
	for _, raw := range fields {
		s := strings.TrimSpace(raw)
		if !sawLabel {
			if _, known := rowKindByLabel[s]; known {
				sawLabel = true
			}
			continue
		}
		if s == "" {
			continue
		}
		if i >= 12 {
			break
		}
		out[i] = raw // preserve whitespace for per-cell warning surfacing
		i++
	}
	return out
}

func parseMonthlyFloats(line string, lineNo int, warnings *[]Warning) [12]*float64 {
	var out [12]*float64
	tokens := monthlyTokens(line)
	for i, tok := range tokens {
		s := strings.TrimSpace(tok)
		if s == "" || isMissingToken(s) {
			out[i] = nil
			continue
		}
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			out[i] = &v
		} else {
			*warnings = append(*warnings, Warning{
				Line:    lineNo,
				Message: fmt.Sprintf("malformed float %q (month %d): %v", tok, i+1, err),
			})
		}
	}
	return out
}

func parseMonthlyInts(line string, lineNo int, warnings *[]Warning) [12]*int {
	var out [12]*int
	tokens := monthlyTokens(line)
	for i, tok := range tokens {
		s := strings.TrimSpace(tok)
		if s == "" || isMissingToken(s) {
			out[i] = nil
			continue
		}
		if v, err := strconv.Atoi(s); err == nil {
			out[i] = &v
		} else {
			*warnings = append(*warnings, Warning{
				Line:    lineNo,
				Message: fmt.Sprintf("malformed int %q (month %d): %v", tok, i+1, err),
			})
		}
	}
	return out
}

// parseMonthlyISODates turns a "FECHA MÁX DIARIA" row into ISO dates.
// CONAGUA's format per cell is "DD/YYYY" where the month is implied
// by the column (ENE=01, FEB=02, ...). Invalid dates (e.g. 30/1998
// under February) are surfaced as warnings and stored as nil.
func parseMonthlyISODates(line string, lineNo int, warnings *[]Warning) [12]*string {
	var out [12]*string
	tokens := monthlyTokens(line)
	for i, tok := range tokens {
		month := i + 1
		s := strings.TrimSpace(tok)
		if s == "" || isMissingToken(s) {
			out[i] = nil
			continue
		}
		parts := strings.SplitN(s, "/", 2)
		if len(parts) != 2 {
			*warnings = append(*warnings, Warning{
				Line:    lineNo,
				Message: fmt.Sprintf("bad date %q (month %d): expected DD/YYYY", tok, month),
			})
			continue
		}
		day, errDay := strconv.Atoi(strings.TrimSpace(parts[0]))
		year, errYear := strconv.Atoi(strings.TrimSpace(parts[1]))
		if errDay != nil || errYear != nil {
			*warnings = append(*warnings, Warning{
				Line:    lineNo,
				Message: fmt.Sprintf("bad date %q (month %d)", tok, month),
			})
			continue
		}
		iso := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
		if _, err := time.Parse("2006-01-02", iso); err != nil {
			*warnings = append(*warnings, Warning{
				Line:    lineNo,
				Message: fmt.Sprintf("invalid date %q for month %d: %v", tok, month, err),
			})
			continue
		}
		out[i] = &iso
	}
	return out
}
