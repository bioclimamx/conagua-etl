package parity

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

// stationOutcome is the comparison result for one station within one
// kind, produced by a pool worker and merged into the KindReport by the
// sequential reducer.
type stationOutcome struct {
	baseOnly bool
	newOnly  bool

	parseErrors []ParseError

	// compared is true when the station was present and parseable on
	// both sides; identical is meaningful only then.
	compared  bool
	identical bool

	rowsIdentical int
	rowsRevised   int
	appended      Capped[RowRef]
	removed       Capped[RowRef]

	revisions   Capped[Revision]
	headerDiffs Capped[Revision]
	warningDiff *WarningCountDiff
}

// compareStation resolves presence and dispatches to the kind-specific
// comparison. Callers only invoke it for value-compared kinds.
func compareStation(kind conagua.Kind, station, baseDir, newDir string, inBase, inNew bool) stationOutcome {
	switch {
	case !inBase:
		return stationOutcome{newOnly: true}
	case !inNew:
		return stationOutcome{baseOnly: true}
	}
	basePath := filepath.Join(baseDir, string(kind), station+".txt")
	newPath := filepath.Join(newDir, string(kind), station+".txt")
	if kind == conagua.KindDaily {
		return compareDailyStation(station, basePath, newPath)
	}
	return compareNormalsStation(kind, station, basePath, newPath)
}

// compareDailyStation parses both sides' daily files and classifies
// every row keyed by date: identical, appended (new-only date), removed
// (base-only date), or revised (same date, any field differs).
func compareDailyStation(station, basePath, newPath string) stationOutcome {
	var o stationOutcome
	baseHdr, baseRows, baseWarns, baseErr := parseDailyFile(basePath)
	newHdr, newRows, newWarns, newErr := parseDailyFile(newPath)
	if baseErr != nil {
		o.parseErrors = append(o.parseErrors, ParseError{Station: station, Side: SideBase, Err: baseErr.Error()})
	}
	if newErr != nil {
		o.parseErrors = append(o.parseErrors, ParseError{Station: station, Side: SideNew, Err: newErr.Error()})
	}
	if baseErr != nil || newErr != nil {
		return o
	}
	o.compared = true

	for _, rev := range diffHeader(station, baseHdr, newHdr) {
		o.headerDiffs.Add(rev)
	}

	for _, date := range sortedUnionKeys(baseRows, newRows) {
		baseRow, inBase := baseRows[date]
		newRow, inNew := newRows[date]
		switch {
		case !inBase:
			o.appended.Add(RowRef{Station: station, Date: date})
		case !inNew:
			o.removed.Add(RowRef{Station: station, Date: date})
		default:
			c := revCollector{station: station, date: date}
			diffDailyRow(&c, baseRow, newRow)
			if len(c.revs) == 0 {
				o.rowsIdentical++
				continue
			}
			o.rowsRevised++
			for _, rev := range c.revs {
				o.revisions.Add(rev)
			}
		}
	}

	if baseWarns != newWarns {
		o.warningDiff = &WarningCountDiff{Station: station, Base: baseWarns, New: newWarns}
	}
	o.identical = o.appended.Total == 0 && o.removed.Total == 0 && o.rowsRevised == 0 &&
		o.headerDiffs.Total == 0 && o.warningDiff == nil
	return o
}

// parseDailyFile parses one daily station file: header, rows keyed by
// date, and the total parser warning count. Duplicate dates within one
// file keep the last occurrence, mirroring ingest's UPSERT convergence.
func parseDailyFile(path string) (header conagua.Header, rows map[string]conagua.DailyRow, warnings int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return conagua.Header{}, nil, 0, err
	}
	defer func() { _ = f.Close() }()

	br := bufio.NewReader(f)
	header, headerWarnings, err := conagua.ParseHeader(br)
	if err != nil {
		return conagua.Header{}, nil, 0, fmt.Errorf("parse header: %w", err)
	}
	warnings = len(headerWarnings)

	rows = make(map[string]conagua.DailyRow)
	err = conagua.ParseDaily(br, 0,
		func(r conagua.DailyRow) { rows[r.Date] = r },
		func(conagua.Warning) { warnings++ },
	)
	if err != nil {
		return conagua.Header{}, nil, 0, fmt.Errorf("parse daily: %w", err)
	}
	return header, rows, warnings, nil
}

// compareNormalsStation parses both sides' normals files for the
// period implied by kind and compares the full parsed structs: header,
// all 12 monthly rows, all 12 extras rows, and the warning count. The
// period itself needs no explicit comparison — ParseNormalsFile
// cross-checks each file's stamped period against the kind-derived
// expectation, so a divergent period on either side surfaces as a
// parse-error finding.
func compareNormalsStation(kind conagua.Kind, station, basePath, newPath string) stationOutcome {
	// Membership in valueKinds guarantees kind is a normals kind here.
	period, _ := conagua.PeriodForKind(kind)

	var o stationOutcome
	baseHdr, baseRows, baseExtras, baseWarns, baseErr := parseNormalsFileAt(basePath, period)
	newHdr, newRows, newExtras, newWarns, newErr := parseNormalsFileAt(newPath, period)
	if baseErr != nil {
		o.parseErrors = append(o.parseErrors, ParseError{Station: station, Side: SideBase, Err: baseErr.Error()})
	}
	if newErr != nil {
		o.parseErrors = append(o.parseErrors, ParseError{Station: station, Side: SideNew, Err: newErr.Error()})
	}
	if baseErr != nil || newErr != nil {
		return o
	}
	o.compared = true

	for _, rev := range diffHeader(station, baseHdr, newHdr) {
		o.headerDiffs.Add(rev)
	}

	// ParseNormalsFile always emits exactly 12 rows and 12 extras
	// (one per calendar month), so positional iteration is safe.
	for i := 0; i < 12; i++ {
		c := revCollector{station: station, month: i + 1}
		diffNormalsRow(&c, baseRows[i], newRows[i])
		diffExtrasRow(&c, baseExtras[i], newExtras[i])
		for _, rev := range c.revs {
			o.revisions.Add(rev)
		}
	}

	if baseWarns != newWarns {
		o.warningDiff = &WarningCountDiff{Station: station, Base: baseWarns, New: newWarns}
	}
	o.identical = o.revisions.Total == 0 && o.headerDiffs.Total == 0 && o.warningDiff == nil
	return o
}

// parseNormalsFileAt opens and parses one normals station file.
func parseNormalsFileAt(path, period string) (conagua.Header, []conagua.MonthlyNormalsRow, []conagua.MonthlyNormalsExtrasRow, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return conagua.Header{}, nil, nil, 0, err
	}
	defer func() { _ = f.Close() }()

	header, rows, extras, warnings, err := conagua.ParseNormalsFile(f, period)
	if err != nil {
		return conagua.Header{}, nil, nil, 0, err
	}
	return header, rows, extras, len(warnings), nil
}

// The diff* functions below itemize every field of their conagua struct
// except the row key (Date / Month), which the caller aligned by
// construction. The completeness spec pins each itemization to the
// struct definition via reflection, so a field added upstream cannot
// silently escape the parity gate.

// diffDailyRow compares every DailyRow value field for one aligned date.
func diffDailyRow(c *revCollector, base, updated conagua.DailyRow) {
	c.addFloat("Tmax", base.Tmax, updated.Tmax)
	c.addFloat("Tmin", base.Tmin, updated.Tmin)
	c.addFloat("Precip", base.Precip, updated.Precip)
	c.addFloat("Evap", base.Evap, updated.Evap)
}

// diffNormalsRow compares every MonthlyNormalsRow value field for one
// aligned calendar month.
func diffNormalsRow(c *revCollector, base, updated conagua.MonthlyNormalsRow) {
	c.addFloat("Tmax", base.Tmax, updated.Tmax)
	c.addFloat("Tmin", base.Tmin, updated.Tmin)
	c.addFloat("Tmean", base.Tmean, updated.Tmean)
	c.addFloat("Precip", base.Precip, updated.Precip)
	c.addFloat("Evap", base.Evap, updated.Evap)
}

// diffExtrasRow compares every MonthlyNormalsExtrasRow value field for
// one aligned calendar month.
func diffExtrasRow(c *revCollector, base, updated conagua.MonthlyNormalsExtrasRow) {
	c.addFloat("TmaxMonthlyExtreme", base.TmaxMonthlyExtreme, updated.TmaxMonthlyExtreme)
	c.addInt("TmaxMonthlyExtremeYear", base.TmaxMonthlyExtremeYear, updated.TmaxMonthlyExtremeYear)
	c.addFloat("TmaxDailyExtreme", base.TmaxDailyExtreme, updated.TmaxDailyExtreme)
	c.addStr("TmaxDailyExtremeDate", base.TmaxDailyExtremeDate, updated.TmaxDailyExtremeDate)
	c.addFloat("TminMonthlyExtreme", base.TminMonthlyExtreme, updated.TminMonthlyExtreme)
	c.addInt("TminMonthlyExtremeYear", base.TminMonthlyExtremeYear, updated.TminMonthlyExtremeYear)
	c.addFloat("TminDailyExtreme", base.TminDailyExtreme, updated.TminDailyExtreme)
	c.addStr("TminDailyExtremeDate", base.TminDailyExtremeDate, updated.TminDailyExtremeDate)
	c.addFloat("PrecipMonthlyExtreme", base.PrecipMonthlyExtreme, updated.PrecipMonthlyExtreme)
	c.addInt("PrecipMonthlyExtremeYear", base.PrecipMonthlyExtremeYear, updated.PrecipMonthlyExtremeYear)
	c.addFloat("PrecipDailyExtreme", base.PrecipDailyExtreme, updated.PrecipDailyExtreme)
	c.addStr("PrecipDailyExtremeDate", base.PrecipDailyExtremeDate, updated.PrecipDailyExtremeDate)
	c.addInt("TmaxYearsWithData", base.TmaxYearsWithData, updated.TmaxYearsWithData)
	c.addInt("TminYearsWithData", base.TminYearsWithData, updated.TminYearsWithData)
	c.addInt("TmeanYearsWithData", base.TmeanYearsWithData, updated.TmeanYearsWithData)
	c.addInt("PrecipYearsWithData", base.PrecipYearsWithData, updated.PrecipYearsWithData)
	c.addInt("EvapYearsWithData", base.EvapYearsWithData, updated.EvapYearsWithData)
	c.addFloat("RainDays", base.RainDays, updated.RainDays)
	c.addInt("RainDaysYearsWithData", base.RainDaysYearsWithData, updated.RainDaysYearsWithData)
}

// diffHeader compares every parsed station-header field and returns one
// Revision per mismatch. The emission stamp and availability lines are
// not part of the parsed Header, so they are excluded by construction.
func diffHeader(station string, base, updated conagua.Header) []Revision {
	c := revCollector{station: station}
	c.addText("Header.ExternalID", base.ExternalID, updated.ExternalID)
	c.addText("Header.Name", base.Name, updated.Name)
	c.addText("Header.State", base.State, updated.State)
	c.addText("Header.Municipality", base.Municipality, updated.Municipality)
	c.addText("Header.Status", string(base.Status), string(updated.Status))
	c.addText("Header.CVEOMM", base.CVEOMM, updated.CVEOMM)
	c.addFloat("Header.Lat", base.Lat, updated.Lat)
	c.addFloat("Header.Lon", base.Lon, updated.Lon)
	c.addFloat("Header.AltitudeM", base.AltitudeM, updated.AltitudeM)
	return c.revs
}

// revCollector accumulates field-level Revisions for one station scope
// (a daily row, a normals month, or the header).
type revCollector struct {
	station string
	date    string
	month   int
	revs    []Revision
}

func (c *revCollector) record(field, baseVal, newVal string) {
	c.revs = append(c.revs, Revision{
		Station: c.station,
		Date:    c.date,
		Month:   c.month,
		Field:   field,
		Base:    baseVal,
		New:     newVal,
	})
}

func (c *revCollector) addFloat(field string, base, updated *float64) {
	if !ptrEq(base, updated) {
		c.record(field, formatFloat(base), formatFloat(updated))
	}
}

func (c *revCollector) addInt(field string, base, updated *int) {
	if !ptrEq(base, updated) {
		c.record(field, formatInt(base), formatInt(updated))
	}
}

func (c *revCollector) addStr(field string, base, updated *string) {
	if !ptrEq(base, updated) {
		c.record(field, formatString(base), formatString(updated))
	}
}

func (c *revCollector) addText(field, base, updated string) {
	if base != updated {
		c.record(field, base, updated)
	}
}

// ptrEq compares two optional values: both nil is equal, one nil is
// not, otherwise the pointees are compared. Parsed floats come from
// strconv over the source tokens on both sides, so exact equality (not
// approximate matching) is the correct comparison.
func ptrEq[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// formatFloat renders an optional float for a Revision; NULL mirrors
// how ingest persists an absent value.
func formatFloat(p *float64) string {
	if p == nil {
		return "NULL"
	}
	return strconv.FormatFloat(*p, 'g', -1, 64)
}

func formatInt(p *int) string {
	if p == nil {
		return "NULL"
	}
	return strconv.Itoa(*p)
}

func formatString(p *string) string {
	if p == nil {
		return "NULL"
	}
	return *p
}
