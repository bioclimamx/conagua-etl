package publish

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// dailyJSONName is the per-station daily series file, profile.json's
// sibling under combined/<slug>/<station_id>/.
const dailyJSONName = "daily.json"

// dailyJSONPath is one station's daily.json inside a JSON archive.
func dailyJSONPath(st State, externalID string) string {
	return "combined/" + st.Slug + "/" + externalID + "/" + dailyJSONName
}

// dailyJSONShape is the daily.json row shape, derived from
// CombinedDaily's columns so daily.json and combined_daily are one
// product in two formats — the same export names, the same decimals,
// the same row set. columns holds the scanned columns in
// select order: the spine's date, then the observed block
// (daily_observations' values in DDL order), then the reanalysis block
// (POWER-31 in registry order); observed and reanalysis are the two
// blocks as sub-slices of columns. The join context (cell_id,
// distance_km) is per station, not per day, and belongs to the
// profile's power_cell block. Every key is rendered once at init, so
// writing a row appends bytes and never marshals.
type dailyJSONShape struct {
	columns    []Column
	dateKey    string
	observed   []Column
	reanalysis []Column
	// observedKeys and reanalysisKeys are the blocks' keys pre-rendered
	// as `"name":`, index-aligned with observed and reanalysis.
	observedKeys   []string
	reanalysisKeys []string
}

// dailyJSON is the row shape of every daily.json.
var dailyJSON = newDailyJSONShape()

// newDailyJSONShape partitions CombinedDaily's columns into the row
// shape. A partition that misses the date or either block means
// CombinedDaily no longer carries the composed blocks this file is
// derived from — a programming error caught at init, before any file is
// written.
func newDailyJSONShape() *dailyJSONShape {
	var (
		s    dailyJSONShape
		date []Column
	)
	for _, c := range CombinedDaily.Columns {
		switch source := CombinedDaily.Source(c); {
		case c.DB == "date" && source == DailyObservations.Table:
			date = append(date, c)
		case source == DailyObservations.Table && !c.Key:
			s.observed = append(s.observed, c)
		case source == PowerDaily.Table:
			s.reanalysis = append(s.reanalysis, c)
		}
	}
	if len(date) != 1 || len(s.observed) == 0 || len(s.reanalysis) == 0 {
		panic("publish: CombinedDaily does not carry the date, observed, and POWER blocks daily.json is derived from")
	}
	s.columns = append(append(append(s.columns, date...), s.observed...), s.reanalysis...)
	s.dateKey = jsonKey(date[0].Name)
	s.observedKeys = jsonKeys(s.observed)
	s.reanalysisKeys = jsonKeys(s.reanalysis)
	return &s
}

// jsonKey renders a column name as an object key with its separator.
func jsonKey(name string) string {
	return string(appendJSONQuoted(nil, name)) + ":"
}

func jsonKeys(cols []Column) []string {
	keys := make([]string, len(cols))
	for i, c := range cols {
		keys[i] = jsonKey(c.Name)
	}
	return keys
}

// dailyJSONSQL is the combined daily spine (combinedDailyFromSQL, the
// clause combined_daily.csv's query is built on) under daily.json's own
// select list: the date, the observed values, POWER-31, then the
// sentinel the row shape needs and the CSV does not — whether the LEFT
// join matched a supplement row at all. ds.cell_id is NOT NULL on the
// supplement side, so it is NULL exactly when no row matched; the 31
// values cannot tell that case from a matched row whose values are all
// NULL, and the row shape renders the two differently. The
// placeholders are the FROM clause's, bound in its order: the cell,
// then the station.
var dailyJSONSQL = "SELECT " + selectColumns(dailyJSON.columns, func(c Column) string {
	if CombinedDaily.Source(c) == PowerDaily.Table {
		return "ds." + c.DB
	}
	return "d." + c.DB
}) + ", ds.cell_id IS NOT NULL" + combinedDailyFromSQL

// dailyJSONEntry is one station's daily.json entry. The query runs when
// the archive writer calls Write, so the single-connection DB is touched
// by one entry at a time. The error is not prefixed with the station:
// the entry path the zip writer adds already carries the id.
func dailyJSONEntry(ctx context.Context, db *sql.DB, st State, station stationRef) archive.Entry {
	return archive.Entry{
		Path: dailyJSONPath(st, station.externalID),
		Write: func(w io.Writer) error {
			return writeDailyJSON(ctx, w, db, station)
		},
	}
}

// writeDailyJSON streams one station's daily series into w as
// daily.json: a top-level array, one row object per line, date
// ascending, compact — "[", the rows separated by ",\n", "]", a trailing
// LF; an empty series is "[\n]\n". A row is
//
//	{"date":…,"observed":{tmax_c,tmin_c,precip_mm,evap_mm},"reanalysis":{POWER-31}|null}
//
// observed is always present, with per-field nulls; reanalysis is the
// whole-object null when the LEFT join matched no supplement row, and
// the full 31-key object — per-field nulls included — when it did.
// Numbers are the fixed-decimal literals the CSV carries, through the
// same formatter, so the file is byte-reproducible. Each row is
// assembled in one reused buffer and written through a bufio.Writer:
// the assembler allocates nothing per row or per value, so a national
// export's per-row cost is the scan alone. Cancellation surfaces
// through rows.Err(): database/sql closes a QueryContext row set when
// its context ends.
func writeDailyJSON(ctx context.Context, w io.Writer, db *sql.DB, station stationRef) error {
	rows, err := db.QueryContext(ctx, dailyJSONSQL, station.cellID, station.id)
	if err != nil {
		return fmt.Errorf("query daily series: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	var (
		date       string
		observed   = make([]sql.NullFloat64, len(dailyJSON.observed))
		reanalysis = make([]sql.NullFloat64, len(dailyJSON.reanalysis))
		hasRow     bool
	)
	dest := make([]any, 0, 2+len(observed)+len(reanalysis))
	dest = append(dest, &date)
	for i := range observed {
		dest = append(dest, &observed[i])
	}
	for i := range reanalysis {
		dest = append(dest, &reanalysis[i])
	}
	dest = append(dest, &hasRow)

	bw := bufio.NewWriter(w)
	if _, err := bw.WriteString("[\n"); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	buf := make([]byte, 0, 1024)
	n := 0
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("scan row %d: %w", n+1, err)
		}
		buf = buf[:0]
		if n > 0 {
			buf = append(buf, ",\n"...)
		}
		buf, err = dailyJSON.appendRow(buf, date, observed, reanalysis, hasRow)
		if err != nil {
			return fmt.Errorf("row %d: %w", n+1, err)
		}
		if _, err := bw.Write(buf); err != nil {
			return fmt.Errorf("write row %d: %w", n+1, err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	end := "]\n"
	if n > 0 {
		end = "\n]\n"
	}
	if _, err := bw.WriteString(end); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

// appendRow appends one row object. The date is the only stored string
// the row carries; one that is not valid UTF-8 is refused rather than
// copied into a document that claims to be UTF-8 — ingest pins the
// YYYY-MM-DD shape, so an invalid byte is a corruption signal.
func (s *dailyJSONShape) appendRow(buf []byte, date string, observed, reanalysis []sql.NullFloat64, hasRow bool,
) ([]byte, error) {
	if !utf8.ValidString(date) {
		return nil, fmt.Errorf("date %q is not valid UTF-8", date)
	}
	buf = append(buf, '{')
	buf = append(buf, s.dateKey...)
	buf = appendJSONQuoted(buf, date)
	buf = append(buf, `,"observed":`...)
	buf, err := appendJSONBlock(buf, s.observedKeys, s.observed, observed)
	if err != nil {
		return nil, err
	}
	buf = append(buf, `,"reanalysis":`...)
	if !hasRow {
		buf = append(buf, "null"...)
	} else if buf, err = appendJSONBlock(buf, s.reanalysisKeys, s.reanalysis, reanalysis); err != nil {
		return nil, err
	}
	return append(buf, '}'), nil
}

// appendJSONBlock appends one {key:value,…} object: each value is its
// column's fixed-decimal literal through appendReal — the CSV's own
// formatter, so the decimals and positive zero are one implementation —
// or null where the CSV has an empty field; a non-finite value is
// refused by checkFinite as the CSV cell is.
func appendJSONBlock(buf []byte, keys []string, cols []Column, vals []sql.NullFloat64) ([]byte, error) {
	buf = append(buf, '{')
	for i, c := range cols {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, keys[i]...)
		if !vals[i].Valid {
			buf = append(buf, "null"...)
			continue
		}
		if err := checkFinite(c.Name, vals[i].Float64); err != nil {
			return nil, err
		}
		buf = appendReal(buf, vals[i].Float64, c.Decimals)
	}
	return append(buf, '}'), nil
}

// appendJSONQuoted appends s as a JSON string with RFC 8259's minimal
// escaping — the quote, the backslash, and control characters; every
// other byte passes through as encodeManifest writes it (HTML escaping
// off), so the caller vouches for UTF-8 validity. The strings this
// writer emits are column names, fixed at init, and dates, which
// appendRow validates.
func appendJSONQuoted(buf []byte, s string) []byte {
	const hex = "0123456789abcdef"
	buf = append(buf, '"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"' || c == '\\':
			buf = append(buf, '\\', c)
		case c == '\n':
			buf = append(buf, '\\', 'n')
		case c == '\r':
			buf = append(buf, '\\', 'r')
		case c == '\t':
			buf = append(buf, '\\', 't')
		case c < 0x20:
			buf = append(buf, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
		default:
			buf = append(buf, c)
		}
	}
	return append(buf, '"')
}
