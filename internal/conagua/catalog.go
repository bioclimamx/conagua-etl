package conagua

import (
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

// Kind identifies one of the 7 file kinds CONAGUA publishes per station:
// daily observations, monthly statistics, historical extremes, and the four
// OMM normals periods.
type Kind string

// The seven kinds. The string values double as snapshot directory names
// (conagua-raw/<date>/<kind>/), so they are load-bearing on disk and must
// stay stable.
const (
	KindDaily            Kind = "daily"
	KindMonthly          Kind = "monthly"
	KindExtremes         Kind = "extremes"
	KindNormals1961_1990 Kind = "normals_1961_1990"
	KindNormals1971_2000 Kind = "normals_1971_2000"
	KindNormals1981_2010 Kind = "normals_1981_2010"
	KindNormals1991_2020 Kind = "normals_1991_2020"
)

// AllKinds lists every Kind in canonical (roughly chronological) order.
// The normals tail derives from NormalsKinds so the four normals
// literals are enumerated exactly once.
var AllKinds = append([]Kind{KindDaily, KindMonthly, KindExtremes}, NormalsKinds...)

// NormalsKinds lists the four ingestable normals kinds, oldest period
// first. It is the single authoritative enumeration of the normals
// vocabulary: consumers derive from it (via PeriodForKind for the DB
// period strings) rather than re-enumerating kinds or periods, so the
// set has exactly one owner.
var NormalsKinds = []Kind{
	KindNormals1961_1990, KindNormals1971_2000,
	KindNormals1981_2010, KindNormals1991_2020,
}

// kindByDir maps the CONAGUA file-tree directory name to its Kind. The
// catalog page links are relative and classified by their directory segment,
// which is more robust to HTML reshuffling than relying on column position.
var kindByDir = map[string]Kind{
	"Diarios":      KindDaily,
	"Mensuales":    KindMonthly,
	"Med-Extr":     KindExtremes,
	"Normales6190": KindNormals1961_1990,
	"Normales7100": KindNormals1971_2000,
	"Normales8110": KindNormals1981_2010,
	"Normales9120": KindNormals1991_2020,
}

// Status is a station's operational state as reported by CONAGUA.
type Status string

// Station operational states, normalized from CONAGUA's Spanish labels
// ("operando" / "suspendida"); anything unrecognized maps to StatusUnknown.
const (
	StatusOperating Status = "operating"
	StatusSuspended Status = "suspended"
	StatusUnknown   Status = "unknown"
)

func parseStatus(s string) Status {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "operando":
		return StatusOperating
	case "suspendida", "suspendido":
		return StatusSuspended
	default:
		return StatusUnknown
	}
}

// FileEntry is a reference to one CONAGUA station file as advertised in a
// state catalog page. The URL is the absolute, resolved form.
type FileEntry struct {
	URL string `json:"url"`
}

// Station is one row of a state catalog page. The ID is the 5-digit
// zero-padded form used in CONAGUA URLs, not the 4-digit form displayed in
// the HTML table.
type Station struct {
	State        StateCode          `json:"state"`
	ID           string             `json:"id"`
	Name         string             `json:"name"`
	Municipality string             `json:"municipality"`
	Status       Status             `json:"status"`
	Files        map[Kind]FileEntry `json:"files"`
}

// stationIDFromFilename captures the trailing run of digits before ".txt" in
// a station file URL. Works for all 7 kinds (e.g. dia01001.txt, mes01001.txt,
// medex01001.txt, nor9120_01001.txt) because the period prefix ("9120_") is
// separated from the ID by a non-digit character.
var stationIDFromFilename = regexp.MustCompile(`(\d+)\.txt$`)

// ShortID converts CONAGUA's 5-digit URL form ("01001") to the short
// integer form the file headers print ("1001"), which is the canonical
// stations.external_id value. Non-numeric IDs are returned as-is so
// historical catalog oddballs still round-trip.
func ShortID(catalogID string) string {
	s := strings.TrimSpace(catalogID)
	if s == "" {
		return s
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return s
		}
	}
	// strings.TrimLeft("0", "0") returns "", so guard the all-zero edge
	// (unlikely in real data but trivial to handle).
	trimmed := strings.TrimLeft(s, "0")
	if trimmed == "" {
		return "0"
	}
	return trimmed
}

// ParseCatalog parses a state catalog page into a list of stations.
//
// state is the StateCode the HTML belongs to (stored on each Station so that
// concatenating catalogs from different states doesn't lose that context).
// pageURL is the URL the HTML was served from; anchor hrefs are resolved
// against it, so it must be accurate (use StateCode.CatalogURL for live fetches).
func ParseCatalog(state StateCode, pageURL string, r io.Reader) ([]Station, error) {
	if !state.Valid() {
		return nil, fmt.Errorf("invalid state code %q", state)
	}
	base, err := url.Parse(pageURL)
	if err != nil {
		return nil, fmt.Errorf("parse pageURL: %w", err)
	}
	root, err := html.Parse(r)
	if err != nil {
		return nil, fmt.Errorf("parse html: %w", err)
	}

	rows := collectDataRows(root)
	stations := make([]Station, 0, len(rows))
	for _, row := range rows {
		st, ok := parseRow(state, base, row)
		if !ok {
			continue
		}
		stations = append(stations, st)
	}
	return stations, nil
}

// collectDataRows returns all <tr> elements that look like station rows.
// Header rows are excluded via isDataRow.
func collectDataRows(n *html.Node) []*html.Node {
	var out []*html.Node
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.DataAtom == atom.Tr {
			if isDataRow(n) {
				out = append(out, n)
			}
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return out
}

// claveRE matches the clave cell text of a real station row: an all-digit
// run with optional surrounding whitespace.
var claveRE = regexp.MustCompile(`^\d+$`)

// isDataRow reports whether tr is a station row (vs a header row). Station
// rows have no colspan/rowspan, have ≥ 8 <td> cells (4 metadata + ≥ 4 file
// cells), and their first cell contains a numeric clave.
func isDataRow(tr *html.Node) bool {
	var first *html.Node
	cells := 0
	for c := tr.FirstChild; c != nil; c = c.NextSibling {
		if c.Type != html.ElementNode || c.DataAtom != atom.Td {
			continue
		}
		for _, a := range c.Attr {
			if a.Key == "colspan" || a.Key == "rowspan" {
				return false
			}
		}
		if first == nil {
			first = c
		}
		cells++
	}
	if cells < 8 || first == nil {
		return false
	}
	return claveRE.MatchString(textContent(first))
}

// parseRow extracts one Station from a data row. Returns ok=false if the
// row is malformed (no clave, fewer than 4 cells).
func parseRow(state StateCode, base *url.URL, tr *html.Node) (Station, bool) {
	var cells []*html.Node
	for c := tr.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && c.DataAtom == atom.Td {
			cells = append(cells, c)
		}
	}
	if len(cells) < 4 {
		return Station{}, false
	}

	clave := textContent(cells[0])
	if clave == "" {
		return Station{}, false
	}

	files := make(map[Kind]FileEntry)
	for _, cell := range cells[4:] {
		href, ok := anchorHref(cell)
		if !ok {
			continue
		}
		resolved, err := base.Parse(href)
		if err != nil {
			continue
		}
		kind, ok := kindFromURL(resolved)
		if !ok {
			continue
		}
		files[kind] = FileEntry{URL: resolved.String()}
	}

	return Station{
		State:        state,
		ID:           canonicalStationID(files, clave),
		Name:         textContent(cells[1]),
		Municipality: textContent(cells[2]),
		Status:       parseStatus(textContent(cells[3])),
		Files:        files,
	}, true
}

// kindFromURL classifies a resolved file URL by its first recognised path
// segment.
func kindFromURL(u *url.URL) (Kind, bool) {
	for p := range strings.SplitSeq(strings.TrimPrefix(u.Path, "/"), "/") {
		if k, ok := kindByDir[p]; ok {
			return k, true
		}
	}
	return "", false
}

// canonicalStationID derives the 5-digit station ID from the first available
// file URL (CONAGUA uses 5-digit IDs in URLs regardless of how the clave is
// rendered in the table). Falls back to zero-padding the displayed clave.
func canonicalStationID(files map[Kind]FileEntry, clave string) string {
	// AllKinds order makes the pick deterministic (not a correctness need —
	// every URL of a consistent catalog row yields the same ID).
	for _, k := range AllKinds {
		if m := stationIDFromFilename.FindStringSubmatch(files[k].URL); len(m) > 1 {
			return m[1]
		}
	}
	if len(clave) < 5 {
		return strings.Repeat("0", 5-len(clave)) + clave
	}
	return clave
}

// anchorHref returns the href of the first <a> descendant of n, if any.
func anchorHref(n *html.Node) (string, bool) {
	var href string
	var found bool
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if found {
			return
		}
		if n.Type == html.ElementNode && n.DataAtom == atom.A {
			for _, a := range n.Attr {
				if a.Key == "href" {
					href = a.Val
					found = true
					return
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return href, found
}

// textContent concatenates the descendant text of n with runs of whitespace
// collapsed into single spaces.
func textContent(n *html.Node) string {
	var sb strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.TextNode {
			sb.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return strings.Join(strings.Fields(sb.String()), " ")
}
