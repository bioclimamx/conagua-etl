package publish

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// The docs group's metadata files: LICENSE, NOTICE, CITATION.cff,
// zenodo-metadata.json, and README.md. Each has one renderer over a
// DocsInput; QA-REPORT.md (RenderQA) and the data dictionary have their
// own. Every renderer is deterministic — no wall clock reaches a file
// except the two build-time stamps, CITATION.cff's date-released and the
// Zenodo stub's publication_date — and every number in the text comes
// from the DB-loaded inputs, the POWER registry, the conagua vocabulary,
// the schema, or the file specs, never from a literal.

// RepositoryURL is the code repository the citation and the README
// point at.
const RepositoryURL = "https://github.com/bioclimamx/conagua-etl"

// The upstream sources NOTICE credits.
const (
	// smnNormalsPageURL is the SMN page the per-state catalog is reached
	// from; conagua.BaseURL is the file tree under it.
	smnNormalsPageURL = "https://smn.conagua.gob.mx/es/climatologia/informacion-climatologica/normales-climatologicas-por-estado"
	// libreUsoMXURL is the canonical name of the Términos de Libre Uso
	// MX, cited as such whether or not the page resolves (NOTICE, Known
	// gaps).
	libreUsoMXURL = "https://datos.gob.mx/libreusomx"
	// gobMXTermsURL is the generic terms page the SMN site footer links —
	// the transparency wrinkle NOTICE states.
	gobMXTermsURL = "https://www.gob.mx/terminos"
	// powerReferencingURL is where NASA POWER's data-reference template
	// and funding acknowledgement come from.
	powerReferencingURL = "https://power.larc.nasa.gov/docs/referencing/"
)

// The Libre Uso MX clauses NOTICE reproduces, verbatim from the license
// text: the attribution elements and the no-mislead / no-endorsement
// line. Source-matching strings — the one Spanish the ETL emits beyond
// data values.
const (
	libreUsoAttributionClause = "Nombre del conjunto de datos, [Dependencia/Entidad siglas]; Liga de internet " +
		"de los datos descargados, y la fecha de consulta en formato numérico [AAAA-MM-DD]"
	libreUsoNoMisleadClause = "No utilizar la información con objeto de engañar o confundir a la población " +
		"variando el sentido original de la información y su veracidad. No aparentar que el uso que usted " +
		"haga de los datos representa una postura oficial del Gobierno o que el mismo está avalado por la " +
		"fuente de origen."
)

// powerFundingSentence is NASA POWER's funding acknowledgement,
// verbatim.
const powerFundingSentence = "The data was obtained from National Aeronautics and Space Administration (NASA) " +
	"Langley Research Center's Prediction Of Worldwide Energy Resources (POWER) project funded through the " +
	"NASA Earth Science Division."

// powerVersionUnknown stands where NASA POWER's reference template puts
// the API version: the ETL never recorded it (power_runs has no version
// column, and no raw response was kept), so the string names the access
// dates and says so rather than inventing a number.
const powerVersionUnknown = "(API version as served on the access dates; the version string was not " +
	"recorded by the ETL — see Known gaps)"

// datasetKeywords is the fixed keyword list CITATION.cff and the Zenodo
// stub share.
var datasetKeywords = []string{
	"climate", "Mexico", "CONAGUA", "weather stations", "climate normals", "daily observations",
	"NASA POWER", "MERRA-2", "reanalysis", "bioclimatic",
}

// docsLineWidth is where the renderers wrap prose. Tables, bullets, and
// verbatim lines are never wrapped.
const docsLineWidth = 78

// licenseText is the CC BY 4.0 legal code as Creative Commons publishes
// it for verbatim inclusion, the text LICENSE carries. Embedded so the
// deposit never depends on a network fetch, and held to its digest at
// init: a binary whose asset drifted refuses to start rather than ship a
// license text that is not the license.
//
//go:embed assets/LICENSE-CC-BY-4.0.txt
var licenseText string

// LicenseSHA256 is the sha256 of the embedded legal code — the digest
// LICENSE ships with.
const LicenseSHA256 = "9ba9550ad48438d0836ddab3da480b3b69ffa0aac7b7878b5a0039e7ab429411"

func init() {
	if got := sha256Hex(licenseText); got != LicenseSHA256 {
		panic(fmt.Sprintf("publish: embedded CC BY 4.0 legal code has sha256 %s, not the pinned %s", got, LicenseSHA256))
	}
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// WriteLicense writes LICENSE: the embedded CC BY 4.0 legal code,
// verbatim.
func WriteLicense(w io.Writer) error {
	if _, err := io.WriteString(w, licenseText); err != nil {
		return fmt.Errorf("write LICENSE: %w", err)
	}
	return nil
}

// DocsInput is what the docs-group renderers read — data Run has already
// loaded by the time the group is built, never re-queried: the build's
// identity and provenance (Meta: the schema version, the git SHA, the
// snapshot date, the runs by natural label, the dataset identity), the
// per-table counts, the QA report's coverage section (the observed and
// reanalysis extents, normals and POWER by period, stations by status),
// every state of the DB (LoadStates' full list, so the docs describe the
// whole deposit whatever --state selected), and Now — the one wall-clock
// input, read only by CITATION.cff's date-released and the Zenodo stub's
// publication_date (its UTC date), the two build-time stamps. README.md
// and NOTICE never read it.
type DocsInput struct {
	Meta ProfileMeta
	// Stations is the CONAGUA conventional station count — the station
	// set the flat files export (LoadStationCount), which Counts.Stations
	// exceeds by any non-conventional stations row the DB holds.
	Stations int64
	Counts   TableCounts
	Coverage QACoverage
	States   []State
	Now      time.Time
	// RawHTMLPages are the snapshot-relative paths of the station files
	// in the raw artifact that are HTML error pages, not CONAGUA data
	// (RawHTMLPages). Nil when there are none, or when the run had no
	// local snapshot to read — a full run always has one, since it ships
	// the raw artifact.
	RawHTMLPages []string
}

// stationCountSQL counts the stations the flat files export: the
// conventional source, as stationListSQL scopes every station list.
const stationCountSQL = `SELECT COUNT(*) FROM stations WHERE source = ?`

// LoadStationCount returns the number of CONAGUA conventional stations
// — the headline count the README and the abstract state.
func LoadStationCount(ctx context.Context, db *sql.DB) (int64, error) {
	var n int64
	if err := db.QueryRowContext(ctx, stationCountSQL, string(ingest.SourceConaguaConventional)).Scan(&n); err != nil {
		return 0, fmt.Errorf("count stations: %w", err)
	}
	return n, nil
}

// buildDate is the build-time stamp the two dated files carry: Now's
// date in UTC, so the value does not depend on the building machine's
// zone.
func (in DocsInput) buildDate() string {
	return in.Now.UTC().Format(snapshotDateLayout)
}

// ceresColumns lists the POWER-31 columns whose values come from NASA
// CERES rather than MERRA-2 — the solar, radiation, and cloud streams —
// read from each parameter's recorded lineage (power.Param.Upstream),
// never inferred from its unit: cloud_amt_pct is CERES and is served in
// percent, so a unit-based rule would misfile it. The remaining
// columns are MERRA-2's meteorology, moisture, and soil.
func ceresColumns() []string {
	return power.ColumnsFrom(power.UpstreamCERES)
}

// upstreamOf names the NASA product a POWER column's values come from,
// for the per-column extent table: it is the lineage that explains why
// the columns start on different dates.
func upstreamOf(column string) string {
	for _, p := range power.Registry {
		if p.Column == column {
			return p.Upstream
		}
	}
	return ""
}

// powerAccessDates returns the distinct dates the provenance power runs
// started on, in NASA POWER's reference format (YYYY/MM/DD), ascending.
// started_at is the RFC 3339 UTC string power writes; anything else is
// refused rather than mis-dated in a credit line.
func powerAccessDates(runs []PowerRunRef) ([]string, error) {
	var dates []string
	for _, r := range runs {
		t, err := time.Parse(time.RFC3339, r.StartedAt)
		if err != nil {
			return nil, fmt.Errorf("power run %s: started_at %q is not RFC 3339: %w", r.RunLabel, r.StartedAt, err)
		}
		dates = append(dates, t.UTC().Format("2006/01/02"))
	}
	slices.Sort(dates)
	return slices.Compact(dates), nil
}

// powerResolutions names the temporal resolutions of the provenance
// power runs as NASA POWER's reference template words them (Daily,
// Monthly), distinct, ascending.
func powerResolutions(runs []PowerRunRef) []string {
	var out []string
	for _, r := range runs {
		out = append(out, capitalize(r.TemporalMode))
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	r, size := utf8.DecodeRuneInString(s)
	return strings.ToUpper(string(r)) + s[size:]
}

// joinAnd renders a list as English prose: "a", "a and b", "a, b, and c".
func joinAnd(items []string) string {
	switch len(items) {
	case 0:
		return ""
	case 1:
		return items[0]
	case 2:
		return items[0] + " and " + items[1]
	}
	return strings.Join(items[:len(items)-1], ", ") + ", and " + items[len(items)-1]
}

// wrap breaks text into lines of at most width runes at spaces; a word
// longer than width (a URL) stands on its own line. Deterministic, so
// wrapped prose is as reproducible as a table.
func wrap(text string, width int) []string {
	var (
		lines []string
		line  string
	)
	for _, word := range strings.Fields(text) {
		switch {
		case line == "":
			line = word
		case utf8.RuneCountInString(line)+1+utf8.RuneCountInString(word) <= width:
			line += " " + word
		default:
			lines = append(lines, line)
			line = word
		}
	}
	if line != "" {
		lines = append(lines, line)
	}
	return lines
}

// extentPhrase renders a dated series' extent for prose. The dates are
// the bounds of the rows carrying a value, never the raw row bounds: a
// mirrored all-NULL row is no observation, and the advertised extent
// must not reach a date nothing was measured on (QA-REPORT.md keeps
// both).
func extentPhrase(e QASeriesExtent, keys string) string {
	if e.FirstDate == nil || e.LastDate == nil {
		return "no rows"
	}
	if e.ValueFirstDate == nil || e.ValueLastDate == nil {
		return fmt.Sprintf("%s rows across %s %s, none carrying a value", formatInt(e.Rows), formatInt(e.Keys), keys)
	}
	return fmt.Sprintf("%s rows across %s %s, %s to %s", formatInt(e.Rows), formatInt(e.Keys), keys,
		*e.ValueFirstDate, *e.ValueLastDate)
}

// periodsSplit separates the reference periods with POWER monthly rows
// from those without, in vocabulary order.
func periodsSplit(monthly []QAPowerPeriod) (with, without []string) {
	for _, p := range monthly {
		if p.Rows > 0 {
			with = append(with, p.Period)
		} else {
			without = append(without, p.Period)
		}
	}
	return with, without
}

// abstract is the dataset description CITATION.cff and the Zenodo stub
// share: one paragraph, every number from the inputs.
func abstract(in DocsInput) string {
	periodsWithPower, _ := periodsSplit(in.Coverage.Power.Monthly)
	var b strings.Builder
	fmt.Fprintf(&b, "Daily observations and published climatological normals from %s CONAGUA conventional "+
		"weather stations across Mexico (Servicio Meteorológico Nacional, Normales Climatológicas archive, "+
		"consulted %s), augmented with the %d NASA POWER reanalysis variables of each station's grid cell on "+
		"the %s grid: meteorology from MERRA-2, the %d solar, radiation, and cloud streams from CERES. ",
		formatInt(in.Stations), in.Meta.SnapshotDate, len(power.Registry), gridPhrase(),
		len(ceresColumns()))
	fmt.Fprintf(&b, "Observed daily series (maximum and minimum temperature, precipitation, evaporation): %s. ",
		extentPhrase(in.Coverage.Daily, "stations"))
	fmt.Fprintf(&b, "Normals cover the %d reference periods %s. ", len(normalsPeriods), joinAnd(normalsPeriods))
	fmt.Fprintf(&b, "POWER daily reanalysis: %s; POWER monthly climatology for %s. ",
		extentPhrase(in.Coverage.Power.Daily, "grid cells"), periodsPhrase(periodsWithPower))
	b.WriteString("The deposit holds per-state and national archives in CSV, Parquet, JSON, and SQLite, " +
		"the raw CONAGUA snapshot, a generated data dictionary, provenance manifests, and a QA report. " +
		"The compilation is licensed CC BY 4.0; upstream data keep their own terms (see NOTICE).")
	return b.String()
}

// gridPhrase renders power.Resolution ("0.5x0.625") as the grid step in
// degrees, latitude by longitude; a constant of another shape passes
// through verbatim.
func gridPhrase() string {
	lat, lon, ok := strings.Cut(power.Resolution, "x")
	if !ok {
		return power.Resolution
	}
	return lat + "° × " + lon + "° (latitude × longitude)"
}

func periodsPhrase(periods []string) string {
	if len(periods) == 0 {
		return "no reference period"
	}
	return joinAnd(periods)
}

// textWriter accumulates a plain-text file: wrapped paragraphs, verbatim
// lines, blank separators; LF line ends.
type textWriter struct {
	b *bytes.Buffer
}

func (t textWriter) line(s string) {
	t.b.WriteString(s)
	t.b.WriteByte('\n')
}

func (t textWriter) blank() { t.b.WriteByte('\n') }

// para writes one wrapped paragraph followed by a blank line.
func (t textWriter) para(text string) {
	for _, l := range wrap(text, docsLineWidth) {
		t.line(l)
	}
	t.blank()
}

// quote writes a wrapped paragraph indented as a quotation, followed by
// a blank line.
func (t textWriter) quote(text string) {
	for _, l := range wrap(text, docsLineWidth-4) {
		t.line("    " + l)
	}
	t.blank()
}

// bullet writes one wrapped list item with a hanging indent, followed
// by a blank line.
func (t textWriter) bullet(text string) {
	for i, l := range wrap(text, docsLineWidth-2) {
		if i == 0 {
			t.line("- " + l)
		} else {
			t.line("  " + l)
		}
	}
	t.blank()
}

// heading writes a section heading underlined with dashes.
func (t textWriter) heading(text string) {
	t.line(text)
	t.line(strings.Repeat("-", utf8.RuneCountInString(text)))
	t.blank()
}

// finish returns the accumulated text ending in exactly one newline.
func (t textWriter) finish() []byte {
	return append(bytes.TrimRight(t.b.Bytes(), "\n"), '\n')
}

// RenderNotice writes NOTICE: the four required elements, in order — the
// CONAGUA / SMN credit per the Términos de Libre Uso MX with the
// consultation date (the snapshot date), the NASA POWER reference string
// with the access dates (the power runs' started_at dates) and the
// funding acknowledgement, the CC BY 4.0 scope statement, and the
// transparency note on CONAGUA's terms — then the known gaps. Plain
// text, deterministic; the only dates are the snapshot's and the runs'.
func RenderNotice(w io.Writer, in DocsInput) error {
	accessDates, err := powerAccessDates(in.Meta.Runs.Power)
	if err != nil {
		return fmt.Errorf("render NOTICE: %w", err)
	}
	ceres := ceresColumns()
	t := textWriter{b: &bytes.Buffer{}}
	t.line("NOTICE — " + in.Meta.Dataset.Title)
	t.blank()
	t.line(fmt.Sprintf("Version %s. Snapshot %s.", in.Meta.Dataset.Version, in.Meta.SnapshotDate))
	t.blank()
	t.para("This file carries the upstream credits the compilation's sources require and states what " +
		"the compilation's own license does and does not cover. It accompanies LICENSE (the CC BY 4.0 " +
		"legal code) and CITATION.cff.")

	t.heading("1. CONAGUA / Servicio Meteorológico Nacional — Términos de Libre Uso MX")
	t.line("Dataset:            Normales Climatológicas por Estado — the per-station")
	t.line("                    climatological normals files and daily observation files")
	t.line("                    of the Servicio Meteorológico Nacional (SMN) archive")
	t.line("Source:             CONAGUA / Servicio Meteorológico Nacional")
	t.line("                    (Comisión Nacional del Agua)")
	t.line("Source URL:         " + conagua.BaseURL)
	t.line("                    (the file tree), reached from")
	t.line("                    " + smnNormalsPageURL)
	t.line("Consultation date:  " + in.Meta.SnapshotDate)
	t.blank()
	t.para("The Términos de Libre Uso MX (" + libreUsoMXURL + ") require the attribution elements above — " +
		"in the license's words, \"" + libreUsoAttributionClause + "\" — and the following, quoted from the " +
		"license text:")
	t.quote("\"" + libreUsoNoMisleadClause + "\"")
	t.para("In English: the data must not be used to mislead or confuse the public by altering the " +
		"original meaning or veracity of the information, and its use must not be presented as an " +
		"official position of the Government of Mexico or as endorsed by the source. This compilation " +
		"does neither: CONAGUA's values are mirrored as published — gaps stay empty and the source's " +
		"quirks are reported in QA-REPORT.md rather than corrected — and nothing in it represents a " +
		"position of, or an endorsement by, CONAGUA, the SMN, or the Government of Mexico.")

	t.heading("2. NASA POWER")
	if len(in.Meta.Runs.Power) == 0 {
		t.para("This build references no NASA POWER run: the database holds no POWER reanalysis rows.")
	} else {
		t.para(fmt.Sprintf("The data was obtained from the POWER Project's %s %s version on %s.",
			joinAnd(powerResolutions(in.Meta.Runs.Power)), powerVersionUnknown, joinAnd(accessDates)))
	}
	t.para(powerFundingSentence)
	t.para(fmt.Sprintf("Reference format per %s. Of the %d POWER variables the compilation carries, %d — the "+
		"solar, radiation, and cloud streams %s — are CERES-based; the remaining %d (meteorology, moisture, "+
		"soil) are MERRA-2-based. NASA POWER data carry no redistribution restriction. No NASA insignia is "+
		"used, and nothing here implies NASA's endorsement of this compilation.",
		powerReferencingURL, len(power.Registry), len(ceres), strings.Join(ceres, ", "),
		len(power.Registry)-len(ceres)))

	t.heading("3. License scope — CC BY 4.0 on the compilation only")
	t.para("The compilation — the selection, curation, arrangement, and formatting of the data, the derived " +
		"fields (the WMO completeness scores, the annual slots, the daily summaries), the joins, and the " +
		"documentation — is licensed under Creative Commons Attribution 4.0 International (CC BY 4.0; SPDX " +
		in.Meta.Dataset.License + "). The legal code is in LICENSE.")
	// The scope statement is about the work: a claim over upstream data
	// is the compilation's, so no creator name is interpolated here.
	t.para("That license covers the compilation only. Upstream data keep their own terms: CONAGUA's raw " +
		"values remain under the Términos de Libre Uso MX, and NASA POWER's values are United States " +
		"Government works in the public domain. This compilation claims no rights over NASA's " +
		"public-domain values or CONAGUA's raw values, and does not relicense either more restrictively than " +
		"its source does.")

	t.heading("4. Transparency note — CONAGUA's terms")
	t.para("CONAGUA's SMN download page footer-links the generic gob.mx terms (" + gobMXTermsURL + "), which " +
		"read as personal, non-commercial, and no-redistribution, while CONAGUA's holdings are published on " +
		"datos.gob.mx under the permissive Términos de Libre Uso MX. This compilation takes the Libre Uso MX " +
		"position: Mexico's open-data decree (DOF 2015-02-20) and the portal's attachment of Libre Uso MX to " +
		"each dataset are the specific rule (lex specialis) that governs the data, over the generic site " + //nolint:misspell // Latin: lex specialis, the legal term the notice quotes
		"footer — and says so rather than hiding it.")
	t.para("The honest caveat: no single official sentence adjudicates the footer against the license, and " +
		"certainty is strongest for data obtained through datos.gob.mx, whereas this compilation pulled the " +
		"files directly from CONAGUA's SMN site. That is mitigated by attributing to the CONAGUA / SMN " +
		"open-data source (element 1) and by leaning on the open-data policy.")

	t.heading("Known gaps")
	t.bullet("The NASA POWER API version string is not recorded: the ETL's run ledger (power_runs) has no " +
		"version column, the client never parsed it from the responses, and no raw response was kept. " +
		"Element 2 therefore names the access dates and identifies the version as the one served on those " +
		"dates rather than stating a number it cannot verify.")
	t.bullet("The canonical Términos de Libre Uso MX URL, " + libreUsoMXURL + ", was returning 404 (redirecting " +
		"to www.datos.gob.mx/libreusomx) when this documentation was prepared. It is cited as the license's " +
		"canonical name regardless; the clauses above are quoted from the license text. The build makes no " +
		"network calls and does not re-check it.")

	if _, err := w.Write(t.finish()); err != nil {
		return fmt.Errorf("write NOTICE: %w", err)
	}
	return nil
}

// yamlQuote renders s as a YAML double-quoted scalar on one line.
// Backslash and the double quote are escaped; the text carries no
// newline, so no other escape is needed.
func yamlQuote(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"`
}

// RenderCitation writes CITATION.cff: CFF 1.2.0, type dataset, the title verbatim,
// the version and license from the dataset identity, date-released
// stamped with the build's UTC date, the creator as the sole author
// with their ORCID URL, the abstract, the keywords, and the repository.
// The doi key is present exactly when the build names a DOI; while it
// is empty a comment stands where the key would, because a placeholder
// there would both fail CFF validation and outlive the build.
func RenderCitation(w io.Writer, in DocsInput) error {
	t := textWriter{b: &bytes.Buffer{}}
	d := in.Meta.Dataset
	t.line("cff-version: 1.2.0")
	t.line("message: " + yamlQuote("If you use this dataset, please cite it as below."))
	t.line("type: dataset")
	t.line("title: " + yamlQuote(d.Title))
	t.line("version: " + yamlQuote(d.Version))
	t.line("license: " + d.License)
	t.line("date-released: " + in.buildDate())
	t.line("authors:")
	t.line("  - family-names: " + yamlQuote(DatasetCreatorFamilyNames))
	t.line("    given-names: " + yamlQuote(DatasetCreatorGivenNames))
	t.line("    orcid: https://orcid.org/" + d.CreatorORCID)
	if d.DOI != "" {
		t.line("doi: " + d.DOI)
	} else {
		t.line("# doi: this build carries no DOI; this version's DOI is reserved on Zenodo before the " +
			"deposited build and rendered here from that one reserved value.")
	}
	t.line("abstract: " + yamlQuote(abstract(in)))
	t.line("keywords:")
	for _, k := range datasetKeywords {
		t.line("  - " + yamlQuote(k))
	}
	t.line("repository-code: " + yamlQuote(RepositoryURL))
	if _, err := w.Write(t.finish()); err != nil {
		return fmt.Errorf("write CITATION.cff: %w", err)
	}
	return nil
}

// zenodoMetadata is zenodo-metadata.json: the deposit metadata a human
// hands to Zenodo's API, and beside it — never inside metadata, which
// Zenodo holds to its known keys — the build note. Field order is the
// file's key order.
type zenodoMetadata struct {
	Metadata  zenodoRecord `json:"metadata"`
	BuildNote string       `json:"build_note"`
}

// zenodoRecord is Zenodo's deposition metadata object: the upload type,
// title, creators, description, access right, and license, then the
// keywords, the version, and the publication date the deposit form also
// takes; license is the SPDX id in Zenodo's lowercase
// form.
type zenodoRecord struct {
	UploadType      string          `json:"upload_type"`
	Title           string          `json:"title"`
	Creators        []zenodoCreator `json:"creators"`
	Description     string          `json:"description"`
	AccessRight     string          `json:"access_right"`
	License         string          `json:"license"`
	Keywords        []string        `json:"keywords"`
	Version         string          `json:"version"`
	PublicationDate string          `json:"publication_date"`
}

type zenodoCreator struct {
	Name  string `json:"name"`
	ORCID string `json:"orcid,omitempty"`
}

// zenodoBuildNote marks the stub as build-only so a reader of the file
// cannot mistake it for a deposited record's metadata.
const zenodoBuildNote = "build-only stub; no deposit performed"

// RenderZenodoMetadata writes zenodo-metadata.json: two-space indented,
// no HTML escaping, a trailing newline; publication_date is the build's
// UTC date, the one stamped field.
func RenderZenodoMetadata(w io.Writer, in DocsInput) error {
	d := in.Meta.Dataset
	m := zenodoMetadata{
		Metadata: zenodoRecord{
			UploadType: "dataset",
			Title:      d.Title,
			Creators: []zenodoCreator{{
				Name:  DatasetCreatorFamilyNames + ", " + DatasetCreatorGivenNames,
				ORCID: d.CreatorORCID,
			}},
			Description:     abstract(in),
			AccessRight:     "open",
			License:         strings.ToLower(d.License),
			Keywords:        slices.Clone(datasetKeywords),
			Version:         d.Version,
			PublicationDate: in.buildDate(),
		},
		BuildNote: zenodoBuildNote,
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(m); err != nil {
		return fmt.Errorf("write zenodo-metadata.json: %w", err)
	}
	return nil
}

// para writes one wrapped Markdown paragraph followed by a blank line.
func (m mdWriter) para(text string) {
	for _, l := range wrap(text, docsLineWidth) {
		m.line("%s", l)
	}
	m.blank()
}

// item writes one wrapped list item with a hanging two-space indent —
// the README's bullets wrap like its paragraphs, where bullet (qa.go)
// keeps a report's items on one line.
func (m mdWriter) item(text string) {
	for i, l := range wrap(text, docsLineWidth-2) {
		if i == 0 {
			m.line("- %s", l)
		} else {
			m.line("  %s", l)
		}
	}
}

// groupDescriptions is what each artifact group's archive holds, as the
// README's artifact table states it.
var groupDescriptions = map[Group]string{
	GroupTabular: "the state's stations as curated CSV in four folders (conagua/, nasa_power/, combined/, " +
		"provenance/), one Parquet twin per data table, and the filtered SQLite database " + stateDBName(placeholderShard),
	GroupJSON: "per station, profile.json and daily.json under " + profilePath(placeholderShard, placeholderStation)[:strings.LastIndex(profilePath(placeholderShard, placeholderStation), "/")+1] +
		", plus provenance/ as JSON",
	GroupNationalCSV:     "every state's per-station and per-cell CSV files and the whole-scope tables at national scope",
	GroupNationalParquet: fmt.Sprintf("the %d per-table Parquet files at national scope", parquetTableCount()),
	GroupNationalJSON:    "every station's profile.json and daily.json, all states",
	GroupNationalSQLite:  nationalDBName + ", the full canonical database (every table, surrogate ids and indexes included)",
	GroupRaw:             "the complete CONAGUA snapshot the database was ingested from, every fetched file verbatim",
}

// docsFileDescriptions is what each docs-group file holds, keyed by the
// names DocsFiles lists; a spec holds the two in lockstep.
var docsFileDescriptions = map[string]string{
	readmeName:             "this file",
	DictionaryMarkdownName: "every exported column: table, type, unit, decimals, and the DDL's own description (generated from the schema)",
	DictionaryJSONName:     "the data dictionary, machine-readable",
	licenseName:            "the CC BY 4.0 legal code, verbatim",
	noticeName:             "the upstream credits (CONAGUA / SMN, NASA POWER), the license scope, and the terms transparency note",
	citationName:           "the citation file (Citation File Format 1.2.0)",
	manifestName:           "the global provenance index: schema version, git SHA, snapshot date, runs, counts, and the sha256 of every other top-level file",
	checksumsName:          "sha256sum-compatible digests of every other top-level file",
	qaReportName:           "the QA report: coverage, counts, null summary, gate results, run counters",
	zenodoMetadataName:     "the build-only Zenodo deposit metadata stub",
}

// readmeSpecs is every flat-file spec whose columns the README's
// decimals table summarizes: the dictionary's files, so the two
// documents describe one set.
func readmeSpecs() []FileSpec {
	defs := dictionaryFileDefs()
	out := make([]FileSpec, len(defs))
	for i, d := range defs {
		out[i] = d.spec
	}
	return out
}

// decimalsRows groups every exported numeric column by its pinned
// decimal count, ascending, each column named once per count; a name
// pinned at two counts in two tables (lat / lon: stations at 6, grid
// cells at 3) is qualified by its table so neither count is hidden.
func decimalsRows() [][]string {
	specs := readmeSpecs()
	countsOf := map[string]map[int]bool{}
	for _, spec := range specs {
		for _, c := range spec.Columns {
			if c.Kind != KindInt && c.Kind != KindReal {
				continue
			}
			if countsOf[c.Name] == nil {
				countsOf[c.Name] = map[int]bool{}
			}
			countsOf[c.Name][c.Decimals] = true
		}
	}
	byDecimals := map[int][]string{}
	seen := map[string]bool{}
	for _, spec := range specs {
		for _, c := range spec.Columns {
			if c.Kind != KindInt && c.Kind != KindReal {
				continue
			}
			label := c.Name
			if len(countsOf[c.Name]) > 1 {
				label += " (" + spec.Table + ")"
			}
			if seen[label] {
				continue
			}
			seen[label] = true
			byDecimals[c.Decimals] = append(byDecimals[c.Decimals], label)
		}
	}
	counts := make([]int, 0, len(byDecimals))
	for d := range byDecimals {
		counts = append(counts, d)
	}
	slices.Sort(counts)
	rows := make([][]string, 0, len(counts))
	for _, d := range counts {
		rows = append(rows, []string{formatInt(int64(d)), strings.Join(byDecimals[d], ", ")})
	}
	return rows
}

// profileKeys lists the JSON profile's top-level keys in file order,
// read from the Profile type as the dictionary reads it, so the README
// cannot drift from the writer.
func profileKeys() []string {
	t := reflect.TypeOf(Profile{})
	keys := make([]string, 0, t.NumField())
	for i := range t.NumField() {
		if key, ok := jsonKeyOf(t.Field(i)); ok {
			keys = append(keys, key)
		}
	}
	return keys
}

// The unit glosses the README adds to the unit map's entries whose
// meaning the unit alone does not carry; a spec holds every key to a
// pattern of the map.
var unitGlosses = map[string]string{
	"_mm":   "a total over the row's period",
	"_mmpd": "a daily rate",
	"_wm2":  "a 24-hour average",
	"lon":   "west negative",
}

// readmeUnits renders the dictionary's unit map — the suffixes, then
// the whole names — as README prose, each entry glossed where
// unitGlosses has one.
func readmeUnits() string {
	units := slices.Concat(unitSuffixes, unitNames)
	parts := make([]string, len(units))
	for i, u := range units {
		parts[i] = mdCode(u.Pattern) + " " + u.Unit
		if gloss, ok := unitGlosses[u.Pattern]; ok {
			parts[i] += " (" + gloss + ")"
		}
	}
	return strings.Join(parts, ", ")
}

// annualTieStep is one unit in the last decimal of a normals
// temperature — the most a derived annual mean may differ from
// CONAGUA's published value on an exact decimal tie — rendered at
// the normals' own decimals.
var annualTieStep = func() string {
	decimals := decimalsOf(MonthlyNormals, ExportName(MonthlyNormals.Table, "tmax"))
	return formatReal(math.Pow10(-decimals), decimals)
}()

// RenderREADME writes README.md: what the dataset is, the artifact set,
// how to read the archives, the conventions, the state code lookup, the
// provenance, the reproducibility guarantee, the license and citation,
// and the known limitations. Deterministic Markdown — no wall clock; the
// header names the snapshot, the schema version, and the git SHA — every
// number from the inputs, every file name from the artifact set's own
// functions.
func RenderREADME(w io.Writer, in DocsInput) error {
	constants, err := buildConstants(buildDropped(schema.Tables()))
	if err != nil {
		return fmt.Errorf("render README: %w", err)
	}
	var b bytes.Buffer
	m := mdWriter{b: &b}
	m.readmeHeader(in)
	m.readmeDataset(in)
	m.readmeArtifacts(in)
	m.readmeReading()
	m.readmeConventions(constants)
	m.readmeStates()
	m.readmeProvenance(in)
	m.readmeReproducibility()
	m.readmeLicense(in)
	m.readmeLimitations(in)
	out := append(bytes.TrimRight(b.Bytes(), "\n"), '\n')
	if _, err := w.Write(out); err != nil {
		return fmt.Errorf("write README: %w", err)
	}
	return nil
}

func (m mdWriter) readmeHeader(in DocsInput) {
	m.line("# %s", in.Meta.Dataset.Title)
	m.blank()
	m.line("Version %s. Snapshot %s, schema version %d, ETL git SHA %s.",
		in.Meta.Dataset.Version, mdCode(in.Meta.SnapshotDate), in.Meta.SchemaVersion, mdCode(in.Meta.ETLGitSHA))
	m.blank()
	m.para("This file describes the dataset, the files of this deposit, how to read them, and the " +
		"conventions every file follows. Every number in it is computed from the shipped database at " +
		"build time; the file carries no timestamp, so two builds of the same database and binary render " +
		"the same text. DATA-DICTIONARY.md describes every exported column; NOTICE carries the upstream " +
		"credits and the license scope; QA-REPORT.md carries the quality numbers.")
}

func (m mdWriter) readmeDataset(in DocsInput) {
	c := in.Coverage
	ceres := ceresColumns()
	withPower, _ := periodsSplit(c.Power.Monthly)
	m.line("## 1. What this dataset is")
	m.blank()
	m.item(fmt.Sprintf("**Stations.** %s CONAGUA conventional weather stations (`%s`) — the Servicio "+
		"Meteorológico Nacional's Normales Climatológicas archive, consulted on %s.",
		formatInt(in.Stations), Stations.Name, in.Meta.SnapshotDate))
	m.item(fmt.Sprintf("**Published normals.** CONAGUA's official monthly climatological normals for the %d "+
		"reference periods %s: %s `%s` rows and %s `%s` rows (the extremes with their years and dates, the "+
		"years-with-data counts, rain days).",
		len(normalsPeriods), joinAnd(normalsPeriods), formatInt(in.Counts.MonthlyNormals), MonthlyNormals.Name,
		formatInt(in.Counts.MonthlyNormalsExtras), MonthlyNormalsExtras.Name))
	m.item(fmt.Sprintf("**Daily observations.** Maximum and minimum temperature, precipitation, and "+
		"evaporation as observed (`%s`): %s.", DailyObservations.Name, extentPhrase(c.Daily, "stations")))
	m.item(fmt.Sprintf("**NASA POWER.** The %d reanalysis variables of each station's POWER grid cell "+
		"(`nasa_power/`; the %s grid; meteorology from MERRA-2, the %d solar, radiation, and cloud streams "+
		"from CERES): %s cells, %s stations linked to one; daily reanalysis %s; monthly climatology for %s.",
		len(power.Registry), gridPhrase(), len(ceres), formatInt(c.Power.Cells),
		formatInt(c.Power.LinkedStations), extentPhrase(c.Power.Daily, "cells"), periodsPhrase(withPower)))
	m.item("**Combined.** The `combined/` folder and the JSON profiles join the two, on the definition below.")
	m.blank()
	m.line("> **\"Combined\" / \"Augmented\" = a CONAGUA-spine record augmented with its station's POWER-cell " +
		"reanalysis on matching keys — a LEFT join from the CONAGUA side. Never a temporal union.**")
	m.blank()
	m.para("Every CONAGUA row is kept; POWER columns are empty where POWER has no matching row — an " +
		"observed date before the POWER daily series begins, a reference period POWER monthly was not " +
		"pulled for — and a station with no grid-cell link keeps its rows with `cell_id` and " +
		"`distance_km` empty as well. A reader wanting the complete reanalysis for a location uses " +
		"`nasa_power/daily` joined through `nasa_power/station_cell_map`.")
}

func (m mdWriter) readmeArtifacts(in DocsInput) {
	m.line("## 2. The artifact set")
	m.blank()
	m.para("One Zenodo record, addressable by geography and format: per state, two archives; for the " +
		"whole country, four; the raw snapshot; and the metadata files. Per-state file names carry the " +
		"lowercase CONAGUA state code (section 5).")
	m.line("### Per-state archives")
	m.blank()
	perState := perStateGroups()
	header := []string{"state", "code"}
	for _, g := range perState {
		header = append(header, string(g))
	}
	rows := make([][]string, 0, len(in.States))
	for _, st := range in.States {
		row := []string{st.Name, st.Code}
		for _, g := range perState {
			row = append(row, mdCode(archiveName(st, g)))
		}
		rows = append(rows, row)
	}
	m.table(header, rows)
	for _, g := range perState {
		m.item(fmt.Sprintf("`%s` — %s.", archiveName(placeholderShard, g), groupDescriptions[g]))
	}
	m.blank()
	m.line("### National archives and the raw snapshot")
	m.blank()
	rows = rows[:0]
	for _, g := range Groups() {
		if g.PerState() || g == GroupDocs {
			continue
		}
		rows = append(rows, []string{mdCode(nationalArchiveName(g, in.Meta.SnapshotDate)), groupDescriptions[g]})
	}
	m.table([]string{"file", "contents"}, rows)
	m.line("### Metadata files")
	m.blank()
	rows = rows[:0]
	for _, name := range DocsFiles() {
		rows = append(rows, []string{mdCode(name), docsFileDescriptions[name]})
	}
	m.table([]string{"file", "contents"}, rows)
}

func (m mdWriter) readmeReading() {
	m.line("## 3. Reading the archives")
	m.blank()
	m.para("Every tabular archive carries four folders — scope is the folder, table is the file: " +
		"`conagua/` (observed and published values, plus the derived WMO completeness scores in the " +
		"stations file), `nasa_power/` (reanalysis, keyed by grid cell), `combined/` (the LEFT join), and " +
		"`provenance/` (the runs, identical in every archive). The long per-station and per-cell series " +
		"are one file per station or cell; every other table is one file.")
	m.table([]string{"file", "grain"}, [][]string{
		{mdCode(Stations.Name + ".csv"), "one row per station"},
		{mdCode(MonthlyNormals.Name + ".csv"), "one row per (station, period, month)"},
		{mdCode(MonthlyNormalsExtras.Name + ".csv"), "one row per (station, period, month)"},
		{mdCode(dailyPath(placeholderShard, placeholderStation)), "one file per station, one row per observed date"},
		{mdCode(Cells.Name + ".csv"), "one row per referenced grid cell"},
		{mdCode(StationCellMap.Name + ".csv"), "one row per station that has a cell"},
		{mdCode(PowerMonthly.Name + ".csv"), "one row per (cell, period, month)"},
		{mdCode(cellDailyPath(placeholderCell)), "one file per cell, one row per date"},
		{mdCode(CombinedMonthly.Name + ".csv"), "the normals rows joined to the cell's monthly row"},
		{mdCode(combinedDailyPath(placeholderShard, placeholderStation)), "one file per station, the observed rows joined to the cell's daily row"},
		{mdCode(ProvenanceIngestRuns.Name + ".csv"), "one row per snapshot date"},
		{mdCode(ProvenancePowerRuns.Name + ".csv"), "one row per referenced power run"},
	})
	m.para("**Parquet twins.** Beside each data table's CSV — or beside its shard folder, for the " +
		"per-station and per-cell tables — sits one Parquet file of the same name (`" +
		parquetPath(DailyObservations) + "` holds every station's rows of the archive). A twin carries the " +
		"same rows in the same order and the same values as its CSV; nulls are native; `provenance/` " +
		"stays CSV-only.")
	m.para(fmt.Sprintf("**%s.** At the root of each tabular archive, a SQLite database with the full schema "+
		"— every table, index, and surrogate id — holding the state's stations, every row keyed to them "+
		"(parsing warnings included), the grid cells those stations reference with those cells' POWER "+
		"rows, and every ingest and power run. It opens read-only from any medium. Its surrogate ids are "+
		"the national %s ids, so joins carry across archives; the sequence counters continue "+
		"from the state's highest id, so ids minted in a writable copy are not globally unique — treat "+
		"the file as a read-only extract.", mdCode(stateDBName(placeholderShard)), mdCode(nationalDBName)))
	m.para(fmt.Sprintf("**JSON archives.** Per station, `%s` and `%s`. The profile's top-level keys, in "+
		"order: %s. Month series are %d-element positional arrays — slots 0–%d are months 1–%d, slot %d "+
		"the annual value (section 4) — and every block carries a `source` tag (`%s`, `%s`, `%s`, `%s`). "+
		"`%s` is an array of day objects, date ascending: `date`, `observed` (%s; always present, with "+
		"per-field nulls), and `reanalysis` (the %d POWER variables, or `null` as a whole when the cell "+
		"has no row for that date).",
		profilePath(placeholderShard, placeholderStation), dailyJSONPath(placeholderShard, placeholderStation),
		mdCodes(profileKeys()), monthSlots, monthSlots-2, monthSlots-1, monthSlots-1,
		sourceConaguaPublished, sourceConaguaObserved, sourceNasaPower, sourceBioclimaDerived,
		dailyJSONName, mdCodes(columnNames(dailyJSON.observed)), len(dailyJSON.reanalysis)))
	m.para(fmt.Sprintf("**Provenance.** The flat files carry no run ids. A POWER value traces to its run "+
		"through its own keys: the value's (temporal mode, period) names a run label "+
		"(`power-<temporal_mode>-<start_year>-<end_year>`), `%s` and `%s` carry that run's request — "+
		"endpoint, parameters in request order, community, period, grid resolution, per-variable unit "+
		"conversions — and a reproducer can rebuild the exact POWER URL from it. `%s` lists the "+
		"ingest and power runs by the same labels, with the per-table counts and the sha256 of every "+
		"other top-level file.", ProvenancePowerRuns.Name+".csv", ProvenancePowerRuns.Name+".json", manifestName))
}

func (m mdWriter) readmeConventions(constants []DictionaryConstant) {
	m.line("## 4. Conventions")
	m.blank()
	m.para("**Units in the column name.** Every value column pins its unit in its suffix, or in its whole " +
		"name where it carries none: " + readmeUnits() + ". Columns not listed — months, years, counts — carry " +
		"no unit. In `combined_monthly`, `precip_mm` is CONAGUA's monthly total and `precip_mmpd` POWER's " +
		"mean daily rate — different quantities, as the suffixes say.")
	m.para("**Fixed decimals.** Every numeric column is written with a fixed number of decimals in CSV, " +
		"JSON, and Parquet alike — the source's own precision for observed values, a rounding that removes " +
		"computed digits the source never had for derived ones. Only the SQLite databases carry the " +
		"native full-precision value.")
	m.table([]string{"decimals", "columns"}, decimalsRows())
	m.para("**Nulls, dates, order.** A gap in the source is an empty CSV field, a JSON `null` (never an " +
		"omitted key), a native Parquet null — never a sentinel number. Dates are `YYYY-MM-DD`, periods " +
		"`YYYY-YYYY`, months integers 1–12; text is as stored (run timestamps RFC 3339 UTC, `state` the " +
		"uppercase CONAGUA code). Rows are sorted by primary key ascending, string keys bytewise; " +
		"`station_id` is an opaque string, never a number.")
	m.para("**The annual slot.** Slot 12 of a JSON month series is recomputed at export from the twelve " +
		"published months — a sum for `precip_mm` and `evap_mm` (monthly totals), a circular mean for the " +
		"two wind directions, an unweighted mean for everything else — and tagged `" + sourceBioclimaDerived +
		"`; it is `null` unless all twelve months are present, and always `null` in the `extras` block. " +
		"The recompute reproduces CONAGUA's own published annual for every sum and every mean that does " +
		"not sit on an exact decimal tie; on a tie the published value follows no decimal rule, and the " +
		"derived annual may differ from it by " + annualTieStep + ". The flat `" + MonthlyNormals.Name +
		"` file stays months 1–12 — a pure source mirror.")
	items := make([]string, len(constants))
	for i, c := range constants {
		items[i] = fmt.Sprintf("%s = %s (%s)", mdCode(c.Table+"."+c.Column), mdCode(c.Value), c.Reason)
	}
	m.para(fmt.Sprintf("**Documented constants.** %d single-valued column%s %s left out of the flat files "+
		"and stated here once: %s. The SQLite databases carry every one.",
		len(constants), plural(len(constants)), isAre(len(constants)), strings.Join(items, "; ")))
	wmo, spans := stationsDerivedColumns()
	m.para(fmt.Sprintf("**Derived columns in `conagua/`.** %d columns of `%s` are computed by the ETL "+
		"rather than mirrored from CONAGUA's catalog: the %d `%s*` scores, from the extras' "+
		"years-with-data counts (WMO-No. 1203 §4.4.2), and %s, from the station's own daily and normals "+
		"rows. They are the folder's only `%s` columns — every other column of every `conagua/` file "+
		"mirrors its source — and %s tags them the same way.",
		len(wmo)+len(spans), Stations.Name, len(wmo), wmoPrefix, joinAnd(mdCodeList(spans)),
		sourceBioclimaDerived, mdCode(DictionaryMarkdownName)))
}

// mdCodeList wraps each name in code ticks, keeping them a list so
// joinAnd can render them as prose rather than a bare comma run.
func mdCodeList(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = mdCode(n)
	}
	return out
}

// stationsDerivedColumns splits the stations file's ETL-computed
// columns into the WMO completeness scores and the rest, both in export
// order. The predicate is stationsTag — the very function the data
// dictionary tags these columns with — so the README's count and the
// dictionary's cannot disagree, as they did when the README named the
// eight scores alone and the dictionary named ten columns.
func stationsDerivedColumns() (wmo, other []string) {
	for _, c := range Stations.Columns {
		if stationsTag(c) != sourceBioclimaDerived {
			continue
		}
		if strings.HasPrefix(c.DB, wmoPrefix) {
			wmo = append(wmo, c.Name)
		} else {
			other = append(other, c.Name)
		}
	}
	return wmo, other
}

// isAre is the verb of a count.
func isAre(n int) string {
	if n == 1 {
		return "is"
	}
	return "are"
}

func (m mdWriter) readmeStates() {
	m.line("## 5. State codes")
	m.blank()
	m.para("`stations.state` holds CONAGUA's own state code, uppercase as stored; file names and " +
		"in-archive shard folders use the same code in lowercase. The 32 codes, in code order — the " +
		"order of the per-state archives above and of `states` in `" + manifestName + "`:")
	codes := slices.Clone(conagua.AllStates)
	slices.Sort(codes)
	rows := make([][]string, 0, len(codes))
	for _, c := range codes {
		rows = append(rows, []string{strings.ToUpper(string(c)), mdCode(string(c)), c.DisplayName()})
	}
	m.table([]string{"code", "file-name slug", "state"}, rows)
}

func (m mdWriter) readmeProvenance(in DocsInput) {
	m.line("## 6. Provenance")
	m.blank()
	m.para(fmt.Sprintf("Built from the snapshot of %s by ETL git SHA %s, schema version %d. The runs the "+
		"shipped rows trace to, by natural label:", mdCode(in.Meta.SnapshotDate), mdCode(in.Meta.ETLGitSHA),
		in.Meta.SchemaVersion))
	rows := make([][]string, 0, len(in.Meta.Runs.Ingest))
	for _, r := range in.Meta.Runs.Ingest {
		rows = append(rows, []string{r.SnapshotDate, r.StartedAt, mdString(r.FinishedAt), r.SinkKind,
			r.Status, mdString(r.ETLGitSHA)})
	}
	m.table([]string{"ingest run (snapshot_date)", "started_at", "finished_at", "sink_kind", "status", "etl_git_sha"}, rows)
	rows = rows[:0]
	for _, r := range in.Meta.Runs.Power {
		rows = append(rows, []string{r.RunLabel, r.TemporalMode, powerSpan(r), r.StartedAt, r.Status, mdString(r.ETLGitSHA)})
	}
	m.table([]string{"power run (run_label)", "temporal_mode", "span", "started_at", "status", "etl_git_sha"}, rows)
}

// The metadata files by what a rebuild reproduces, the classes README
// §7 states. stampedDocs are the files that carry a build date and the
// key it sits in; digestDocs carry the sha256 of the other top-level
// files, so they move whenever a content-reproducible archive is
// rebuilt even though every value they state stays the same. Every
// other docs file is byte-reproducible in full — that class is derived
// from the artifact set rather than listed, so a new metadata file
// cannot land silently unclassified.
var (
	stampedDocs = map[string]string{
		citationName:       "date-released",
		zenodoMetadataName: "publication_date",
	}
	digestDocs = []string{manifestName, checksumsName}
)

// fullyReproducibleDocs is the docs group less the stamped and the
// digest-carrying files: the class whose every byte a rebuild repeats.
func fullyReproducibleDocs() []string {
	out := make([]string, 0, len(docsFiles))
	for _, name := range docsFiles {
		if _, stamped := stampedDocs[name]; stamped || slices.Contains(digestDocs, name) {
			continue
		}
		out = append(out, name)
	}
	return out
}

// stampedDocsPhrase renders the stamped files as prose, each with the
// key that moves in it, in artifact-set order.
func stampedDocsPhrase() string {
	var parts []string
	for _, name := range docsFiles {
		if key, ok := stampedDocs[name]; ok {
			parts = append(parts, mdCode(key)+" in "+mdCode(name))
		}
	}
	return joinAnd(parts)
}

func (m mdWriter) readmeReproducibility() {
	m.line("## 7. Reproducibility")
	m.blank()
	m.para("CSV and JSON files are byte-reproducible: the same database and binary yield identical bytes " +
		"on every build (fixed decimals plus the primary-key sort). Parquet files and the SQLite databases " +
		"are content-reproducible: the same values on every build, verifiable by re-export, with the " +
		"bytes not guaranteed (the Parquet writer's metadata and page layout; SQLite's file layout and " +
		"run timestamps). The archives inherit the class of what they hold.")
	m.para("The metadata files fall into three classes of their own.")
	m.item("Byte-reproducible in full — " + mdCodes(fullyReproducibleDocs()) + ": no clock reaches them, " +
		"and every number in them is computed from the shipped database.")
	m.item("Byte-reproducible but for one stamped date each — " + stampedDocsPhrase() +
		", the build's UTC date. The rest of both files is reproducible.")
	m.item("Byte-reproducible in what they state, not in what they carry — " + mdCodes(digestDocs) +
		". `" + manifestName + "` also stamps `generated_at`. Both list the sha256 of every other " +
		"top-level file, and the digests of the Parquet-bearing and SQLite-bearing archives change on " +
		"every rebuild because those archives are content- but not byte-reproducible (above). The " +
		"values these two files state — the counts, the runs, the file names and sizes — do not change; " +
		"the digest lines of those archives do.")
	m.blank()
	m.para("So a rebuild reproduces the data, not the digest list: verify a download against the `" +
		manifestName + "` and `" + checksumsName + "` it was published with, never against ones built " +
		"from a rebuilt deposit.")
	m.para("To verify a download, run `sha256sum -c " + checksumsName + "` in the directory holding the " +
		"files; `" + manifestName + "` lists the same digests with each file's size.")
}

func (m mdWriter) readmeLicense(in DocsInput) {
	d := in.Meta.Dataset
	m.line("## 8. License and citation")
	m.blank()
	m.para("The compilation is licensed under Creative Commons Attribution 4.0 International (" + d.License +
		"); the legal code is in LICENSE. Upstream data keep their own terms — CONAGUA's under the " +
		"Términos de Libre Uso MX, NASA POWER's in the public domain — as NOTICE states, with the " +
		"credits both sources require.")
	if d.DOI != "" {
		m.para("Suggested citation (`" + citationName + "` carries the same in Citation File Format, with " +
			"this version's DOI as its `doi` key; the Zenodo record also lists a concept DOI that " +
			"resolves to the latest version):")
	} else {
		m.para("Suggested citation (`" + citationName + "` carries the same in Citation File Format). This " +
			"build carries no DOI — the version's DOI is reserved on Zenodo and baked into the deposited " +
			"build, so a copy whose citation names none is not the deposited copy:")
	}
	m.line("> %s", d.SuggestedCitation)
	m.blank()
	m.para("Source code: " + RepositoryURL + ".")
}

func (m mdWriter) readmeLimitations(in DocsInput) {
	c := in.Coverage
	_, withoutPower := periodsSplit(c.Power.Monthly)
	m.line("## 9. Known limitations")
	m.blank()
	m.item("The derived annual slot may differ from CONAGUA's published annual by " + annualTieStep + " on an " +
		"exact decimal tie of the twelve monthly means (section 4); every sum and every non-tie mean reproduces.")
	m.item("The NASA POWER API version is not recorded: the run ledger has no version column and no raw " +
		"response was kept. NOTICE names the access dates; a reproducer should expect the version POWER " +
		"served on those dates.")
	if last := c.Power.Daily.ValueLastDate; last != nil && *last < in.Meta.SnapshotDate {
		m.item(fmt.Sprintf("POWER's daily series ends on %s, before the snapshot date %s: POWER publishes "+
			"daily values some days behind real time, so the newest observed dates have no reanalysis.",
			*last, in.Meta.SnapshotDate))
	}
	if c.Power.Daily.ValueFirstDate != nil {
		m.item(fmt.Sprintf("POWER's daily series begins on %s; observed dates before it carry empty POWER "+
			"columns in `combined_daily` and `\"reanalysis\": null` in `%s`.", *c.Power.Daily.ValueFirstDate, dailyJSONName))
	}
	if d := c.Daily; d.LastDate != nil && d.ValueLastDate != nil && *d.LastDate > *d.ValueLastDate {
		m.item(fmt.Sprintf("`%s` also holds rows dated up to %s that carry no measurement, mirrored as CONAGUA "+
			"publishes them; every date range this README states is that of the rows carrying a value.",
			DailyObservations.Name, *d.LastDate))
	}
	if pages := in.RawHTMLPages; len(pages) > 0 {
		m.item(rawHTMLPagesNote(pages, in.Meta.SnapshotDate))
	}
	if len(withoutPower) > 0 {
		m.item(fmt.Sprintf("POWER monthly climatology was not pulled for %s: `combined_monthly` carries "+
			"those periods with empty POWER columns, and the profile's `power_monthly` block has them `null`.",
			joinAnd(withoutPower)))
	}
	m.item(fmt.Sprintf("The `extras` month series have no annual value (slot %d is always `null`), and an annual "+
		"over an incomplete year of published months is `null` rather than a partial aggregate.", monthSlots-1))
	m.item("Normals are CONAGUA's published values as ingested, not derived from the daily series.")
	m.item(fmt.Sprintf("The surrogate ids in %s are the national ids of a read-only extract (section 3); "+
		"ids minted in a writable copy are not globally unique.", mdCode(stateDBName(placeholderShard))))
	m.blank()
	m.readmePowerColumnExtents(c)
}

// rawHTMLPagesNote is README §9's statement of the raw artifact's files
// that are not CONAGUA data: how many, which, why they are there, and —
// only when it is true of every one of them — that no value in the
// deposit was read from them.
func rawHTMLPagesNote(pages []string, snapshotDate string) string {
	listed := make([]string, len(pages))
	unread := true
	for i, p := range pages {
		listed[i] = mdCode(p)
		kind, _, _ := strings.Cut(p, "/")
		if ingestedKind(conagua.Kind(kind)) {
			unread = false
		}
	}
	note := fmt.Sprintf("%s of the station files in `%s` are not CONAGUA data but HTML error pages the SMN "+
		"server returned in place of the station file, with a success status, so the pull recorded them as "+
		"fetched; they are shipped as pulled: %s.", formatInt(int64(len(pages))), RawArchiveName(snapshotDate),
		strings.Join(listed, ", "))
	if unread {
		note += " None of them is in a folder ingest reads, so no value in the deposit comes from them."
	}
	return note
}

// readmePowerColumnExtents states every POWER column's own dated extent
// (§9). The whole-table extent §1 and the bullets above give is the
// rows'; POWER's variables come from two upstream products whose
// records begin years apart, so a column can be null through the whole
// early part of a row set that exists. A reader given only the table's
// bounds reads that honest absence as missing data — the reason this is
// a per-column table and not a summary by cohort. Every date comes from
// the coverage the QA loader computed; none is a literal. A hand-built
// coverage carries no per-column extents, and the section then simply
// ends at the bullets.
func (m mdWriter) readmePowerColumnExtents(c QACoverage) {
	cols := c.Power.DailyColumnExtents
	if len(cols) == 0 {
		return
	}
	table := c.Power.Daily
	var late, early, empty int
	rows := make([][]string, 0, len(cols))
	for _, col := range cols {
		switch {
		case col.FirstDate == nil || col.LastDate == nil:
			empty++
		default:
			if table.ValueFirstDate != nil && *col.FirstDate > *table.ValueFirstDate {
				late++
			}
			if table.ValueLastDate != nil && *col.LastDate < *table.ValueLastDate {
				early++
			}
		}
		rows = append(rows, []string{mdCode(col.Column), upstreamOf(col.Column),
			mdString(col.FirstDate), mdString(col.LastDate), formatInt(col.NonNull)})
	}
	var divergences []string
	if late > 0 {
		divergences = append(divergences, fmt.Sprintf("%d begin later", late))
	}
	if early > 0 {
		divergences = append(divergences, fmt.Sprintf("%d end earlier", early))
	}
	if empty > 0 {
		divergences = append(divergences, fmt.Sprintf("%d carry no value at all", empty))
	}
	lead := fmt.Sprintf("**Per-column POWER extents.** Every one of the %d POWER columns spans the daily "+
		"series' full extent above.", len(cols))
	if len(divergences) > 0 {
		lead = fmt.Sprintf("**Per-column POWER extents.** The %d POWER columns do not all span the daily "+
			"series' extent above: %s. The two upstream products have different records — %s's streams "+
			"begin years after %s's — and a variable can also stop earlier than the table's last date. "+
			"Each column's own first and last dated value, and how many rows carry it:",
			len(cols), joinAnd(divergences), power.UpstreamCERES, power.UpstreamMERRA2)
	}
	m.para(lead)
	m.table([]string{"column", "upstream", "first date", "last date", "values"}, rows)
	m.para(fmt.Sprintf("Outside a column's own range the reanalysis row still exists: the date is present "+
		"in `%s` and in `%s`, and `%s`'s `reanalysis` object for it is not `null` — only that column's "+
		"field is empty (CSV) or `null` (JSON, Parquet). Read the table above before treating a run of "+
		"empty values as missing data.",
		PowerDaily.Name, CombinedDaily.Name, dailyJSONName))
}

func columnNames(cols []Column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}
