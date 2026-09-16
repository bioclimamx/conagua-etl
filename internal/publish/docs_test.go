package publish_test

// Specs for the docs group's metadata files — LICENSE, NOTICE,
// CITATION.cff, zenodo-metadata.json, README.md — rendered from a
// DocsInput the real loaders fill over the shared seed (the seam Run
// crosses when it builds the group): every file byte-identical across
// two renders and across two build times except the two build-time
// stamps; LICENSE pinned by digest and length; NOTICE's four elements in
// order with the dates from the inputs; CITATION.cff parsed back field
// by field; the Zenodo stub's exact key set and order; the README naming
// every top-level file of the artifact set, the 32-row state table, and
// the numbers of the seed; the README's unit sentence and state table
// held to the dictionary's unit map and to code order; and the identity
// hard stop — exactly the settled creator (name + ORCID) where a creator
// is named, no other personal fields — over every file.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// loadDocsInput fills a DocsInput the way Run does — the provenance
// runs, the station count, the table counts, the QA coverage, and the
// state list, all through the exported loaders over the same DB — with
// the seeded snapshot and a fixed build time.
func loadDocsInput(db *sql.DB, now time.Time) publish.DocsInput {
	GinkgoHelper()
	ctx := context.Background()
	runs, counts, states := loadQAInputs(db)
	stations, err := publish.LoadStationCount(ctx, db)
	Expect(err).NotTo(HaveOccurred())
	q, err := publish.LoadQA(ctx, db, "2026-06-08", runs, counts, nil, states)
	Expect(err).NotTo(HaveOccurred())
	return publish.DocsInput{
		Meta: publish.ProfileMeta{
			SchemaVersion: schema.Version, ETLGitSHA: "abc123", SnapshotDate: "2026-06-08",
			Runs: runs, Dataset: publish.DatasetMetadata(seededSnapshot, ""),
		},
		Stations: stations,
		Counts:   counts,
		Coverage: q.Coverage,
		States:   states,
		Now:      now,
	}
}

func renderDoc(render func(io.Writer, publish.DocsInput) error, in publish.DocsInput) string {
	GinkgoHelper()
	var buf bytes.Buffer
	Expect(render(&buf, in)).To(Succeed())
	return buf.String()
}

// unwrapped folds a wrapped text to single spaces so a sentence can be
// asserted whole regardless of where the renderer broke its lines.
func unwrapped(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// wantCERESColumns is the CERES-based POWER set written out: the eight
// radiation fluxes, the clearness index, and cloud_amt_pct — the one a
// unit-based predicate would credit to MERRA-2, rendering the split as
// 9 / 22. Spelled out rather than derived so a lineage that
// silently moves in the registry fails here.
var wantCERESColumns = []string{
	"solar_ghi_wm2", "solar_dhi_wm2", "solar_dni_wm2", "solar_clrsky_wm2", "clearness_index",
	"par_wm2", "uva_wm2", "uvb_wm2", "lw_dwn_wm2", "cloud_amt_pct",
}

// ceresColumnsFromRegistry is the spec's own derivation of the
// CERES-based POWER columns: the rows whose recorded lineage is CERES,
// in registry order — the order the docs list them in.
func ceresColumnsFromRegistry() []string {
	var cols []string
	for _, p := range power.Registry {
		if p.Upstream == power.UpstreamCERES {
			cols = append(cols, p.Column)
		}
	}
	return cols
}

// cffFields parses a CITATION.cff back with a line-based reader —
// scalars as `key: value`, lists as `key:` followed by `  - item` lines,
// comments skipped — so no YAML library is needed to hold every field.
func cffFields(text string) (scalars map[string]string, lists map[string][]string, comments []string) {
	scalars, lists = map[string]string{}, map[string][]string{}
	var current string
	for _, line := range strings.Split(strings.TrimRight(text, "\n"), "\n") {
		switch {
		case strings.HasPrefix(line, "#"):
			comments = append(comments, line)
		case strings.HasPrefix(line, "  - "):
			lists[current] = append(lists[current], strings.TrimPrefix(line, "  - "))
		default:
			key, value, found := strings.Cut(line, ":")
			Expect(found).To(BeTrue(), line)
			value = strings.TrimPrefix(value, " ")
			if value == "" {
				current = key
				lists[key] = []string{}
			} else {
				scalars[key] = value
			}
		}
	}
	return scalars, lists, comments
}

// jsonKeyOrder returns the keys of the object at the decoder's next
// token position, in file order, descending into nothing.
func jsonKeyOrder(data []byte, path ...string) []string {
	GinkgoHelper()
	var obj map[string]json.RawMessage
	Expect(json.Unmarshal(data, &obj)).To(Succeed())
	for _, p := range path {
		var next map[string]json.RawMessage
		Expect(json.Unmarshal(obj[p], &next)).To(Succeed())
		obj = next
	}
	// Re-scan the raw object with the token decoder for the order.
	raw := data
	for _, p := range path {
		var outer map[string]json.RawMessage
		Expect(json.Unmarshal(raw, &outer)).To(Succeed())
		raw = outer[p]
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	Expect(err).NotTo(HaveOccurred())
	Expect(tok).To(Equal(json.Delim('{')))
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		Expect(err).NotTo(HaveOccurred())
		keys = append(keys, tok.(string))
		var skip json.RawMessage
		Expect(dec.Decode(&skip)).To(Succeed())
	}
	return keys
}

var _ = Describe("The docs group", func() {
	var (
		db    *sql.DB
		in    publish.DocsInput
		ceres = ceresColumnsFromRegistry()
	)

	BeforeEach(func() {
		db = openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		in = loadDocsInput(db, fixedNow)
	})

	Describe("the advertised extent", func() {
		It("stops at the last observation, not at a trailing row that carries none, and discloses that row instead", func() {
			// CONAGUA publishes a few all-NULL daily rows dated after the
			// snapshot. Mirrored as they are, they must not stretch the
			// range the abstract and README advertise past the last date
			// anything was measured on.
			before := *in.Coverage.Daily.ValueLastDate
			var station int64
			Expect(db.QueryRow(`SELECT id FROM stations WHERE external_id = '31001'`).Scan(&station)).To(Succeed())
			_, err := db.Exec(`INSERT INTO daily_observations (station_id, date) VALUES (?, '2027-01-04')`, station)
			Expect(err).NotTo(HaveOccurred())
			future := loadDocsInput(db, fixedNow)

			Expect(*future.Coverage.Daily.LastDate).To(Equal("2027-01-04"))
			Expect(*future.Coverage.Daily.ValueLastDate).To(Equal(before))

			readme := unwrapped(renderDoc(publish.RenderREADME, future))
			cff := renderDoc(publish.RenderCitation, future)
			Expect(readme).To(ContainSubstring("(`conagua/daily_observations`): " +
				extentOf(future.Coverage.Daily, "stations") + "."))
			Expect(extentOf(future.Coverage.Daily, "stations")).To(HaveSuffix(" to " + before))
			Expect(cff).NotTo(ContainSubstring("2027-01-04"))
			Expect(cff).To(ContainSubstring(" to " + before))
			Expect(readme).To(ContainSubstring("`conagua/daily_observations` also holds rows dated up to 2027-01-04 " +
				"that carry no measurement, mirrored as CONAGUA publishes them; every date range this README " +
				"states is that of the rows carrying a value."))

			// Without such a row the disclosure is absent.
			Expect(unwrapped(renderDoc(publish.RenderREADME, in))).NotTo(ContainSubstring("carry no measurement"))
		})
	})

	Describe("the CERES derivation the NOTICE and README state", func() {
		It("selects ten of the registry's columns — the cloud stream among them — leaving 21 to MERRA-2", func() {
			Expect(ceres).To(Equal(wantCERESColumns))
			Expect(ceres).To(HaveLen(10))
			Expect(ceres).To(ContainElement("cloud_amt_pct"))
			Expect(power.Registry).To(HaveLen(31))
			Expect(len(power.Registry) - len(ceres)).To(Equal(21))
			// The lineage is a recorded field, not the unit: the cloud
			// stream is CERES and is served in percent, which is what a
			// unit-based predicate would get wrong.
			for _, p := range power.Registry {
				if p.Column == "cloud_amt_pct" {
					Expect(p.Upstream).To(Equal(power.UpstreamCERES))
					Expect(p.PowerUnit).NotTo(Equal(power.UnitMJPerM2Day))
				}
			}
		})
	})

	Describe("LICENSE", func() {
		It("is the CC BY 4.0 legal code verbatim, pinned by sha256 and byte length", func() {
			var buf bytes.Buffer
			Expect(publish.WriteLicense(&buf)).To(Succeed())
			Expect(sha256Hex(buf.Bytes())).To(Equal("9ba9550ad48438d0836ddab3da480b3b69ffa0aac7b7878b5a0039e7ab429411"))
			Expect(publish.LicenseSHA256).To(Equal("9ba9550ad48438d0836ddab3da480b3b69ffa0aac7b7878b5a0039e7ab429411"))
			Expect(buf.Len()).To(Equal(18657))
			Expect(buf.String()).To(HavePrefix("Attribution 4.0 International\n"))
			Expect(buf.String()).To(HaveSuffix("Creative Commons may be contacted at creativecommons.org.\n\n"))
			Expect(buf.String()).NotTo(ContainSubstring("\r"))
		})
	})

	Describe("NOTICE", func() {
		var notice, flat string

		BeforeEach(func() {
			notice = renderDoc(publish.RenderNotice, in)
			flat = unwrapped(notice)
		})

		It("carries the four NOTICE sections in order, then the known gaps", func() {
			for _, marker := range []string{
				"NOTICE — " + publish.DatasetTitle,
				"1. CONAGUA / Servicio Meteorológico Nacional — Términos de Libre Uso MX",
				"2. NASA POWER",
				"3. License scope — CC BY 4.0 on the compilation only",
				"4. Transparency note — CONAGUA's terms",
				"Known gaps",
			} {
				Expect(notice).To(ContainSubstring(marker + "\n"))
			}
			positions := []int{
				strings.Index(notice, "\n1. CONAGUA"), strings.Index(notice, "\n2. NASA POWER"),
				strings.Index(notice, "\n3. License scope"), strings.Index(notice, "\n4. Transparency"),
				strings.Index(notice, "\nKnown gaps\n----------\n"),
			}
			Expect(slices.IsSorted(positions)).To(BeTrue(), "%v", positions)
			Expect(notice).To(HaveSuffix("\n"))
			Expect(notice).NotTo(HaveSuffix("\n\n"))
		})

		It("credits CONAGUA / SMN per Libre Uso MX: the dataset, the source URLs, the snapshot date as the consultation date, and the required clauses in Spanish with an English rendering", func() {
			Expect(notice).To(ContainSubstring("Dataset:            Normales Climatológicas por Estado"))
			Expect(notice).To(ContainSubstring("Source:             CONAGUA / Servicio Meteorológico Nacional"))
			Expect(notice).To(ContainSubstring("Source URL:         " + conagua.BaseURL + "\n"))
			Expect(notice).To(ContainSubstring(
				"https://smn.conagua.gob.mx/es/climatologia/informacion-climatologica/normales-climatologicas-por-estado\n"))
			Expect(notice).To(ContainSubstring("Consultation date:  2026-06-08\n"))
			Expect(flat).To(ContainSubstring("https://datos.gob.mx/libreusomx"))
			Expect(flat).To(ContainSubstring(`"Nombre del conjunto de datos, [Dependencia/Entidad siglas]; ` +
				`Liga de internet de los datos descargados, y la fecha de consulta en formato numérico [AAAA-MM-DD]"`))
			Expect(flat).To(ContainSubstring(`"No utilizar la información con objeto de engañar o confundir a la ` +
				`población variando el sentido original de la información y su veracidad. No aparentar que el uso ` +
				`que usted haga de los datos representa una postura oficial del Gobierno o que el mismo está ` +
				`avalado por la fuente de origen."`))
			Expect(flat).To(ContainSubstring("must not be used to mislead or confuse the public"))
			Expect(flat).To(ContainSubstring("must not be presented as an official position of the Government of Mexico " +
				"or as endorsed by the source"))
		})

		It("credits NASA POWER with the reference string over the runs' access dates and resolutions, the unrecorded version stated as such, the funding sentence verbatim, and the CERES / MERRA-2 split from the registry", func() {
			Expect(flat).To(ContainSubstring("The data was obtained from the POWER Project's Daily and Monthly " +
				"(API version as served on the access dates; the version string was not recorded by the ETL — " +
				"see Known gaps) version on 2026/07/01 and 2026/07/02."))
			Expect(flat).To(ContainSubstring("The data was obtained from National Aeronautics and Space Administration " +
				"(NASA) Langley Research Center's Prediction Of Worldwide Energy Resources (POWER) project funded " +
				"through the NASA Earth Science Division."))
			Expect(flat).To(ContainSubstring("https://power.larc.nasa.gov/docs/referencing/"))
			Expect(flat).To(ContainSubstring("Of the 31 POWER variables the compilation carries, 10 — the solar, " +
				"radiation, and cloud streams " + strings.Join(ceres, ", ") + " — are CERES-based; the remaining 21 " +
				"(meteorology, moisture, soil) are MERRA-2-based."))
			Expect(ceres).To(Equal(wantCERESColumns))
			Expect(flat).NotTo(ContainSubstring("the solar and radiation streams"),
				"the cloud stream is CERES too and is credited by name")
			Expect(flat).To(ContainSubstring("No NASA insignia is used"))
			Expect(flat).To(ContainSubstring("nothing here implies NASA's endorsement"))
		})

		It("scopes CC BY 4.0 to the compilation and states the transparency wrinkle with its caveat", func() {
			Expect(flat).To(ContainSubstring("(CC BY 4.0; SPDX CC-BY-4.0). The legal code is in LICENSE."))
			Expect(flat).To(ContainSubstring("That license covers the compilation only."))
			// The license scope is the work's, never the creator's: no
			// personal name stands in this sentence.
			Expect(flat).To(ContainSubstring("This compilation claims no rights over NASA's public-domain values or " +
				"CONAGUA's raw values, and does not relicense either more restrictively than its source does."))
			Expect(flat).NotTo(ContainSubstring(publish.DatasetCreator))
			Expect(flat).To(ContainSubstring("https://www.gob.mx/terminos"))
			Expect(flat).To(ContainSubstring("personal, non-commercial, and no-redistribution"))
			Expect(flat).To(ContainSubstring("open-data decree (DOF 2015-02-20)"))
			Expect(flat).To(ContainSubstring("(lex specialis)")) //nolint:misspell // Latin: lex specialis
			Expect(flat).To(ContainSubstring("no single official sentence adjudicates the footer against the license"))
			Expect(flat).To(ContainSubstring("certainty is strongest for data obtained through datos.gob.mx"))
			Expect(flat).To(ContainSubstring("pulled the files directly from CONAGUA's SMN site"))
			Expect(flat).To(ContainSubstring("attributing to the CONAGUA / SMN open-data source"))
		})

		It("lists the known gaps: the unrecorded POWER API version and the unreachable Libre Uso MX URL, without claiming a network check", func() {
			Expect(notice).To(ContainSubstring("\n- The NASA POWER API version string is not recorded: the ETL's run ledger\n  (power_runs)"))
			Expect(flat).To(ContainSubstring("- The canonical Términos de Libre Uso MX URL, https://datos.gob.mx/libreusomx, " +
				"was returning 404"))
			Expect(flat).To(ContainSubstring("The build makes no network calls and does not re-check it."))
		})

		It("renders the same bytes on every build and at any build time", func() {
			Expect(renderDoc(publish.RenderNotice, in)).To(Equal(notice))
			later := in
			later.Now = fixedNow.Add(400 * 24 * time.Hour)
			Expect(renderDoc(publish.RenderNotice, later)).To(Equal(notice))
		})

		It("says so when the build references no power run, and refuses a started_at that is not RFC 3339", func() {
			none := in
			none.Meta.Runs.Power = nil
			Expect(unwrapped(renderDoc(publish.RenderNotice, none))).To(ContainSubstring(
				"This build references no NASA POWER run: the database holds no POWER reanalysis rows."))

			bad := in
			bad.Meta.Runs.Power = slices.Clone(in.Meta.Runs.Power)
			bad.Meta.Runs.Power[0].StartedAt = "yesterday"
			var buf bytes.Buffer
			err := publish.RenderNotice(&buf, bad)
			Expect(err).To(MatchError(ContainSubstring(`render NOTICE: power run power-daily-1981-2026: started_at "yesterday" is not RFC 3339`)))
			Expect(buf.Len()).To(BeZero())
		})
	})

	Describe("CITATION.cff", func() {
		It("carries every CITATION.cff field, the build's UTC date as date-released, the settled personal creator with their ORCID URL, no doi key, and the shared abstract and keywords", func() {
			text := renderDoc(publish.RenderCitation, in)
			scalars, lists, comments := cffFields(text)
			Expect(scalars).To(Equal(map[string]string{
				"cff-version":     "1.2.0",
				"message":         `"If you use this dataset, please cite it as below."`,
				"type":            "dataset",
				"title":           `"BioclimaMX Stations: Mexican Climate Station Records (CONAGUA), Augmented with NASA POWER"`,
				"version":         `"0.1"`,
				"license":         "CC-BY-4.0",
				"date-released":   "2026-08-29",
				"abstract":        scalars["abstract"],
				"    given-names": `"Pablo"`,
				"    orcid":       "https://orcid.org/0009-0007-4050-494X",
				"repository-code": `"https://github.com/bioclimamx/conagua-etl"`,
			}))
			Expect(lists).To(Equal(map[string][]string{
				"authors": {`family-names: "Trinidad"`},
				"keywords": {
					`"climate"`, `"Mexico"`, `"CONAGUA"`, `"weather stations"`, `"climate normals"`,
					`"daily observations"`, `"NASA POWER"`, `"MERRA-2"`, `"reanalysis"`, `"bioclimatic"`,
				},
			}))
			Expect(scalars).NotTo(HaveKey("doi"))
			Expect(comments).To(HaveLen(1))
			Expect(comments[0]).To(Equal("# doi: this build carries no DOI; this version's DOI is reserved on " +
				"Zenodo before the deposited build and rendered here from that one reserved value."))
			lines := strings.Split(text, "\n")
			authorsAt := slices.Index(lines, "authors:")
			Expect(authorsAt).To(BeNumerically(">", 0))
			Expect(lines[authorsAt+1]).To(Equal(`  - family-names: "Trinidad"`))
			Expect(lines[authorsAt+2]).To(Equal(`    given-names: "Pablo"`))
			Expect(lines[authorsAt+3]).To(Equal("    orcid: https://orcid.org/" + publish.DatasetCreatorORCID))
			Expect(text).To(HaveSuffix("\n"))
			Expect(text).NotTo(HaveSuffix("\n\n"))
		})

		It("stamps date-released from the build time's UTC date, the one field that varies between builds", func() {
			first := renderDoc(publish.RenderCitation, in)
			Expect(renderDoc(publish.RenderCitation, in)).To(Equal(first))

			// 01:00 on the 30th at UTC+5 is 20:00 on the 29th in UTC: the
			// local date does not leak into the file.
			local := in
			local.Now = time.Date(2026, 8, 30, 1, 0, 0, 0, time.FixedZone("UTC+5", 5*3600))
			Expect(renderDoc(publish.RenderCitation, local)).To(Equal(first))

			nextYear := in
			nextYear.Now = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
			second := renderDoc(publish.RenderCitation, nextYear)
			Expect(second).To(Equal(strings.ReplaceAll(first, "date-released: 2026-08-29", "date-released: 2027-01-01")))
		})

		It("quotes the abstract on one line with the seed's numbers, identical to the Zenodo description", func() {
			text := renderDoc(publish.RenderCitation, in)
			scalars, _, _ := cffFields(text)
			var unquoted string
			Expect(json.Unmarshal([]byte(scalars["abstract"]), &unquoted)).To(Succeed(),
				"a YAML double-quoted scalar with only \\\\ and \\\" escapes is JSON-decodable")
			Expect(unquoted).To(HavePrefix("Daily observations and published climatological normals from 6 CONAGUA " +
				"conventional weather stations across Mexico (Servicio Meteorológico Nacional, Normales " +
				"Climatológicas archive, consulted 2026-06-08), augmented with the 31 NASA POWER reanalysis " +
				"variables of each station's grid cell on the 0.5° × 0.625° (latitude × longitude) grid: " +
				"meteorology from MERRA-2, the 10 solar, radiation, and cloud streams from CERES. "))
			Expect(unquoted).To(ContainSubstring("Normals cover the 4 reference periods 1961-1990, 1971-2000, 1981-2010, and 1991-2020."))
			Expect(unquoted).To(ContainSubstring("POWER monthly climatology for 1981-2010 and 1991-2020."))
			Expect(unquoted).To(HaveSuffix("The compilation is licensed CC BY 4.0; upstream data keep their own terms (see NOTICE)."))

			var stub struct {
				Metadata struct {
					Description string `json:"description"`
				} `json:"metadata"`
			}
			Expect(json.Unmarshal([]byte(renderDoc(publish.RenderZenodoMetadata, in)), &stub)).To(Succeed())
			Expect(stub.Metadata.Description).To(Equal(unquoted))
		})
	})

	Describe("zenodo-metadata.json", func() {
		It("is the build-only Zenodo metadata stub: exactly Zenodo's fields inside metadata, in order, and the build note beside it", func() {
			data := []byte(renderDoc(publish.RenderZenodoMetadata, in))
			Expect(jsonKeyOrder(data)).To(Equal([]string{"metadata", "build_note"}))
			Expect(jsonKeyOrder(data, "metadata")).To(Equal([]string{
				"upload_type", "title", "creators", "description", "access_right", "license", "keywords",
				"version", "publication_date",
			}))
			var stub struct {
				Metadata struct {
					UploadType  string              `json:"upload_type"`
					Title       string              `json:"title"`
					Creators    []map[string]string `json:"creators"`
					Description string              `json:"description"`
					AccessRight string              `json:"access_right"`
					License     string              `json:"license"`
					Keywords    []string            `json:"keywords"`
					Version     string              `json:"version"`
					PubDate     string              `json:"publication_date"`
				} `json:"metadata"`
				BuildNote string `json:"build_note"`
			}
			Expect(json.Unmarshal(data, &stub)).To(Succeed())
			Expect(stub.Metadata.UploadType).To(Equal("dataset"))
			Expect(stub.Metadata.Title).To(Equal(publish.DatasetTitle))
			Expect(stub.Metadata.Creators).To(Equal([]map[string]string{{"name": "Trinidad, Pablo", "orcid": "0009-0007-4050-494X"}}))
			Expect(stub.Metadata.Description).To(HavePrefix("Daily observations and published climatological normals from 6 "))
			Expect(stub.Metadata.AccessRight).To(Equal("open"))
			Expect(stub.Metadata.License).To(Equal("cc-by-4.0"))
			Expect(stub.Metadata.Keywords).To(Equal([]string{
				"climate", "Mexico", "CONAGUA", "weather stations", "climate normals", "daily observations",
				"NASA POWER", "MERRA-2", "reanalysis", "bioclimatic",
			}))
			Expect(stub.Metadata.Version).To(Equal("0.1"))
			Expect(stub.Metadata.PubDate).To(Equal("2026-08-29"))
			Expect(stub.BuildNote).To(Equal("build-only stub; no deposit performed"))
			Expect(string(data)).To(HavePrefix("{\n  \"metadata\": {\n    \"upload_type\": \"dataset\",\n"))
			Expect(string(data)).To(HaveSuffix("}\n"))
		})

		It("renders the same bytes on every build, varying only in publication_date across build times", func() {
			first := renderDoc(publish.RenderZenodoMetadata, in)
			Expect(renderDoc(publish.RenderZenodoMetadata, in)).To(Equal(first))
			shifted := in
			shifted.Now = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
			second := renderDoc(publish.RenderZenodoMetadata, shifted)
			Expect(second).To(Equal(strings.ReplaceAll(first, `"publication_date": "2026-08-29"`, `"publication_date": "2027-01-01"`)))
		})
	})

	Describe("README.md", func() {
		var readme, flat string

		BeforeEach(func() {
			readme = renderDoc(publish.RenderREADME, in)
			flat = unwrapped(readme)
		})

		// section returns the text between the heading that starts with
		// from and the next "## " heading.
		section := func(from string) string {
			GinkgoHelper()
			start := strings.Index(readme, "\n"+from)
			Expect(start).To(BeNumerically(">=", 0), from)
			rest := readme[start+1:]
			if end := strings.Index(rest[len(from):], "\n## "); end >= 0 {
				return rest[:len(from)+end]
			}
			return rest
		}
		tableRows := func(text string) []string {
			var rows []string
			for _, line := range strings.Split(text, "\n") {
				if strings.HasPrefix(line, "| ") && !strings.HasPrefix(line, "|---") {
					rows = append(rows, line)
				}
			}
			return rows
		}

		It("opens with the dataset identity — the title, version, snapshot, schema version, git SHA — and no timestamp", func() {
			Expect(readme).To(HavePrefix("# " + publish.DatasetTitle + "\n\nVersion 0.1. Snapshot `2026-06-08`, " +
				"schema version 1, ETL git SHA `abc123`.\n\n"))
			Expect(readme).To(HaveSuffix("\n"))
			Expect(readme).NotTo(HaveSuffix("\n\n"))
			Expect(readme).NotTo(ContainSubstring("2026-08-29"))
			for i, h := range []string{
				"## 1. What this dataset is", "## 2. The artifact set", "## 3. Reading the archives",
				"## 4. Conventions", "## 5. State codes", "## 6. Provenance", "## 7. Reproducibility",
				"## 8. License and citation", "## 9. Known limitations",
			} {
				Expect(readme).To(ContainSubstring("\n"+h+"\n"), "%d", i)
			}
		})

		It("states the dataset in the seed's numbers and the combined LEFT join definition verbatim", func() {
			Expect(flat).To(ContainSubstring("**Stations.** 6 CONAGUA conventional weather stations (`conagua/stations`) — " +
				"the Servicio Meteorológico Nacional's Normales Climatológicas archive, consulted on 2026-06-08."))
			Expect(flat).To(ContainSubstring("for the 4 reference periods 1961-1990, 1971-2000, 1981-2010, and 1991-2020: " +
				"7 `conagua/monthly_normals` rows and 5 `conagua/monthly_normals_extras` rows"))
			Expect(flat).To(ContainSubstring("(`conagua/daily_observations`): 6 rows across 4 stations, 1975-06-15 to 2020-01-02."))
			Expect(flat).To(ContainSubstring("The 31 reanalysis variables of each station's POWER grid cell (`nasa_power/`; " +
				"the 0.5° × 0.625° (latitude × longitude) grid; meteorology from MERRA-2, the 10 solar, radiation, and cloud streams from CERES): " +
				"7 cells, 5 stations linked to one; daily reanalysis " +
				unwrapped(extentOf(in.Coverage.Power.Daily, "cells")) +
				"; monthly climatology for 1981-2010 and 1991-2020."))
			Expect(flat).To(ContainSubstring(`> **"Combined" / "Augmented" = a CONAGUA-spine record augmented with its ` +
				`station's POWER-cell reanalysis on matching keys — a LEFT join from the CONAGUA side. Never a temporal union.**`))
		})

		It("names every top-level file of the artifact set — the per-state archives of every state, the national archives, the raw snapshot, and the docs group", func() {
			var expected []string
			for _, g := range publish.Groups() {
				switch {
				case g == publish.GroupDocs:
					expected = append(expected, publish.DocsFiles()...)
				case g.PerState():
					for _, st := range in.States {
						expected = append(expected, st.Slug+"-"+string(g)+".zip")
					}
				case g == publish.GroupRaw:
					expected = append(expected, publish.RawArchiveName("2026-06-08"))
				default:
					expected = append(expected, string(g)+".zip")
				}
			}
			Expect(expected).To(HaveLen(3*2 + 4 + 1 + 10))
			artifacts := section("## 2. The artifact set")
			for _, name := range expected {
				Expect(artifacts).To(ContainSubstring("`"+name+"`"), name)
			}
			Expect(tableRows(artifacts)).To(HaveLen(3 + 1 + 5 + 1 + 10 + 1))
			Expect(artifacts).To(ContainSubstring("| Aguascalientes | AGS | `ags-tabular.zip` | `ags-json.zip` |\n"))
			Expect(artifacts).To(ContainSubstring("| Yucatán | YUC | `yuc-tabular.zip` | `yuc-json.zip` |\n"))
			Expect(artifacts).To(ContainSubstring("| `conagua-raw-2026-06-08.zip` | the complete CONAGUA snapshot"))
			for _, row := range tableRows(artifacts) {
				cells := strings.Split(strings.Trim(row, "| "), " | ")
				for _, c := range cells {
					Expect(c).NotTo(BeEmpty(), row)
				}
			}
		})

		It("describes the archives by the paths the writers use and the profile keys the writer emits", func() {
			reading := section("## 3. Reading the archives")
			for _, path := range []string{
				"conagua/stations.csv", "conagua/monthly_normals.csv", "conagua/monthly_normals_extras.csv",
				"conagua/daily_observations/<state>/daily-<station_id>.csv",
				"nasa_power/cells.csv", "nasa_power/station_cell_map.csv", "nasa_power/monthly.csv",
				"nasa_power/daily/daily-<cell_id>.csv",
				"combined/combined_monthly.csv", "combined/combined_daily/<state>/daily-<station_id>.csv",
				"provenance/ingest_runs.csv", "provenance/power_runs.csv",
				"conagua/daily_observations.parquet",
				"combined/<state>/<station_id>/profile.json", "combined/<state>/<station_id>/daily.json",
			} {
				Expect(reading).To(ContainSubstring("`"+path+"`"), path)
			}
			Expect(unwrapped(reading)).To(ContainSubstring("The profile's top-level keys, in order: `station_id`, `identity`, " +
				"`wmo_completeness`, `normals`, `extras`, `power_cell`, `power_monthly`, `daily_summary`, `meta`."))
			Expect(unwrapped(reading)).To(ContainSubstring("`observed` (`tmax_c`, `tmin_c`, `precip_mm`, `evap_mm`; always present"))
			Expect(unwrapped(reading)).To(ContainSubstring("`reanalysis` (the 31 POWER variables, or `null` as a whole"))
			Expect(unwrapped(reading)).To(ContainSubstring("Its surrogate ids are the national `bioclima.db` ids, so joins carry " +
				"across archives; the sequence counters continue from the state's highest id, so ids minted in a " +
				"writable copy are not globally unique"))
			Expect(unwrapped(reading)).To(ContainSubstring("(`power-<temporal_mode>-<start_year>-<end_year>`)"))
		})

		It("states the conventions from the specs, the registry, and the schema: the unit map, the decimals table, the annual convention with its tie caveat, the documented constants", func() {
			conv := section("## 4. Conventions")
			// The unit sentence is the dictionary's unit map, every entry
			// with its unit, in the map's order.
			units := buildDictionary().Units
			Expect(units).To(HaveLen(19))
			last := -1
			for _, u := range units {
				entry := "`" + u.Pattern + "` " + u.Unit
				at := strings.Index(unwrapped(conv), entry)
				Expect(at).To(BeNumerically(">", last), entry)
				last = at
			}
			Expect(unwrapped(conv)).To(ContainSubstring("`_mm` mm (a total over the row's period), `_mmpd` mm/day (a daily rate)"))
			Expect(unwrapped(conv)).To(ContainSubstring("`_wm2` W/m² (a 24-hour average)"))
			Expect(unwrapped(conv)).To(ContainSubstring("`_days` days, `lat` decimal degrees, `lon` decimal degrees (west negative), `clearness_index` dimensionless"))
			Expect(unwrapped(conv)).To(ContainSubstring("`wmo_completeness_*` dimensionless. Columns not listed"))
			Expect(conv).To(ContainSubstring("| decimals | columns |\n"))
			Expect(conv).To(ContainSubstring("| 0 | first_year, last_year, month, "))
			// A name pinned at two counts is qualified by its table, so
			// neither count is hidden: the normals' one decimal and the
			// observed values' two both show under their own table.
			Expect(conv).To(ContainSubstring("| 1 | altitude_m, tmax_c (monthly_normals), tmin_c (monthly_normals), " +
				"tmean_c, precip_mm (monthly_normals), evap_mm (monthly_normals), "))
			Expect(conv).To(ContainSubstring("| 2 | tmax_daily_extreme_c, "))
			Expect(conv).To(ContainSubstring("tmax_c (daily_observations), tmin_c (daily_observations), " +
				"precip_mm (daily_observations), evap_mm (daily_observations), t2m_c, "))
			Expect(conv).To(ContainSubstring("| 3 | lat (nasa_power_grid_cells), lon (nasa_power_grid_cells), distance_km |\n"))
			Expect(conv).To(ContainSubstring("| 4 | wmo_completeness_bin_1961_1990, "))
			Expect(conv).To(ContainSubstring("| 6 | lat (stations), lon (stations) |\n"))
			Expect(tableRows(conv)).To(HaveLen(1 + 6))
			Expect(unwrapped(conv)).To(ContainSubstring("a sum for `precip_mm` and `evap_mm` (monthly totals), a circular mean " +
				"for the two wind directions, an unweighted mean for everything else — and tagged `bioclima_derived`"))
			Expect(unwrapped(conv)).To(ContainSubstring("on a tie the published value follows no decimal rule, and the derived " +
				"annual may differ from it by 0.1."))
			// The documented constants are the dictionary's, value and reason.
			constants := buildDictionary().Constants
			Expect(constants).To(HaveLen(3))
			Expect(unwrapped(conv)).To(ContainSubstring("**Documented constants.** 3 single-valued columns are left out of the flat files and stated here once: "))
			for _, c := range constants {
				Expect(unwrapped(conv)).To(ContainSubstring("`" + c.Table + "." + c.Column + "` = `" + c.Value + "` (" + c.Reason + ")"))
			}
			Expect(unwrapped(conv)).To(ContainSubstring("`stations.source` = `conagua_conventional` (every exported station"))
			Expect(unwrapped(conv)).To(ContainSubstring("`nasa_power_grid_cells.grid_resolution` = `0.5x0.625` ("))
			Expect(unwrapped(conv)).To(ContainSubstring("`power_runs.solar_conversion` = `11.574074074074074` (the MJ/m²/day → W/m² factor (1e6 / 86400)"))
			Expect(unwrapped(conv)).To(ContainSubstring("never a sentinel number"))
		})

		It("counts the conagua/ folder's derived columns as the dictionary does — the 8 WMO scores plus first_year and last_year", func() {
			conv := unwrapped(section("## 4. Conventions"))
			Expect(conv).To(ContainSubstring("**Derived columns in `conagua/`.** 10 columns of `conagua/stations` " +
				"are computed by the ETL rather than mirrored from CONAGUA's catalog: the 8 `wmo_completeness_*` " +
				"scores, from the extras' years-with-data counts (WMO-No. 1203 §4.4.2), and `first_year` and " +
				"`last_year`, from the station's own daily and normals rows. They are the folder's only " +
				"`bioclima_derived` columns"))
			Expect(conv).NotTo(ContainSubstring("the one `bioclima_derived` exception"))
			// The count is the dictionary's own: the two documents tag
			// the same stations columns, and the README states that
			// count rather than a second one of its own.
			var derived []string
			for _, f := range buildDictionary().Files {
				if f.Name != publish.Stations.Name {
					continue
				}
				for _, c := range f.Columns {
					if c.Tag != nil && *c.Tag == "bioclima_derived" {
						derived = append(derived, c.Name)
					}
				}
			}
			Expect(derived).To(HaveLen(10))
			Expect(derived).To(ContainElements("first_year", "last_year"))
			Expect(conv).To(ContainSubstring(fmt.Sprintf("**Derived columns in `conagua/`.** %d columns of", len(derived))))
		})

		It("tabulates the 32 CONAGUA state codes with the file-name slug and the official name, in code order — the order of the per-state archive table", func() {
			states := section("## 5. State codes")
			rows := tableRows(states)
			Expect(rows).To(HaveLen(1 + 32))
			Expect(rows[0]).To(Equal("| code | file-name slug | state |"))
			codes := slices.Clone(conagua.AllStates)
			slices.Sort(codes)
			Expect(codes).NotTo(Equal(conagua.AllStates), "CONAGUA's own order is not code order (CHIH before COAH)")
			for i, c := range codes {
				Expect(rows[i+1]).To(Equal("| " + strings.ToUpper(string(c)) + " | `" + string(c) + "` | " + c.DisplayName() + " |"))
			}
			Expect(states).To(ContainSubstring("| CHIH | `chih` | Chihuahua |\n| CHIS | `chis` | Chiapas |\n| COAH | `coah` | Coahuila |\n"))
			Expect(states).To(ContainSubstring("| DF | `df` | Ciudad de México |\n"))
			// The per-state archive table follows the same order.
			var archiveCodes []string
			for _, row := range tableRows(section("## 2. The artifact set")) {
				cells := strings.Split(strings.Trim(row, "| "), " | ")
				if len(cells) == 4 && cells[1] != "code" {
					archiveCodes = append(archiveCodes, cells[1])
				}
			}
			Expect(archiveCodes).To(Equal([]string{"AGS", "YUC", "ZAC"}))
			Expect(slices.IsSorted(archiveCodes)).To(BeTrue())
		})

		It("lists the provenance runs by natural label with their timestamps, and the reproducibility and citation sections", func() {
			prov := section("## 6. Provenance")
			Expect(prov).To(ContainSubstring("| 2026-05-01 | 2026-05-01T10:00:00Z | 2026-05-01T12:00:00Z | r2 | complete | 0ld5ha |\n"))
			Expect(prov).To(ContainSubstring("| 2026-06-08 | 2026-06-10T01:00:00Z | 2026-06-10T02:30:00Z | local | complete | null |\n"))
			Expect(prov).To(ContainSubstring("| power-daily-1981-2026 | daily | 1981-01-01 to 2026-06-08 | 2026-07-02T00:00:00Z | complete | null |\n"))
			Expect(prov).To(ContainSubstring("| power-monthly-1981-2010 | monthly | 1981 to 2010 | 2026-07-01T00:00:00Z | complete | p0w3r |\n"))
			repro := unwrapped(section("## 7. Reproducibility"))
			Expect(repro).To(ContainSubstring("run `sha256sum -c CHECKSUMS`"))
			license := section("## 8. License and citation")
			Expect(license).To(ContainSubstring("> " + in.Meta.Dataset.SuggestedCitation + "\n"))
			Expect(license).To(ContainSubstring("https://github.com/bioclimamx/conagua-etl"))
		})

		It("states the reproducibility guarantee per class of metadata file, and does not claim the digest files are byte-reproducible", func() {
			repro := unwrapped(section("## 7. Reproducibility"))
			// No blanket claim that every metadata file is
			// byte-reproducible: manifest.json and CHECKSUMS carry digest
			// lines that move on every rebuild of a content-reproducible
			// archive.
			Expect(repro).NotTo(ContainSubstring("every other byte of every metadata file is reproducible"))
			Expect(repro).To(ContainSubstring("- Byte-reproducible in full — `README.md`, `DATA-DICTIONARY.md`, " +
				"`DATA-DICTIONARY.json`, `LICENSE`, `NOTICE`, `QA-REPORT.md`: no clock reaches them, and every " +
				"number in them is computed from the shipped database."))
			Expect(repro).To(ContainSubstring("- Byte-reproducible but for one stamped date each — `date-released` " +
				"in `CITATION.cff` and `publication_date` in `zenodo-metadata.json`, the build's UTC date."))
			Expect(repro).To(ContainSubstring("- Byte-reproducible in what they state, not in what they carry — " +
				"`manifest.json`, `CHECKSUMS`. `manifest.json` also stamps `generated_at`."))
			Expect(repro).To(ContainSubstring("the digests of the Parquet-bearing and SQLite-bearing archives " +
				"change on every rebuild because those archives are content- but not byte-reproducible"))
			Expect(repro).To(ContainSubstring("verify a download against the `manifest.json` and `CHECKSUMS` it " +
				"was published with, never against ones built from a rebuilt deposit."))
			// No metadata file is left unclassified: the section names
			// every file of the docs group.
			for _, name := range publish.DocsFiles() {
				Expect(repro).To(ContainSubstring("`"+name+"`"), name)
			}
		})

		It("states the known limitations from the coverage: the tie caveat, the unrecorded POWER version, the reanalysis extent against the snapshot, and the periods without POWER monthly", func() {
			limits := unwrapped(section("## 9. Known limitations"))
			Expect(limits).To(ContainSubstring("may differ from CONAGUA's published annual by 0.1 on an exact decimal tie"))
			Expect(limits).To(ContainSubstring("The NASA POWER API version is not recorded"))
			Expect(limits).To(ContainSubstring("POWER's daily series ends on " + *in.Coverage.Power.Daily.ValueLastDate +
				", before the snapshot date 2026-06-08"))
			Expect(limits).To(ContainSubstring("POWER's daily series begins on " + *in.Coverage.Power.Daily.ValueFirstDate))
			Expect(limits).To(ContainSubstring("POWER monthly climatology was not pulled for 1961-1990 and 1971-2000"))
		})

		It("discloses every POWER column's own extent — all 31, from the coverage, never a literal date — and says the row exists where the field is null", func() {
			limits := section("## 9. Known limitations")
			extents := in.Coverage.Power.DailyColumnExtents
			Expect(extents).To(HaveLen(len(power.Registry)))
			Expect(limits).To(ContainSubstring("| column | upstream | first date | last date | values |\n"))
			for _, e := range extents {
				want := "| `" + e.Column + "` | " + upstreamName(e.Column) + " | " +
					dateCell(e.FirstDate) + " | " + dateCell(e.LastDate) + " | " +
					strconv.FormatInt(e.NonNull, 10) + " |"
				Expect(limits).To(ContainSubstring(want+"\n"), e.Column)
			}
			flatLimits := unwrapped(limits)
			Expect(flatLimits).To(ContainSubstring("Outside a column's own range the reanalysis row still exists: " +
				"the date is present in `nasa_power/daily` and in `combined/combined_daily`, and `daily.json`'s " +
				"`reanalysis` object for it is not `null` — only that column's field is empty (CSV) or `null` " +
				"(JSON, Parquet)."))
			Expect(flatLimits).To(ContainSubstring("Read the table above before treating a run of empty values as missing data."))
			// The dates are the coverage's, not the renderer's: a shifted
			// coverage shifts the table and nothing is hardcoded.
			shifted := in
			shifted.Coverage.Power.DailyColumnExtents = slices.Clone(extents)
			moved := "1997-03-04"
			shifted.Coverage.Power.DailyColumnExtents[0].FirstDate = &moved
			Expect(section2(renderDoc(publish.RenderREADME, shifted), "## 9. Known limitations")).
				To(ContainSubstring("| `" + extents[0].Column + "` | " + upstreamName(extents[0].Column) + " | " + moved + " |"))
		})

		It("names the divergent columns by count when the coverage has them, and skips the table when the input carries no per-column extents", func() {
			later, earlier := "2001-01-01", "2019-12-31"
			var (
				first = "1981-01-01"
				last  = "2026-05-30"
			)
			shaped := in
			shaped.Coverage.Power.Daily = publish.QASeriesExtent{Rows: 4, Keys: 1, FirstDate: &first, LastDate: &last,
				ValueFirstDate: &first, ValueLastDate: &last}
			shaped.Coverage.Power.DailyColumnExtents = []publish.QAColumnExtent{
				{Column: "t2m_c", NonNull: 4, FirstDate: &first, LastDate: &last},
				{Column: "solar_dni_wm2", NonNull: 2, FirstDate: &later, LastDate: &last},
				{Column: "cloud_amt_pct", NonNull: 1, FirstDate: &first, LastDate: &earlier},
				{Column: "uvb_wm2", NonNull: 0},
			}
			flat := unwrapped(renderDoc(publish.RenderREADME, shaped))
			Expect(flat).To(ContainSubstring("**Per-column POWER extents.** The 4 POWER columns do not all span " +
				"the daily series' extent above: 1 begin later, 1 end earlier, and 1 carry no value at all."))
			Expect(flat).To(ContainSubstring("CERES's streams begin years after MERRA-2's"))
			Expect(flat).To(ContainSubstring("| `uvb_wm2` | CERES | null | null | 0 |"))

			// A hand-built coverage with no per-column extents renders
			// the section without the table rather than an empty one.
			none := in
			none.Coverage.Power.DailyColumnExtents = nil
			text := renderDoc(publish.RenderREADME, none)
			Expect(text).NotTo(ContainSubstring("Per-column POWER extents"))
			Expect(text).NotTo(ContainSubstring("| column | upstream |"))
			Expect(text).To(ContainSubstring("## 9. Known limitations"))
		})

		It("renders the same bytes on every build and at any build time, and reads the coverage for its extents rather than a query", func() {
			Expect(renderDoc(publish.RenderREADME, in)).To(Equal(readme))
			later := in
			later.Now = fixedNow.Add(400 * 24 * time.Hour)
			Expect(renderDoc(publish.RenderREADME, later)).To(Equal(readme))

			empty := in
			empty.Coverage.Daily = publish.QASeriesExtent{}
			empty.Coverage.Power.Daily = publish.QASeriesExtent{}
			empty.Coverage.Power.Monthly = nil
			text := unwrapped(renderDoc(publish.RenderREADME, empty))
			Expect(text).To(ContainSubstring("(`conagua/daily_observations`): no rows."))
			Expect(text).To(ContainSubstring("daily reanalysis no rows; monthly climatology for no reference period."))
			Expect(text).NotTo(ContainSubstring("POWER's daily series ends on"))

			current := in
			current.Meta.SnapshotDate = *in.Coverage.Power.Daily.ValueLastDate
			Expect(unwrapped(renderDoc(publish.RenderREADME, current))).NotTo(ContainSubstring("POWER's daily series ends on"))
		})
	})

	Describe("the identity hard stop", func() {
		It("names exactly the settled creator where a creator is named — one ORCID, no other personal fields — and nowhere else", func() {
			files := map[string]string{
				"NOTICE":               renderDoc(publish.RenderNotice, in),
				"CITATION.cff":         renderDoc(publish.RenderCitation, in),
				"zenodo-metadata.json": renderDoc(publish.RenderZenodoMetadata, in),
				"README.md":            renderDoc(publish.RenderREADME, in),
			}
			var license bytes.Buffer
			Expect(publish.WriteLicense(&license)).To(Succeed())
			files["LICENSE"] = license.String()
			bareORCID := regexp.MustCompile(`\b\d{4}-\d{4}-\d{4}-\d{3}[\dX]\b`)
			for name, text := range files {
				for _, forbidden := range []string{"affiliation", "@"} {
					Expect(text).NotTo(ContainSubstring(forbidden), name)
				}
				// Every ORCID-shaped id in any file is the creator's own.
				for _, match := range bareORCID.FindAllString(text, -1) {
					Expect(match).To(Equal("0009-0007-4050-494X"), name)
				}
			}
			Expect(files["CITATION.cff"]).To(ContainSubstring("\nauthors:\n  - family-names: \"Trinidad\"\n    given-names: \"Pablo\"\n    orcid: https://orcid.org/0009-0007-4050-494X\n"))
			Expect(files["zenodo-metadata.json"]).To(ContainSubstring("\"name\": \"Trinidad, Pablo\""))
			Expect(files["zenodo-metadata.json"]).To(ContainSubstring("\"orcid\": \"0009-0007-4050-494X\""))
			// The license text and the README carry no creator fields.
			Expect(strings.ToLower(files["LICENSE"])).NotTo(ContainSubstring("orcid"))
			Expect(strings.ToLower(files["LICENSE"])).NotTo(ContainSubstring("pablo trinidad"))
			for _, hint := range []string{"real name", "pseudonym", "persona", "organization)", "to be settled by the maintainer"} {
				Expect(files["CITATION.cff"]).NotTo(ContainSubstring(hint), hint)
			}
		})
	})
})

// upstreamName is the spec's own lookup of a POWER column's upstream
// product, as the README's per-column extent table prints it.
func upstreamName(column string) string {
	GinkgoHelper()
	for _, p := range power.Registry {
		if p.Column == column {
			return p.Upstream
		}
	}
	Fail("no registry row for column " + column)
	return ""
}

// dateCell renders a nullable extent date as the Markdown table's cell.
func dateCell(d *string) string {
	if d == nil {
		return "null"
	}
	return *d
}

// section2 returns the text of a README section of a rendered README —
// the package-level twin of the README suite's own closure, for a
// second render inside one spec.
func section2(readme, from string) string {
	GinkgoHelper()
	start := strings.Index(readme, "\n"+from)
	Expect(start).To(BeNumerically(">=", 0), from)
	rest := readme[start+1:]
	if end := strings.Index(rest[len(from):], "\n## "); end >= 0 {
		return rest[:len(from)+end]
	}
	return rest
}

var _ = Describe("the concept DOI across the metadata files", func() {
	var in publish.DocsInput

	BeforeEach(func() {
		db := openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		in = loadDocsInput(db, fixedNow)
	})

	It("ships no DOI and no placeholder anywhere when the build names none", func() {
		Expect(in.Meta.Dataset.DOI).To(BeEmpty())
		Expect(in.Meta.Dataset.SuggestedCitation).To(Equal(publish.DatasetCreator + " (2026). " +
			publish.DatasetTitle + ". Version 0.1. Zenodo."))

		files := map[string]string{
			"NOTICE":               renderDoc(publish.RenderNotice, in),
			"CITATION.cff":         renderDoc(publish.RenderCitation, in),
			"zenodo-metadata.json": renderDoc(publish.RenderZenodoMetadata, in),
			"README.md":            renderDoc(publish.RenderREADME, in),
		}
		for name, text := range files {
			Expect(text).NotTo(ContainSubstring("placeholder"), name)
			Expect(text).NotTo(ContainSubstring("<concept-DOI"), name)
			Expect(text).NotTo(ContainSubstring("DOI: "), name)
		}
		scalars, _, comments := cffFields(files["CITATION.cff"])
		Expect(scalars).NotTo(HaveKey("doi"))
		Expect(comments).To(HaveLen(1))
		Expect(comments[0]).To(HavePrefix("# doi: this build carries no DOI"))
		Expect(unwrapped(files["README.md"])).To(ContainSubstring("This build carries no DOI — the version's DOI is " +
			"reserved on Zenodo and baked into the deposited build"))
		Expect(files["README.md"]).To(ContainSubstring("\n> " + in.Meta.Dataset.SuggestedCitation + "\n"))
	})

	It("renders the reserved DOI in the citation, CITATION.cff's doi key, and the README once it is set", func() {
		const doi = "10.5281/zenodo.9999999"
		set := in
		set.Meta.Dataset.DOI = doi
		set.Meta.Dataset.SuggestedCitation = in.Meta.Dataset.SuggestedCitation + " DOI: " + doi

		citation := renderDoc(publish.RenderCitation, set)
		scalars, _, comments := cffFields(citation)
		Expect(scalars).To(HaveKeyWithValue("doi", doi))
		Expect(comments).To(BeEmpty())
		Expect(citation).To(ContainSubstring("\n    orcid: https://orcid.org/" + publish.DatasetCreatorORCID + "\ndoi: " + doi + "\n"))

		readme := renderDoc(publish.RenderREADME, set)
		Expect(readme).To(ContainSubstring("\n> " + set.Meta.Dataset.SuggestedCitation + "\n"))
		Expect(unwrapped(readme)).To(ContainSubstring("with this version's DOI as its `doi` key; the Zenodo record " +
			"also lists a concept DOI that resolves to the latest version"))
		Expect(readme).To(ContainSubstring(doi))
	})
})

// extentOf mirrors the README's extent phrase for the spec's own
// expectation over the loaded coverage.
func extentOf(e publish.QASeriesExtent, keys string) string {
	if e.FirstDate == nil || e.LastDate == nil {
		return "no rows"
	}
	// The advertised bounds are those of the rows carrying a value.
	return fmt.Sprintf("%d rows across %d %s, %s to %s", e.Rows, e.Keys, keys, *e.ValueFirstDate, *e.ValueLastDate)
}
