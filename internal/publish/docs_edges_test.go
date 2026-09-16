package publish_test

// Edge specs for the two docs renderers whose text turns on the
// inputs' dates: NOTICE's NASA POWER reference string over the power
// runs' started_at — several distinct dates, duplicates, a non-UTC
// offset, one resolution, the production shape, no run at all — and
// CITATION.cff held line by line: the exact line sequence, the one
// author line with the settled creator's ORCID, no doi key while none
// is baked in — the comment that says so standing where it would go —
// date-released as the build clock's UTC date and nothing else moving
// with the clock. Both over a hand-built DocsInput with no database:
// the two renderers read nothing the input does not carry.

import (
	"bytes"
	"regexp"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// edgeInput is a DocsInput over no database: the production snapshot's
// identity, the given power runs, and the build clock.
func edgeInput(now time.Time, power []publish.PowerRunRef) publish.DocsInput {
	return publish.DocsInput{
		Meta: publish.ProfileMeta{
			SchemaVersion: schema.Version, ETLGitSHA: "abc123", SnapshotDate: "2026-07-18",
			Runs:    publish.Runs{Ingest: []publish.IngestRunRef{}, Power: power},
			Dataset: publish.DatasetMetadata(time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC), ""),
		},
		Stations: 5524,
		States:   []publish.State{yucatan},
		Now:      now,
	}
}

// powerRunAt is a referenced power run of one temporal mode and span,
// started at the given RFC 3339 instant.
func powerRunAt(mode string, startYear, endYear int64, startedAt string) publish.PowerRunRef {
	return publish.PowerRunRef{
		RunLabel: publish.RunLabel(mode, startYear, endYear), StartedAt: startedAt, Status: "complete",
		EndpointURL: "https://power.larc.nasa.gov/api/temporal/" + mode + "/point", Parameters: "T2M",
		Community: "AG", PeriodStartYear: startYear, PeriodEndYear: endYear, GridResolution: "0.5x0.625",
		TemporalMode: mode,
	}
}

// powerVersionSlot is what stands in the NASA POWER reference's version
// slot, since the ETL does not record the API version string.
const powerVersionSlot = "(API version as served on the access dates; the version string was not recorded by the ETL — see Known gaps)"

// referenceSentence is NOTICE's NASA POWER data-reference string for
// the given resolutions and dates.
func referenceSentence(resolutions, dates string) string {
	return "The data was obtained from the POWER Project's " + resolutions + " " + powerVersionSlot + " version on " + dates + "."
}

var _ = Describe("NOTICE's NASA POWER reference over the runs' access dates", func() {
	It("names each distinct started_at date once, in UTC, ascending, joined as prose, whatever the runs' order", func() {
		runs := []publish.PowerRunRef{
			powerRunAt("daily", 1981, 2026, "2026-07-21T10:00:00Z"),
			// 23:30 on the 18th at UTC-2 is 01:30 on the 19th in UTC.
			powerRunAt("monthly", 1961, 1990, "2026-07-18T23:30:00-02:00"),
			powerRunAt("monthly", 1981, 2010, "2026-07-20T00:00:00Z"),
			powerRunAt("monthly", 1991, 2020, "2026-07-20T23:59:59Z"),
		}
		text := renderDoc(publish.RenderNotice, edgeInput(fixedNow, runs))
		flat := unwrapped(text)
		Expect(flat).To(ContainSubstring(referenceSentence("Daily and Monthly", "2026/07/19, 2026/07/20, and 2026/07/21")))
		Expect(strings.Count(flat, "version on ")).To(Equal(1))
		Expect(flat).NotTo(ContainSubstring("2026/07/18"), "the offset instant is dated in UTC")
		Expect(flat).NotTo(ContainSubstring("2026-07-18T23:30"), "no raw timestamp reaches the credit")

		reversed := slices.Clone(runs)
		slices.Reverse(reversed)
		Expect(renderDoc(publish.RenderNotice, edgeInput(fixedNow, reversed))).To(Equal(text))
	})

	DescribeTable("words the resolutions and the dates as the template needs",
		func(runs []publish.PowerRunRef, resolutions, dates string) {
			flat := unwrapped(renderDoc(publish.RenderNotice, edgeInput(fixedNow, runs)))
			Expect(flat).To(ContainSubstring(referenceSentence(resolutions, dates)))
			Expect(strings.Count(flat, "version on ")).To(Equal(1))
		},
		Entry("the production shape: two monthly runs and one daily, all on one date",
			[]publish.PowerRunRef{
				powerRunAt("daily", 1981, 2026, "2026-07-20T18:12:03Z"),
				powerRunAt("monthly", 1981, 2010, "2026-07-20T09:00:00Z"),
				powerRunAt("monthly", 1991, 2020, "2026-07-20T12:00:00Z"),
			}, "Daily and Monthly", "2026/07/20"),
		Entry("two dates", []publish.PowerRunRef{
			powerRunAt("monthly", 1981, 2010, "2026-07-20T00:00:00Z"),
			powerRunAt("daily", 1981, 2026, "2026-07-22T00:00:00Z"),
		}, "Daily and Monthly", "2026/07/20 and 2026/07/22"),
		Entry("daily runs only", []publish.PowerRunRef{
			powerRunAt("daily", 1981, 2026, "2026-07-21T00:00:00Z"),
		}, "Daily", "2026/07/21"),
		Entry("monthly runs only, one date twice", []publish.PowerRunRef{
			powerRunAt("monthly", 1981, 2010, "2026-07-20T00:00:00Z"),
			powerRunAt("monthly", 1991, 2020, "2026-07-20T06:00:00Z"),
		}, "Monthly", "2026/07/20"),
	)

	It("says so when the build references no power run — nil or empty — and still renders the funding sentence and the rest of the file", func() {
		for _, power := range [][]publish.PowerRunRef{nil, {}} {
			var buf bytes.Buffer
			Expect(publish.RenderNotice(&buf, edgeInput(fixedNow, power))).To(Succeed())
			flat := unwrapped(buf.String())
			Expect(flat).To(ContainSubstring("This build references no NASA POWER run: the database holds no POWER reanalysis rows."))
			Expect(flat).NotTo(ContainSubstring("version on "))
			Expect(flat).To(ContainSubstring("Langley Research Center's Prediction Of Worldwide Energy Resources (POWER) project"))
			Expect(flat).To(ContainSubstring("3. License scope"))
			Expect(flat).To(ContainSubstring("4. Transparency note"))
			Expect(flat).To(ContainSubstring("Known gaps"))
			Expect(buf.String()).To(HaveSuffix("\n"))
			Expect(buf.String()).NotTo(HaveSuffix("\n\n"))
		}
	})

	It("never carries the build clock: two clocks, one text", func() {
		runs := []publish.PowerRunRef{powerRunAt("daily", 1981, 2026, "2026-07-20T00:00:00Z")}
		a := renderDoc(publish.RenderNotice, edgeInput(fixedNow, runs))
		b := renderDoc(publish.RenderNotice, edgeInput(fixedNow.AddDate(1, 2, 3), runs))
		Expect(b).To(Equal(a))
		Expect(a).NotTo(ContainSubstring(fixedNow.Format("2006-01-02")))
	})
})

var _ = Describe("CITATION.cff line by line", func() {
	// 03:00 on the 30th at UTC+5 is 22:00 on the 29th in UTC.
	clock := time.Date(2026, 8, 30, 3, 0, 0, 0, time.FixedZone("UTC+5", 5*3600))
	runs := []publish.PowerRunRef{powerRunAt("daily", 1981, 2026, "2026-07-20T00:00:00Z")}
	var lines []string

	BeforeEach(func() {
		text := renderDoc(publish.RenderCitation, edgeInput(clock, runs))
		Expect(text).To(HaveSuffix("\n"))
		Expect(text).NotTo(HaveSuffix("\n\n"))
		Expect(text).NotTo(ContainSubstring("\r"))
		Expect(text).NotTo(ContainSubstring("\t"))
		lines = strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	})

	It("is exactly these 25 lines, in this order", func() {
		Expect(lines).To(HaveLen(25))
		Expect(lines[:7]).To(Equal([]string{
			"cff-version: 1.2.0",
			`message: "If you use this dataset, please cite it as below."`,
			"type: dataset",
			`title: "BioclimaMX Stations: Mexican Climate Station Records (CONAGUA), Augmented with NASA POWER"`,
			`version: "0.1"`,
			"license: CC-BY-4.0",
			"date-released: 2026-08-29",
		}))
		Expect(lines[7:11]).To(Equal([]string{"authors:", `  - family-names: "Trinidad"`, `    given-names: "Pablo"`,
			"    orcid: https://orcid.org/0009-0007-4050-494X"}))
		Expect(lines[11]).To(Equal("# doi: this build carries no DOI; this version's DOI is reserved on Zenodo " +
			"before the deposited build and rendered here from that one reserved value."))
		Expect(lines[12]).To(HavePrefix(`abstract: "Daily observations and published climatological normals from 5524 `))
		Expect(lines[12]).To(HaveSuffix(`"`))
		Expect(lines[13]).To(Equal("keywords:"))
		Expect(lines[14:24]).To(Equal([]string{
			`  - "climate"`, `  - "Mexico"`, `  - "CONAGUA"`, `  - "weather stations"`, `  - "climate normals"`,
			`  - "daily observations"`, `  - "NASA POWER"`, `  - "MERRA-2"`, `  - "reanalysis"`, `  - "bioclimatic"`,
		}))
		Expect(lines[24]).To(Equal(`repository-code: "https://github.com/bioclimamx/conagua-etl"`))
	})

	It("has no doi key: 'doi:' occurs once, at the head of the comment that says why", func() {
		for _, l := range lines {
			Expect(strings.TrimLeft(l, " -")).NotTo(HavePrefix("doi:"), l)
		}
		text := strings.Join(lines, "\n")
		Expect(strings.Count(text, "doi:")).To(Equal(1))
		Expect(lines[11]).To(HavePrefix("# doi: "))
	})

	It("puts the reserved DOI on a doi key in the comment's place, keeping the line count and every other line", func() {
		in := edgeInput(clock, runs)
		in.Meta.Dataset.DOI = "10.5281/zenodo.9999999"
		withDOI := strings.Split(strings.TrimSuffix(renderDoc(publish.RenderCitation, in), "\n"), "\n")
		Expect(withDOI).To(HaveLen(len(lines)))
		Expect(withDOI[11]).To(Equal("doi: 10.5281/zenodo.9999999"))
		for i := range lines {
			if i == 11 {
				continue
			}
			Expect(withDOI[i]).To(Equal(lines[i]), "line %d", i+1)
		}
		// A CFF file either states a DOI or says nothing: never both,
		// and never a placeholder value a validator would accept.
		text := strings.Join(withDOI, "\n")
		Expect(strings.Count(text, "doi:")).To(Equal(1))
		Expect(text).NotTo(ContainSubstring("#"))
	})

	It("names exactly one author — the settled creator as a CFF person, with their ORCID URL — and no other personal fields", func() {
		var nameLines []string
		for _, l := range lines {
			if strings.Contains(l, "name") && !strings.HasPrefix(l, "title:") {
				nameLines = append(nameLines, l)
			}
		}
		// The person form: CFF reserves a bare `name` for an entity,
		// which citation tools render as a corporate author.
		Expect(nameLines).To(Equal([]string{`  - family-names: "Trinidad"`, `    given-names: "Pablo"`}))
		Expect(slices.Index(lines, "authors:")).To(Equal(7))
		Expect(slices.Index(lines, "    orcid: https://orcid.org/0009-0007-4050-494X")).To(Equal(10))
		text := strings.ToLower(strings.Join(lines, "\n"))
		for _, forbidden := range []string{"  - name:", "affiliation", "email", "@",
			"real name", "pseudonym", "persona"} {
			Expect(text).NotTo(ContainSubstring(forbidden), forbidden)
		}
		// The creator's own ORCID is the only ORCID anywhere in the file:
		// twice on its one line, as the key and inside the canonical URL.
		Expect(strings.Count(strings.Join(lines, "\n"), "orcid")).To(Equal(2))
		Expect(strings.Count(strings.Join(lines, "\n"), "orcid.org")).To(Equal(1))
		Expect(regexp.MustCompile(`(?i)\b\d{4}-\d{4}-\d{4}-\d{3}[\dX]\b`).FindAllString(text, -1)).To(HaveLen(1))
	})

	It("stamps date-released as the clock's UTC date in YYYY-MM-DD and moves nothing else with the clock", func() {
		Expect(lines[6]).To(MatchRegexp(`^date-released: \d{4}-\d{2}-\d{2}$`))
		Expect(lines[6]).To(Equal("date-released: " + clock.UTC().Format("2006-01-02")))
		Expect(clock.Format("2006-01-02")).To(Equal("2026-08-30"), "the local date, which must not leak")

		later := renderDoc(publish.RenderCitation, edgeInput(time.Date(2027, 2, 28, 23, 59, 59, 0, time.UTC), runs))
		laterLines := strings.Split(strings.TrimSuffix(later, "\n"), "\n")
		Expect(laterLines).To(HaveLen(len(lines)))
		for i := range lines {
			if i == 6 {
				Expect(laterLines[i]).To(Equal("date-released: 2027-02-28"))
				continue
			}
			Expect(laterLines[i]).To(Equal(lines[i]), "line %d", i+1)
		}
	})

	It("keeps MERRA-2 out of the CITATION.cff title while the keywords carry it", func() {
		Expect(lines[3]).NotTo(ContainSubstring("MERRA"))
		Expect(lines[3]).To(ContainSubstring("NASA POWER"))
		Expect(lines).To(ContainElement(`  - "MERRA-2"`))
	})

	It("renders over zero coverage and no power run without a crash: the abstract states no rows", func() {
		scalars, lists, _ := cffFields(renderDoc(publish.RenderCitation, edgeInput(clock, nil)))
		Expect(scalars["abstract"]).To(ContainSubstring("Observed daily series (maximum and minimum temperature, precipitation, evaporation): no rows."))
		Expect(scalars["abstract"]).To(ContainSubstring("POWER daily reanalysis: no rows; POWER monthly climatology for no reference period."))
		Expect(lists["authors"]).To(Equal([]string{`family-names: "Trinidad"`}))
		Expect(strings.Join(lines, "\n")).To(ContainSubstring("\n    orcid: https://orcid.org/0009-0007-4050-494X\n"))
	})
})
