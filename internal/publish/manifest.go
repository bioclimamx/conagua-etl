package publish

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/power"
	"github.com/bioclimamx/conagua-etl/internal/schema"
)

// The dataset's fixed identity. Exported so every metadata artifact
// (manifest.json, CITATION.cff, README, the Zenodo stub) states the
// same title, version, license, and creator from one source; the DOI
// is per deposit (Options.DOI).
const (
	// DatasetTitle names the product under the BioclimaMX scheme,
	// "BioclimaMX <product>", so the umbrella project's own name is not
	// spent on one dataset. The scheme names the dataset only: the
	// bioclima_derived provenance tag and the shipped bioclima.db keep
	// their names, which are the producing project's. Never "MERRA-2" in
	// the title: ten of the 31 POWER variables come from NASA CERES, and
	// "NASA POWER" is the name the license obliges us to cite.
	DatasetTitle = "BioclimaMX Stations: Mexican Climate Station Records (CONAGUA), Augmented with NASA POWER"
	// DatasetVersion is the first release; a data refresh is a new
	// version on the same concept DOI.
	DatasetVersion = "0.1"
	// DatasetLicense is the SPDX identifier of the compilation's license.
	DatasetLicense = "CC-BY-4.0"
	// DatasetCreatorGivenNames and DatasetCreatorFamilyNames are the
	// personal creator: full name, ORCID, no affiliation. A Zenodo
	// record is immutable once published, so the identity is chosen
	// deliberately and every artifact states it from this one source.
	// The name is kept in its two parts because the metadata formats
	// order them differently:
	// CITATION.cff takes a person as family-names / given-names (its
	// bare `name` key is the organization form, which citation tools
	// print as a corporate author), and Zenodo takes "Family, Given".
	DatasetCreatorGivenNames  = "Pablo"
	DatasetCreatorFamilyNames = "Trinidad"
	// DatasetCreator is the creator's name in reading order, for prose.
	DatasetCreator = DatasetCreatorGivenNames + " " + DatasetCreatorFamilyNames
	// DatasetCreatorORCID is the creator's ORCID iD in bare form;
	// renderers prefix the canonical https://orcid.org/ URL where the
	// target format expects it.
	DatasetCreatorORCID = "0009-0007-4050-494X"
)

// doiRE is the shape of a bare DOI: the 10. directory indicator, a
// registrant code, a slash, and a suffix of the characters DOIs are
// minted with (Crossref's recommended pattern, which DataCite and
// Zenodo DOIs also satisfy).
var doiRE = regexp.MustCompile(`(?i)^10\.\d{4,9}/[-._;()/:a-z0-9]+$`)

// ValidateDOI accepts an empty DOI (the deposit then carries none) or a
// bare DOI such as 10.5281/zenodo.1234567. The DOI is baked into files
// Zenodo will never let be edited — the citation rides in every
// profile.json — so a malformed value is refused rather than shipped,
// and a resolver URL or a "doi:" prefix is refused with the bare form
// to pass instead of being rewritten silently.
func ValidateDOI(doi string) error {
	if doi == "" {
		return nil
	}
	lower := strings.ToLower(doi)
	for _, prefix := range []string{"https://doi.org/", "http://doi.org/", "https://dx.doi.org/", "doi:"} {
		if strings.HasPrefix(lower, prefix) {
			return fmt.Errorf("DOI %q: pass the bare DOI, %q", doi, strings.TrimSpace(doi[len(prefix):]))
		}
	}
	if !doiRE.MatchString(doi) {
		return fmt.Errorf("DOI %q is not a DOI (expected the form 10.5281/zenodo.1234567)", doi)
	}
	return nil
}

// Manifest is manifest.json, the deposit's global provenance index:
// the schema and code that produced the deposit, the snapshot it was
// built from, the dataset identity, the states and the artifacts built
// for each, the national and raw groups and the artifact built for
// each, the provenance runs by natural label, the
// POWER parameter registry, per-table row counts, and the checksum of
// every other top-level file written. Two things move between builds of
// the same DB: generated_at and the digests in files of the
// Parquet- and SQLite-bearing archives, which are content- but not
// byte-reproducible (README §7). Everything else is byte-reproducible,
// so this file is not itself a fixed digest — a download is verified
// against the manifest it shipped with, never a rebuilt one.
type Manifest struct {
	SchemaVersion   int                `json:"schema_version"`
	ETLGitSHA       string             `json:"etl_git_sha"`
	GeneratedAt     string             `json:"generated_at"`
	SnapshotDate    string             `json:"snapshot_date"`
	Dataset         Dataset            `json:"dataset"`
	States          []ManifestState    `json:"states"`
	National        []ManifestNational `json:"national"`
	Runs            Runs               `json:"runs"`
	PowerParameters []PowerParameter   `json:"power_parameters"`
	Counts          TableCounts        `json:"counts"`
	Files           []ManifestFile     `json:"files"`
}

// ManifestNational is one deposit-wide group the run built — a national
// archive or the raw snapshot — by group name, with the top-level
// artifact written for it: one name, or empty, never null, when the
// archive failed, so the manifest is honest about a degraded build as
// it is for a state. A subset build selects none, and the list is
// empty.
type ManifestNational struct {
	Group     string   `json:"group"`
	Artifacts []string `json:"artifacts"`
}

// Dataset is the citable identity of the deposit. DOI is omitted from
// the JSON while it is empty: a deposit with no DOI yet carries no doi
// field rather than an empty one a tool could read as an identifier.
type Dataset struct {
	Title             string `json:"title"`
	Version           string `json:"version"`
	DOI               string `json:"doi,omitempty"`
	License           string `json:"license"`
	Creator           string `json:"creator"`
	CreatorORCID      string `json:"creator_orcid"`
	SuggestedCitation string `json:"suggested_citation"`
}

// DatasetMetadata returns the dataset identity every metadata artifact
// carries, with doi the deposit's DOI or "" for none. The suggested citation's year is the snapshot's year — the
// data's own date, so the citation is the same on every rebuild of the
// same DB and stays byte-reproducible wherever it rides; a
// wall-clock year would drift across a New Year, a literal would go
// stale silently.
func DatasetMetadata(snapshot time.Time, doi string) Dataset {
	return Dataset{
		Title:             DatasetTitle,
		Version:           DatasetVersion,
		DOI:               doi,
		License:           DatasetLicense,
		Creator:           DatasetCreator,
		CreatorORCID:      DatasetCreatorORCID,
		SuggestedCitation: suggestedCitation(snapshot.Year(), doi),
	}
}

// suggestedCitation renders the citation every artifact repeats. The
// DOI clause exists only when a DOI is baked in: an unminted DOI is
// left out of the sentence entirely rather than stood in for, so no
// build can ship a placeholder where a reader expects an identifier.
func suggestedCitation(year int, doi string) string {
	cite := fmt.Sprintf("%s (%d). %s. Version %s. Zenodo.",
		DatasetCreator, year, DatasetTitle, DatasetVersion)
	if doi == "" {
		return cite
	}
	return cite + " DOI: " + doi
}

// ManifestState is one state the run built: CONAGUA's code as stored,
// the official name (the code → name lookup), and the top-level
// artifacts written for it — empty, never null, when its archive
// failed, so the manifest is honest about a degraded build.
type ManifestState struct {
	Code      string   `json:"code"`
	Name      string   `json:"name"`
	Artifacts []string `json:"artifacts"`
}

// Runs is the provenance index: the ingest and power runs the shipped
// rows trace to, identified by natural label, never by surrogate id.
type Runs struct {
	Ingest []IngestRunRef `json:"ingest"`
	Power  []PowerRunRef  `json:"power"`
}

// IngestRunRef is one ingest_runs row keyed by snapshot_date — the
// latest complete run per snapshot.
// Nullable columns are pointers so a NULL reaches the file as an
// explicit null, never a zero.
type IngestRunRef struct {
	SnapshotDate      string  `json:"snapshot_date"`
	StartedAt         string  `json:"started_at"`
	FinishedAt        *string `json:"finished_at"`
	SinkKind          string  `json:"sink_kind"`
	ETLGitSHA         *string `json:"etl_git_sha"`
	Status            string  `json:"status"`
	StationsAttempted *int64  `json:"stations_attempted"`
	StationsSucceeded *int64  `json:"stations_succeeded"`
	StationsFailed    *int64  `json:"stations_failed"`
	DailyRows         *int64  `json:"daily_rows"`
	NormalsRows       *int64  `json:"normals_rows"`
	ExtrasRows        *int64  `json:"extras_rows"`
	WarningsTotal     *int64  `json:"warnings_total"`
}

// PowerRunRef is one power_runs row keyed by its run_label, its
// fields in the provenance/power_runs column order (natural key
// first, then DDL order) so a JSON rendering of the provenance file
// carries the same order as the CSV. The runs listed are those at
// least one supplement row references. The surrogate id and the
// documented constant solar_conversion are not exported.
// UnitConversions is embedded from the stored JSON text — key order and
// number literals untouched, whitespace re-indented by the encoder —
// never decoded into a Go value and re-serialized; a NULL column is a
// JSON null.
type PowerRunRef struct {
	RunLabel        string          `json:"run_label"`
	StartedAt       string          `json:"started_at"`
	FinishedAt      *string         `json:"finished_at"`
	Status          string          `json:"status"`
	EndpointURL     string          `json:"endpoint_url"`
	Parameters      string          `json:"parameters"`
	Community       string          `json:"community"`
	PeriodStartYear int64           `json:"period_start_year"`
	PeriodEndYear   int64           `json:"period_end_year"`
	GridResolution  string          `json:"grid_resolution"`
	UnitConversions json.RawMessage `json:"unit_conversions"`
	TemporalMode    string          `json:"temporal_mode"`
	PeriodStartDate *string         `json:"period_start_date"`
	PeriodEndDate   *string         `json:"period_end_date"`
	CellsAttempted  *int64          `json:"cells_attempted"`
	CellsSucceeded  *int64          `json:"cells_succeeded"`
	CellsFailed     *int64          `json:"cells_failed"`
	SupplementRows  *int64          `json:"supplement_rows"`
	ETLGitSHA       *string         `json:"etl_git_sha"`
}

// PowerParameter is one row of the POWER parameter registry as the
// manifest publishes it: the POWER API id, the exported column it lands
// in, POWER's native unit, the unit stored, and the factor applied once
// at the writer. It is what lets a reproducer map an id in a run's
// parameters string (T2M) to a flat-file column (t2m_c) and invert the
// conversion.
type PowerParameter struct {
	ID         string  `json:"id"`
	Column     string  `json:"column"`
	PowerUnit  string  `json:"power_unit"`
	StoredUnit string  `json:"stored_unit"`
	Factor     float64 `json:"factor"`
}

// PowerParameters renders power.Registry — the single source of truth
// for parameter identity, order, and units — in registry order, which
// is also the supplement DDL column order.
func PowerParameters() []PowerParameter {
	out := make([]PowerParameter, len(power.Registry))
	for i, p := range power.Registry {
		out[i] = PowerParameter{
			ID: p.Name, Column: p.Column, PowerUnit: p.PowerUnit, StoredUnit: p.StoredUnit, Factor: p.Factor,
		}
	}
	return out
}

// TableCounts is COUNT(*) of every table in schema.sql, one field per
// table in DDL order. The lockstep spec holds this struct to the tables
// the embedded DDL creates, so a new table cannot land uncounted.
type TableCounts struct {
	Stations             int64 `json:"stations"`
	MonthlyNormals       int64 `json:"monthly_normals"`
	MonthlyNormalsExtras int64 `json:"monthly_normals_extras"`
	DailyObservations    int64 `json:"daily_observations"`
	ParsingWarnings      int64 `json:"parsing_warnings"`
	IngestRuns           int64 `json:"ingest_runs"`
	PowerRuns            int64 `json:"power_runs"`
	NasaPowerGridCells   int64 `json:"nasa_power_grid_cells"`
	StationPowerCell     int64 `json:"station_power_cell"`
	MonthlySupplement    int64 `json:"monthly_supplement"`
	DailySupplement      int64 `json:"daily_supplement"`
}

// ManifestFile is one top-level file the run wrote, with the sha256 and
// byte count of the bytes on disk. manifest.json itself is never listed
// here — it cannot carry its own digest — and appears in CHECKSUMS only.
type ManifestFile struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

// RunLabel derives a power run's natural label,
// power-<temporal_mode>-<period_start_year>-<period_end_year>. The
// schema populates the year columns in daily mode too, so both modes
// label the same way; daily's exact bounds ride in period_*_date.
func RunLabel(temporalMode string, periodStartYear, periodEndYear int64) string {
	return fmt.Sprintf("power-%s-%d-%d", temporalMode, periodStartYear, periodEndYear)
}

// ingestRunsSQL selects the latest complete run per snapshot_date —
// latest by started_at, id breaking a tie — in snapshot_date order.
// Nothing FKs to ingest_runs, so a same-snapshot re-ingest is a
// legitimate idempotent operation and the latest complete run simply
// wins; there is no collision rule here.
const ingestRunsSQL = `
SELECT snapshot_date, started_at, finished_at, sink_kind, etl_git_sha, status,
       stations_attempted, stations_succeeded, stations_failed,
       daily_rows, normals_rows, extras_rows, warnings_total
  FROM ingest_runs r
 WHERE status = 'complete'
   AND id = (SELECT id FROM ingest_runs
              WHERE snapshot_date = r.snapshot_date AND status = 'complete'
              ORDER BY started_at DESC, id DESC LIMIT 1)
 ORDER BY snapshot_date`

// LoadIngestRuns returns the manifest's ingest run list: the latest
// complete run per snapshot_date, in snapshot_date order.
func LoadIngestRuns(ctx context.Context, db *sql.DB) ([]IngestRunRef, error) {
	rows, err := db.QueryContext(ctx, ingestRunsSQL)
	if err != nil {
		return nil, fmt.Errorf("load ingest runs: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	runs := []IngestRunRef{}
	for rows.Next() {
		var (
			r                                IngestRunRef
			finishedAt, gitSHA               sql.NullString
			attempted, succeeded, failed     sql.NullInt64
			daily, normals, extras, warnings sql.NullInt64
		)
		if err := rows.Scan(&r.SnapshotDate, &r.StartedAt, &finishedAt, &r.SinkKind, &gitSHA, &r.Status,
			&attempted, &succeeded, &failed, &daily, &normals, &extras, &warnings); err != nil {
			return nil, fmt.Errorf("load ingest runs: scan: %w", err)
		}
		r.FinishedAt = nullString(finishedAt)
		r.ETLGitSHA = nullString(gitSHA)
		r.StationsAttempted = nullInt(attempted)
		r.StationsSucceeded = nullInt(succeeded)
		r.StationsFailed = nullInt(failed)
		r.DailyRows = nullInt(daily)
		r.NormalsRows = nullInt(normals)
		r.ExtrasRows = nullInt(extras)
		r.WarningsTotal = nullInt(warnings)
		runs = append(runs, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load ingest runs: %w", err)
	}
	return runs, nil
}

// powerRunsSQL selects the runs at least one supplement row references.
// Neither supplement table indexes power_run_id, so the referenced-id
// set is materialized once by the non-correlated IN subquery (SQLite
// evaluates it into an ephemeral index) and the whole query costs
// exactly one scan of each supplement table, however many power_runs
// rows exist — a correlated probe per run would cost a full scan per
// unreferenced run.
const powerRunsSQL = `
SELECT id, started_at, finished_at, status, endpoint_url, parameters, community,
       period_start_year, period_end_year, grid_resolution, unit_conversions,
       temporal_mode, period_start_date, period_end_date,
       cells_attempted, cells_succeeded, cells_failed, supplement_rows, etl_git_sha
  FROM power_runs
 WHERE id IN (SELECT power_run_id FROM monthly_supplement WHERE power_run_id IS NOT NULL
              UNION
              SELECT power_run_id FROM daily_supplement WHERE power_run_id IS NOT NULL)
 ORDER BY id`

// LoadPowerRuns returns the manifest's power run list: every run that at
// least one monthly_supplement or daily_supplement row references,
// labelled per RunLabel and sorted by label. Two referenced runs sharing
// a label is an integrity signal — supplement rows would carry
// provenance the label cannot represent — and fails the build naming
// both ids (recovery: re-run power for that period to convergence). A
// unit_conversions text that is not valid JSON is refused for the same
// reason: embedding it would corrupt the manifest.
func LoadPowerRuns(ctx context.Context, db *sql.DB) ([]PowerRunRef, error) {
	rows, err := db.QueryContext(ctx, powerRunsSQL)
	if err != nil {
		return nil, fmt.Errorf("load power runs: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-side close; the row set was fully consumed or the error already reported

	runs := []PowerRunRef{}
	owner := map[string]int64{}
	for rows.Next() {
		var (
			r                                      PowerRunRef
			id                                     int64
			finishedAt, conversions                sql.NullString
			startDate, endDate, gitSHA             sql.NullString
			attempted, succeeded, failed, suppRows sql.NullInt64
		)
		if err := rows.Scan(&id, &r.StartedAt, &finishedAt, &r.Status, &r.EndpointURL, &r.Parameters, &r.Community,
			&r.PeriodStartYear, &r.PeriodEndYear, &r.GridResolution, &conversions,
			&r.TemporalMode, &startDate, &endDate,
			&attempted, &succeeded, &failed, &suppRows, &gitSHA); err != nil {
			return nil, fmt.Errorf("load power runs: scan: %w", err)
		}
		r.RunLabel = RunLabel(r.TemporalMode, r.PeriodStartYear, r.PeriodEndYear)
		if prev, dup := owner[r.RunLabel]; dup {
			return nil, fmt.Errorf("load power runs: power_runs %d and %d share run_label %q "+
				"(re-run power for that period to convergence)", prev, id, r.RunLabel)
		}
		owner[r.RunLabel] = id
		if conversions.Valid {
			if !json.Valid([]byte(conversions.String)) {
				return nil, fmt.Errorf("load power runs: power_runs %d: unit_conversions is not valid JSON", id)
			}
			r.UnitConversions = json.RawMessage(conversions.String)
		}
		r.FinishedAt = nullString(finishedAt)
		r.PeriodStartDate = nullString(startDate)
		r.PeriodEndDate = nullString(endDate)
		r.CellsAttempted = nullInt(attempted)
		r.CellsSucceeded = nullInt(succeeded)
		r.CellsFailed = nullInt(failed)
		r.SupplementRows = nullInt(suppRows)
		r.ETLGitSHA = nullString(gitSHA)
		runs = append(runs, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load power runs: %w", err)
	}
	slices.SortFunc(runs, func(a, b PowerRunRef) int { return strings.Compare(a.RunLabel, b.RunLabel) })
	return runs, nil
}

// LoadRuns loads both provenance row sets. The set is global — identical
// in every archive — and a power label collision is an integrity signal,
// so Run calls this once before any archive is written and refuses the
// whole build on a collision rather than after every state has shipped.
func LoadRuns(ctx context.Context, db *sql.DB) (Runs, error) {
	ingest, err := LoadIngestRuns(ctx, db)
	if err != nil {
		return Runs{}, err
	}
	pow, err := LoadPowerRuns(ctx, db)
	if err != nil {
		return Runs{}, err
	}
	return Runs{Ingest: ingest, Power: pow}, nil
}

// LoadCounts returns COUNT(*) of every table.
func LoadCounts(ctx context.Context, db *sql.DB) (TableCounts, error) {
	var c TableCounts
	for _, t := range []struct {
		table string
		dst   *int64
	}{
		{"stations", &c.Stations},
		{"monthly_normals", &c.MonthlyNormals},
		{"monthly_normals_extras", &c.MonthlyNormalsExtras},
		{"daily_observations", &c.DailyObservations},
		{"parsing_warnings", &c.ParsingWarnings},
		{"ingest_runs", &c.IngestRuns},
		{"power_runs", &c.PowerRuns},
		{"nasa_power_grid_cells", &c.NasaPowerGridCells},
		{"station_power_cell", &c.StationPowerCell},
		{"monthly_supplement", &c.MonthlySupplement},
		{"daily_supplement", &c.DailySupplement},
	} {
		// The table name is one of the literals above, never input.
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+t.table).Scan(t.dst); err != nil {
			return TableCounts{}, fmt.Errorf("count %s: %w", t.table, err)
		}
	}
	return c, nil
}

// buildManifest assembles the manifest for one run from the provenance
// loaded before the state loop, the per-table counts read after it, and
// the artifacts the run wrote. Files are listed by name so the list
// does not depend on build order.
func buildManifest(opts Options, snapshot time.Time, generatedAt string, runs Runs, counts TableCounts,
	states []ManifestState, national []ManifestNational, files []ManifestFile,
) *Manifest {
	sorted := slices.Clone(files)
	slices.SortFunc(sorted, func(a, b ManifestFile) int { return strings.Compare(a.Name, b.Name) })
	return &Manifest{
		SchemaVersion:   schema.Version,
		ETLGitSHA:       opts.ETLGitSHA,
		GeneratedAt:     generatedAt,
		SnapshotDate:    snapshot.Format(snapshotDateLayout),
		Dataset:         DatasetMetadata(snapshot, opts.DOI),
		States:          states,
		National:        national,
		Runs:            runs,
		PowerParameters: PowerParameters(),
		Counts:          counts,
		Files:           sorted,
	}
}

// encodeManifest writes m as two-space-indented JSON with a trailing
// newline. HTML escaping is off so an '&' in an endpoint URL — the
// query separator of a reproducible POWER request — reads as written;
// this is a provenance file for humans and tools, never a web page.
func encodeManifest(w io.Writer, m *Manifest) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(m); err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	return nil
}

// WriteManifest writes m to path atomically and returns the size and
// sha256 of the bytes on disk — what its CHECKSUMS line needs.
func WriteManifest(path string, m *Manifest) (archive.Result, error) {
	res, err := archive.WriteFileAtomic(path, func(w io.Writer) error { return encodeManifest(w, m) })
	if err != nil {
		return archive.Result{}, fmt.Errorf("write manifest: %w", err)
	}
	return res, nil
}

func nullString(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	s := v.String
	return &s
}

func nullInt(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}
	n := v.Int64
	return &n
}
