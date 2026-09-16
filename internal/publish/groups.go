package publish

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// Group is one selectable artifact group of the deposit — what --only
// names. The per-state groups are built for every selected state as
// their own fail-soft archives; the national groups and the raw
// snapshot are one archive each, built only in a full run, after every
// state's archives, in the order listed.
type Group string

const (
	// GroupTabular is {state}-tabular.zip: the four CSV scope folders,
	// the per-table Parquet twins, and the filtered {state}.db.
	GroupTabular Group = "tabular"
	// GroupJSON is {state}-json.zip: per-station profile.json + daily.json
	// under combined/ and the provenance pair as JSON.
	GroupJSON Group = "json"
	// GroupNationalCSV is national-csv.zip: every state's per-station and
	// per-cell CSV files copied raw from the state tabular archives, the
	// seven whole-scope tables regenerated at national scope, and
	// provenance/ once.
	GroupNationalCSV Group = "national-csv"
	// GroupNationalParquet is national-parquet.zip: the ten per-table
	// Parquet files regenerated at national scope.
	GroupNationalParquet Group = "national-parquet"
	// GroupNationalJSON is national-json.zip: every state's station
	// pairs copied raw from the state JSON archives and provenance/ once.
	GroupNationalJSON Group = "national-json"
	// GroupNationalSQLite is national-sqlite.zip: the full canonical
	// bioclima.db as its one entry.
	GroupNationalSQLite Group = "national-sqlite"
	// GroupRaw is conagua-raw-<snapshot_date>.zip: the complete local
	// snapshot directory of the shipped DB's snapshot, verbatim.
	GroupRaw Group = "raw"
	// GroupDocs is the metadata group: the top-level files beside the
	// archives that describe the deposit and the DB rather than one
	// archive (builtDocsFiles — the README, the data dictionary in both
	// forms, LICENSE, NOTICE, CITATION.cff, the QA report, and the
	// Zenodo metadata stub). It is not an archive: each of its files is
	// written on its own, atomically, listed in manifest.json's files[]
	// and in CHECKSUMS like an archive. It describes the DB, not the
	// archive set, so it is allowed in a --state run too.
	GroupDocs Group = "docs"
)

// allGroups is every group in build order — the order the archives are
// built in and the order the valid names are listed. The docs group
// comes last: its files describe the DB the archives were just built
// from.
var allGroups = []Group{
	GroupTabular, GroupJSON,
	GroupNationalCSV, GroupNationalParquet, GroupNationalJSON, GroupNationalSQLite,
	GroupRaw,
	GroupDocs,
}

// The docs group's files by name. The data dictionary's two names are exported
// beside its model (DictionaryMarkdownName, DictionaryJSONName).
const (
	readmeName         = "README.md"
	licenseName        = "LICENSE"
	noticeName         = "NOTICE"
	citationName       = "CITATION.cff"
	qaReportName       = "QA-REPORT.md"
	zenodoMetadataName = "zenodo-metadata.json"
)

// docsFiles names the docs group of the artifact set: the ten top-level files beside the archives, in
// the order the artifact set lists them — what DocsFiles hands out, the
// README's metadata table follows, and the cap spec counts.
var docsFiles = []string{
	readmeName, DictionaryMarkdownName, DictionaryJSONName, licenseName, noticeName, citationName,
	manifestName, checksumsName, qaReportName, zenodoMetadataName,
}

// builtDocsFiles lists the files the docs group writes, in write order,
// each its own fail-soft artifact: every docs file but manifest.json
// and CHECKSUMS, which close the run and list the group's files beside
// the archives. It is what the out-dir checks expect of the group and
// what the group's writer dispatches over (docsInputs.writer).
var builtDocsFiles = slices.DeleteFunc(slices.Clone(docsFiles), func(name string) bool {
	return name == manifestName || name == checksumsName
})

// Groups lists every artifact group in build order, so the CLI's help
// names exactly the groups ResolveOnly accepts.
func Groups() []Group {
	return slices.Clone(allGroups)
}

// DocsFiles lists the docs group's top-level file names — a copy, so a
// caller cannot reorder or truncate the artifact set the cap spec
// counts.
func DocsFiles() []string {
	return slices.Clone(docsFiles)
}

// PerState reports whether the group is built once per state, as
// opposed to once for the deposit.
func (g Group) PerState() bool {
	return g == GroupTabular || g == GroupJSON
}

// FullRunOnly reports whether the group is built only in a full run —
// the national archives and the raw snapshot, each every state's, so a
// subset build has nothing whole to put in them. The per-state groups
// and the docs group, which describes the DB rather than the archive
// set, are allowed in any run.
func (g Group) FullRunOnly() bool {
	return !g.PerState() && g != GroupDocs
}

// copiesFrom names the per-state group a copy-built national archive
// reads its per-unit files from, and whether the group is one.
func (g Group) copiesFrom() (source Group, copies bool) {
	switch g {
	case GroupNationalCSV:
		return GroupTabular, true
	case GroupNationalJSON:
		return GroupJSON, true
	}
	return "", false
}

// ResolveOnly selects the groups named in requested, matched
// case-insensitively. The result keeps build order and drops
// duplicates, so the artifact order never depends on how flags were
// typed. subset reports a run scoped to a subset of states (--state),
// which excludes the full-run-only groups: the national archives and
// the raw snapshot are every state's, so a national or raw name is
// refused and an empty request selects the per-state groups alone —
// the docs group, which describes the DB rather than the archive set,
// is allowed in a subset run when named. In a full run an empty
// request selects every group, docs included. A copy-built national
// group needs the per-state group it copies from in the same run, and
// is refused without it; an unknown name is refused listing the valid
// ones.
func ResolveOnly(requested []string, subset bool) ([]Group, error) {
	if len(requested) == 0 {
		if subset {
			return perStateGroups(), nil
		}
		return Groups(), nil
	}
	wanted := make(map[Group]bool, len(requested))
	for _, r := range requested {
		g := Group(strings.ToLower(r))
		if !slices.Contains(allGroups, g) {
			return nil, fmt.Errorf("unknown artifact group %q (valid: %s)", r, strings.Join(groupNames(allGroups), ", "))
		}
		if subset && g.FullRunOnly() {
			return nil, fmt.Errorf("artifact group %q is built only in a full run: drop --state", r)
		}
		wanted[g] = true
	}
	// The source check runs in build order, so when two copy-built
	// groups both lack their source the refusal names the same one on
	// every run.
	var out []Group
	for _, g := range allGroups {
		if !wanted[g] {
			continue
		}
		if src, copies := g.copiesFrom(); copies && !wanted[src] {
			return nil, fmt.Errorf("artifact group %q copies from the %s archives: add %s to --only", g, src, src)
		}
		out = append(out, g)
	}
	return out, nil
}

// perStateGroups is every per-state group in build order.
func perStateGroups() []Group {
	var out []Group
	for _, g := range allGroups {
		if g.PerState() {
			out = append(out, g)
		}
	}
	return out
}

func groupNames(groups []Group) []string {
	out := make([]string, len(groups))
	for i, g := range groups {
		out[i] = string(g)
	}
	return out
}

// archiveName is a state's top-level archive for one per-state group, on
// the lowercase slug: yuc-tabular.zip, yuc-json.zip.
func archiveName(st State, g Group) string {
	return st.Slug + "-" + string(g) + ".zip"
}

// nationalArchiveName is the top-level archive of one deposit-wide
// group: the group's own name for the four national archives
// (national-csv.zip), the dated name for the raw snapshot.
func nationalArchiveName(g Group, snapshotDate string) string {
	if g == GroupRaw {
		return RawArchiveName(snapshotDate)
	}
	return string(g) + ".zip"
}

// entries assembles one state's archive for a per-state group. tmpDir
// is the run's temp directory for entries that build on disk before
// they stream (the tabular group's state database).
func (g Group) entries(ctx context.Context, db *sql.DB, st State, runs Runs, meta ProfileMeta, tmpDir string,
	onUnit UnitFunc,
) (*assembly, error) {
	var (
		entries []archive.Entry
		units   int
		err     error
	)
	switch g {
	case GroupTabular:
		entries, units, err = TabularEntries(ctx, db, st, runs, tmpDir, onUnit)
	case GroupJSON:
		entries, units, err = JSONEntries(ctx, db, st, runs, meta, onUnit)
	default:
		return nil, fmt.Errorf("artifact group %q is not a per-state group", g)
	}
	if err != nil {
		return nil, err
	}
	return generated(entries, units), nil
}
