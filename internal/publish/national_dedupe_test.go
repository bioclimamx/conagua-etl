package publish_test

// Specs for the shared-file rule of the copy-built national archives: a
// path several state archives carry is copied once, from the first
// state in state order, and every later copy is held to it by content
// — CRC-32 and uncompressed size — so the same bytes stored under a
// different compression still count as one file; a copy that differs
// by a byte, or by length alone, is refused with the file and both
// archives named, whichever two states disagree and whatever the
// record's CRC and size, in the message's exact shape. At the Run seam,
// a state archive whose shared cell file no longer matches the first
// state's fails national-csv.zip soft, naming both archives, while the
// archives that do not copy from it build.

import (
	"archive/zip"
	"context"
	"database/sql"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/ingest"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// sharedCellFile is the one path two state tabular archives can both
// carry: the per-cell daily file of a cell their stations share.
var sharedCellFile = "nasa_power/daily/daily-" + cellShared + ".csv"

// occurrences counts how many times name appears in names.
func occurrences(names []string, name string) int {
	n := 0
	for _, x := range names {
		if x == name {
			n++
		}
	}
	return n
}

// differsError renders the refusal for name carried differently by the
// archives first (content a) and later (content b).
func differsError(name, first string, a []byte, later string, b []byte) string {
	return fmt.Sprintf("entry %q differs between %s (crc32 %08x, %d bytes) and %s (crc32 %08x, %d bytes)",
		name, first, crc32.ChecksumIEEE(a), len(a), later, crc32.ChecksumIEEE(b), len(b))
}

// writeStoredZip writes files to path with every entry Stored
// (uncompressed) under the seeded snapshot's timestamp — a source whose
// records differ from a Deflate-written archive's in every byte but the
// content's CRC and size.
func writeStoredZip(path string, files map[string]string) {
	GinkgoHelper()
	f, err := os.Create(path)
	Expect(err).NotTo(HaveOccurred())
	zw := zip.NewWriter(f)
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		w, err := zw.CreateHeader(&zip.FileHeader{Name: n, Method: zip.Store, Modified: seededSnapshot})
		Expect(err).NotTo(HaveOccurred())
		_, err = io.WriteString(w, files[n])
		Expect(err).NotTo(HaveOccurred())
	}
	Expect(zw.Close()).To(Succeed())
	Expect(f.Close()).To(Succeed())
}

// rewriteEntry rewrites the zip at path with every entry's bytes as they
// were except name's, replaced by content, under the seeded snapshot's
// timestamp — a state archive of the run whose one shared file no
// longer matches its sibling's.
func rewriteEntry(ctx context.Context, path, name string, content []byte) {
	GinkgoHelper()
	names, contents := zipContents(path)
	Expect(contents).To(HaveKey(name))
	contents[name] = content
	entries := make([]archive.Entry, len(names))
	for i, n := range names {
		data := contents[n]
		entries[i] = archive.Entry{Path: n, Write: func(w io.Writer) error {
			_, err := w.Write(data)
			return err
		}}
	}
	_, err := archive.WriteZip(ctx, path, entries, archive.ZipOptions{Modified: seededSnapshot})
	Expect(err).NotTo(HaveOccurred())
}

var _ = Describe("the shared-file rule of the copy-built national archives", func() {
	var (
		ctx    context.Context
		db     *sql.DB
		runs   publish.Runs
		states []publish.State
		dir    string
	)

	BeforeEach(func() {
		ctx = context.Background()
		db = openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		var err error
		runs, err = publish.LoadRuns(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		states, err = publish.LoadStates(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		Expect(codesOf(states)).To(Equal([]string{"AGS", "YUC", "ZAC"}))
		dir = GinkgoT().TempDir()
	})

	// perStationFile is the one per-station file each fixture archive
	// carries so it has something to copy.
	perStationFile := func(st publish.State) string {
		return "conagua/daily_observations/" + st.Slug + "/daily-1.csv"
	}
	// fixtures writes one tabular-shaped archive per state, each with its
	// per-station file plus extra's files for that state, and returns
	// them in state order.
	fixtures := func(extra map[string]map[string]string) []publish.StateArchive {
		GinkgoHelper()
		archives := make([]publish.StateArchive, len(states))
		for i, st := range states {
			files := map[string]string{perStationFile(st): "station_id,date\n1,2020-01-01\n"}
			for name, content := range extra[st.Code] {
				files[name] = content
			}
			p := filepath.Join(dir, st.Slug+"-tabular.zip")
			writeFixtureZip(p, files)
			archives[i] = publish.StateArchive{State: st, Path: p}
		}
		return archives
	}
	// copiesOf lists the copied entries of an assembly by path, with the
	// source archive each reads from.
	copiesOf := func(n *publish.NationalEntries) (paths []string, source map[string]*zip.File) {
		source = map[string]*zip.File{}
		for _, e := range n.Entries {
			if e.From != nil {
				paths = append(paths, e.Path)
				source[e.Path] = e.From
			}
		}
		return paths, source
	}

	const same = "cell_id,date,t2m_c\n21.5N_89.3750W,2020-01-01,24.48\n"

	DescribeTable("refuses a shared file whose copies disagree, naming the file and the two archives with each record's CRC-32 and size",
		func(contents map[string]string, first, later string) {
			extra := map[string]map[string]string{}
			for code, content := range contents {
				extra[code] = map[string]string{sharedCellFile: content}
			}
			n, err := publish.NationalCSVEntries(ctx, db, fixtures(extra), runs, nil)
			Expect(n).To(BeNil())
			Expect(err).To(MatchError(differsError(sharedCellFile,
				strings.ToLower(first)+"-tabular.zip", []byte(contents[first]),
				strings.ToLower(later)+"-tabular.zip", []byte(contents[later]))))
		},
		Entry("the first and second states, one byte apart at equal length",
			map[string]string{"AGS": same, "YUC": strings.Replace(same, "24.48", "24.49", 1)}, "AGS", "YUC"),
		Entry("the first and second states, a trailing byte more",
			map[string]string{"AGS": same, "YUC": same + "\n"}, "AGS", "YUC"),
		Entry("the second and third states, the first not carrying the file",
			map[string]string{"YUC": same, "ZAC": same + "21.5N_89.3750W,2020-01-02,\n"}, "YUC", "ZAC"),
		Entry("the first and third states, the second agreeing with the first",
			map[string]string{"AGS": same, "YUC": same, "ZAC": "cell_id,date,t2m_c\n"}, "AGS", "ZAC"),
		Entry("an empty file against a header-only one",
			map[string]string{"AGS": "", "YUC": "cell_id,date,t2m_c\n"}, "AGS", "YUC"),
	)

	It("copies a file every state carries identically once, from the first state's archive, its record the first's", func() {
		archives := fixtures(map[string]map[string]string{
			"AGS": {sharedCellFile: same}, "YUC": {sharedCellFile: same}, "ZAC": {sharedCellFile: same},
		})
		var units []string
		n, err := publish.NationalCSVEntries(ctx, db, archives, runs, func(label string) { units = append(units, label) })
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(n.Close()).To(Succeed()) }()

		paths, source := copiesOf(n)
		Expect(paths).To(Equal([]string{
			perStationFile(states[0]), perStationFile(states[1]), perStationFile(states[2]), sharedCellFile,
		}))
		ags := zipFiles(archives[0].Path)
		Expect(source[sharedCellFile].Name).To(Equal(sharedCellFile))
		Expect(rawRecord(source[sharedCellFile])).To(Equal(rawRecord(ags[sharedCellFile])))
		Expect(source[sharedCellFile].CRC32).To(Equal(crc32.ChecksumIEEE([]byte(same))))
		Expect(n.Units).To(Equal(3))

		path := filepath.Join(dir, "national-csv.zip")
		_, err = archive.WriteZipMixed(ctx, path, n.Entries, archive.ZipOptions{Modified: seededSnapshot})
		Expect(err).NotTo(HaveOccurred())
		names, contents := zipContents(path)
		Expect(occurrences(names, sharedCellFile)).To(Equal(1))
		Expect(string(contents[sharedCellFile])).To(Equal(same))
		// The shared file is the last copy of every archive, so the
		// unit of the first state fires there and the others earlier.
		Expect(units).To(Equal([]string{"yuc-tabular.zip", "zac-tabular.zip", "ags-tabular.zip"}))
	})

	It("holds identity to the content, not the encoding: the same bytes Stored in one archive and Deflated in another are one file, copied as the first records it", func() {
		archives := fixtures(map[string]map[string]string{"AGS": {sharedCellFile: same}})
		// Yucatán's archive is rewritten Stored: the same content, a
		// different compressed record.
		writeStoredZip(archives[1].Path, map[string]string{perStationFile(states[1]): "station_id,date\n1,2020-01-01\n", sharedCellFile: same})
		ags, yuc := zipFiles(archives[0].Path), zipFiles(archives[1].Path)
		Expect(yuc[sharedCellFile].Method).To(Equal(zip.Store))
		Expect(ags[sharedCellFile].Method).To(Equal(zip.Deflate))
		Expect(rawRecord(yuc[sharedCellFile])).NotTo(Equal(rawRecord(ags[sharedCellFile])))
		Expect(yuc[sharedCellFile].CRC32).To(Equal(ags[sharedCellFile].CRC32))

		n, err := publish.NationalCSVEntries(ctx, db, archives, runs, nil)
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(n.Close()).To(Succeed()) }()
		paths, source := copiesOf(n)
		Expect(occurrences(paths, sharedCellFile)).To(Equal(1))
		Expect(source[sharedCellFile].Method).To(Equal(zip.Deflate), "Aguascalientes' record, the first in state order")
		Expect(rawRecord(source[sharedCellFile])).To(Equal(rawRecord(ags[sharedCellFile])))
	})

	It("refuses the shared file wherever the disagreement is, before any archive is written, leaving every source open only for the assembly", func() {
		archives := fixtures(map[string]map[string]string{"AGS": {sharedCellFile: same}, "ZAC": {sharedCellFile: same + "x"}})
		_, err := publish.NationalCSVEntries(ctx, db, archives, runs, nil)
		Expect(err).To(MatchError(differsError(sharedCellFile, "ags-tabular.zip", []byte(same), "zac-tabular.zip", []byte(same+"x"))))
		// Nothing was written beside the fixtures.
		Expect(dirNames(dir)).To(Equal([]string{"ags-tabular.zip", "yuc-tabular.zip", "zac-tabular.zip"}))
	})
})

var _ = Describe("Run when two state archives disagree on a shared cell's file", func() {
	It("fails national-csv.zip soft naming both archives and the file, while national-json.zip and the regenerated archives build", func() {
		ctx := context.Background()
		db := openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		// The cell two Yucatán stations share, referenced from
		// Aguascalientes too, so both tabular archives carry its file.
		ids["conv/1002"] = upsertStation(db, ingest.StationUpsert{
			Source: ingest.SourceConaguaConventional, ExternalID: "1002", Name: "Calvillo", State: "AGS",
		})
		insertStationCell(db, ids["conv/1002"], cellShared, 40.0)
		root := GinkgoT().TempDir()
		seedRawSnapshot(root)
		out := filepath.Join(GinkgoT().TempDir(), "publish")
		rec := &recorder{}

		// As soon as yuc-tabular.zip lands, its copy of the shared file
		// is rewritten a byte longer — the corruption the rule exists
		// for, staged between the state loop and the national one.
		tampered := []byte("tampered\n")
		var original []byte
		report, err := publish.Run(ctx, db, publish.Options{OutDir: out, SnapshotRoot: root, Now: fixedClock},
			func(ev publish.ProgressEvent) {
				rec.record(ev)
				if ev.Artifact == "yuc-tabular.zip" && ev.Unit == "" && ev.Err == nil {
					_, contents := zipContents(filepath.Join(out, "yuc-tabular.zip"))
					original = contents[sharedCellFile]
					rewriteEntry(ctx, filepath.Join(out, "yuc-tabular.zip"), sharedCellFile, append(slices.Clone(original), tampered...))
				}
			})
		Expect(err).NotTo(HaveOccurred())
		Expect(original).NotTo(BeEmpty())
		Expect(report.Failed).To(Equal(1))
		_, ags := zipContents(filepath.Join(out, "ags-tabular.zip"))
		Expect(ags[sharedCellFile]).To(Equal(original), "the first state's copy is the untouched one")
		Expect(artifactByName(report, "national-csv.zip").Err).To(Equal(differsError(sharedCellFile,
			"ags-tabular.zip", original, "yuc-tabular.zip", append(slices.Clone(original), tampered...))))
		for _, name := range []string{"national-parquet.zip", "national-json.zip", "national-sqlite.zip", "conagua-raw-2026-06-08.zip"} {
			Expect(artifactByName(report, name).Err).To(BeEmpty(), name)
		}
		Expect(eventsOf(rec, "national-csv.zip")).To(HaveLen(1), "no unit of the copy was written")
		Expect(dirNames(out)).NotTo(ContainElement("national-csv.zip"))
		Expect(dirNames(out)).To(ContainElements("national-json.zip", "national-parquet.zip", "national-sqlite.zip"))
		m := readManifest(out)
		Expect(m.National[0]).To(Equal(publish.ManifestNational{Group: "national-csv", Artifacts: []string{}}))
		Expect(m.National[2]).To(Equal(publish.ManifestNational{Group: "national-json", Artifacts: []string{"national-json.zip"}}))
		var sumNames []string
		for _, s := range readChecksums(out) {
			sumNames = append(sumNames, s.Name)
		}
		Expect(sumNames).NotTo(ContainElement("national-csv.zip"))
		Expect(tempDirs(out)).To(BeEmpty())
	})
})
