package publish_test

// Specs for the raw snapshot artifact: a fixture snapshot tree — files
// across the kind directories, one placed through the real LocalFS sink,
// the two metadata files, an empty body — listed Path-sorted with
// slash-separated relative paths and round-tripped byte for byte under
// the snapshot timestamp, identical across two writes; the refusals — a
// missing directory named with --root, a symlinked directory refused at
// the pre-flight, a date that is not a snapshot date, a symlink, a
// writer's temp residue (the snapshot package's own convention), an
// empty directory; at the Run seam a foreign file in the directory
// failing the raw archive soft; and the artifact's name.

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/conagua"
	"github.com/bioclimamx/conagua-etl/internal/publish"
	"github.com/bioclimamx/conagua-etl/internal/snapshot"
)

const rawDate = "2026-06-08"

// seedRawSnapshot lays out a snapshot under root as pull does — one
// file through the LocalFS sink itself, the rest written directly in
// the same layout — and returns the files by slash path. The bodies
// carry what CONAGUA's do: a BOM, a Latin-1 byte, CRLF line endings, a
// missing-value token; one body is empty.
func seedRawSnapshot(root string) map[string]string {
	GinkgoHelper()
	sink := snapshot.NewLocalFS(root)
	files := map[string]string{
		"daily/31001.txt":             "\xef\xbb\xbfESTACI\xd3N : 31001\r\n01/01/1981\t31.0\t14.5\tNULO\t0.0\r\n",
		"daily/1001.txt":              "ESTACI\xd3N : 1001\r\n01/01/1981\t20.0\t5.0\t0.0\t6.0\r\n",
		"monthly/31001.txt":           "MENSUALES 31001\r\n",
		"extremes/31001.txt":          "EXTREMOS 31001\r\n",
		"extremes/1001.txt":           "",
		"normals_1961_1990/31001.txt": "NORMALES 1961-1990\r\nTEMPERATURA M\xc1XIMA\r\n",
		"normals_1991_2020/31001.txt": "NORMALES 1991-2020\r\n",
		"_index.json":                 `{"snapshot_date":"2026-06-08","stations":[]}` + "\n",
		"_progress.json":              `{"snapshot_date":"2026-06-08"}` + "\n",
	}
	_, err := sink.Put(context.Background(), snapshot.Address{Date: rawDate, Kind: conagua.KindDaily, StationID: "31001"},
		strings.NewReader(files["daily/31001.txt"]))
	Expect(err).NotTo(HaveOccurred())
	dir := sink.SnapshotDir(rawDate)
	for name, body := range files {
		if name == "daily/31001.txt" {
			continue
		}
		p := filepath.Join(dir, filepath.FromSlash(name))
		Expect(os.MkdirAll(filepath.Dir(p), 0o755)).To(Succeed())
		Expect(os.WriteFile(p, []byte(body), 0o600)).To(Succeed())
	}
	return files
}

var _ = Describe("RawSnapshotEntries", func() {
	var (
		ctx   context.Context
		root  string
		dir   string
		files map[string]string
	)

	BeforeEach(func() {
		ctx = context.Background()
		root = GinkgoT().TempDir()
		files = seedRawSnapshot(root)
		dir = publish.RawSnapshotDir(root, rawDate)
		Expect(dir).To(Equal(filepath.Join(root, "conagua-raw", rawDate)))
	})

	It("lists every file of the snapshot directory, Path-sorted, by its slash-separated relative path", func() {
		entries, err := publish.RawSnapshotEntries(ctx, root, rawDate)
		Expect(err).NotTo(HaveOccurred())
		var want []string
		for name := range files {
			want = append(want, name)
		}
		slices.Sort(want)
		Expect(entryPaths(entries)).To(Equal(want))
		Expect(want[0]).To(Equal("_index.json"))
	})

	It("round-trips every file byte for byte through the archive writer under the snapshot timestamp, identically on two writes", func() {
		entries, err := publish.RawSnapshotEntries(ctx, root, rawDate)
		Expect(err).NotTo(HaveOccurred())
		for _, e := range entries {
			Expect(render(e)).To(Equal([]byte(files[e.Path])), e.Path)
		}

		p1 := filepath.Join(GinkgoT().TempDir(), publish.RawArchiveName(rawDate))
		p2 := filepath.Join(GinkgoT().TempDir(), publish.RawArchiveName(rawDate))
		opts := archive.ZipOptions{Modified: seededSnapshot}
		res1, err := archive.WriteZip(ctx, p1, entries, opts)
		Expect(err).NotTo(HaveOccurred())
		res2, err := archive.WriteZip(ctx, p2, entries, opts)
		Expect(err).NotTo(HaveOccurred())
		Expect(res2).To(Equal(res1))
		Expect(res1.Entries).To(Equal(len(files)))

		names, contents := zipContents(p1)
		Expect(names).To(Equal(entryPaths(entries)))
		for name, body := range files {
			Expect(string(contents[name])).To(Equal(body), name)
		}
		for name, f := range zipFiles(p1) {
			Expect(f.Modified.Unix()).To(Equal(seededSnapshot.Unix()), name)
		}
		b1, err := os.ReadFile(p1)
		Expect(err).NotTo(HaveOccurred())
		b2, err := os.ReadFile(p2)
		Expect(err).NotTo(HaveOccurred())
		Expect(b2).To(Equal(b1))
	})

	It("names the artifact conagua-raw-<snapshot_date>.zip", func() {
		Expect(publish.RawArchiveName(rawDate)).To(Equal("conagua-raw-2026-06-08.zip"))
	})

	Describe("refusals", func() {
		It("refuses a missing snapshot directory, naming the path and --root, before and at assembly", func() {
			err := publish.CheckRawSnapshot(root, "2026-01-01")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(filepath.Join(root, "conagua-raw", "2026-01-01")))
			Expect(err.Error()).To(ContainSubstring("--root"))
			Expect(err.Error()).To(ContainSubstring("does not exist"))

			_, err = publish.RawSnapshotEntries(ctx, root, "2026-01-01")
			Expect(err).To(MatchError(ContainSubstring("--root")))

			Expect(publish.CheckRawSnapshot(root, rawDate)).To(Succeed())
		})

		It("refuses a symlinked snapshot directory at the pre-flight, without following it, naming --root's role", func() {
			linked := GinkgoT().TempDir()
			Expect(os.MkdirAll(filepath.Join(linked, "conagua-raw"), 0o755)).To(Succeed())
			link := filepath.Join(linked, "conagua-raw", rawDate)
			Expect(os.Symlink(dir, link)).To(Succeed())
			// The target is a real, complete snapshot: only the link is refused.
			Expect(publish.CheckRawSnapshot(root, rawDate)).To(Succeed())
			err := publish.CheckRawSnapshot(linked, rawDate)
			Expect(err).To(MatchError("snapshot directory " + link + " is a symlink, not a directory " +
				"(point --root at the snapshot root it resolves under)"))
			_, err = publish.RawSnapshotEntries(ctx, linked, rawDate)
			Expect(err).To(MatchError(ContainSubstring(link + " is a symlink, not a directory")))

			// A plain file at the directory's path is refused as such.
			plain := GinkgoT().TempDir()
			Expect(os.MkdirAll(filepath.Join(plain, "conagua-raw"), 0o755)).To(Succeed())
			Expect(os.WriteFile(filepath.Join(plain, "conagua-raw", rawDate), []byte("x"), 0o600)).To(Succeed())
			Expect(publish.CheckRawSnapshot(plain, rawDate)).To(MatchError(
				"snapshot directory " + filepath.Join(plain, "conagua-raw", rawDate) + " is not a directory"))
		})

		It("refuses a date that is not a snapshot date", func() {
			_, err := publish.RawSnapshotEntries(ctx, root, "latest")
			Expect(err).To(MatchError(ContainSubstring(`snapshot date "latest" is not YYYY-MM-DD`)))
			_, err = publish.RawSnapshotEntries(ctx, root, "../"+rawDate)
			Expect(err).To(MatchError(ContainSubstring("is not YYYY-MM-DD")))
		})

		It("refuses a symlink, to a file or to a directory, naming it", func() {
			Expect(os.Symlink(filepath.Join(dir, "daily", "31001.txt"), filepath.Join(dir, "daily", "link.txt"))).To(Succeed())
			_, err := publish.RawSnapshotEntries(ctx, root, rawDate)
			Expect(err).To(MatchError(ContainSubstring("daily/link.txt is a symlink, not a regular file")))
			Expect(os.Remove(filepath.Join(dir, "daily", "link.txt"))).To(Succeed())

			Expect(os.Symlink(filepath.Join(dir, "daily"), filepath.Join(dir, "hourly"))).To(Succeed())
			_, err = publish.RawSnapshotEntries(ctx, root, rawDate)
			Expect(err).To(MatchError(ContainSubstring("hourly is a symlink, not a regular file")))
		})

		It("refuses a writer's temp residue — a file named as the snapshot writers name their staging files", func() {
			residue := filepath.Join(dir, "daily", "31002.txt"+snapshot.TempSuffix+"123456")
			Expect(snapshot.IsTempResidue(filepath.Base(residue))).To(BeTrue())
			Expect(os.WriteFile(residue, []byte("partial"), 0o600)).To(Succeed())
			_, err := publish.RawSnapshotEntries(ctx, root, rawDate)
			Expect(err).To(MatchError(ContainSubstring("daily/31002.txt.tmp-123456 is a writer's temp residue")))
		})

		It("refuses an empty snapshot directory", func() {
			empty := GinkgoT().TempDir()
			Expect(os.MkdirAll(publish.RawSnapshotDir(empty, rawDate), 0o755)).To(Succeed())
			_, err := publish.RawSnapshotEntries(ctx, empty, rawDate)
			Expect(err).To(MatchError(ContainSubstring("holds no files")))
		})
	})
})

var _ = Describe("Run over a snapshot directory holding a foreign file", func() {
	It("fails the raw archive and the README soft, naming the file, while every other artifact builds and the manifest is honest about it", func() {
		ctx := context.Background()
		db := openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		root := GinkgoT().TempDir()
		seedRawSnapshot(root)
		// A common local layout: the ingest database and its side files
		// inside the snapshot directory.
		dir := publish.RawSnapshotDir(root, rawDate)
		for _, name := range []string{"ingest.db", "ingest.db-wal", "ingest.db-shm"} {
			Expect(os.WriteFile(filepath.Join(dir, name), []byte("not CONAGUA's"), 0o600)).To(Succeed())
		}
		out := filepath.Join(GinkgoT().TempDir(), "publish")
		rec := &recorder{}

		report, err := publish.Run(ctx, db, publish.Options{OutDir: out, SnapshotRoot: root, Now: fixedClock}, rec.record)
		Expect(err).NotTo(HaveOccurred())
		// The README states which raw files are not CONAGUA data, so a
		// snapshot it cannot read fails it too, with the same reason.
		Expect(report.Failed).To(Equal(2))
		rawName := publish.RawArchiveName(rawDate)
		refusal := "walk snapshot directory " + dir +
			": ingest.db is not part of the snapshot layout (a kind directory, _index.json, or _progress.json)"
		Expect(artifactByName(report, rawName).Err).To(Equal(refusal))
		Expect(artifactByName(report, "README.md").Err).To(HaveSuffix(": " + refusal))
		for _, a := range report.Artifacts {
			if a.Name != rawName && a.Name != "README.md" {
				Expect(a.Err).To(BeEmpty(), a.Name)
			}
		}
		Expect(eventsOf(rec, rawName)).To(HaveLen(1), "no unit of the raw archive was written")
		Expect(dirNames(out)).NotTo(ContainElement(rawName))
		Expect(dirNames(out)).To(ContainElements("national-csv.zip", "national-sqlite.zip"))
		m := readManifest(out)
		Expect(m.National[4]).To(Equal(publish.ManifestNational{Group: "raw", Artifacts: []string{}}))
		var sumNames []string
		for _, s := range readChecksums(out) {
			sumNames = append(sumNames, s.Name)
		}
		Expect(sumNames).NotTo(ContainElement(rawName))
	})
})

var _ = Describe("RawHTMLPages", func() {
	var root string

	BeforeEach(func() {
		root = GinkgoT().TempDir()
		seedRawSnapshot(root)
		dir := publish.RawSnapshotDir(root, rawDate)
		// What the SMN server returns in place of a station file, with a
		// success status — once plain, once behind a BOM and blank lines.
		page := `<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"><html><head><title>Error</title></head></html>`
		for name, body := range map[string]string{
			"monthly/02011.txt":           page,
			"extremes/9001.txt":           "\xef\xbb\xbf\r\n  " + page,
			"monthly/1001.txt":            "MENSUALES 1001 <no es html>\r\n",
			"normals_1971_2000/31001.txt": "NORMALES 1971-2000\r\n",
		} {
			path := filepath.Join(dir, filepath.FromSlash(name))
			Expect(os.MkdirAll(filepath.Dir(path), 0o755)).To(Succeed())
			Expect(os.WriteFile(path, []byte(body), 0o600)).To(Succeed())
		}
	})

	It("lists exactly the station files whose body is an HTML page, Path-sorted, and never the metadata files", func() {
		pages, err := publish.RawHTMLPages(context.Background(), root, rawDate)
		Expect(err).NotTo(HaveOccurred())
		// A '<' past the first byte is text, an empty body is text, and
		// _index.json is not a station file.
		Expect(pages).To(Equal([]string{"extremes/9001.txt", "monthly/02011.txt"}))
	})

	It("is nil over a snapshot with none, and refuses a snapshot it cannot walk", func() {
		clean := GinkgoT().TempDir()
		seedRawSnapshot(clean)
		pages, err := publish.RawHTMLPages(context.Background(), clean, rawDate)
		Expect(err).NotTo(HaveOccurred())
		Expect(pages).To(BeNil())

		_, err = publish.RawHTMLPages(context.Background(), GinkgoT().TempDir(), rawDate)
		Expect(err).To(MatchError(ContainSubstring("does not exist")))
	})

	It("reaches README §9 through Run, listing every page it found", func() {
		ctx := context.Background()
		db := openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		out := filepath.Join(GinkgoT().TempDir(), "publish")
		_, err := publish.Run(ctx, db, publish.Options{OutDir: out, SnapshotRoot: root, Now: fixedClock,
			Only: []string{string(publish.GroupDocs)}}, (&recorder{}).record)
		Expect(err).NotTo(HaveOccurred())
		readme, err := os.ReadFile(filepath.Join(out, "README.md"))
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.Join(strings.Fields(string(readme)), " ")).To(ContainSubstring(
			"2 of the station files in `conagua-raw-" + rawDate + ".zip` are not CONAGUA data but HTML error " +
				"pages the SMN server returned in place of the station file, with a success status, so the pull " +
				"recorded them as fetched; they are shipped as pulled: `extremes/9001.txt`, `monthly/02011.txt`. " +
				"None of them is in a folder ingest reads, so no value in the deposit comes from them."))
	})
})
