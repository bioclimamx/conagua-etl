package publish_test

// Specs for RawSnapshotEntries as a directory walker held to pull's
// layout: every <station_id>.txt of every kind directory and the two
// metadata files are entries — an empty station file too, a kind
// directory that is empty or missing contributing nothing — ordered
// bytewise (a digit before a letter, "31001" before "3101", '_' before
// 'd'), each file's bytes streamed verbatim under the archive's pinned
// timestamp rather than the file's own mtime; and everything the layout
// does not account for is refused naming its path — a database and its
// side files parked beside _index.json (a layout that keeps an ingest
// database inside its snapshot), a directory of notes, a dotfile at the
// root or inside a kind directory, a nested directory, a non-.txt or
// bare-.txt file inside a kind directory, temp residue however deep, a
// symlink; and a cancelled context stops the walk.

import (
	"context"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// walkerMtime is the mtime every seeded file is set to: far from the
// snapshot date, so a file's own timestamp leaking into an entry would
// show.
var walkerMtime = time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)

// seedWalkerSnapshot lays out a snapshot of rawDate under root in pull's
// layout, with names that exercise the walker's ordering and file kinds,
// every file stamped walkerMtime, plus an empty kind directory and no
// normals_1961_1990 directory at all; it returns the files by slash
// path.
func seedWalkerSnapshot(root string) map[string]string {
	GinkgoHelper()
	files := map[string]string{
		"_index.json":                 `{"snapshot_date":"` + rawDate + `"}` + "\n",
		"_progress.json":              `{"snapshot_date":"` + rawDate + `","files":[]}` + "\n",
		"daily/1001.txt":              "ESTACI\xd3N : 1001\r\n",
		"daily/31001.txt":             "\xef\xbb\xbfESTACI\xd3N : 31001\r\n01/01/1981\t31.0\t14.5\tNULO\t0.0\r\n",
		"daily/3101.txt":              "ESTACI\xd3N : 3101\r\n",
		"daily/A9.txt":                "an opaque station id\r\n",
		"extremes/1001.txt":           "",
		"normals_1991_2020/31001.txt": "NORMALES 1991-2020\r\n",
	}
	dir := publish.RawSnapshotDir(root, rawDate)
	for name, body := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		Expect(os.MkdirAll(filepath.Dir(p), 0o755)).To(Succeed())
		Expect(os.WriteFile(p, []byte(body), 0o600)).To(Succeed())
		Expect(os.Chtimes(p, walkerMtime, walkerMtime)).To(Succeed())
	}
	Expect(os.MkdirAll(filepath.Join(dir, "monthly"), 0o755)).To(Succeed())
	return files
}

// walkerOrder is the bytewise order of seedWalkerSnapshot's paths.
var walkerOrder = []string{
	"_index.json",
	"_progress.json",
	"daily/1001.txt",
	"daily/31001.txt",
	"daily/3101.txt",
	"daily/A9.txt",
	"extremes/1001.txt",
	"normals_1991_2020/31001.txt",
}

var _ = Describe("RawSnapshotEntries as a walker", func() {
	var (
		ctx   context.Context
		root  string
		dir   string
		files map[string]string
	)

	BeforeEach(func() {
		ctx = context.Background()
		root = GinkgoT().TempDir()
		files = seedWalkerSnapshot(root)
		dir = publish.RawSnapshotDir(root, rawDate)
	})

	It("lists every station file of every kind directory and the two metadata files — an empty file included, an empty or missing kind directory contributing nothing — in bytewise path order", func() {
		entries, err := publish.RawSnapshotEntries(ctx, root, rawDate)
		Expect(err).NotTo(HaveOccurred())
		Expect(entryPaths(entries)).To(Equal(walkerOrder))
		Expect(entries).To(HaveLen(len(files)))
		Expect(entryPaths(entries)).NotTo(ContainElement(HavePrefix("monthly")))
		Expect(entryPaths(entries)).NotTo(ContainElement(HavePrefix("normals_1961_1990")))
	})

	It("streams each file's bytes verbatim, the empty one as zero bytes", func() {
		entries, err := publish.RawSnapshotEntries(ctx, root, rawDate)
		Expect(err).NotTo(HaveOccurred())
		for _, e := range entries {
			Expect(render(e)).To(Equal([]byte(files[e.Path])), e.Path)
		}
		Expect(render(entryByPath(entries, "extremes/1001.txt"))).To(BeEmpty())
	})

	It("stamps every entry with the archive's timestamp, never the file's own mtime, and so writes the same bytes twice", func() {
		entries, err := publish.RawSnapshotEntries(ctx, root, rawDate)
		Expect(err).NotTo(HaveOccurred())
		for _, p := range walkerOrder {
			st, err := os.Stat(filepath.Join(dir, filepath.FromSlash(p)))
			Expect(err).NotTo(HaveOccurred())
			Expect(st.ModTime().UTC()).To(Equal(walkerMtime), p)
		}
		p1 := filepath.Join(GinkgoT().TempDir(), publish.RawArchiveName(rawDate))
		p2 := filepath.Join(GinkgoT().TempDir(), publish.RawArchiveName(rawDate))
		opts := archive.ZipOptions{Modified: seededSnapshot}
		res1, err := archive.WriteZip(ctx, p1, entries, opts)
		Expect(err).NotTo(HaveOccurred())
		res2, err := archive.WriteZip(ctx, p2, entries, opts)
		Expect(err).NotTo(HaveOccurred())
		Expect(res2).To(Equal(res1))
		Expect(res1.Entries).To(Equal(len(walkerOrder)))

		names, contents := zipContents(p1)
		Expect(names).To(Equal(walkerOrder))
		for name, f := range zipFiles(p1) {
			Expect(f.Modified.UTC()).To(Equal(seededSnapshot), name)
			Expect(f.Modified.UTC()).NotTo(Equal(walkerMtime), name)
			Expect(string(contents[name])).To(Equal(files[name]), name)
		}
		b1, err := os.ReadFile(p1)
		Expect(err).NotTo(HaveOccurred())
		b2, err := os.ReadFile(p2)
		Expect(err).NotTo(HaveOccurred())
		Expect(b2).To(Equal(b1))
	})

	// A file the layout does not account for is refused by its relative
	// path, whatever else the directory holds.
	DescribeTable("refuses a file outside the layout, naming its path",
		func(name, want string) {
			p := filepath.Join(dir, filepath.FromSlash(name))
			Expect(os.MkdirAll(filepath.Dir(p), 0o755)).To(Succeed())
			Expect(os.WriteFile(p, []byte("foreign"), 0o600)).To(Succeed())
			entries, err := publish.RawSnapshotEntries(ctx, root, rawDate)
			Expect(entries).To(BeNil())
			Expect(err).To(MatchError(HavePrefix("walk snapshot directory " + dir + ": ")))
			Expect(err).To(MatchError(ContainSubstring(want)))
		},
		Entry("a database beside _index.json — a layout that keeps an ingest database inside its snapshot",
			"ingest.db", "ingest.db is not part of the snapshot layout (a kind directory, _index.json, or _progress.json)"),
		Entry("the database's WAL side file",
			"ingest.db-wal", "ingest.db-wal is not part of the snapshot layout"),
		Entry("the database's shared-memory side file",
			"ingest.db-shm", "ingest.db-shm is not part of the snapshot layout"),
		Entry("a note at the root", "daily-notes.txt", "daily-notes.txt is not part of the snapshot layout"),
		Entry("a .txt at the root", "31001.txt", "31001.txt is not part of the snapshot layout"),
		Entry("a dotfile at the root", ".DS_Store", ".DS_Store is not part of the snapshot layout"),
		Entry("a third JSON file at the root", "_catalog.json", "_catalog.json is not part of the snapshot layout"),
		Entry("a metadata file's name inside a kind directory",
			"daily/_index.json", "daily/_index.json is not a station file of the daily kind (<station_id>.txt)"),
		Entry("a non-.txt file inside a kind directory",
			"daily/notes.md", "daily/notes.md is not a station file of the daily kind (<station_id>.txt)"),
		Entry("a bare .txt inside a kind directory",
			"extremes/.txt", "extremes/.txt is not a station file of the extremes kind (<station_id>.txt)"),
		Entry("a dotfile inside a kind directory",
			"daily/.keep", "daily/.keep is not a station file of the daily kind (<station_id>.txt)"),
		Entry("a hidden .txt inside a kind directory",
			"normals_1991_2020/.31001.txt", "normals_1991_2020/.31001.txt is not a station file of the normals_1991_2020 kind"),
		Entry("a file in a nested directory — refused at the directory, before its file is reached",
			"daily/archive/2020/old.txt", "daily/archive is a directory inside a kind directory (the layout holds only <station_id>.txt files there)"),
		Entry("a file under a directory that is not a kind",
			"hourly/31001.txt", "hourly is not a kind directory of the snapshot layout (daily, monthly, extremes, "+
				"normals_1961_1990, normals_1971_2000, normals_1981_2010, normals_1991_2020)"),
		Entry("a file under a kind's name in the wrong case",
			"Daily/31001.txt", "Daily is not a kind directory of the snapshot layout"),
	)

	It("refuses an empty directory outside the layout too: a directory of notes at the root, a nested directory inside a kind", func() {
		Expect(os.MkdirAll(filepath.Join(dir, "archive"), 0o755)).To(Succeed())
		_, err := publish.RawSnapshotEntries(ctx, root, rawDate)
		Expect(err).To(MatchError(ContainSubstring("archive is not a kind directory of the snapshot layout")))
		Expect(os.Remove(filepath.Join(dir, "archive"))).To(Succeed())

		Expect(os.MkdirAll(filepath.Join(dir, "monthly", "2020"), 0o755)).To(Succeed())
		_, err = publish.RawSnapshotEntries(ctx, root, rawDate)
		Expect(err).To(MatchError(ContainSubstring("monthly/2020 is a directory inside a kind directory")))
	})

	It("refuses temp residue ahead of the layout, however deep, and a symlink to a nested directory, naming the path relative to the snapshot", func() {
		residue := filepath.Join(dir, "daily", "31002.txt.tmp-9")
		Expect(os.WriteFile(residue, []byte("partial"), 0o600)).To(Succeed())
		_, err := publish.RawSnapshotEntries(ctx, root, rawDate)
		Expect(err).To(MatchError(ContainSubstring("daily/31002.txt.tmp-9 is a writer's temp residue")))
		Expect(os.Remove(residue)).To(Succeed())

		residue = filepath.Join(dir, "_index.json.tmp-9")
		Expect(os.WriteFile(residue, []byte("partial"), 0o600)).To(Succeed())
		_, err = publish.RawSnapshotEntries(ctx, root, rawDate)
		Expect(err).To(MatchError(ContainSubstring("_index.json.tmp-9 is a writer's temp residue")))
		Expect(os.Remove(residue)).To(Succeed())

		Expect(os.Symlink(filepath.Join(dir, "daily"), filepath.Join(dir, "daily", "archive-link"))).To(Succeed())
		_, err = publish.RawSnapshotEntries(ctx, root, rawDate)
		Expect(err).To(MatchError(ContainSubstring("daily/archive-link is a symlink, not a regular file")))
	})

	It("stops at a cancelled context before listing a directory, and lists nothing", func() {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		entries, err := publish.RawSnapshotEntries(cctx, root, rawDate)
		Expect(entries).To(BeNil())
		Expect(err).To(MatchError(context.Canceled))
		Expect(err).To(MatchError(HavePrefix("walk snapshot directory " + dir + ": ")))
	})
})
