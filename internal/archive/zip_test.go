package archive_test

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// stringEntry is a fixture entry whose content is a literal.
func stringEntry(path, content string) archive.Entry {
	return archive.Entry{Path: path, Write: func(w io.Writer) error {
		_, err := io.WriteString(w, content)
		return err
	}}
}

// expectNoTempResidue asserts the atomic-write discipline left nothing
// behind: no dot-prefixed temp sibling anywhere under dir.
func expectNoTempResidue(dir string) {
	GinkgoHelper()
	var residue []string
	Expect(filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.Contains(d.Name(), ".tmp-") {
			residue = append(residue, p)
		}
		return nil
	})).To(Succeed())
	Expect(residue).To(BeEmpty())
}

func expectAbsent(path string) {
	GinkgoHelper()
	_, err := os.Stat(path)
	Expect(err).To(MatchError(os.ErrNotExist))
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

var _ = Describe("WriteZip", func() {
	var (
		ctx      context.Context
		dir      string
		modified time.Time
		opts     archive.ZipOptions
		contents map[string]string
		entries  []archive.Entry
	)

	BeforeEach(func() {
		ctx = context.Background()
		dir = GinkgoT().TempDir()
		modified = time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
		opts = archive.ZipOptions{Modified: modified}
		// Path-sorted, as publish passes them; the daily file is
		// repetitive enough that Deflate must actually shrink it, and the
		// stations file carries non-ASCII data as the real one does.
		contents = map[string]string{
			"conagua/daily_observations/yuc/daily-31001.csv": "station_id,date,tmax_c,tmin_c,precip_mm,evap_mm\n" +
				strings.Repeat("31001,1981-01-01,31.0,14.5,0.0,\n", 200),
			"conagua/monthly_normals.csv": "station_id,period,month,tmax_c\n31001,1981-2010,1,27.4\n",
			"conagua/stations.csv":        "station_id,name,state\n31001,MÉRIDA (OBS),YUC\n",
		}
		entries = []archive.Entry{
			stringEntry("conagua/daily_observations/yuc/daily-31001.csv", contents["conagua/daily_observations/yuc/daily-31001.csv"]),
			stringEntry("conagua/monthly_normals.csv", contents["conagua/monthly_normals.csv"]),
			stringEntry("conagua/stations.csv", contents["conagua/stations.csv"]),
		}
	})

	Describe("byte reproducibility", func() {
		It("writes byte-identical archives and identical Results for the same entries", func() {
			p1 := filepath.Join(dir, "one", "yuc-tabular.zip")
			p2 := filepath.Join(dir, "two", "yuc-tabular.zip")

			res1, err := archive.WriteZip(ctx, p1, entries, opts)
			Expect(err).NotTo(HaveOccurred())
			res2, err := archive.WriteZip(ctx, p2, entries, opts)
			Expect(err).NotTo(HaveOccurred())

			b1, err := os.ReadFile(p1)
			Expect(err).NotTo(HaveOccurred())
			b2, err := os.ReadFile(p2)
			Expect(err).NotTo(HaveOccurred())
			Expect(b2).To(Equal(b1))
			Expect(res2).To(Equal(res1))

			// The Result describes the bytes on disk, field by field.
			Expect(res1.Bytes).To(Equal(int64(len(b1))))
			Expect(res1.SHA256).To(Equal(sha256Hex(b1)))
			Expect(res1.Entries).To(Equal(len(entries)))
			expectNoTempResidue(dir)
		})

		It("pins the timestamp to the instant, not the zone", func() {
			p1 := filepath.Join(dir, "utc.zip")
			p2 := filepath.Join(dir, "local.zip")
			local := archive.ZipOptions{Modified: modified.In(time.FixedZone("CST", -6*3600))}

			_, err := archive.WriteZip(ctx, p1, entries, opts)
			Expect(err).NotTo(HaveOccurred())
			_, err = archive.WriteZip(ctx, p2, entries, local)
			Expect(err).NotTo(HaveOccurred())

			b1, err := os.ReadFile(p1)
			Expect(err).NotTo(HaveOccurred())
			b2, err := os.ReadFile(p2)
			Expect(err).NotTo(HaveOccurred())
			Expect(b2).To(Equal(b1))
		})

		It("changes the bytes when Modified changes", func() {
			p1 := filepath.Join(dir, "a.zip")
			p2 := filepath.Join(dir, "b.zip")
			later := archive.ZipOptions{Modified: modified.Add(24 * time.Hour)}

			res1, err := archive.WriteZip(ctx, p1, entries, opts)
			Expect(err).NotTo(HaveOccurred())
			res2, err := archive.WriteZip(ctx, p2, entries, later)
			Expect(err).NotTo(HaveOccurred())

			b1, err := os.ReadFile(p1)
			Expect(err).NotTo(HaveOccurred())
			b2, err := os.ReadFile(p2)
			Expect(err).NotTo(HaveOccurred())
			Expect(b2).NotTo(Equal(b1))
			Expect(res2.SHA256).NotTo(Equal(res1.SHA256))
			// Only the timestamps moved, so the size is unchanged.
			Expect(res2.Bytes).To(Equal(res1.Bytes))
		})
	})

	Describe("read-back through archive/zip", func() {
		It("yields the same paths, order, timestamps, and exact contents, with no comments", func() {
			path := filepath.Join(dir, "yuc-tabular.zip")
			_, err := archive.WriteZip(ctx, path, entries, opts)
			Expect(err).NotTo(HaveOccurred())

			rc, err := zip.OpenReader(path)
			Expect(err).NotTo(HaveOccurred())
			defer rc.Close() //nolint:errcheck // read-side close; no recovery possible

			Expect(rc.Comment).To(BeEmpty())
			Expect(rc.File).To(HaveLen(len(entries)))
			for i, f := range rc.File {
				want := contents[entries[i].Path]
				Expect(f.Name).To(Equal(entries[i].Path), "entry %d out of order", i)
				Expect(f.Method).To(Equal(zip.Deflate))
				Expect(f.Modified.Unix()).To(Equal(modified.Unix()))
				Expect(f.Comment).To(BeEmpty())
				Expect(f.UncompressedSize64).To(Equal(uint64(len(want))))

				r, err := f.Open()
				Expect(err).NotTo(HaveOccurred())
				got, err := io.ReadAll(r)
				Expect(r.Close()).To(Succeed())
				Expect(err).NotTo(HaveOccurred())
				Expect(string(got)).To(Equal(want))
			}
			// Deflate is in effect, not merely declared.
			daily := rc.File[0]
			Expect(daily.CompressedSize64).To(BeNumerically("<", daily.UncompressedSize64))
		})

		It("writes a valid empty archive for zero entries", func() {
			path := filepath.Join(dir, "empty.zip")
			res, err := archive.WriteZip(ctx, path, nil, opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Entries).To(BeZero())

			rc, err := zip.OpenReader(path)
			Expect(err).NotTo(HaveOccurred())
			defer rc.Close() //nolint:errcheck // read-side close; no recovery possible
			Expect(rc.File).To(BeEmpty())
		})
	})

	Describe("atomicity", func() {
		It("creates the parent directory", func() {
			path := filepath.Join(dir, "nested", "deeper", "x.zip")
			_, err := archive.WriteZip(ctx, path, entries, opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(path).To(BeAnExistingFile())
		})

		It("replaces an existing archive in place, leaving no residue", func() {
			path := filepath.Join(dir, "x.zip")
			_, err := archive.WriteZip(ctx, path, entries[:1], opts)
			Expect(err).NotTo(HaveOccurred())
			_, err = archive.WriteZip(ctx, path, entries, opts)
			Expect(err).NotTo(HaveOccurred())

			rc, err := zip.OpenReader(path)
			Expect(err).NotTo(HaveOccurred())
			defer rc.Close() //nolint:errcheck // read-side close; no recovery possible
			Expect(rc.File).To(HaveLen(len(entries)))
			expectNoTempResidue(dir)
		})

		It("leaves no file at path and no temp sibling when an entry's Write fails", func() {
			path := filepath.Join(dir, "x.zip")
			errBoom := errors.New("boom")
			failing := []archive.Entry{
				entries[0],
				{Path: "conagua/monthly_normals.csv", Write: func(io.Writer) error { return errBoom }},
				entries[2],
			}

			_, err := archive.WriteZip(ctx, path, failing, opts)
			Expect(err).To(MatchError(errBoom))
			Expect(err.Error()).To(ContainSubstring(`entry "conagua/monthly_normals.csv"`))
			expectAbsent(path)
			expectNoTempResidue(dir)
		})

		It("preserves the previous archive when a rewrite fails", func() {
			path := filepath.Join(dir, "x.zip")
			res, err := archive.WriteZip(ctx, path, entries, opts)
			Expect(err).NotTo(HaveOccurred())

			failing := []archive.Entry{{Path: "a.csv", Write: func(io.Writer) error { return errors.New("boom") }}}
			_, err = archive.WriteZip(ctx, path, failing, opts)
			Expect(err).To(HaveOccurred())

			sum, n, err := archive.SHA256File(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(sum).To(Equal(res.SHA256))
			Expect(n).To(Equal(res.Bytes))
			expectNoTempResidue(dir)
		})

		It("stops at the next entry and leaves no file when ctx is cancelled mid-way", func() {
			path := filepath.Join(dir, "x.zip")
			cctx, cancel := context.WithCancel(ctx)
			defer cancel()
			reachedThird := false
			cancelling := []archive.Entry{
				entries[0],
				{Path: "conagua/monthly_normals.csv", Write: func(io.Writer) error { cancel(); return nil }},
				{Path: "conagua/stations.csv", Write: func(io.Writer) error { reachedThird = true; return nil }},
			}

			_, err := archive.WriteZip(cctx, path, cancelling, opts)
			Expect(err).To(MatchError(context.Canceled))
			Expect(err.Error()).To(ContainSubstring(`entry "conagua/stations.csv"`))
			Expect(reachedThird).To(BeFalse())
			expectAbsent(path)
			expectNoTempResidue(dir)
		})

		It("refuses an already-cancelled ctx before touching the filesystem", func() {
			path := filepath.Join(dir, "sub", "x.zip")
			cctx, cancel := context.WithCancel(ctx)
			cancel()

			_, err := archive.WriteZip(cctx, path, entries, opts)
			Expect(err).To(MatchError(context.Canceled))
			expectAbsent(filepath.Dir(path))
		})
	})

	Describe("input validation", func() {
		DescribeTable("rejects an invalid entry set before writing anything",
			func(bad []archive.Entry, reason string) {
				path := filepath.Join(dir, "sub", "x.zip")
				_, err := archive.WriteZip(ctx, path, bad, opts)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(reason))
				expectAbsent(filepath.Dir(path))
			},
			Entry("duplicate path", []archive.Entry{stringEntry("a.csv", "1"), stringEntry("a.csv", "2")}, "duplicate path"),
			Entry("leading slash", []archive.Entry{stringEntry("/a.csv", "1")}, "leading slash"),
			Entry("parent segment", []archive.Entry{stringEntry("conagua/../a.csv", "1")}, "parent segment"),
			Entry("bare parent", []archive.Entry{stringEntry("..", "1")}, "parent segment"),
			Entry("trailing slash", []archive.Entry{stringEntry("conagua/", "1")}, "trailing slash"),
			Entry("backslash", []archive.Entry{stringEntry(`conagua\a.csv`, "1")}, "backslash"),
			Entry("empty path", []archive.Entry{stringEntry("", "1")}, "empty path"),
			Entry("nil Write", []archive.Entry{{Path: "a.csv"}}, "nil Write"),
		)

		It("requires a pinned Modified time", func() {
			path := filepath.Join(dir, "x.zip")
			_, err := archive.WriteZip(ctx, path, entries, archive.ZipOptions{})
			Expect(err).To(MatchError(ContainSubstring("Modified is required")))
			expectAbsent(path)
		})
	})
})
