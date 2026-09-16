package archive_test

// Specs for WriteZipMixed, the raw-copy form beside the generated one:
// a copied entry's record — CRC-32, sizes, method, flags, timestamps,
// extra field, and the compressed bytes themselves — is the source's,
// its timestamp the source's rather than the archive's; a mix of the
// two forms reads back in the order given with every content intact
// and is accepted whole by Info-ZIP; two writes are byte-identical; the
// source archives are only ever read; the Written hook fires once per
// entry in write order for both forms; cancellation and the form rules
// are refused with nothing left at the path.

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
)

// rawBytes reads f's record as stored: the compressed bytes, without
// decompression.
func rawBytes(f *zip.File) []byte {
	GinkgoHelper()
	r, err := f.OpenRaw()
	Expect(err).NotTo(HaveOccurred())
	data, err := io.ReadAll(r)
	Expect(err).NotTo(HaveOccurred())
	return data
}

// contentOf decompresses f.
func contentOf(f *zip.File) string {
	GinkgoHelper()
	r, err := f.Open()
	Expect(err).NotTo(HaveOccurred())
	data, err := io.ReadAll(r)
	Expect(r.Close()).To(Succeed())
	Expect(err).NotTo(HaveOccurred())
	return string(data)
}

// openZip opens path and schedules its close.
func openZip(path string) *zip.ReadCloser {
	GinkgoHelper()
	rc, err := zip.OpenReader(path)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = rc.Close() })
	return rc
}

// fileByName finds the entry named name in rc.
func fileByName(rc *zip.ReadCloser, name string) *zip.File {
	GinkgoHelper()
	for _, f := range rc.File {
		if f.Name == name {
			return f
		}
	}
	Fail("no entry " + name)
	return nil
}

var _ = Describe("WriteZipMixed", func() {
	var (
		ctx      context.Context
		dir      string
		snapshot time.Time
		opts     archive.ZipOptions
		contents map[string]string
		source   *zip.ReadCloser
	)

	// The source archive: a state archive's shape — two per-station
	// files under a shard, one per-cell file, one whole-state table —
	// written by WriteZip under the snapshot timestamp.
	sourceNames := []string{
		"conagua/daily_observations/yuc/daily-31001.csv",
		"conagua/daily_observations/yuc/daily-3101.csv",
		"conagua/stations.csv",
		"nasa_power/daily/daily-21.5N_89.3750W.csv",
	}

	BeforeEach(func() {
		ctx = context.Background()
		dir = GinkgoT().TempDir()
		snapshot = time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
		opts = archive.ZipOptions{Modified: snapshot}
		contents = map[string]string{
			sourceNames[0]: "station_id,date,tmax_c\n" + strings.Repeat("31001,1981-01-01,31.0\n", 300),
			sourceNames[1]: "station_id,date,tmax_c\n3101,1975-06-15,36.2\n",
			sourceNames[2]: "station_id,name,state\n31001,MÉRIDA (OBS),YUC\n",
			sourceNames[3]: "cell_id,date,t2m_c\n" + strings.Repeat("21.5N_89.3750W,2020-01-01,24.48\n", 200),
		}
		var entries []archive.Entry
		for _, n := range sourceNames {
			entries = append(entries, stringEntry(n, contents[n]))
		}
		_, err := archive.WriteZip(ctx, filepath.Join(dir, "yuc-tabular.zip"), entries, opts)
		Expect(err).NotTo(HaveOccurred())
		source = openZip(filepath.Join(dir, "yuc-tabular.zip"))
	})

	// mixed assembles the national-style set: the per-unit files copied
	// raw and the table regenerated, Path-sorted.
	mixed := func(table string) []archive.MixedEntry {
		return []archive.MixedEntry{
			archive.Copied(fileByName(source, sourceNames[0])),
			archive.Copied(fileByName(source, sourceNames[1])),
			archive.Generated(stringEntry("conagua/stations.csv", table)),
			archive.Copied(fileByName(source, sourceNames[3])),
		}
	}
	national := "station_id,name,state\n1001,Aguascalientes (OBS),AGS\n31001,MÉRIDA (OBS),YUC\n"

	Describe("the raw copy", func() {
		It("transfers a copied entry's record — CRC-32, sizes, method, flags, timestamps, extra field, compressed bytes — as the source's, and decompresses to the same content", func() {
			path := filepath.Join(dir, "national-csv.zip")
			res, err := archive.WriteZipMixed(ctx, path, mixed(national), opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Entries).To(Equal(4))

			got := openZip(path)
			Expect(got.File).To(HaveLen(4))
			for _, name := range []string{sourceNames[0], sourceNames[1], sourceNames[3]} {
				src, cp := fileByName(source, name), fileByName(got, name)
				Expect(cp.Method).To(Equal(src.Method), name)
				Expect(cp.Flags).To(Equal(src.Flags), name)
				Expect(cp.CRC32).To(Equal(src.CRC32), name)
				Expect(cp.CompressedSize64).To(Equal(src.CompressedSize64), name)
				Expect(cp.UncompressedSize64).To(Equal(src.UncompressedSize64), name)
				Expect(cp.ModifiedTime).To(Equal(src.ModifiedTime), name)
				Expect(cp.ModifiedDate).To(Equal(src.ModifiedDate), name)
				Expect(cp.Modified.Equal(src.Modified)).To(BeTrue(), name)
				Expect(cp.Extra).To(Equal(src.Extra), name)
				Expect(cp.Comment).To(BeEmpty(), name)
				Expect(rawBytes(cp)).To(Equal(rawBytes(src)), name)
				Expect(contentOf(cp)).To(Equal(contents[name]), name)
			}
			// Deflate is in effect on the copies, as it was at the source.
			Expect(fileByName(got, sourceNames[0]).CompressedSize64).To(BeNumerically("<", uint64(len(contents[sourceNames[0]]))))
		})

		It("keeps a copied entry's timestamp as the source's, and stamps the generated one with the archive's", func() {
			later := archive.ZipOptions{Modified: snapshot.Add(48 * time.Hour)}
			path := filepath.Join(dir, "national-csv.zip")
			_, err := archive.WriteZipMixed(ctx, path, mixed(national), later)
			Expect(err).NotTo(HaveOccurred())

			got := openZip(path)
			Expect(fileByName(got, sourceNames[0]).Modified.Unix()).To(Equal(snapshot.Unix()))
			Expect(fileByName(got, "conagua/stations.csv").Modified.Unix()).To(Equal(later.Modified.Unix()))
		})

		It("never modifies a source archive", func() {
			before, n, err := archive.SHA256File(filepath.Join(dir, "yuc-tabular.zip"))
			Expect(err).NotTo(HaveOccurred())
			_, err = archive.WriteZipMixed(ctx, filepath.Join(dir, "national-csv.zip"), mixed(national), opts)
			Expect(err).NotTo(HaveOccurred())
			after, m, err := archive.SHA256File(filepath.Join(dir, "yuc-tabular.zip"))
			Expect(err).NotTo(HaveOccurred())
			Expect(after).To(Equal(before))
			Expect(m).To(Equal(n))
		})
	})

	Describe("the mixed set", func() {
		It("reads back in the order given, every entry's content intact, with no comments", func() {
			path := filepath.Join(dir, "national-csv.zip")
			_, err := archive.WriteZipMixed(ctx, path, mixed(national), opts)
			Expect(err).NotTo(HaveOccurred())

			got := openZip(path)
			Expect(got.Comment).To(BeEmpty())
			var names []string
			for _, f := range got.File {
				names = append(names, f.Name)
			}
			Expect(names).To(Equal([]string{sourceNames[0], sourceNames[1], "conagua/stations.csv", sourceNames[3]}))
			Expect(contentOf(fileByName(got, "conagua/stations.csv"))).To(Equal(national))
			Expect(fileByName(got, "conagua/stations.csv").Method).To(Equal(zip.Deflate))
		})

		It("writes byte-identical archives and identical Results on two writes", func() {
			p1 := filepath.Join(dir, "one", "national-csv.zip")
			p2 := filepath.Join(dir, "two", "national-csv.zip")
			res1, err := archive.WriteZipMixed(ctx, p1, mixed(national), opts)
			Expect(err).NotTo(HaveOccurred())
			res2, err := archive.WriteZipMixed(ctx, p2, mixed(national), opts)
			Expect(err).NotTo(HaveOccurred())

			b1, err := os.ReadFile(p1)
			Expect(err).NotTo(HaveOccurred())
			b2, err := os.ReadFile(p2)
			Expect(err).NotTo(HaveOccurred())
			Expect(b2).To(Equal(b1))
			Expect(res2).To(Equal(res1))
			Expect(res1.Bytes).To(Equal(int64(len(b1))))
			Expect(res1.SHA256).To(Equal(sha256Hex(b1)))
			expectNoTempResidue(dir)
		})

		It("is accepted whole by Info-ZIP unzip, every entry extracting as written", func() {
			if _, err := exec.LookPath("unzip"); err != nil {
				Skip("unzip not on PATH")
			}
			path := filepath.Join(dir, "national-csv.zip")
			_, err := archive.WriteZipMixed(ctx, path, mixed(national), opts)
			Expect(err).NotTo(HaveOccurred())

			out, err := exec.Command("unzip", "-t", path).CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), string(out))
			Expect(string(out)).To(ContainSubstring("No errors detected"))

			extract := filepath.Join(dir, "x")
			out, err = exec.Command("unzip", "-q", path, "-d", extract).CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), string(out))
			want := map[string]string{
				sourceNames[0]: contents[sourceNames[0]], sourceNames[1]: contents[sourceNames[1]],
				"conagua/stations.csv": national, sourceNames[3]: contents[sourceNames[3]],
			}
			for name, content := range want {
				got, err := os.ReadFile(filepath.Join(extract, filepath.FromSlash(name)))
				Expect(err).NotTo(HaveOccurred(), name)
				Expect(string(got)).To(Equal(content), name)
			}
		})

		It("calls Written once per entry, after its content is written, in write order, for both forms", func() {
			var fired []string
			entries := mixed(national)
			for i := range entries {
				name := entries[i].Path
				entries[i].Written = func() { fired = append(fired, name) }
			}
			_, err := archive.WriteZipMixed(ctx, filepath.Join(dir, "national-csv.zip"), entries, opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(fired).To(Equal([]string{sourceNames[0], sourceNames[1], "conagua/stations.csv", sourceNames[3]}))
		})

		It("writes a valid empty archive for zero entries", func() {
			path := filepath.Join(dir, "empty.zip")
			res, err := archive.WriteZipMixed(ctx, path, nil, opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Entries).To(BeZero())
			Expect(openZip(path).File).To(BeEmpty())
		})
	})

	Describe("atomicity", func() {
		It("leaves no file and no residue when a generated entry's Write fails after a copy", func() {
			path := filepath.Join(dir, "national-csv.zip")
			errBoom := errors.New("boom")
			entries := mixed(national)
			entries[2].Write = func(io.Writer) error { return errBoom }
			_, err := archive.WriteZipMixed(ctx, path, entries, opts)
			Expect(err).To(MatchError(errBoom))
			Expect(err.Error()).To(ContainSubstring(`entry "conagua/stations.csv"`))
			expectAbsent(path)
			expectNoTempResidue(dir)
		})

		It("stops before the next copy and leaves no file when ctx is cancelled by a Written hook", func() {
			path := filepath.Join(dir, "national-csv.zip")
			cctx, cancel := context.WithCancel(ctx)
			defer cancel()
			entries := mixed(national)
			entries[1].Written = cancel
			reachedTable := false
			entries[2].Written = func() { reachedTable = true }
			_, err := archive.WriteZipMixed(cctx, path, entries, opts)
			Expect(err).To(MatchError(context.Canceled))
			Expect(err.Error()).To(ContainSubstring(`entry "conagua/stations.csv"`))
			Expect(reachedTable).To(BeFalse())
			expectAbsent(path)
			expectNoTempResidue(dir)
		})

		It("refuses an already-cancelled ctx before touching the filesystem", func() {
			path := filepath.Join(dir, "sub", "x.zip")
			cctx, cancel := context.WithCancel(ctx)
			cancel()
			_, err := archive.WriteZipMixed(cctx, path, mixed(national), opts)
			Expect(err).To(MatchError(context.Canceled))
			expectAbsent(filepath.Dir(path))
		})
	})

	Describe("input validation", func() {
		// absoluteSource is a hand-built archive whose one entry carries
		// a name archive/zip writes but a release must never copy.
		absoluteSource := func() *zip.ReadCloser {
			var buf bytes.Buffer
			zw := zip.NewWriter(&buf)
			w, err := zw.Create("/abs.csv")
			Expect(err).NotTo(HaveOccurred())
			_, err = io.WriteString(w, "x")
			Expect(err).NotTo(HaveOccurred())
			Expect(zw.Close()).To(Succeed())
			p := filepath.Join(dir, "abs.zip")
			Expect(os.WriteFile(p, buf.Bytes(), 0o600)).To(Succeed())
			return openZip(p)
		}

		It("rejects an invalid mixed set before writing anything", func() {
			cases := []struct {
				name    string
				entries func() []archive.MixedEntry
				reason  string
			}{
				{"a copy and a generated entry sharing a path", func() []archive.MixedEntry {
					return []archive.MixedEntry{
						archive.Copied(fileByName(source, sourceNames[2])),
						archive.Generated(stringEntry(sourceNames[2], national)),
					}
				}, "duplicate path"},
				{"a copy renamed away from its source", func() []archive.MixedEntry {
					return []archive.MixedEntry{{Path: "renamed.csv", From: fileByName(source, sourceNames[2])}}
				}, "a raw copy keeps the source name"},
				{"both forms on one entry", func() []archive.MixedEntry {
					e := archive.Copied(fileByName(source, sourceNames[2]))
					e.Write = func(io.Writer) error { return nil }
					return []archive.MixedEntry{e}
				}, "both Write and From"},
				{"neither form", func() []archive.MixedEntry {
					return []archive.MixedEntry{{Path: "a.csv"}}
				}, "neither Write nor From"},
				{"a copy whose source name has a leading slash", func() []archive.MixedEntry {
					return []archive.MixedEntry{archive.Copied(absoluteSource().File[0])}
				}, "leading slash"},
			}
			for _, c := range cases {
				path := filepath.Join(dir, "sub", c.name+".zip")
				_, err := archive.WriteZipMixed(ctx, path, c.entries(), opts)
				Expect(err).To(HaveOccurred(), c.name)
				Expect(err.Error()).To(ContainSubstring(c.reason), c.name)
				expectAbsent(filepath.Dir(path))
			}
		})

		It("requires a pinned Modified time even for copies alone", func() {
			path := filepath.Join(dir, "x.zip")
			_, err := archive.WriteZipMixed(ctx, path,
				[]archive.MixedEntry{archive.Copied(fileByName(source, sourceNames[2]))}, archive.ZipOptions{})
			Expect(err).To(MatchError(ContainSubstring("Modified is required")))
			expectAbsent(path)
		})
	})
})
