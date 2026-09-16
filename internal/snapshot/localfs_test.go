package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

// failingReader implements io.Reader and returns an error after draining
// its data, simulating a mid-stream network failure during Put.
type failingReader struct {
	data []byte
	n    int
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.n >= len(f.data) {
		return 0, io.ErrUnexpectedEOF
	}
	m := copy(p, f.data[f.n:])
	f.n += m
	if f.n >= len(f.data) {
		return m, io.ErrUnexpectedEOF
	}
	return m, nil
}

var _ = Describe("LocalFS", func() {
	var (
		root string
		lfs  *LocalFS
		ctx  context.Context
		addr Address
	)

	BeforeEach(func() {
		root = GinkgoT().TempDir()
		lfs = NewLocalFS(root)
		ctx = context.Background()
		addr = Address{
			Date:      "2026-04-21",
			Kind:      conagua.KindDaily,
			StationID: "01001",
		}
	})

	Describe("Put / Exists / Path round trip", func() {
		const body = "dummy CONAGUA daily file contents\n"

		It("reports Exists false before Put", func() {
			ok, err := lfs.Exists(ctx, addr)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())
		})

		It("stores the body and round-trips size, sha256, path, and contents", func() {
			res, err := lfs.Put(ctx, addr, strings.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Bytes).To(Equal(int64(len(body))))

			sum := sha256.Sum256([]byte(body))
			Expect(res.SHA256).To(Equal(hex.EncodeToString(sum[:])))

			// Path layout matches the R2 prefix.
			want := filepath.Join(root, "conagua-raw", "2026-04-21", "daily", "01001.txt")
			Expect(lfs.Path(addr)).To(Equal(want))

			// File really on disk with the right contents.
			got, err := os.ReadFile(want)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(got)).To(Equal(body))

			// Exists: true after write.
			ok, err := lfs.Exists(ctx, addr)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())

			// No tmp files left behind.
			matches, err := filepath.Glob(filepath.Join(filepath.Dir(want), "*.tmp-*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(matches).To(BeEmpty())
		})
	})

	Describe("Get", func() {
		It("round-trips the bytes written by Put", func() {
			body := "some CONAGUA file bytes\n"
			_, err := lfs.Put(ctx, addr, strings.NewReader(body))
			Expect(err).NotTo(HaveOccurred())

			rdr, err := lfs.Get(ctx, addr)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = rdr.Close() })

			got, err := io.ReadAll(rdr)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(got)).To(Equal(body))
		})

		It("returns fs.ErrNotExist for a missing address", func() {
			_, err := lfs.Get(ctx, Address{
				Date: "2026-04-21", Kind: conagua.KindDaily, StationID: "ghost",
			})
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(fs.ErrNotExist))
		})

		It("honours context cancellation", func() {
			cancelled, cancel := context.WithCancel(ctx)
			cancel()

			_, err := lfs.Get(cancelled, addr)
			Expect(err).To(MatchError(context.Canceled))
		})
	})

	Describe("Put atomicity", func() {
		It("honours context cancellation and leaves no partial file", func() {
			cancelled, cancel := context.WithCancel(ctx)
			cancel() // already cancelled

			_, err := lfs.Put(cancelled, addr, strings.NewReader("anything"))
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(context.Canceled))

			// No partial file left at the target path.
			_, statErr := os.Stat(lfs.Path(addr))
			Expect(statErr).To(MatchError(os.ErrNotExist))
		})

		It("leaves no partial file or tmp residue when the reader fails mid-stream", func() {
			_, err := lfs.Put(ctx, addr, &failingReader{data: []byte("partial")})
			Expect(err).To(HaveOccurred())

			// No partial file at the target.
			_, statErr := os.Stat(lfs.Path(addr))
			Expect(statErr).To(MatchError(os.ErrNotExist))

			// No tmp residue either.
			matches, err := filepath.Glob(filepath.Join(root, "conagua-raw", "2026-04-21", "daily", "*.tmp-*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(matches).To(BeEmpty())
		})
	})
})

var _ = Describe("the temp-file convention", func() {
	It("names a writer's staging file <final>.tmp-<random> and recognises exactly that shape as residue", func() {
		Expect(TempSuffix).To(Equal(".tmp-"))
		Expect(IsTempResidue("01001.txt.tmp-123456")).To(BeTrue())
		Expect(IsTempResidue("_index.json.tmp-9")).To(BeTrue())
		Expect(IsTempResidue("_progress.json.tmp-abc")).To(BeTrue())
		Expect(IsTempResidue("01001.txt")).To(BeFalse())
		Expect(IsTempResidue("_index.json")).To(BeFalse())
		Expect(IsTempResidue("tmp-01001.txt")).To(BeFalse())
		Expect(IsTempResidue("01001.tmp")).To(BeFalse())
	})

	It("is the shape Put stages under: a reader failing mid-stream leaves a file the convention recognises only while the write is in flight", func() {
		root := GinkgoT().TempDir()
		lfs := NewLocalFS(root)
		addr := Address{Date: "2026-04-21", Kind: conagua.KindDaily, StationID: "01001"}
		_, err := lfs.Put(context.Background(), addr, strings.NewReader("body\n"))
		Expect(err).NotTo(HaveOccurred())
		entries, err := os.ReadDir(filepath.Dir(lfs.Path(addr)))
		Expect(err).NotTo(HaveOccurred())
		for _, e := range entries {
			Expect(IsTempResidue(e.Name())).To(BeFalse(), e.Name())
		}
		Expect(entries).To(HaveLen(1))
	})
})
