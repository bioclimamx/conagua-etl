package snapshot

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

// fakePrimary is a minimal Sink for testing CachingSink. It tracks how
// many times each method is called so specs can assert "second Get did
// not hit the network".
type fakePrimary struct {
	objects map[string][]byte // keyed by LocalFS-style path

	getCalls    atomic.Int64
	existsCalls atomic.Int64
	putCalls    atomic.Int64
	getErr      error // if non-nil, Get returns this
}

func newFakePrimary() *fakePrimary {
	return &fakePrimary{objects: map[string][]byte{}}
}

func (f *fakePrimary) key(addr Address) string {
	return addr.Date + "/" + string(addr.Kind) + "/" + addr.StationID
}

func (f *fakePrimary) Exists(_ context.Context, addr Address) (bool, error) {
	f.existsCalls.Add(1)
	_, ok := f.objects[f.key(addr)]
	return ok, nil
}

func (f *fakePrimary) Get(_ context.Context, addr Address) (io.ReadCloser, error) {
	f.getCalls.Add(1)
	if f.getErr != nil {
		return nil, f.getErr
	}
	body, ok := f.objects[f.key(addr)]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func (f *fakePrimary) Put(_ context.Context, addr Address, r io.Reader) (PutResult, error) {
	f.putCalls.Add(1)
	body, err := io.ReadAll(r)
	if err != nil {
		return PutResult{}, err
	}
	f.objects[f.key(addr)] = body
	return PutResult{Bytes: int64(len(body))}, nil
}

// indexedPrimary is a fakePrimary that also implements FetchIndex, so
// specs can prove CachingSink forwards it when available.
type indexedPrimary struct {
	fakePrimary
	index []byte
	dates []string
}

func (f *indexedPrimary) FetchIndex(_ context.Context, date string) ([]byte, error) {
	f.dates = append(f.dates, date)
	return f.index, nil
}

var _ = Describe("CachingSink", func() {
	var (
		primary *fakePrimary
		cache   *LocalFS
		c       *CachingSink
		ctx     context.Context
		addr    Address
	)

	BeforeEach(func() {
		primary = newFakePrimary()
		cache = NewLocalFS(GinkgoT().TempDir())
		c = NewCachingSink(primary, cache, quietLogger())
		ctx = context.Background()
		addr = Address{Date: "2026-04-21", Kind: conagua.KindDaily, StationID: "01001"}
	})

	Describe("Get", func() {
		It("serves the second Get from the cache without touching the primary", func() {
			primary.objects[primary.key(addr)] = []byte("hello world\n")

			// 1st Get: miss, goes to primary, populates cache.
			rc, err := c.Get(ctx, addr)
			Expect(err).NotTo(HaveOccurred())
			body1, err := io.ReadAll(rc)
			Expect(err).NotTo(HaveOccurred())
			Expect(rc.Close()).To(Succeed())
			Expect(string(body1)).To(Equal("hello world\n"))
			Expect(primary.getCalls.Load()).To(Equal(int64(1)))

			// The miss wrote through: the on-disk cached copy equals the body.
			cached, err := os.ReadFile(cache.Path(addr))
			Expect(err).NotTo(HaveOccurred())
			Expect(string(cached)).To(Equal("hello world\n"))

			// 2nd Get for same addr: must serve from cache, must NOT touch primary.
			rc, err = c.Get(ctx, addr)
			Expect(err).NotTo(HaveOccurred())
			body2, err := io.ReadAll(rc)
			Expect(err).NotTo(HaveOccurred())
			Expect(rc.Close()).To(Succeed())
			Expect(string(body2)).To(Equal("hello world\n"))
			Expect(primary.getCalls.Load()).To(Equal(int64(1)), "cache hit must not touch primary")

			hits, misses := c.Stats()
			Expect(hits).To(Equal(int64(1)))
			Expect(misses).To(Equal(int64(1)))
		})

		It("propagates fs.ErrNotExist from the primary without counting a miss", func() {
			_, err := c.Get(ctx, Address{
				Date: "2026-04-21", Kind: conagua.KindDaily, StationID: "ghost",
			})
			Expect(err).To(MatchError(fs.ErrNotExist),
				"ingest needs a matchable fs.ErrNotExist to detect missing files")

			hits, misses := c.Stats()
			Expect(hits).To(Equal(int64(0)))
			Expect(misses).To(Equal(int64(0)), "a primary ErrNotExist is not a cache miss")
		})

		It("returns primary bytes unchanged when the cache cannot be written", func() {
			primary.objects[primary.key(addr)] = []byte("primary bytes\n")

			// Make the cache root a regular file — any MkdirAll under it fails.
			cacheRoot := GinkgoT().TempDir() + "/not-a-dir"
			f, err := os.Create(cacheRoot)
			Expect(err).NotTo(HaveOccurred())
			Expect(f.Close()).To(Succeed())
			c = NewCachingSink(primary, NewLocalFS(cacheRoot), quietLogger())

			rc, err := c.Get(ctx, addr)
			Expect(err).NotTo(HaveOccurred(), "Get must not fail on cache-write failure")
			body, err := io.ReadAll(rc)
			Expect(err).NotTo(HaveOccurred())
			Expect(rc.Close()).To(Succeed())
			Expect(string(body)).To(Equal("primary bytes\n"))

			// Second Get: cache is still broken, so primary is hit again. The
			// caller sees no error either way.
			rc, err = c.Get(ctx, addr)
			Expect(err).NotTo(HaveOccurred())
			body2, err := io.ReadAll(rc)
			Expect(err).NotTo(HaveOccurred())
			Expect(rc.Close()).To(Succeed())
			Expect(string(body2)).To(Equal("primary bytes\n"))
			Expect(primary.getCalls.Load()).To(Equal(int64(2)), "cache never caught up")
		})
	})

	Describe("Exists", func() {
		It("forwards to the primary", func() {
			present := Address{Date: "d", Kind: conagua.KindDaily, StationID: "x"}
			primary.objects[primary.key(present)] = []byte("ok")

			ok, err := c.Exists(ctx, present)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())

			ok, err = c.Exists(ctx, Address{Date: "d", Kind: conagua.KindDaily, StationID: "missing"})
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())

			Expect(primary.existsCalls.Load()).To(Equal(int64(2)))
		})
	})

	Describe("Put", func() {
		It("forwards to the primary and the body round-trips", func() {
			target := Address{Date: "d", Kind: conagua.KindDaily, StationID: "x"}
			_, err := c.Put(ctx, target, strings.NewReader("body"))
			Expect(err).NotTo(HaveOccurred())
			Expect(primary.putCalls.Load()).To(Equal(int64(1)))

			// Read the bytes back through the primary to prove the payload
			// arrived intact, not merely that a call happened.
			rc, err := primary.Get(ctx, target)
			Expect(err).NotTo(HaveOccurred())
			got, err := io.ReadAll(rc)
			Expect(err).NotTo(HaveOccurred())
			Expect(rc.Close()).To(Succeed())
			Expect(string(got)).To(Equal("body"))
		})
	})

	Describe("FetchIndex", func() {
		It("errors when the primary does not implement it", func() {
			_, err := c.FetchIndex(ctx, "2026-04-21")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("does not implement FetchIndex"))
		})

		It("forwards to a primary that implements it", func() {
			idx := &indexedPrimary{index: []byte(`{"snapshot_date":"2026-04-21"}`)}
			c = NewCachingSink(idx, cache, quietLogger())

			got, err := c.FetchIndex(ctx, "2026-04-21")
			Expect(err).NotTo(HaveOccurred())
			Expect(string(got)).To(Equal(`{"snapshot_date":"2026-04-21"}`))
			Expect(idx.dates).To(Equal([]string{"2026-04-21"}))
		})
	})
})
