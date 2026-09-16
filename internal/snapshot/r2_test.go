package snapshot

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/conagua"
)

// testR2Config is a syntactically-valid config for constructing an R2Sink
// without any network interaction (minio.New only parses the endpoint).
func testR2Config() R2Config {
	return R2Config{
		AccountID:       "test-account",
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: "secret",
		Bucket:          "test-bucket",
	}
}

var _ = Describe("R2Config", func() {
	Describe("LoadR2Config", func() {
		setEnv := func(account, key, secret, bucket string) {
			GinkgoT().Setenv("R2_ACCOUNT_ID", account)
			GinkgoT().Setenv("R2_ACCESS_KEY_ID", key)
			GinkgoT().Setenv("R2_SECRET_ACCESS_KEY", secret)
			GinkgoT().Setenv("R2_SNAPSHOTS_BUCKET", bucket)
		}

		It("round-trips every env var into the config", func() {
			setEnv("acct-1", "key-1", "secret-1", "custom-bucket")

			c, err := LoadR2Config()
			Expect(err).NotTo(HaveOccurred())
			Expect(c.AccountID).To(Equal("acct-1"))
			Expect(c.AccessKeyID).To(Equal("key-1"))
			Expect(c.SecretAccessKey).To(Equal("secret-1"))
			Expect(c.Bucket).To(Equal("custom-bucket"))
		})

		It("defaults the bucket when R2_SNAPSHOTS_BUCKET is unset", func() {
			setEnv("acct-1", "key-1", "secret-1", "")

			c, err := LoadR2Config()
			Expect(err).NotTo(HaveOccurred())
			Expect(c.Bucket).To(Equal("bioclima-snapshots"))
			Expect(c.Bucket).To(Equal(DefaultSnapshotsBucket))
		})

		DescribeTable("errors naming each missing credential var",
			func(account, key, secret string, wantMissing string) {
				setEnv(account, key, secret, "")

				_, err := LoadR2Config()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("R2 env vars missing: " + wantMissing))
			},
			Entry("account id", "", "k", "s", "R2_ACCOUNT_ID"),
			Entry("access key", "a", "", "s", "R2_ACCESS_KEY_ID"),
			Entry("secret key", "a", "k", "", "R2_SECRET_ACCESS_KEY"),
			Entry("all three", "", "", "",
				"R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY"),
		)

		It("does not fail on a missing bucket alone", func() {
			setEnv("a", "k", "s", "")
			_, err := LoadR2Config()
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Endpoint", func() {
		It("builds the account-scoped R2 endpoint", func() {
			c := R2Config{AccountID: "abc123"}
			Expect(c.Endpoint()).To(Equal("abc123.r2.cloudflarestorage.com"))
		})
	})
})

var _ = Describe("R2Sink", func() {
	var r2 *R2Sink

	BeforeEach(func() {
		var err error
		r2, err = NewR2Sink(testR2Config())
		Expect(err).NotTo(HaveOccurred())
	})

	It("exposes the configured bucket", func() {
		Expect(r2.Bucket()).To(Equal("test-bucket"))
	})

	Describe("Exists", func() {
		It("wraps transport failures with the stat operation and object key", func() {
			// A pre-cancelled context fails the HEAD before any network I/O,
			// pinning the wrapped error — and the key it names — without
			// touching live R2.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			addr := Address{Date: "2026-04-21", Kind: conagua.KindDaily, StationID: "01001"}
			ok, err := r2.Exists(ctx, addr)
			Expect(ok).To(BeFalse())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(HavePrefix("r2 stat conagua-raw/2026-04-21/daily/01001.txt:"))
			Expect(strings.Contains(err.Error(), "context canceled")).To(BeTrue())
		})
	})

	Describe("Key", func() {
		addr := Address{Date: "2026-04-21", Kind: conagua.KindDaily, StationID: "01001"}

		It("pins the object key layout", func() {
			Expect(r2.Key(addr)).To(Equal("conagua-raw/2026-04-21/daily/01001.txt"))
		})

		It("mirrors the LocalFS path exactly, so the sinks are interchangeable", func() {
			root := GinkgoT().TempDir()
			local := NewLocalFS(root)
			rel, err := filepath.Rel(root, local.Path(addr))
			Expect(err).NotTo(HaveOccurred())
			Expect(r2.Key(addr)).To(Equal(filepath.ToSlash(rel)))
		})

		It("covers every CONAGUA kind with the same shape", func() {
			for _, kind := range conagua.AllKinds {
				a := Address{Date: "2026-06-08", Kind: kind, StationID: "14001"}
				Expect(r2.Key(a)).To(Equal(
					fmt.Sprintf("conagua-raw/2026-06-08/%s/14001.txt", kind)))
			}
		})
	})
})

var _ = Describe("isNotFound", func() {
	It("is false for nil", func() {
		Expect(isNotFound(nil)).To(BeFalse())
	})

	It("matches a minio 404 by status code", func() {
		Expect(isNotFound(minio.ErrorResponse{StatusCode: 404})).To(BeTrue())
	})

	It("matches a minio NoSuchKey by code", func() {
		Expect(isNotFound(minio.ErrorResponse{StatusCode: 500, Code: "NoSuchKey"})).To(BeTrue())
	})

	It("matches a wrapped minio 404", func() {
		wrapped := fmt.Errorf("r2 get x: %w", minio.ErrorResponse{StatusCode: 404, Code: "NoSuchKey"})
		Expect(isNotFound(wrapped)).To(BeTrue())
	})

	It("rejects a minio error that is not a 404", func() {
		Expect(isNotFound(minio.ErrorResponse{StatusCode: 403, Code: "AccessDenied"})).To(BeFalse())
	})

	It("falls back to string matching for non-minio wrappings", func() {
		Expect(isNotFound(fmt.Errorf("NoSuchKey: blob gone"))).To(BeTrue())
		Expect(isNotFound(fmt.Errorf("The specified key does not exist."))).To(BeTrue())
	})

	It("rejects unrelated errors", func() {
		Expect(isNotFound(fmt.Errorf("connection refused"))).To(BeFalse())
	})
})

var _ = Describe("MirrorIndexToSink", func() {
	p := Progress{
		SnapshotDate:  "2026-04-21",
		SchemaVersion: ProgressSchemaVersion,
	}

	It("is a no-op for a LocalFS sink (WriteIndex already covered it)", func() {
		local := NewLocalFS(GinkgoT().TempDir())
		Expect(MirrorIndexToSink(context.Background(), local, "2026-04-21", p)).To(Succeed())

		// Nothing was written under the root.
		_, err := os.Stat(IndexPath(local, "2026-04-21"))
		Expect(err).To(MatchError(os.ErrNotExist))
	})

	It("is a no-op for a non-R2 custom sink", func() {
		fake := newFakePrimary()
		Expect(MirrorIndexToSink(context.Background(), fake, "2026-04-21", p)).To(Succeed())
		Expect(fake.putCalls.Load()).To(BeZero(), "the mirror bypasses Sink.Put; only *R2Sink uploads")
	})

	It("uploads to the pinned index key for an R2 sink", func() {
		r2, err := NewR2Sink(testR2Config())
		Expect(err).NotTo(HaveOccurred())

		// A pre-cancelled context makes the PUT fail before any network
		// I/O, proving the R2 path attempts the upload — and pinning the
		// object key via the wrapped error — without touching live R2.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = MirrorIndexToSink(ctx, r2, "2026-04-21", p)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(HavePrefix("r2 put conagua-raw/2026-04-21/_index.json:"))
		Expect(strings.Contains(err.Error(), "context canceled")).To(BeTrue())
	})
})
