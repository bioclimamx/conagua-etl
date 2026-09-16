package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// DefaultSnapshotsBucket is the R2 bucket name we use when the
// R2_SNAPSHOTS_BUCKET env var is empty.
const DefaultSnapshotsBucket = "bioclima-snapshots"

// R2Config is the subset of env config R2Sink needs.
type R2Config struct {
	AccountID       string
	AccessKeyID     string
	SecretAccessKey string
	Bucket          string // defaults to DefaultSnapshotsBucket when empty
}

// Endpoint returns the S3-compatible URL for this account.
// R2's endpoint form: https://<account-id>.r2.cloudflarestorage.com.
func (c R2Config) Endpoint() string {
	return fmt.Sprintf("%s.r2.cloudflarestorage.com", c.AccountID)
}

// LoadR2Config reads R2_* env vars into an R2Config, filling the bucket
// default if omitted. Returns a helpful error listing every missing var.
func LoadR2Config() (R2Config, error) {
	c := R2Config{
		AccountID:       os.Getenv("R2_ACCOUNT_ID"),
		AccessKeyID:     os.Getenv("R2_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("R2_SECRET_ACCESS_KEY"),
		Bucket:          os.Getenv("R2_SNAPSHOTS_BUCKET"),
	}
	if c.Bucket == "" {
		c.Bucket = DefaultSnapshotsBucket
	}
	var missing []string
	if c.AccountID == "" {
		missing = append(missing, "R2_ACCOUNT_ID")
	}
	if c.AccessKeyID == "" {
		missing = append(missing, "R2_ACCESS_KEY_ID")
	}
	if c.SecretAccessKey == "" {
		missing = append(missing, "R2_SECRET_ACCESS_KEY")
	}
	if len(missing) > 0 {
		return c, fmt.Errorf("R2 env vars missing: %s", strings.Join(missing, ", "))
	}
	return c, nil
}

// R2Sink is a Sink backed by Cloudflare R2 via the S3-compatible endpoint.
// Object keys mirror LocalFS exactly, so the two sinks are interchangeable
// for the rest of the pipeline:
//
//	conagua-raw/<Date>/<Kind>/<StationID>.txt
type R2Sink struct {
	client *minio.Client
	bucket string
	cfg    R2Config
}

var _ Sink = (*R2Sink)(nil)

// NewR2Sink constructs an R2Sink from an R2Config.
func NewR2Sink(c R2Config) (*R2Sink, error) {
	cli, err := minio.New(c.Endpoint(), &minio.Options{
		Creds:  credentials.NewStaticV4(c.AccessKeyID, c.SecretAccessKey, ""),
		Secure: true,
		Region: "auto", // R2 accepts "auto" for the virtual-hosted endpoint
	})
	if err != nil {
		return nil, fmt.Errorf("minio client: %w", err)
	}
	return &R2Sink{client: cli, bucket: c.Bucket, cfg: c}, nil
}

// Bucket returns the bucket name this sink writes to.
func (r *R2Sink) Bucket() string { return r.bucket }

// Client returns the underlying minio client. Exposed so diagnostic
// tools can run raw LIST queries without bloating the Sink interface
// with discovery methods.
func (r *R2Sink) Client() *minio.Client { return r.client }

// Key returns the object key R2Sink uses for addr.
func (r *R2Sink) Key(addr Address) string {
	return fmt.Sprintf("conagua-raw/%s/%s/%s.txt",
		addr.Date, string(addr.Kind), addr.StationID)
}

// Exists implements Sink via HEAD.
func (r *R2Sink) Exists(ctx context.Context, addr Address) (bool, error) {
	key := r.Key(addr)
	_, err := r.client.StatObject(ctx, r.bucket, key, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, fmt.Errorf("r2 stat %s: %w", key, err)
}

// Get implements Sink. minio GetObject is lazy — the first Read or Stat
// triggers the fetch — so we Stat up-front to surface not-found errors
// immediately, matching LocalFS semantics. That's one extra round trip
// per file, but ingest is bounded by parse cost, not by Sink latency.
func (r *R2Sink) Get(ctx context.Context, addr Address) (io.ReadCloser, error) {
	key := r.Key(addr)
	obj, err := r.client.GetObject(ctx, r.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("r2 get %s: %w", key, err)
	}
	if _, err := obj.Stat(); err != nil {
		obj.Close() //nolint:errcheck // read-side close; no recovery possible
		if isNotFound(err) {
			return nil, fmt.Errorf("r2 get %s: %w", key, fs.ErrNotExist)
		}
		return nil, fmt.Errorf("r2 get %s: %w", key, err)
	}
	return obj, nil
}

// Put implements Sink. Reads the body into memory (CONAGUA files are small
// — largest daily file observed is ~800 KB), hashes it, then PUTs to R2.
// The upload is atomic from the reader's POV: a failed Put leaves nothing
// at the key.
//
// We pass a *bytes.Reader (not *bytes.Buffer) into PutObject so that
// minio-go's internal retry can Seek(0) and re-read the body on a
// transport failure. A *bytes.Buffer drains once and then reads empty,
// which surfaced in 2026-04 as "ContentLength=X with Body length 0"
// errors on 6 files in the national pull.
func (r *R2Sink) Put(ctx context.Context, addr Address, body io.Reader) (PutResult, error) {
	var buf bytes.Buffer
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(&buf, h), contextReader{ctx: ctx, r: body})
	if err != nil {
		return PutResult{}, fmt.Errorf("read body: %w", err)
	}
	key := r.Key(addr)
	_, err = r.client.PutObject(ctx, r.bucket, key, bytes.NewReader(buf.Bytes()), n, minio.PutObjectOptions{
		ContentType: "text/plain; charset=utf-8",
	})
	if err != nil {
		return PutResult{}, fmt.Errorf("r2 put %s: %w", key, err)
	}
	return PutResult{
		Bytes:  n,
		SHA256: hex.EncodeToString(h.Sum(nil)),
	}, nil
}

// FetchIndex reads the _index.json blob for a snapshot date from R2.
// Symmetric with MirrorIndexToSink: `snapshot pull` writes the file,
// `ingest` (or any other consumer) fetches it. A missing key maps to
// fs.ErrNotExist so callers can differentiate "no such snapshot" from
// transport failures.
//
// Key: conagua-raw/<date>/_index.json.
func (r *R2Sink) FetchIndex(ctx context.Context, date string) ([]byte, error) {
	key := fmt.Sprintf("conagua-raw/%s/_index.json", date)
	obj, err := r.client.GetObject(ctx, r.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("r2 get %s: %w", key, err)
	}
	defer obj.Close() //nolint:errcheck // read-side close; no recovery possible
	if _, err := obj.Stat(); err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("r2 get %s: %w", key, fs.ErrNotExist)
		}
		return nil, fmt.Errorf("r2 get %s: %w", key, err)
	}
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("r2 get %s: %w", key, err)
	}
	return data, nil
}

// isNotFound identifies a minio-wrapped 404.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	if resp, ok := errors.AsType[minio.ErrorResponse](err); ok {
		return resp.StatusCode == 404 || resp.Code == "NoSuchKey"
	}
	// minio wraps some errors differently; string match is a fallback.
	return strings.Contains(err.Error(), "NoSuchKey") ||
		strings.Contains(err.Error(), "key does not exist")
}
