package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3 is an S3-compatible Store (AWS S3, MinIO, R2, ...). PutIfAbsent uses
// "If-None-Match: *" (S3 conditional writes, GA Aug 2024); a 412 response
// maps to ErrExists. Stores that silently ignore the header are NOT safe for
// writing; run CheckConditional before trusting a new endpoint.
type S3 struct {
	c      *minio.Client
	bucket string
	prefix string
}

func NewS3FromURL(u *url.URL) (*S3, error) {
	q := u.Query()
	endpoint := q.Get("endpoint")
	if endpoint == "" {
		endpoint = "s3.amazonaws.com"
	}
	ak := firstEnv("AWS_ACCESS_KEY_ID", "MINIO_ROOT_USER")
	sk := firstEnv("AWS_SECRET_ACCESS_KEY", "MINIO_ROOT_PASSWORD")
	c, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(ak, sk, ""),
		Secure: q.Get("insecure") == "",
		Region: q.Get("region"),
	})
	if err != nil {
		return nil, err
	}
	if q.Get("create") != "" {
		ctx := context.Background()
		if ok, err := c.BucketExists(ctx, u.Host); err == nil && !ok {
			if err := c.MakeBucket(ctx, u.Host, minio.MakeBucketOptions{Region: q.Get("region")}); err != nil {
				// a concurrent opener may have created it first; that's fine
				if ok, err2 := c.BucketExists(ctx, u.Host); err2 != nil || !ok {
					return nil, err
				}
			}
		}
	}
	p := strings.Trim(u.Path, "/")
	if p != "" {
		p += "/"
	}
	return &S3{c: c, bucket: u.Host, prefix: p}, nil
}

func NewS3(c *minio.Client, bucket, prefix string) *S3 {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &S3{c: c, bucket: bucket, prefix: prefix}
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

func (s *S3) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream", DisableMultipart: true}
	opts.SetMatchETagExcept("*")
	_, err := s.c.PutObject(ctx, s.bucket, s.prefix+key, bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		var er minio.ErrorResponse
		if errors.As(err, &er) && (er.StatusCode == http.StatusPreconditionFailed || er.Code == "PreconditionFailed" || er.StatusCode == http.StatusConflict) {
			return ErrExists
		}
		return err
	}
	return nil
}

func (s *S3) Get(ctx context.Context, key string) ([]byte, error) {
	o, err := s.c.GetObject(ctx, s.bucket, s.prefix+key, minio.GetObjectOptions{})
	if err != nil {
		return nil, mapNF(err)
	}
	defer o.Close()
	b, err := io.ReadAll(o)
	if err != nil {
		return nil, mapNF(err)
	}
	return b, nil
}

// GetRange implements RangeReader with an HTTP Range request.
func (s *S3) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if off < 0 || n <= 0 {
		if n == 0 && off >= 0 {
			return []byte{}, nil
		}
		return nil, fmt.Errorf("store: bad range %d+%d", off, n)
	}
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(off, off+n-1); err != nil {
		return nil, err
	}
	o, err := s.c.GetObject(ctx, s.bucket, s.prefix+key, opts)
	if err != nil {
		return nil, mapNF(err)
	}
	defer o.Close()
	b, err := io.ReadAll(o)
	if err != nil {
		return nil, mapNF(err)
	}
	if int64(len(b)) != n {
		return nil, fmt.Errorf("store: range %d+%d of %s: got %d bytes", off, n, key, len(b))
	}
	return b, nil
}

func mapNF(err error) error {
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return ErrNotFound
	}
	return err
}

func (s *S3) List(ctx context.Context, prefix string) ([]string, error) {
	var out []string
	for o := range s.c.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.prefix + prefix}) {
		if o.Err != nil {
			return nil, o.Err
		}
		k := strings.TrimPrefix(o.Key, s.prefix)
		if strings.HasSuffix(k, "/") {
			continue
		}
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	return s.c.RemoveObject(ctx, s.bucket, s.prefix+key, minio.RemoveObjectOptions{})
}

// CheckConditional verifies the endpoint honours If-None-Match (a second
// create of the same key must fail). Refuse to write if it does not.
func CheckConditional(ctx context.Context, s Store) error {
	k := fmt.Sprintf("_probe/%d", os.Getpid())
	_ = s.Delete(ctx, k)
	if err := s.PutIfAbsent(ctx, k, []byte("a")); err != nil {
		return err
	}
	defer s.Delete(ctx, k)
	if err := s.PutIfAbsent(ctx, k, []byte("b")); !errors.Is(err, ErrExists) {
		return fmt.Errorf("store does not enforce create-only writes (got %v); refusing to open for write", err)
	}
	return nil
}
