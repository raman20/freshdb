// Package store is freshdb's only coordination primitive: a blob store with
// create-only writes. Every object freshdb writes is immutable and created with
// PutIfAbsent (S3 "If-None-Match: *"), which is what gives single-writer
// fencing without Raft, ZooKeeper or leases.
package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

var (
	ErrExists   = errors.New("store: object already exists") // create-only conflict (S3 412)
	ErrNotFound = errors.New("store: not found")
)

// Store is a minimal object store.
type Store interface {
	// PutIfAbsent atomically creates key with data, or fails with ErrExists.
	// Readers never observe a partially written object.
	PutIfAbsent(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	// List returns the sorted keys directly under prefix (prefix must end in "/").
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, key string) error
}

// Open opens a store from a URL: mem://name, file:///path (or a bare path),
// journal:///path (one append-only file; see Journal),
// s3://bucket/prefix?endpoint=host:port&insecure=1 (credentials from
// AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY or MINIO_ROOT_USER/PASSWORD).
func Open(raw string) (Store, error) {
	if !strings.Contains(raw, "://") {
		return NewFS(raw)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "mem":
		return SharedMem(u.Host + u.Path), nil
	case "file":
		return NewFS(u.Path)
	case "journal":
		return NewJournal(u.Path)
	case "s3":
		return NewS3FromURL(u)
	}
	return nil, fmt.Errorf("store: unsupported scheme %q", u.Scheme)
}
