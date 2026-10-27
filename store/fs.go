package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// FS is a local-directory Store. PutIfAbsent = write temp file, fsync it,
// renameat2(RENAME_NOREPLACE) into place (atomic create-only, falls back to
// link(2)), then fsync the directory. A crash at any point leaves either no
// object or the complete object; stray temp files are removed on open.
type FS struct {
	root   string
	noSync bool
	fds    fdCache
}

// SetUnsafeNoSync disables every fsync. A crash or power loss can then lose
// acknowledged writes or leave the log unrecoverable. It exists only so
// benchmarks can report fsync-off numbers next to other engines' no-sync modes.
func (s *FS) SetUnsafeNoSync(on bool) { s.noSync = on }

const tmpDir = ".tmp"

func NewFS(root string) (*FS, error) {
	if root == "" {
		return nil, errors.New("store: empty path")
	}
	if err := os.MkdirAll(filepath.Join(root, tmpDir), 0o755); err != nil {
		return nil, err
	}
	// Remove leftovers from crashed writes; they were never visible.
	ents, _ := os.ReadDir(filepath.Join(root, tmpDir))
	for _, e := range ents {
		os.Remove(filepath.Join(root, tmpDir, e.Name()))
	}
	return &FS{root: root}, nil
}

func (s *FS) Root() string { return s.root }

func (s *FS) path(key string) (string, error) {
	if key == "" || strings.Contains(key, "..") || strings.HasPrefix(key, "/") || strings.HasPrefix(key, tmpDir) {
		return "", fmt.Errorf("store: bad key %q", key)
	}
	return filepath.Join(s.root, filepath.FromSlash(key)), nil
}

func (s *FS) mkdirs(dir string) error {
	if _, err := os.Stat(dir); err == nil {
		return nil
	}
	if err := s.mkdirs(filepath.Dir(dir)); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}
	return s.syncDir(filepath.Dir(dir))
}

func (s *FS) PutIfAbsent(_ context.Context, key string, data []byte) error {
	p, err := s.path(key)
	if err != nil {
		return err
	}
	if err := s.mkdirs(filepath.Dir(p)); err != nil {
		return err
	}
	var rb [8]byte
	rand.Read(rb[:])
	tmp := filepath.Join(s.root, tmpDir, hex.EncodeToString(rb[:]))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := s.fsync(f); err != nil { // durable BEFORE it becomes visible
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	err = publishNoReplace(tmp, p)
	if err != nil {
		os.Remove(tmp)
		if errors.Is(err, fs.ErrExist) {
			return ErrExists
		}
		return err
	}
	return s.syncDir(filepath.Dir(p))
}

func (s *FS) Get(_ context.Context, key string) ([]byte, error) {
	p, err := s.path(key)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	return b, err
}

func (s *FS) List(_ context.Context, prefix string) ([]string, error) {
	dir := filepath.Join(s.root, filepath.FromSlash(strings.TrimSuffix(prefix, "/")))
	if !strings.HasSuffix(prefix, "/") {
		return nil, fmt.Errorf("store: list prefix must end in /: %q", prefix)
	}
	ents, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		if !e.IsDir() {
			out = append(out, prefix+e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *FS) Delete(_ context.Context, key string) error {
	p, err := s.path(key)
	if err != nil {
		return err
	}
	s.fds.drop(p)
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return s.syncDir(filepath.Dir(p))
}

func (s *FS) fsync(f *os.File) error {
	if s.noSync {
		return nil
	}
	return f.Sync()
}

func (s *FS) syncDir(dir string) error {
	if s.noSync {
		return nil
	}
	return syncDir(dir)
}
