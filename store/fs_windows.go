//go:build windows

package store

import "os"

// publishNoReplace uses CreateHardLink, which fails if p exists.
// Windows support is build-only and untested; see docs/faq.md.
func publishNoReplace(tmp, p string) error {
	err := os.Link(tmp, p)
	os.Remove(tmp)
	return err
}

// syncDir is a no-op: Windows cannot fsync a directory handle. NTFS journals
// metadata, but directory-entry durability after power loss is not verified.
func syncDir(string) error { return nil }
