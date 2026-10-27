//go:build linux

package store

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// publishNoReplace atomically makes tmp visible at p, failing with an error
// matching fs.ErrExist if p already exists. tmp is gone afterwards on success.
func publishNoReplace(tmp, p string) error {
	err := unix.Renameat2(unix.AT_FDCWD, tmp, unix.AT_FDCWD, p, unix.RENAME_NOREPLACE)
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOSYS) {
		// filesystem without RENAME_NOREPLACE: link(2) is also atomic create-only
		err = os.Link(tmp, p)
		os.Remove(tmp)
	}
	return err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
