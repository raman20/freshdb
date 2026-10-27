//go:build !linux && !windows

package store

import "os"

// publishNoReplace uses link(2), which is atomic and fails if p exists.
// Non-Linux platforms build and pass unit tests in CI but get less testing.
func publishNoReplace(tmp, p string) error {
	err := os.Link(tmp, p)
	os.Remove(tmp)
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
