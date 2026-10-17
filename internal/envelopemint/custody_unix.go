//go:build unix

package envelopemint

import (
	"os"
	"syscall"
)

// openNoFollow opens path in one open(2) that refuses a symlink final
// component and never blocks on a FIFO; every later check runs on the
// descriptor, so a path swapped afterwards changes nothing.
func openNoFollow(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
}
