//go:build unix

package keystore

import (
	"os"
	"syscall"
)

// openNoFollow opens path read-only with O_NOFOLLOW and O_NONBLOCK set as part
// of the single open(2) that obtains the descriptor.
//
// There is deliberately no separate pre-open check (no lstat), so there is no
// window between "check" and "open" in which the path's target can be swapped:
// the kernel itself refuses to open a path whose final component is a symlink,
// and refuses to block inside open(2) waiting for a FIFO writer that may never
// arrive. Every subsequent check runs against the already-open descriptor, so
// nothing that happens to the path afterwards can change what is inspected or
// read.
//
// This mirrors acr's internal/sidecar bounded-file reader, which ACP-ADR-02 §3
// names as the platform reference for signing-key custody. It is reimplemented
// rather than imported because acr is a separate Go module.
func openNoFollow(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
}
