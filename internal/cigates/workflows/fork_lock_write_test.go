package workflows_test

import (
	"os"
	"syscall"
)

// writeExecFile is os.WriteFile for a file a test may exec. It holds syscall.ForkLock for reading while the write fd is
// open: forkExec takes the write lock, so no concurrent fork can inherit that fd and a later exec of the file never
// fails with ETXTBSY (golang/go#22315).
func writeExecFile(path string, data []byte, mode os.FileMode) error {
	syscall.ForkLock.RLock()
	defer syscall.ForkLock.RUnlock()
	return os.WriteFile(path, data, mode)
}
