package fixture

import "os/exec"

func commitWithFlag(dir string) error {
	cmd := exec.Command("git", "-c", "maintenance.auto=false", "-c", "gc.auto=0", "commit", "-m", "x")
	cmd.Dir = dir
	return cmd.Run()
}
