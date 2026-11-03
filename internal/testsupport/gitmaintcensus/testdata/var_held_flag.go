package fixture

import "os/exec"

var scratchGitConfig = []string{"-c", "maintenance.auto=false", "-c", "gc.auto=0"}

func varHeldFlag(dir string, args ...string) error {
	cmd := exec.Command("git", append(scratchGitConfig, args...)...)
	cmd.Dir = dir
	return cmd.Run()
}
