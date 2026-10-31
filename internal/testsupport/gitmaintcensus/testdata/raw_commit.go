package fixture

import "os/exec"

func rawCommit(dir string) error {
	cmd := exec.Command("git", "commit", "-m", "x")
	cmd.Dir = dir
	return cmd.Run()
}
