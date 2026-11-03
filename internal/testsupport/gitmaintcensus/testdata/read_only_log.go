package fixture

import "os/exec"

func readOnlyLog(dir string) ([]byte, error) {
	cmd := exec.Command("git", "-C", dir, "log", "--oneline")
	return cmd.Output()
}
