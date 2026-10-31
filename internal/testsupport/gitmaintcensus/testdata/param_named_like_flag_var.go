package fixture

import "os/exec"

var quietCfg = []string{"-c", "maintenance.auto=false"}

func param(quietCfg []string) error {
	cmd := exec.Command("git", quietCfg...)
	return cmd.Run()
}
