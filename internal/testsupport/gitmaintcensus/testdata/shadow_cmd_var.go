package fixture

import "os/exec"

var quietCfg = []string{"-c", "maintenance.auto=false"}

func shadow() error {
	quietCfg := []string{"commit"}
	_ = quietCfg
	cmd := exec.Command("git", quietCfg...)
	return cmd.Run()
}
