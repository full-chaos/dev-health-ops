package fixture

import "os/exec"

const quietCfg = "-c maintenance.auto=false"

func shadow() error {
	quietCfg := "commit"
	_ = quietCfg
	cmd := exec.Command("git", quietCfg)
	return cmd.Run()
}
