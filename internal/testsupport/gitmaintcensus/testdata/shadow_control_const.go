package fixture

import "os/exec"

const quietCfg = "-c maintenance.auto=false"

func control() error {
	cmd := exec.Command("git", quietCfg)
	return cmd.Run()
}
