package fixture

import "os/exec"

func use() error {
	cmd := exec.Command("git", quietCfg...)
	return cmd.Run()
}
