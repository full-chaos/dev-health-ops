package fixture

import "os/exec"

func nonLiteralSubcommand(dir, sub string) error {
	cmd := exec.Command("git", sub)
	cmd.Dir = dir
	return cmd.Run()
}
