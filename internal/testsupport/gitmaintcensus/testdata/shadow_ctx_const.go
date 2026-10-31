package fixture

import (
	"context"
	"os/exec"
)

const quietCfg = "-c maintenance.auto=false"

func shadow(ctx context.Context) error {
	quietCfg := "commit"
	_ = quietCfg
	cmd := exec.CommandContext(ctx, "git", quietCfg)
	return cmd.Run()
}
