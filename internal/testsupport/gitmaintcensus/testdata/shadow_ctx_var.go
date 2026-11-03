package fixture

import (
	"context"
	"os/exec"
)

var quietCfg = []string{"-c", "maintenance.auto=false"}

func shadow(ctx context.Context) error {
	quietCfg := []string{"commit"}
	_ = quietCfg
	cmd := exec.CommandContext(ctx, "git", quietCfg...)
	return cmd.Run()
}
