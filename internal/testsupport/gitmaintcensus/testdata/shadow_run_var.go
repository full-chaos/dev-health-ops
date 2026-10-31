package fixture

type spec struct{}

func (spec) run(name string, args ...string) int { return 0 }

var quietCfg = []string{"-c", "maintenance.auto=false"}

func shadow(s spec) int {
	quietCfg := []string{"commit"}
	_ = quietCfg
	return s.run("git", quietCfg...)
}
