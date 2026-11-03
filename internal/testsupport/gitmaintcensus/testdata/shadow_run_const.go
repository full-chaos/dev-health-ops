package fixture

type spec struct{}

func (spec) run(name string, args ...string) int { return 0 }

const quietCfg = "-c maintenance.auto=false"

func shadow(s spec) int {
	quietCfg := "commit"
	_ = quietCfg
	return s.run("git", quietCfg)
}
