package fixture

type spec struct{ dir string }

func (s spec) run(name string, args ...string) int { return 0 }

func gitQuiet(s spec, args ...string) int {
	return s.run("git", append([]string{"-c", "maintenance.auto=false", "-c", "gc.auto=0"}, args...)...)
}
