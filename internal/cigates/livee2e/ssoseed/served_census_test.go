package ssoseed_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The seed is a live-e2e job-local stand-in (deleted when CHAOS-13636 PR2 lands): it writes public.sso_providers, which
// auth-service alone may write in a served binary. So no binary other than the CI tool cigate (run with `go run`, built
// into no image) may link it, directly or through livee2e. A new main package that gains it in its dependencies fails here.
const seedPackage = "github.com/full-chaos/dev-health-ops/internal/cigates/livee2e/ssoseed"

var ciOnlyTools = map[string]string{
	"github.com/full-chaos/dev-health-ops/cmd/cigate": "the CI gate tool, run by `go run` in the workflows; no Dockerfile builds it",
}

func TestNoServedBinaryLinksTheSSOSeed(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "list", "-f", `{{if eq .Name "main"}}{{.ImportPath}} {{join .Deps " "}}{{end}}`, "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	out, err := command.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	mains := 0
	linking := []string{}
	cigateLinks := false
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		mains++
		links := false
		for _, dep := range fields[1:] {
			links = links || dep == seedPackage
		}
		if !links {
			continue
		}
		if _, ok := ciOnlyTools[fields[0]]; ok {
			cigateLinks = true
			continue
		}
		linking = append(linking, fields[0])
	}
	if mains < 20 {
		t.Fatalf("only %d main packages found: the census measures nothing", mains)
	}
	if !cigateLinks {
		t.Error("cigate no longer links the seed: delete this census and its allowlist row with the seed (13636 PR2)")
	}
	sort.Strings(linking)
	if len(linking) > 0 {
		t.Fatalf("main packages that link the SSO seed (a served binary must not): %v", linking)
	}
}

func TestNoDockerfileBuildsTheCITool(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(root, "docker", "*"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no docker files (err=%v)", err)
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		if strings.Contains(string(raw), "cmd/cigate") {
			t.Errorf("%s names cmd/cigate: it links the SSO seed, so no image may build it", filepath.Base(file))
		}
	}
}
