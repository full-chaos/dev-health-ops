package workflows_test

import (
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var dockerCopyLine = regexp.MustCompile(`(?m)^COPY\s+(.+)$`)

// The go-api-tools build stage copies an enumerated set of contract files into
// its context. Every contract package the two binaries link, and every file of
// it the compiler or an embed directive reads, must be copied, or the image
// build fails closed on a missing package.
func TestGoAPIToolsBuildStageCopiesEveryContractFileTheBinariesLink(t *testing.T) {
	root := rootOf(t)
	module := goListOutput(t, root, "list", "-m")
	module = strings.TrimSpace(module)
	listing := goListOutput(t, root, "list", "-deps", "-f",
		"{{.ImportPath}}\t{{.Dir}}\t{{join .GoFiles \",\"}}\t{{join .EmbedFiles \",\"}}", "./cmd/dho", "./cmd/registrydump")

	var sources []string
	for _, match := range dockerCopyLine.FindAllStringSubmatch(readFile(t, filepath.Join(root, "docker", "go-api-tools.Dockerfile")), -1) {
		fields := strings.Fields(match[1])
		var args []string
		for _, field := range fields {
			if !strings.HasPrefix(field, "--") {
				args = append(args, field)
			}
		}
		if len(args) < 2 {
			continue
		}
		for _, source := range args[:len(args)-1] {
			sources = append(sources, strings.TrimSuffix(strings.TrimPrefix(source, "./"), "/"))
		}
	}
	if len(sources) == 0 {
		t.Fatal("no COPY source found in docker/go-api-tools.Dockerfile")
	}
	covered := func(file string) bool {
		for _, source := range sources {
			if file == source || strings.HasPrefix(file, source+"/") {
				return true
			}
		}
		return false
	}

	contractPackages := 0
	for _, line := range strings.Split(strings.TrimSpace(listing), "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) != 4 || !strings.HasPrefix(parts[0], module+"/contracts/") {
			continue
		}
		contractPackages++
		dir := strings.TrimPrefix(parts[0], module+"/")
		for _, group := range []string{parts[2], parts[3]} {
			for _, name := range strings.Split(group, ",") {
				if name == "" {
					continue
				}
				if file := dir + "/" + name; !covered(file) {
					t.Errorf("docker/go-api-tools.Dockerfile does not COPY %s, which a binary of the image reads", file)
				}
			}
		}
	}
	if contractPackages < 3 {
		t.Fatalf("found %d contract packages in the dependency closure; the listing measured nothing", contractPackages)
	}
}

func goListOutput(t *testing.T, root string, args ...string) string {
	t.Helper()
	command := exec.Command("go", args...)
	command.Dir = root
	command.Env = append(command.Environ(), "GOFLAGS=-mod=readonly", "GOWORK=off")
	out, err := command.Output()
	if err != nil {
		t.Fatalf("go %s: %v", strings.Join(args, " "), err)
	}
	return string(out)
}
