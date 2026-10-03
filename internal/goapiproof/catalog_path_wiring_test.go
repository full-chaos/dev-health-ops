package goapiproof

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The catalog is read by path at run time, from four places that must agree:
// DefaultCatalogPath (the Go default, relative to the working directory), the
// tools image (docker/go-api-tools.Dockerfile copies the file under its WORKDIR
// so the chart's routing carry/repoint hooks need no -catalog flag), the bigboy
// scripts (an absolute in-image path, and a fetch of the file from GitHub at a
// sha), and the repo itself. A path that no longer resolves in the tree, or two
// spellings that drifted apart, turns this red -- not a hook on a roll.

const toolsImageWorkdir = "/app/go-api"

func readFileAtRoot(t *testing.T, root, relative string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		t.Fatalf("%s: %v", relative, err)
	}
	return string(raw)
}

func TestCatalogDefaultPathResolvesInTheTree(t *testing.T) {
	root := repoRootFromTest(t)
	if _, err := LoadOperationCatalog(filepath.Join(root, filepath.FromSlash(DefaultCatalogPath))); err != nil {
		t.Fatalf("DefaultCatalogPath %q does not resolve to a usable catalog in the tree: %v", DefaultCatalogPath, err)
	}
	if strings.HasPrefix(DefaultCatalogPath, "src/dev_health_ops/api/") {
		t.Fatalf("DefaultCatalogPath %q is inside the Python api tree, which is being deleted", DefaultCatalogPath)
	}
}

func TestToolsImageCopiesTheCatalogToItsDefaultPathUnderWorkdir(t *testing.T) {
	root := repoRootFromTest(t)
	dockerfile := readFileAtRoot(t, root, "docker/go-api-tools.Dockerfile")
	wantCopy := "COPY " + DefaultCatalogPath + " " + toolsImageWorkdir + "/" + DefaultCatalogPath
	copyFound := false
	for _, line := range strings.Split(dockerfile, "\n") {
		if strings.TrimSpace(line) == wantCopy {
			copyFound = true
		}
	}
	if !copyFound {
		t.Errorf("docker/go-api-tools.Dockerfile has no (uncommented) line %q: the hooks' relative default would not resolve in the image", wantCopy)
	}
	if !regexp.MustCompile(`(?m)^WORKDIR ` + regexp.QuoteMeta(toolsImageWorkdir) + `$`).MatchString(dockerfile) {
		t.Errorf("docker/go-api-tools.Dockerfile has no WORKDIR %s: DefaultCatalogPath is relative to it", toolsImageWorkdir)
	}
}

func TestEveryCatalogPathNamedByScriptsAndChartAgreesWithTheDefault(t *testing.T) {
	root := repoRootFromTest(t)
	inImage := toolsImageWorkdir + "/" + DefaultCatalogPath
	absolute := regexp.MustCompile(`-catalog\s+(/app/go-api/\S+)`)
	fetched := regexp.MustCompile(`contents/([^?"\s]*go_api_operations\.json)\?ref=`)

	var files []string
	for _, dir := range []string{"ci", "scripts", "deploy/helm/dev-health/templates", "docker"} {
		err := filepath.WalkDir(filepath.Join(root, filepath.FromSlash(dir)), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() {
				switch filepath.Ext(path) {
				case ".sh", ".py", ".yaml", ".yml", ".Dockerfile":
					files = append(files, path)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	files = append(files, filepath.Join(root, "compose.yml"))
	seen := 0
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		relative, _ := filepath.Rel(root, file)
		for _, match := range absolute.FindAllStringSubmatch(string(raw), -1) {
			seen++
			if match[1] != inImage {
				t.Errorf("%s passes -catalog %s; the tools image holds the catalog at %s", relative, match[1], inImage)
			}
		}
		for _, match := range fetched.FindAllStringSubmatch(string(raw), -1) {
			seen++
			if match[1] != DefaultCatalogPath {
				t.Errorf("%s fetches %s from GitHub; the catalog lives at %s", relative, match[1], DefaultCatalogPath)
			}
			if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(match[1]))); err != nil {
				t.Errorf("%s fetches %s, which does not exist in the tree: %v", relative, match[1], err)
			}
		}
	}
	if seen == 0 {
		t.Fatal("found no catalog path in any script or chart template: the scan matched nothing, so it measured nothing")
	}
}

// The routing carry/repoint hooks pass no -catalog and rely on the image's
// WORKDIR. A workingDir override would silently break the relative default.
func TestRoutingHooksRelyOnTheImageWorkdir(t *testing.T) {
	root := repoRootFromTest(t)
	hooks := readFileAtRoot(t, root, "deploy/helm/dev-health/templates/routing-carry-hooks.yaml")
	if !strings.Contains(hooks, "dho goapi routing carry") {
		t.Fatal("routing-carry-hooks.yaml no longer runs `dho goapi routing carry`: this guard measures nothing")
	}
	if strings.Contains(hooks, "workingDir:") {
		t.Error("routing-carry-hooks.yaml sets workingDir: the hooks' relative DefaultCatalogPath would not resolve")
	}
	if strings.Contains(hooks, "-catalog") {
		t.Error("routing-carry-hooks.yaml passes -catalog: it must then agree with the image path; extend this guard")
	}
}
