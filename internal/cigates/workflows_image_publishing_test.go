package cigates_test

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10177: Go port of the real-tree guards of tests/tooling/test_go_image_publishing.py (CHAOS-3923): every Go
// image a deployment renderer names must be one CI publishes, the build and merge matrices equal the container
// gate's ALL_TARGETS, and a change to the publish workflow republishes. The file's seven ci/ script-token tests
// exercised a shell tokenizer that no guard in the file (or elsewhere in the repo) consumes: DELETED, see PR body.

const publishWorkflow = ".github/workflows/docker-images.yml"
const goYmlFile = ".github/workflows/go.yml"

var (
	allTargetsRe = regexp.MustCompile(`(?m)^readonly ALL_TARGETS=\(([^)]*)\)`)
	ghcrImageRe  = regexp.MustCompile(`ghcr\.io/[^/\s]+/dev-health-go-([a-z0-9-]+)`)
)

var renderersNamingGoImages = []string{"deploy/go-api/compose-query-api.yml", "deploy/helm/dev-health/values.yaml"}

// stripYAMLComments drops full-line and trailing comments so a stale comment never counts as a deployed image.
func stripYAMLComments(text string) string {
	lines := strings.Split(text, "\n")
	for i, l := range lines {
		for j := 0; j < len(l); j++ {
			if l[j] == '#' && (j == 0 || l[j-1] == ' ' || l[j-1] == '\t') {
				lines[i] = l[:j]
				break
			}
		}
	}
	return strings.Join(lines, "\n")
}

func publishedTargets(doc yamlMap, job string) []string {
	var out []string
	for _, x := range asList(asMap(asMap(asMap(jobsOf(doc)[job])["strategy"])["matrix"])["target"]) {
		out = append(out, asString(x))
	}
	sort.Strings(out)
	return out
}

func imagePublishingViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	doc := loadWf(t, root, "docker-images.yml")

	m := allTargetsRe.FindStringSubmatch(readFile(t, filepath.Join(root, "ci/check_go_containers.sh")))
	var gate []string
	if m == nil {
		add("ALL_TARGETS is no longer declared as one literal array in ci/check_go_containers.sh")
	} else {
		gate = strings.Fields(m[1])
		sort.Strings(gate)
	}
	build, merge := publishedTargets(doc, "go-build"), publishedTargets(doc, "go-merge")
	if !reflect.DeepEqual(build, gate) {
		add("the go-build publish matrix %v differs from the container gate's ALL_TARGETS %v: one list, not two; a second shorter publish list is how the next target ships unpublished", build, gate)
	}
	if !reflect.DeepEqual(merge, build) {
		add("the go-merge matrix %v differs from the go-build matrix %v: a target built but never merged pushes digests no tag points at", merge, build)
	}

	published := map[string]bool{}
	for _, tgt := range merge {
		published[tgt] = true
	}
	referenced := map[string][]string{}
	var scanned []string
	namedBy := map[string]bool{}
	err := filepath.WalkDir(filepath.Join(root, "deploy"), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if ext := filepath.Ext(p); ext != ".yml" && ext != ".yaml" {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		scanned = append(scanned, filepath.ToSlash(rel))
		for _, mm := range ghcrImageRe.FindAllStringSubmatch(stripYAMLComments(readFile(t, p)), -1) {
			referenced[mm[1]] = append(referenced[mm[1]], rel)
			namedBy[filepath.ToSlash(rel)] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(referenced) == 0 {
		add("no renderer references a Go image: the guard would pass vacuously")
	}
	// the renderers that name a Go image today must keep naming one: swapping the active image for a foreign one
	// would otherwise leave the guard with nothing to compare
	for _, rel := range renderersNamingGoImages {
		if !namedBy[rel] {
			add("renderer %s no longer names a Go image (ghcr.io/<owner>/dev-health-go-<target>) outside comments: its active image changed or the guard reads nothing from it", rel)
		}
	}
	// a renderer the guard reads must itself trigger the Go workflow: a change limited to it would otherwise skip the guard
	for _, event := range []string{"push", "pull_request"} {
		paths := goPaths(t, root, event)
		for _, rel := range scanned {
			if !matchesAny(t, rel, paths) {
				add("renderer %s is scanned by this guard but matches no %s path filter in go.yml: a change limited to it skips the Go tests", rel, event)
			}
		}
	}
	var targets []string
	for tgt := range referenced {
		targets = append(targets, tgt)
	}
	sort.Strings(targets)
	for _, tgt := range targets {
		if !published[tgt] {
			add("deployment renderers name Go image %q that CI never publishes: %v", tgt, referenced[tgt])
		}
	}

	var filters yamlMap
	steps := stepsOf(jobsOf(doc)["changes"])
	if len(steps) > 1 {
		filters = parseYAML(t, asString(asMap(steps[1]["with"])["filters"]), "docker-images.yml changes filters")
	}
	found := false
	for _, p := range asList(filters["code"]) {
		if asString(p) == publishWorkflow {
			found = true
		}
	}
	if !found {
		add("the changes filter `code` does not list %s: a change to the publish pipeline would skip every build job and publish nothing while reporting success", publishWorkflow)
	}
	return out
}

func TestGoImagePublishing(t *testing.T) {
	expectNoViolations(t, imagePublishingViolations(t, repoRoot(t)))
}

func TestGoImagePublishingPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	files := []string{publishWorkflow, "ci/check_go_containers.sh", ".github/workflows/go.yml"}
	var deploy []string
	_ = filepath.WalkDir(filepath.Join(root, "deploy"), func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && (filepath.Ext(p) == ".yml" || filepath.Ext(p) == ".yaml") && !strings.Contains(p, "/vendor/") {
			rel, _ := filepath.Rel(root, p)
			deploy = append(deploy, rel)
		}
		return nil
	})
	files = append(files, deploy...)
	const values = "deploy/helm/dev-health/values.yaml"
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"a gate target is missing from the build matrix", []mutation{{publishWorkflow, "          - auth-migrate\n        platform:", "        platform:"}}, []string{"go-build publish matrix", "ALL_TARGETS"}},
		{"a built target is never merged", []mutation{{publishWorkflow, "          - auth-migrate\n    steps:", "    steps:"}}, []string{"go-merge matrix", "never merged"}},
		{"a renderer names an unpublished image", []mutation{{values, "ghcr.io/full-chaos/dev-health-go-dho", "ghcr.io/full-chaos/dev-health-go-ghost"}}, []string{"never publishes", "ghost"}},
		{"a renderer's active image is swapped while a stale comment still names the Go image", []mutation{{"deploy/go-api/compose-query-api.yml", "ghcr.io/full-chaos/dev-health-go-dho:local}", "docker.io/library/busybox:latest}"}}, []string{"deploy/go-api/compose-query-api.yml no longer names a Go image"}},
		{"a renderer's active image names an unpublished target while a comment names a published one", []mutation{{"deploy/go-api/compose-query-api.yml", "ghcr.io/full-chaos/dev-health-go-dho:local}", "ghcr.io/full-chaos/dev-health-go-ghost:local}"}}, []string{"never publishes", "ghost"}},
		{"a renderer outside the go.yml path filters is invisible to the Go workflow", []mutation{{goYmlFile, "      - 'deploy/go-api/**'\n", allMarker}, {goYmlFile, "      - 'deploy/**'\n", allMarker}}, []string{"deploy/go-api/compose-query-api.yml", "matches no push path filter"}},
		{"the publish workflow cannot republish itself", []mutation{{publishWorkflow, "              - '.github/workflows/docker-images.yml'\n", ""}}, []string{"changes filter", "publish pipeline"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, imagePublishingViolations(t, tree), c.want...)
		})
	}
}
