// Package devhealth renders this chart with default values and asserts every
// container/init-container/hook image field resolves to a repository this
// project actually publishes (or a third-party image pulled from its own
// upstream registry). A local/kind profile side-loads its own commit-tagged
// application image, but that is never the chart's own default.
package devhealth_test

import (
	"bytes"
	"io"
	"os/exec"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Repositories the default (no --set overrides) render may reference.
// ghcr.io/full-chaos/* entries are this project's own published images
// (.github/workflows/docker-images.yml); the rest are third-party images
// pulled verbatim from their own upstream registries.
var publishedImageRepositories = map[string]bool{
	"ghcr.io/full-chaos/dev-hops-api":         true,
	"ghcr.io/full-chaos/dev-health-web":       true,
	"ghcr.io/full-chaos/dev-health-query-api": true,
	"ghcr.io/full-chaos/dev-health-go-worker": true,
	"ghcr.io/full-chaos/dev-health-go-dho":    true,
	"valkey/valkey":                           true,
	"clickhouse/clickhouse-server":            true,
	"postgres":                                true,
	"edoburu/pgbouncer":                       true,
}

// repositoryOf strips a trailing ":tag" and/or "@sha256:<digest>" from a
// rendered image reference, leaving the bare repository.
func repositoryOf(image string) string {
	if idx := strings.Index(image, "@"); idx != -1 {
		image = image[:idx]
	}
	lastSlash := strings.LastIndex(image, "/")
	if lastColon := strings.LastIndex(image, ":"); lastColon > lastSlash {
		image = image[:lastColon]
	}
	return image
}

// collectImages walks a decoded YAML document for any "image" key holding a
// non-empty string, covering containers, initContainers, and hook Jobs alike
// without hand-listing every template that sets one.
func collectImages(node interface{}, out *[]string) {
	switch v := node.(type) {
	case map[string]interface{}:
		for key, val := range v {
			if key == "image" {
				if s, ok := val.(string); ok && s != "" {
					*out = append(*out, s)
				}
			}
			collectImages(val, out)
		}
	case []interface{}:
		for _, item := range v {
			collectImages(item, out)
		}
	}
}

func renderChartDefaults(t *testing.T) []byte {
	t.Helper()
	cmd := exec.Command("helm", "template", "dev-health-images-test", ".")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, out)
	}
	return out
}

func renderedImages(t *testing.T, rendered []byte) []string {
	t.Helper()
	dec := yaml.NewDecoder(bytes.NewReader(rendered))
	var images []string
	for {
		var doc interface{}
		if err := dec.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decoding rendered manifest: %v", err)
		}
		collectImages(doc, &images)
	}
	return images
}

// TestDefaultRenderUsesOnlyPublishedImageRepositories renders this chart
// with its own default values.yaml (no overrides -- exactly what a fresh
// `helm install` with no -f/--set gets) and fails if any image field
// resolves to a repository this project never publishes. A previous default
// (ghcr.io/full-chaos/dev-health-ops) named no such package; every non-local
// profile overrides every image with a digest pin, so this default is only
// ever reached by local/dev evaluation, but it still must resolve to a real,
// pullable image there.
func TestDefaultRenderUsesOnlyPublishedImageRepositories(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	images := renderedImages(t, renderChartDefaults(t))
	if len(images) == 0 {
		t.Fatal("rendered manifest carried no image fields -- the chart or this test's extraction broke")
	}
	for _, image := range images {
		repo := repositoryOf(image)
		if !publishedImageRepositories[repo] {
			t.Errorf("image %q resolves to repository %q, which this project does not publish", image, repo)
		}
	}
}
