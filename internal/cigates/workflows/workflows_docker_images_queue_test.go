package workflows_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// docker-images.yml lists merge_group as a trigger. Every "publish unless this is a pull request" condition must also
// exclude merge_group, or a merge queue run pushes the images of a tree that has not merged.
func TestDockerImagesNeverPublishOnMergeGroup(t *testing.T) {
	root := rootOf(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "docker-images.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	const notPR = "github.event_name != 'pull_request'"
	const guarded = "(github.event_name != 'pull_request' && github.event_name != 'merge_group')"
	total := strings.Count(text, notPR)
	if total < 10 {
		t.Fatalf("found %d %q conditions, expected the publish steps to carry them", total, notPR)
	}
	if got := strings.Count(text, guarded); got != total {
		t.Errorf("%d of %d publish conditions exclude merge_group", got, total)
	}
}
