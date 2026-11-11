package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

// skippedForeignAnchors are the anchors of the other repos: their sources are not in this tree, so the census skips them
// by repo name and asserts how many it skipped. A new repo prefix or a changed count goes red here.
var skippedForeignAnchors = map[string]int{"acr": 47, "web": 20}

type anchorCensus struct {
	read    int
	ops     int
	skipped map[string]int
	failed  []string
}

func runAnchorCensus(document []byte, root string, wantSkipped map[string]int) anchorCensus {
	census := anchorCensus{skipped: map[string]int{}}
	refs, err := collectCredentialAnchorRefs(document)
	if err != nil {
		census.failed = append(census.failed, fmt.Sprintf("credential-classes.json does not parse: %v", err))
		return census
	}
	census.read = len(refs)
	for _, ref := range refs {
		label := fmt.Sprintf("class %s anchor %s %s:%d", ref.classID, strings.TrimPrefix(ref.where, "/"), ref.path, ref.line)
		switch {
		case ref.repo == "ops":
			census.ops++
			switch {
			case strings.HasSuffix(ref.path, ".py"):
				census.failed = append(census.failed, label+": an ops anchor must point at a Go file, not a Python file")
			case !filepath.IsLocal(ref.path):
				census.failed = append(census.failed, label+": path is not a clean relative path inside the checkout")
			default:
				if _, err := os.Stat(filepath.Join(root, ref.path)); err != nil {
					census.failed = append(census.failed, label+": path is absent from the tree")
				}
			}
		case wantSkipped[ref.repo] > 0:
			census.skipped[ref.repo]++
		default:
			census.failed = append(census.failed, fmt.Sprintf("%s: repo %q is neither ops nor a named skipped repo", label, ref.repo))
		}
	}
	if census.read == 0 {
		census.failed = append(census.failed, "zero anchors were read: the census measured nothing")
	}
	if census.ops == 0 {
		census.failed = append(census.failed, "zero ops anchors were read: the census measured nothing")
	}
	for repo, want := range wantSkipped {
		if census.skipped[repo] != want {
			census.failed = append(census.failed, fmt.Sprintf("skipped %s anchors: got %d, want %d", repo, census.skipped[repo], want))
		}
	}
	sort.Strings(census.failed)
	return census
}

func TestEveryOpsCredentialAnchorPointsAtAGoFileInTheTree(t *testing.T) {
	census := runAnchorCensus(authv1.CredentialClasses, testRoot(t), skippedForeignAnchors)
	for _, f := range census.failed {
		t.Error(f)
	}
	t.Logf("anchor census: read=%d ops=%d skipped=%v", census.read, census.ops, census.skipped)
}

func TestTheAnchorCensusFailsOnAPythonAnchorAMissingPathAndAnEmptyRead(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.go"), []byte("package a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	anchor := func(repo, path string) string {
		return fmt.Sprintf(`{"repo":%q,"path":%q,"line":1}`, repo, path)
	}
	doc := func(anchors ...string) []byte {
		return []byte(`{"classes":[{"class_id":"c","issuer":{"anchors":[` + strings.Join(anchors, ",") + `]}}]}`)
	}
	skip := map[string]int{"acr": 1}
	cases := []struct {
		name    string
		doc     []byte
		want    string
		wantBad bool
	}{
		{"good", doc(anchor("ops", "a.go"), anchor("acr", "x.py")), "", false},
		{"python anchor", doc(anchor("ops", "a.go"), anchor("ops", "b.py"), anchor("acr", "x")), "not a Python file", true},
		{"missing path", doc(anchor("ops", "a.go"), anchor("ops", "gone.go"), anchor("acr", "x")), "absent from the tree", true},
		{"zero anchors", []byte(`{"classes":[]}`), "zero anchors were read", true},
		{"unnamed repo", doc(anchor("ops", "a.go"), anchor("acr", "x"), anchor("mcp", "y")), `repo "mcp"`, true},
		{"skipped count moved", doc(anchor("ops", "a.go"), anchor("acr", "x"), anchor("acr", "y")), "skipped acr anchors: got 2, want 1", true},
		{"bad json", []byte("{"), "does not parse", true},
	}
	for _, c := range cases {
		census := runAnchorCensus(c.doc, root, skip)
		joined := strings.Join(census.failed, "\n")
		if c.wantBad && !strings.Contains(joined, c.want) {
			t.Errorf("%s: want a failure containing %q, got %q", c.name, c.want, joined)
		}
		if !c.wantBad && len(census.failed) != 0 {
			t.Errorf("%s: want no failure, got %q", c.name, joined)
		}
	}
}
