package providersync

import (
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/repokey"
	"github.com/full-chaos/dev-health-ops/internal/repoinventory"
)

var inventorySQLPattern = regexp.MustCompile(`match\(\s*(?:canonical_repo|lowerUTF8\(trimBoth\(repo\)\)),\s*'([^']*)'\s*\)`)

// The GitLab writers name a project in nested groups by its full path, and the
// ops repository inventory keeps that name: Canonical and both inventory
// statements agree with repokey.Valid on it.
func TestGitLabNestedGroupRepositoryNameIsInTheInventory(t *testing.T) {
	const path = "grp-synth/sub-synth/deep-synth/svc-synth"
	payload := repositoryPayload{ID: "123", Name: "Svc Synth", Path: "svc-synth", PathWithNS: path}
	native, err := (NativeRESTHandler{}).normalizeRepository(nativeTestClaim("gitlab", "repo-metadata"), "gitlab:project:123", payload)
	if err != nil {
		t.Fatal(err)
	}
	for writer, name := range map[string]string{
		"gitLabProjectFullName":          gitLabProjectFullName(payload),
		"NativeRESTHandler repositories": native.Attributes["name"],
	} {
		if name != path {
			t.Fatalf("%s = %q, want %q", writer, name, path)
		}
		if !repokey.Valid(name) {
			t.Fatalf("%s: repokey.Valid(%q) = false", writer, name)
		}
		items, err := repoinventory.Canonical([]repoinventory.Row{{Value: name, Count: 1}})
		if err != nil || len(items) != 1 || items[0].Slug != name {
			t.Errorf("%s: Canonical(%q) = %v, %v; want the path kept", writer, name, items, err)
		}
		for statement, sql := range map[string]string{"Statement": repoinventory.Statement(""), "ExistsStatement": repoinventory.ExistsStatement("")} {
			found := inventorySQLPattern.FindStringSubmatch(sql)
			if found == nil {
				t.Fatalf("%s: no match(...) pattern", statement)
			}
			if !regexp.MustCompile(found[1]).MatchString(strings.ToLower(name)) {
				t.Errorf("%s: %s drops %q", writer, statement, name)
			}
		}
	}
}
