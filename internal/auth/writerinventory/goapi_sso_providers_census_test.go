package writerinventory

import (
	"strings"
	"testing"
)

var goAPIRoots = []string{"internal/api/", "internal/apiservice/", "cmd/dho/"}

func goAPIFile(file string) bool {
	for _, root := range goAPIRoots {
		if strings.HasPrefix(file, root) {
			return true
		}
	}
	return false
}

// goAPISSOProvidersException is go-api's one named write of public.sso_providers: the org purge DELETE (CHAOS-14131). auth-service
// owns every other provider write.
var goAPISSOProvidersException = Entry{File: "internal/apiservice/admin/orgdeletion.go", Func: "deletionTarget.deleteSQL",
	Table: "public.sso_providers", Verb: "DELETE"}

// CHAOS-14131: go-api writes public.sso_providers only through the org purge DELETE; no static writer in the go-api tree,
// no allowlist row of one, and exactly the one dynamic row.
func TestGoAPIWritesNoSSOProviderRowButThePurge(t *testing.T) {
	root := repoRoot(t)
	watched, err := Watched(root)
	if err != nil {
		t.Fatal(err)
	}
	if !watched["public.sso_providers"] {
		t.Fatal("public.sso_providers is not a watched table")
	}
	found, err := Scan(root, watched)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range found {
		if w.Table == "public.sso_providers" && goAPIFile(w.File) {
			t.Errorf("go-api writes public.sso_providers: %s", w.Key())
		}
	}
	for _, e := range Allowlist {
		if e.Table == "public.sso_providers" && goAPIFile(e.File) {
			t.Errorf("allowlist row of a go-api public.sso_providers writer: %s", e.Key())
		}
	}
	var dynamic []string
	for _, e := range DynamicAllowlist {
		if e.Table == "public.sso_providers" && goAPIFile(e.File) {
			dynamic = append(dynamic, e.Key())
		}
	}
	if len(dynamic) != 1 || dynamic[0] != goAPISSOProvidersException.Key() {
		t.Errorf("go-api dynamic public.sso_providers rows = %v, want only %s", dynamic, goAPISSOProvidersException.Key())
	}
}
