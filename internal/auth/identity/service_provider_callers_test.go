package identity

import (
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// Every non-test caller of IdentityBySubject under internal/auth, found through go/types (method values and method
// expressions included), must pass the call-site guard: it checks the ServiceProvider field of the principal it gets
// back; a caller that ignores it signs a service-provider principal in. The check is a floor: each path also has its
// own behavioural test.
func TestEveryCallerOfIdentityBySubjectReadsTheServiceProviderField(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  root,
	}, "./...")
	if err != nil || packages.PrintErrors(loaded) > 0 {
		t.Fatalf("load internal/auth: %v", err)
	}
	callers := map[string]bool{}
	allowed := map[int]bool{}
	for _, pkg := range loaded {
		res := spCheck(pkg.Fset, pkg.Syntax, pkg.TypesInfo)
		for _, gap := range res.gaps {
			t.Error(gap)
		}
		for file := range res.files {
			rel, err := filepath.Rel(root, file)
			if err != nil {
				t.Fatal(err)
			}
			callers[filepath.ToSlash(rel)] = true
		}
		for i := range res.allowed {
			allowed[i] = true
		}
	}
	for i, a := range spAllowlist {
		if !allowed[i] {
			t.Errorf("allowlist entry %s:%s (%s) matches no function: remove it", a.file, a.fn, a.reason)
		}
	}
	var got []string
	for file := range callers {
		if !strings.HasSuffix(file, "_test.go") {
			got = append(got, file)
		}
	}
	sort.Strings(got)
	want := []string{
		"authruntime/sso.go", "identity/oidc/identitystore.go", "identity/oidc/service.go",
		"identity/saml/identitystore.go", "identity/saml/service.go", "identity/social/service.go",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("callers = %v, want %v: a new caller needs the ServiceProvider mapping and a row here", got, want)
	}
}
