package identity

import (
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// Every non-test caller of IdentityBySubject under internal/auth must read the ServiceProvider field of the
// principal it gets back; a caller that ignores it signs a service-provider principal in. The check is a floor:
// each path also has its own behavioural test.
func TestEveryCallerOfIdentityBySubjectReadsTheServiceProviderField(t *testing.T) {
	call := regexp.MustCompile(`\.IdentityBySubject\(`)
	root := filepath.Join("..")
	var callers []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if call.Match(body) {
			callers = append(callers, path)
			if !strings.Contains(string(body), "ServiceProvider") {
				t.Errorf("%s calls IdentityBySubject and never reads ServiceProvider", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"../authruntime/sso.go", "../identity/oidc/identitystore.go", "../identity/oidc/service.go",
		"../identity/saml/identitystore.go", "../identity/saml/service.go", "../identity/social/service.go",
	}
	if !reflect.DeepEqual(callers, want) {
		t.Fatalf("callers = %v, want %v: a new caller needs the ServiceProvider mapping and a row here", callers, want)
	}
}
