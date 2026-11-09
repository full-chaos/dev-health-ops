package authruntime

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
)

// A test credential is built at run time. No fixed credential-shaped literal (32 or more base64url characters mixing
// upper case, lower case and digits) and no fixed role password (a password name bound to a string literal, or an
// inline SQL role password literal) stands in the package's sources, where a secret scanner would read it as a key.
func TestNoFixedCredentialLiteralInThePackage(t *testing.T) {
	shaped := regexp.MustCompile("[\"`][A-Za-z0-9_+=-]{32,}[\"`]")
	mixed := func(text string) bool {
		return strings.ContainsAny(text, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") && strings.ContainsAny(text, "abcdefghijklmnopqrstuvwxyz") && strings.ContainsAny(text, "0123456789")
	}
	bound := regexp.MustCompile("(?i)\\b[a-z0-9_]*passw(or)?d[a-z0-9_]*\\s*:?=\\s*[\"`]")
	inline := regexp.MustCompile(`(?i)\bpassword\s+'[^'%]`)
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	scanned := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		raw, err := os.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		scanned++
		for number, line := range strings.Split(string(raw), "\n") {
			for _, found := range shaped.FindAllString(line, -1) {
				if mixed(found) {
					t.Errorf("%s:%d holds a fixed credential-shaped literal of %d characters", entry.Name(), number+1, len(found)-2)
				}
			}
			if bound.MatchString(line) || inline.MatchString(line) {
				t.Errorf("%s:%d holds a fixed password literal", entry.Name(), number+1)
			}
		}
	}
	if scanned == 0 {
		t.Fatal("no Go source scanned: the scan measured nothing")
	}
}

func aliasedPasswordFindings(name string, src []byte) ([]string, error) {
	return nil, nil
}

func TestAliasedPasswordConstIsFound(t *testing.T) {
	src, err := os.ReadFile("testdata/pw_alias_fixture.txt")
	if err != nil {
		t.Fatal(err)
	}
	found, err := aliasedPasswordFindings("pw_alias_fixture.go", src)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int]bool{8: true, 13: true, 17: true}
	for _, line := range found {
		var number int
		if _, err := fmt.Sscanf(line, "%d", &number); err != nil {
			t.Fatalf("finding %q has no leading line number", line)
		}
		if !want[number] {
			t.Errorf("unexpected finding %q", line)
		}
		delete(want, number)
	}
	for number := range want {
		t.Errorf("fixture line %d is not reported", number)
	}
}
