package identity

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// placeholderCopy matches a hand-written copy of the placeholder provider literal in Go source or embedded SQL.
var placeholderCopy = regexp.MustCompile(`["'` + "`" + `]service["'` + "`" + `]`)

// TestUsersOwnedGuardAndBackfillCarryNoCopyOfThePlaceholderRule keeps the one definition here: the guard and the
// backfill packages must name it through IsServicePlaceholder / ServicePlaceholderSQL and carry no literal of their
// own, so a planted copy fails here even when it matches today.
func TestUsersOwnedGuardAndBackfillCarryNoCopyOfThePlaceholderRule(t *testing.T) {
	uses := map[string]string{"../authruntime": "ServicePlaceholderSQL(", "../usersbackfill": "IsServicePlaceholder("}
	for dir, want := range uses {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil || len(files) == 0 {
			t.Fatalf("%s: no Go files (%v)", dir, err)
		}
		used := false
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			src, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if loc := placeholderCopy.FindIndex(src); loc != nil {
				t.Errorf("%s carries its own copy of the placeholder literal at byte %d; use identity.IsServicePlaceholder or identity.ServicePlaceholderSQL", file, loc[0])
			}
			if strings.Contains(string(src), "identity."+want) {
				used = true
			}
		}
		if !used {
			t.Errorf("%s no longer names identity.%s", dir, want)
		}
	}
}
