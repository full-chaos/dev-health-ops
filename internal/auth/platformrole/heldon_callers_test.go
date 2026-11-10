package platformrole

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var heldOnCall = regexp.MustCompile(`\.HeldOn\(`)

// HeldOn ignores the principal's state, so it is no authority read: its one production caller is the user-admin
// update, which compares is_superuser with the stored assignment. A second caller could decide by it as if it were
// one.
func TestHeldOnHasOneProductionCaller(t *testing.T) {
	type site struct{ file, fn string }
	var sites []site
	err := filepath.WalkDir(filepath.Join("..", ".."), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		src := string(raw)
		for _, at := range heldOnCall.FindAllStringIndex(src, -1) {
			fn := "(top level)"
			if start := strings.LastIndex(src[:at[0]], "\nfunc "); start >= 0 {
				line := src[start+1:]
				fn = line[:strings.IndexByte(line, '(')]
				if strings.HasPrefix(line, "func (") {
					rest := line[strings.IndexByte(line, ')')+2:]
					fn = rest[:strings.IndexByte(rest, '(')]
				}
			}
			sites = append(sites, site{filepath.ToSlash(path), fn})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := site{"../../auth/authapi/provisioning_users.go", "usersUpdate"}
	if len(sites) != 1 || sites[0] != want {
		t.Fatalf("HeldOn production call sites = %v, want only %v", sites, want)
	}
}
