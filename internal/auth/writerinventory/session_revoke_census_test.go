package writerinventory

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// sessionRevokeSites are the sources that write auth.sessions.revoked_at
// (rulings on CHAOS-10987). A base session's end must reach its delegated
// children, so a new site needs a ruling and a place here.
var sessionRevokeSites = map[string]string{
	"internal/auth/session/store.go":               "Store.endSessions: ends the delegated children of ids",
	"internal/auth/keyrotation/store.go":           "Operator.revokeSessions: sessionsToRevoke selects the children",
	"internal/auth/session/unpinned.go":            "RevokeUnpinned: no child possible, a live base has a pin (sessions_pin_present)",
	"internal/auth/delegationend/delegationend.go": "End: child rows only",
}

// principalDisableSites write auth.principals.disabled_at. A disabled user's
// delegated sessions end there; a workload never holds a platform role.
var principalDisableSites = map[string]string{
	"internal/auth/identity/store.go":  "the projection ends the actor's delegated sessions",
	"internal/auth/workload/store.go":  "exempt: rule.ActiveRule admits user principals only",
	"internal/auth/workload/enable.go": "exempt: sets disabled_at NULL only",
}

var sessionRevokeWrite = regexp.MustCompile(`(?s)\.sessions\s+SET\s+revoked_at`)

func TestSessionRevokeSitesAreTheCensus(t *testing.T) {
	root := repoRoot(t)
	var found []string
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if sessionRevokeWrite.Match(raw) {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			found = append(found, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(found)
	var want []string
	for file := range sessionRevokeSites {
		want = append(want, file)
	}
	sort.Strings(want)
	if strings.Join(found, "\n") != strings.Join(want, "\n") {
		t.Fatalf("auth.sessions revoked_at writers:\n%s\nwant:\n%s", strings.Join(found, "\n"), strings.Join(want, "\n"))
	}
}

var principalDisableWrite = regexp.MustCompile(`(?s)\.principals\s+SET\s+disabled_at`)

func TestPrincipalDisableSitesAreTheCensus(t *testing.T) {
	root := repoRoot(t)
	var unlisted []string
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !principalDisableWrite.Match(raw) {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if _, ok := principalDisableSites[filepath.ToSlash(rel)]; !ok {
			unlisted = append(unlisted, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(unlisted) > 0 {
		t.Fatalf("auth.principals disabled_at writers outside the census: %v", unlisted)
	}
}
