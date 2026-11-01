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
	"internal/auth/session/store.go":               "Store.endSessions: ends the delegated children of ids; delegationend.Record ends the public rows",
	"internal/auth/keyrotation/store.go":           "Operator.revokeSessions: sessionsToRevoke selects the children; delegationend.Record ends the public rows",
	"internal/auth/session/unpinned.go":            "RevokeUnpinned: no child possible, a live base has a pin (sessions_pin_present); no public row",
	"internal/auth/delegationend/delegationend.go": "End: child rows only; each caller's delegationend.Record ends the public rows",
}

// delegationEnders call delegationend.End or lock delegated rows to end them.
// Each one calls delegationend.Record, which ends the public row: a new
// end path outside Record leaves go-api's row open.
var delegationEnders = []string{
	"internal/auth/keyrotation/store.go",
	"internal/auth/session/delegation.go",
	"internal/auth/session/store.go",
}

// principalDisableSites write auth.principals.disabled_at. A disabled user's
// delegated sessions end there; a workload or a service account is never the
// actor or the target of a delegation.
var principalDisableSites = map[string]string{
	"internal/auth/identity/store.go":       "the projection ends the actor's delegated sessions",
	"internal/auth/serviceaccount/store.go": "exempt: kind service_account only (store.go:330); Start refuses a non-user actor and target",
	"internal/auth/workload/store.go":       "exempt: rule.ActiveRule admits user principals only",
	"internal/auth/workload/enable.go":      "exempt: sets disabled_at NULL only",
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

var delegationEnd = regexp.MustCompile(`delegationend\.(End|Lock|LockOfActor)\(`)

func TestEveryRevokeSiteEndsThePublicRow(t *testing.T) {
	root := repoRoot(t)
	for file, how := range sessionRevokeSites {
		if !strings.Contains(how, "public row") {
			t.Errorf("%s: the census does not name how the public row ends: %q", file, how)
		}
	}
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
		if !delegationEnd.Match(raw) {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		found = append(found, filepath.ToSlash(rel))
		if !strings.Contains(string(raw), "delegationend.Record(") {
			t.Errorf("%s ends delegated rows and does not call delegationend.Record", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(found)
	if strings.Join(found, "\n") != strings.Join(delegationEnders, "\n") {
		t.Fatalf("delegated row enders:\n%s\nwant:\n%s", strings.Join(found, "\n"), strings.Join(delegationEnders, "\n"))
	}
	raw, err := os.ReadFile(filepath.Join(root, "internal/auth/delegationend/delegationend.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`(?s)func Record\(.*endPublicRows\(`).Match(raw) {
		t.Fatal("delegationend.Record does not end the public rows")
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
