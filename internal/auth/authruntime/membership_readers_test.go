package authruntime

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// membershipSQL matches SQL that reads or joins a memberships table of either
// schema.
var membershipSQL = regexp.MustCompile("(?is)(FROM|JOIN)\\s+(public\\.|`\\s*\\+\\s*[A-Za-z.()]+\\s*\\+\\s*`\\.|%s\\.)?memberships\\b")

// membershipReaders are the files that read memberships. The ones that serve a
// sign-in follow the users-cutover switch: with it off they read
// public.memberships only (byte for byte as before), with it on they also read
// the auth memberships and an auth row wins on role. A new reader must join
// this list and follow the same rule.
var membershipReaders = map[string]string{
	"internal/auth/authapi/users.go":            "switch: PGUsers.UsersOwned",
	"internal/auth/identity/oidc/membership.go": "switch: PGMemberships.UsersOwned",
	"internal/auth/identity/saml/pgstore.go":    "switch: PGStore.WithUsersOwned",
	"internal/auth/identity/identities.go":      "legacy lookup, skipped once the users are owned",
	"internal/auth/identity/jit.go":             "reads the auth memberships only, inside the join's commit",
}

func TestEveryMembershipReaderFollowsTheUsersCutoverSwitch(t *testing.T) {
	root := ".."
	found := map[string]bool{}
	scanned := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return walkErr
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		scanned++
		if membershipSQL.Match(raw) {
			rel, _ := filepath.Rel(root, path)
			found["internal/auth/"+filepath.ToSlash(rel)] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 50 {
		t.Fatalf("only %d files scanned: the census measured nothing", scanned)
	}
	for file := range found {
		if _, ok := membershipReaders[file]; !ok {
			t.Errorf("%s reads memberships but is not in membershipReaders: follow the users-cutover switch and list it", file)
		}
	}
	for file := range membershipReaders {
		if !found[file] {
			t.Errorf("%s is listed as a membership reader but reads none: remove it", file)
		}
	}
}
