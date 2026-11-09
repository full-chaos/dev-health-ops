package workflows_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-13356 (PR-V of CHAOS-13351): every venue and CI leg runs with users owned by auth-service, as prod does
// (deploy values.prod.yaml: phase 4, usersOwnedStage owned). No venue file may set AUTH_USERS_OWNED to false: not a
// compose value, an override default (${AUTH_USERS_OWNED:-false}), a one-shot `-e AUTH_USERS_OWNED=false`, a flag
// table row, or a secret file named false. The seed tool (`dho auth users-backfill -insert-only`) runs owned.
// The chart (deploy/helm) is out of this cell: its usersOwnedStage key goes in its own PR (CHAOS-13351 PR-C).

// venueRoots are the trees that hold venues and CI legs: the compose files at the root, the CI scripts, overrides
// and tables, the drill scripts, the image build inputs and the workflows.
var venueRoots = []string{"ci", "scripts", "docker", ".github/workflows"}

// usersOwnedFalseRe matches AUTH_USERS_OWNED followed by a separator (=, :, a tab, quotes, spaces) and a value
// that is false: the literal, a ${AUTH_USERS_OWNED:-false} default, or a file path ending in /false.
var usersOwnedFalseRe = regexp.MustCompile(`AUTH_USERS_OWNED["']?[ \t]*[:=\t][ \t]*["']?(?:\$\{AUTH_USERS_OWNED:?-|\$[A-Za-z_{}]+/)?false\b`)

func usersOwnedFalseLines(text string) []int {
	var lines []int
	for i, line := range strings.Split(text, "\n") {
		if usersOwnedFalseRe.MatchString(line) {
			lines = append(lines, i+1)
		}
	}
	return lines
}

func TestUsersOwnedFalseMatcher(t *testing.T) {
	for _, line := range []string{
		`      AUTH_USERS_OWNED: "false"`,
		`  AUTH_USERS_OWNED: 'false'`,
		`      - AUTH_USERS_OWNED=${AUTH_USERS_OWNED:-false}`,
		`      AUTH_USERS_OWNED: ${AUTH_USERS_OWNED:-false}`,
		`run --rm --no-deps -e AUTH_DATABASE_URI -e AUTH_USERS_OWNED=false migrate auth users-backfill`,
		`    AUTH_DATABASE_SCHEMA="${SCHEMA}" AUTH_USERS_OWNED=false \`,
		"auth-service\tAUTH_USERS_OWNED\tfalse\tchart rule\t-",
		`    "AUTH_SESSION_READER_ROLES=$sec/readers" "AUTH_USERS_OWNED=$sec/false"`,
		`export AUTH_USERS_OWNED=false`,
	} {
		if !usersOwnedFalseRe.MatchString(line) {
			t.Errorf("the matcher misses a false setting: %q", line)
		}
	}
	for _, line := range []string{
		`      AUTH_USERS_OWNED: ${AUTH_USERS_OWNED:-true}`,
		`      AUTH_USERS_OWNED: "true"`,
		"auth-service\tAUTH_USERS_OWNED\ttrue\tprod\t-",
		`  expect_env U3 auth-service.yaml AUTH_USERS_OWNED true "U3"`,
		`  expect_env P3 auth-service.yaml AUTH_USERS_OWNED "" "phase 3"`,
	} {
		if usersOwnedFalseRe.MatchString(line) {
			t.Errorf("the matcher flags a line that does not set false: %q", line)
		}
	}
}

func TestNoVenueSetsUsersOwnedFalse(t *testing.T) {
	root := repoRoot(t)
	var files []string
	tops, err := filepath.Glob(filepath.Join(root, "compose*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, tops...)
	files = append(files, filepath.Join(root, ".env.example"))
	for _, dir := range venueRoots {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	sort.Strings(files)
	seen := map[string]bool{}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		rel, _ := filepath.Rel(root, path)
		seen[filepath.ToSlash(rel)] = true
		for _, n := range usersOwnedFalseLines(string(raw)) {
			t.Errorf("FAIL: %s:%d sets AUTH_USERS_OWNED to false; every venue runs users owned (CHAOS-13356)", filepath.ToSlash(rel), n)
		}
	}
	// Non-vacuity: the walk reached the venues that set the switch before CHAOS-13356.
	for _, want := range []string{"compose.yml", "ci/auth_corpus_external.acp.override.yml", "ci/auth_webproof_compose.override.yml",
		"ci/live_e2e_auth_stack.sh", "ci/live_e2e_auth_flags.tsv", "ci/bigboy/compose.bigboy.yml"} {
		if !seen[want] {
			t.Errorf("FAIL: the venue walk did not read %s", want)
		}
	}
}
