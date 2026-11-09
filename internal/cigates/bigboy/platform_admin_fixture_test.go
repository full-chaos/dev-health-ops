package bigboy_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/bigboydir"
)

// CHAOS-13356 (acp Q2): the drift-S control of bigboy-platform-admin-fixture.sh reads the platform roles of the auth
// schema (auth.platform_role_assignments under the platform role store's active rule), never public.users.is_superuser,
// and the fixture plants and clears the drift directly in that table (no go-api route, no PATCH). The script runs here
// with a fake docker on PATH (D6387): the fake answers the script's tagged psql queries from a state file (drift-S's
// assignment rows, the other active platform_admin count) and applies the plant and the clear to it, unless a case
// makes it ignore one. Red on a planted drift, green when clean, red when the clear does not read back.

const fixtureFakeDocker = `set -u
st="${FAKE_STATE:?}"
printf '%s\n' "$*" >>"$st/calls"
case " $* " in
  *" users-backfill "*)
    printf 'mode=insert-only\nseen=3\ninserted=0\nupdated=0\nunchanged=0\nkept=3\nplatform_role_granted=0\nplatform_role_revoked=0\n'
    exit 0 ;;
  *" exec -T postgres psql "*) ;;
  *) echo "fake docker: unexpected call: $*" >&2; exit 97 ;;
esac
q="${!#}"
rows=$(cat "$st/ds_rows")
case "$q" in
  *"INSERT INTO auth.platform_role_assignments"*) [[ -e $st/ignore_plant ]] || echo 1 >"$st/ds_rows" ;;
  *"DELETE FROM auth.platform_role_assignments"*) [[ -e $st/ignore_clear ]] || echo 0 >"$st/ds_rows" ;;
  *"/* drift_s_user */"*) echo 1 ;;
  *"/* drift_s_rows */"*) echo "$rows" ;;
  *"/* drift_s_active */"*) if [[ -e $st/blind ]]; then echo 0; else echo "$rows"; fi ;;
  *"/* other_active */"*) cat "$st/others" ;;
  *) echo "fake docker: unknown query: $q" >&2; exit 98 ;;
esac
`

const fixtureDriftS = "0b5c3a1e-4f6d-4a7b-9c2d-1e3f5a7b9c0d"

type fixtureCase struct {
	name   string
	args   []string
	rows   string   // drift-S assignment rows at the start
	others string   // other active platform_admin holders
	flags  []string // ignore_plant, ignore_clear, blind
	code   int
	want   string // a line the output must hold
	rowsAt string // drift-S rows at the end ("" = not checked)
}

func runFixture(t *testing.T, script string, c fixtureCase) (scriptRun, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "_records"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, root, "_records/images.env", "BIGBOY_DHO_IMAGE=ghcr.io/example/dho@sha256:0\n")
	st := t.TempDir()
	writeFile(t, st, "ds_rows", c.rows+"\n")
	writeFile(t, st, "others", c.others+"\n")
	writeFile(t, st, "calls", "")
	for _, f := range c.flags {
		writeFile(t, st, f, "")
	}
	bin := stubBin(t, map[string]string{"docker": fixtureFakeDocker, "curl": "echo \"curl must not run: $*\" >&2; exit 96\n"})
	env := []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "FAKE_STATE=" + st, "BIGBOY_ROOT=" + root, "BIGBOY_DRIFT_S_ID=" + fixtureDriftS}
	r := runScript(t, root, env, "bash", append([]string{script}, c.args...)...)
	return r, st
}

func TestPlatformAdminFixtureDriftSControl(t *testing.T) {
	script := bigboydir.Path(t, realRoot(t), "bigboy-platform-admin-fixture.sh")
	cases := []fixtureCase{
		{name: "clean fixture run plants, sees red, clears and ends green", rows: "0", others: "2", code: 0, want: "platform_admin_fixture_ok", rowsAt: "0"},
		{name: "check on a clean state is green", args: []string{"check"}, rows: "0", others: "2", code: 0, want: "control: green"},
		{name: "check on a planted drift is red", args: []string{"check"}, rows: "1", others: "2", code: 20, want: "STOP exit=20"},
		{name: "check with no other active platform_admin is red", args: []string{"check"}, rows: "0", others: "0", code: 20, want: "STOP exit=20"},
		{name: "drift left from an earlier run stops before the seed", rows: "1", others: "2", code: 11, want: "STOP exit=11"},
		{name: "a plant that does not read back stops", rows: "0", others: "2", flags: []string{"ignore_plant"}, code: 16, want: "STOP exit=16", rowsAt: "0"},
		{name: "a control blind to the planted drift stops", rows: "0", others: "2", flags: []string{"blind"}, code: 14, want: "STOP exit=14", rowsAt: "0"},
		{name: "a clear that does not read back stops", rows: "0", others: "2", flags: []string{"ignore_clear"}, code: 17, want: "STOP exit=17"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r, st := runFixture(t, script, c)
			out := r.stdout + r.stderr
			if r.code != c.code || !strings.Contains(out, c.want) {
				t.Fatalf("exit %d, want %d with %q:\n%s", r.code, c.code, c.want, out)
			}
			calls := mustRead(t, filepath.Join(st, "calls"))
			if c.rowsAt != "" {
				if got := strings.TrimSpace(mustRead(t, filepath.Join(st, "ds_rows"))); got != c.rowsAt {
					t.Errorf("drift-S rows at the end = %s, want %s (the trap clears the plant)", got, c.rowsAt)
				}
			}
			if strings.Contains(calls, "is_superuser") || strings.Contains(calls, "public.users") {
				t.Errorf("the fixture reads or writes public.users:\n%s", calls)
			}
			seeded := strings.Contains(calls, "users-backfill")
			switch {
			case c.code == 11 || len(c.args) > 0:
				if seeded {
					t.Errorf("users-backfill ran on a check or before a failed precondition:\n%s", calls)
				}
			case !strings.Contains(calls, "auth users-backfill -insert-only"):
				t.Errorf("the seed is not `users-backfill -insert-only`:\n%s", calls)
			}
			if c.name == "clean fixture run plants, sees red, clears and ends green" {
				for _, w := range []string{"control: green", "control: red", "clear read-back: drift-S rows 0"} {
					if !strings.Contains(out, w) {
						t.Errorf("the clean run lacks %q:\n%s", w, out)
					}
				}
				if strings.Index(out, "control: red") > strings.LastIndex(out, "control: green") {
					t.Errorf("the clean run does not end green after the red:\n%s", out)
				}
			}
		})
	}
}

// The fixture names no legacy writer or reader of the platform flag: no go-api route, no curl, no is_superuser.
func TestPlatformAdminFixtureUsesNoLegacyPath(t *testing.T) {
	body := mustRead(t, bigboydir.Path(t, realRoot(t), "bigboy-platform-admin-fixture.sh"))
	for _, bad := range []string{"is_superuser", "curl", "/api/v1/admin/users", "BIGBOY_ADMIN_AUTH_HDR", "public.users"} {
		if strings.Contains(body, bad) {
			t.Errorf("bigboy-platform-admin-fixture.sh names %q: the drift-S control reads and writes auth.platform_role_assignments only", bad)
		}
	}
}
