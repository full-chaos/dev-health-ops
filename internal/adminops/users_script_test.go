package adminops

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

// The fixture passwords of the users session. Each is at least 12 characters
// (CHAOS-10943: the operator verbs apply the shared password policy). The
// frozen Python golden (testdata/golden/users.json) was recorded with the
// legacy literals, so the golden's key is built from the script with every
// fixture swapped back to its legacy literal (legacyPasswords); the golden file
// is not edited.
const (
	fixturePasswordUnicode = "pässwörd-2026x"
	fixturePasswordNew     = "Newpass-99-zq1"
	// refusedShortPassword is short on purpose: the steps that use it expect a refusal.
	refusedShortPassword = "seven77"
)

var (
	fixturePassword1 = "Fixture" + "-pw-" + "1" + "a"
	fixturePassword2 = "Fixture" + "-pw-" + "2" + "b"
	fixturePassword3 = "Fixture" + "-pw-" + "3" + "c"

	fixturePasswordLongCreate = strings.Repeat("a", 72) + "1"
	fixturePasswordLongUpdate = strings.Repeat("b", 72) + "1"
)

// legacyPasswords maps each fixture to the literal the Python session used.
var legacyPasswords = []struct{ fixture, legacy string }{
	{fixturePassword1, "password1"},
	{fixturePassword2, "password2"},
	{fixturePassword3, "password3"},
	{fixturePasswordUnicode, "pässwörd"},
	{fixturePasswordNew, "newpass99"},
	{fixturePasswordLongCreate, strings.Repeat("a", 73)},
	{fixturePasswordLongUpdate, strings.Repeat("b", 73)},
}

// retiredUsersSteps are the steps whose Python answer the stricter policy
// retires (D6129): the 7-character refusals and the 73-byte refusals.
var retiredUsersSteps = map[int]bool{3: true, 6: true, 48: true, 50: true}

// legacyArgs is args as the Python session ran them.
func legacyArgs(args []string) []string {
	out := make([]string, len(args))
	for index, arg := range args {
		out[index] = arg
		for _, pair := range legacyPasswords {
			if arg == pair.fixture {
				out[index] = pair.legacy
			}
		}
	}
	return out
}

// usersGoldenInput is the golden's request body: the script as Python ran it.
func usersGoldenInput(t testing.TB) []byte {
	t.Helper()
	script := make([]map[string]any, len(usersScript))
	for index, s := range usersScript {
		script[index] = map[string]any{"args": legacyArgs(s.args), "seedTokenFor": s.seedTokenFor}
	}
	input, err := json.Marshal(script)
	if err != nil {
		t.Fatal(err)
	}
	return input
}

// usersGoldenBodySHA256 is body_sha256 of (sha256 of the base64 of the script JSON) testdata/golden/users.json.
const usersGoldenBodySHA256 = "1731fc546a60940f665ed4861aca9d77c5be76781c5acd728175ecfd224e14bd"

func sha256Text(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type step struct {
	args []string
	// seedTokenFor inserts a live refresh token for the user with this email
	// before the step runs (a password change must revoke it).
	seedTokenFor string
}

func verb(args ...string) step { return step{args: args} }

// A placeholder {{org:slug}} or {{user:email}} in an argument is replaced by the
// id the run holds for it.
var usersScript = []step{
	verb("users", "create", "--email", "alice@example.com", "--password", fixturePassword1, "--username", "alice", "--full-name", "Alice A", "--superuser"),
	verb("users", "create", "--email", "alice@example.com", "--password", fixturePassword1),
	verb("users", "create", "--email", "other@example.com", "--password", fixturePassword1, "--username", "alice"),
	verb("users", "create", "--email", "short@example.com", "--password", refusedShortPassword),
	verb("users", "create", "--email", "bob@example.com", "--password", fixturePassword2),
	verb("users", "create", "--email", "carol@example.com", "--password", fixturePasswordUnicode),
	verb("users", "create", "--email", "long@example.com", "--password", fixturePasswordLongCreate),
	verb("users", "create", "--email", "Upper@Example.COM", "--password", fixturePassword3, "--username", ""),
	verb("users", "create", "--email", "upper@example.com", "--password", fixturePassword3),
	verb("users", "create", "--password", fixturePassword3),
	// Python's str.lower() and str.strip() (not Go's): U+0130 lowers to two code
	// points, and \x1c..\x1f are whitespace to Python only.
	verb("users", "create", "--email", "\u0130-Review@Example.com", "--password", fixturePassword3),
	verb("users", "create", "--email", "\u0130-Review@Example.com", "--password", fixturePassword3),
	verb("users", "create", "--email", "i\u0307-review@example.com", "--password", fixturePassword3),
	verb("users", "create", "--email", "\x1cpadded@example.com\x1f", "--password", fixturePassword3, "--username", "\x1e\u0130User\x1c"),
	verb("users", "create", "--email", "other-padded@example.com", "--password", fixturePassword3, "--username", "i\u0307user"),
	verb("users", "list"),
	verb("users", "list", "--limit", "2"),
	verb("orgs", "create", "--name", "Acme Inc", "--owner-email", "alice@example.com"),
	verb("orgs", "create", "--name", "\x1c"),
	verb("orgs", "create", "--name", "\x1f\x1eTrimmed \u2003Name\x1c", "--slug", "trimmed"),
	verb("orgs", "create", "--name", "\x1c\u0130stanbul\x1f"),
	verb("orgs", "create", "--name", "Acme Inc"),
	verb("orgs", "create", "--name", "Acme Inc", "--slug", "acme-inc"),
	verb("orgs", "create", "--name", "   "),
	verb("orgs", "create", "--name", strings.Repeat("n", 101)),
	verb("orgs", "create", "--name", strings.Repeat("n", 100), "--slug", "hundred"),
	verb("orgs", "create", "--name", "Ünïcode Wörks & Co!", "--tier", "pro", "--description", "d"),
	verb("orgs", "create", "--name", "Ghost Owner", "--owner-email", "missing@example.com"),
	verb("orgs", "create", "--name", "Owned By Bob", "--owner-email", "bob@example.com", "--tier", "enterprise"),
	verb("orgs", "list"),
	verb("orgs", "list", "--limit", "2"),
	verb("users", "update"),
	verb("users", "update", "--email", "alice@example.com", "--role", "admin"),
	verb("users", "update", "--full-name", "Nobody"),
	verb("users", "update", "--email", "nobody@example.com", "--full-name", "Nobody"),
	verb("users", "update", "--id", "{{user:alice@example.com}}", "--full-name", "Alice Again", "--new-username", "alice2"),
	verb("users", "update", "--username", "alice2", "--new-username", ""),
	verb("users", "update", "--username", "alice2", "--full-name", "x"),
	verb("users", "update", "--email", "alice@example.com", "--new-username", "alice"),
	verb("users", "update", "--email", "bob@example.com", "--new-email", "  Bobby@Example.com ", "--no-verified", "--no-active"),
	verb("users", "list"),
	verb("users", "list", "--include-inactive"),
	verb("users", "update", "--email", "bobby@example.com", "--active", "--verified", "--no-superuser"),
	verb("users", "update", "--email", "bobby@example.com", "--new-email", "\x1c\u0130-Bobby@Example.com\x1f", "--new-username", "\x1e\u0130Bob\x1c"),
	verb("users", "update", "--email", "i\u0307-bobby@example.com", "--new-email", "\u0130-Review@Example.com"),
	verb("users", "update", "--email", "i\u0307-bobby@example.com", "--new-email", "bobby@example.com", "--new-username", ""),
	verb("users", "update", "--email", "bobby@example.com", "--new-email", "alice@example.com", "--full-name", "Rolled Back"),
	verb("users", "update", "--email", "bobby@example.com", "--new-username", "alice", "--full-name", "Rolled Back"),
	verb("users", "update", "--email", "bobby@example.com", "--full-name", "Half", "--password", refusedShortPassword),
	{args: []string{"users", "update", "--email", "bobby@example.com", "--password", fixturePasswordNew}, seedTokenFor: "bobby@example.com"},
	verb("users", "update", "--email", "bobby@example.com", "--password", fixturePasswordLongUpdate),
	verb("users", "update", "--email", "carol@example.com", "--org", "acme-inc"),
	verb("users", "update", "--email", "carol@example.com", "--org", "acme-inc", "--role", "viewer"),
	verb("users", "update", "--email", "carol@example.com", "--org", "{{org:acme-inc}}", "--role", "admin"),
	verb("users", "update", "--email", "carol@example.com", "--org", "acme-inc"),
	verb("users", "update", "--email", "carol@example.com", "--org", "no-such-org", "--full-name", "Rolled Back"),
	verb("users", "update", "--email", "carol@example.com", "--org", "00000000-0000-4000-8000-000000000000"),
	verb("users", "update", "--email", "carol@example.com", "--org", "{{org:owned-by-bob}}", "--role", "member"),
	verb("users", "update", "--email", "bobby@example.com", "--remove-from-org", "owned-by-bob"),
	verb("users", "update", "--email", "alice@example.com", "--remove-from-org", "acme-inc"),
	verb("users", "update", "--email", "carol@example.com", "--remove-from-org", "acme-inc"),
	verb("users", "update", "--email", "carol@example.com", "--remove-from-org", "acme-inc"),
	verb("users", "update", "--email", "carol@example.com", "--remove-from-org", "no-such-org"),
	verb("users", "update", "--email", "carol@example.com", "--org", "acme-inc", "--remove-from-org", "acme-inc"),
	verb("users", "update", "--email", "carol@example.com", "--org", "acme-inc", "--role", "root"),
	verb("users", "update", "--id", "not-a-uuid", "--full-name", "x"),
	verb("orgs", "list", "--include-inactive"),
	verb("users", "list", "--limit", "0"),
}
