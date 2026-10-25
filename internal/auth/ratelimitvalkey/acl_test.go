package ratelimitvalkey

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The ACL rule is written in three places (the code constant, the runbook line,
// the chart's ACL file format); a change to one that misses another would give
// the service a login it cannot use or one that reaches too far.
func TestTheDocumentedAclRuleIsTheCodesRule(t *testing.T) {
	for _, rel := range []string{"docs/reference/auth/configuration.md", "deploy/helm/dev-health/values.yaml"} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "..", filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), ACLUser) || !strings.Contains(string(raw), ACLRules) {
			t.Errorf("%s does not carry the user %q with the rule %q", rel, ACLUser, ACLRules)
		}
	}
}

func TestTheAclRuleReachesOnlyTheServicesPrefix(t *testing.T) {
	if !strings.Contains(ACLRules, "~"+keyPrefix+"auth_svc_*") {
		t.Fatalf("the rule %q does not name the key prefix %q", ACLRules, keyPrefix+"auth_svc_*")
	}
	for _, word := range []string{"resetkeys", "resetchannels", "-@all"} {
		if !strings.Contains(ACLRules, word) {
			t.Errorf("the rule lacks %s", word)
		}
	}
	for _, forbidden := range []string{"+@all", "allkeys", "~*", "&*", "nopass", "+flushall", "+keys", "+config"} {
		if strings.Contains(ACLRules, forbidden) {
			t.Errorf("the rule grants %s", forbidden)
		}
	}
}

// The file's shape is pinned for a fixed password: two lines, the default line as
// today, then the rate-limit user with "#" and the 64 lowercase hex characters of
// the password's SHA-256. The password itself appears nowhere in it.
func TestTheAclFileHoldsTheHashAndNeverThePassword(t *testing.T) {
	const password = "pin-test-password"
	const hash = "21d3c976eedf39b691cc76987b76c3a58b28949b3062cb31a3b030f194f5af26"
	file := ACLFile(password)
	want := "user default on nopass ~* &* +@all\n" +
		"user auth-service-ratelimit on #" + hash + " " + ACLRules + "\n"
	if file != want {
		t.Fatalf("ACL file =\n%s\nwant\n%s", file, want)
	}
	if strings.Contains(file, password) {
		t.Fatal("the plain password is in the ACL file")
	}
	lines := strings.Split(strings.TrimSuffix(file, "\n"), "\n")
	if len(lines) != 2 || lines[0] != ACLDefaultUserLine {
		t.Fatalf("lines %q", lines)
	}
	if !regexp.MustCompile(`^user ` + ACLUser + ` on #[0-9a-f]{64} `).MatchString(lines[1]) {
		t.Fatalf("the user line is not '#<sha256 hex>': %q", lines[1])
	}
}

// The command list is the one the code calls: the store's builder commands, the
// commands inside its Lua script, the EVAL that runs the script, the readiness
// and open PINGs, and the valkey-go handshake (HELLO, SELECT for the URL's
// database, CLIENT TRACKING while client caching stays on; the last three are
// what the CHAOS-10684 review observed the handshake sending). A command added
// to the store without the rule, or a grant the code does not use, fails here.
func TestTheAclCommandsAreExactlyTheOnesTheCodeCalls(t *testing.T) {
	read := func(rel string) string {
		raw, err := os.ReadFile(filepath.FromSlash(rel))
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	used := map[string]bool{"eval": true, "hello": true, "select": true, "client|tracking": true}
	store := read("store.go")
	for _, m := range regexp.MustCompile(`\.B\(\)\.(\w+)\(\)`).FindAllStringSubmatch(store, -1) {
		used[strings.ToLower(m[1])] = true
	}
	for _, m := range regexp.MustCompile(`redis\.call\('(\w+)'`).FindAllStringSubmatch(store, -1) {
		used[strings.ToLower(m[1])] = true
	}
	for _, rel := range []string{"../../storage/valkey/factory.go", "../authruntime/ratelimit.go"} {
		for _, m := range regexp.MustCompile(`\.B\(\)\.(\w+)\(\)`).FindAllStringSubmatch(read(rel), -1) {
			used[strings.ToLower(m[1])] = true
		}
	}
	granted := map[string]bool{}
	for _, token := range strings.Fields(ACLRules) {
		if strings.HasPrefix(token, "+") {
			granted[strings.TrimPrefix(token, "+")] = true
		}
	}
	for command := range used {
		if !granted[command] {
			t.Errorf("the code calls %q and the rule does not grant it", command)
		}
	}
	for command := range granted {
		if !used[command] {
			t.Errorf("the rule grants %q and nothing in the code calls it", command)
		}
	}
	if len(used) != 8 {
		t.Errorf("%d commands found in the code (%v), want 8: a parser that reads less must not pass", len(used), used)
	}
}
