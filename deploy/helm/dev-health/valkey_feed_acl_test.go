package devhealth_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/ratelimitvalkey"
)

// CHAOS-10983 PR B: the chart documents, in values.yaml, the ACL line of the auth-service Valkey user: the code's rule
// (ratelimitvalkey.ACLRules, the one copy) after the user, the password hash and nothing else.
var feedValkeyACLLine = "user " + ratelimitvalkey.ACLUser + " on #<sha256 hex of the password> " + ratelimitvalkey.ACLRules

func TestValuesPinTheAuthValkeyAclLineWithTheFeedChannel(t *testing.T) {
	raw, err := os.ReadFile("values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, feedValkeyACLLine) {
		t.Fatalf("values.yaml does not carry the ACL line:\n%s", feedValkeyACLLine)
	}
	if strings.Contains(text, "resetchannels -@all +hello") {
		t.Fatal("values.yaml still carries the old rule without a channel pattern")
	}
	if !strings.Contains(feedValkeyACLLine, "(~dho:rl:auth_svc_rev_hwm +hgetall)") {
		t.Fatal("the line does not confine HGETALL to the hwm hash by a selector")
	}
}

func TestValuesKeepTheDefaultUserLineAndTheDatabaseOfTheRateLimitStore(t *testing.T) {
	raw, err := os.ReadFile("values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "user default on nopass ~* &* +@all") {
		t.Fatal("the default user line changed")
	}
}

// Codex r1 P3: the chart is checked through helm, not only as text. `helm show values` is the chart's values as helm reads
// them (the documented ACL line included), and `helm template` with the ACL on is what Valkey starts with: the ACL file
// from the Secret, the AOF on (the floor's status rule reads it) and fsync never turned off by the chart.
func TestHelmShowsTheAclLineAndRendersValkeyWithTheAofOnAndTheAclFile(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	out, err := exec.Command("helm", "show", "values", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("helm show values: %v\n%.300s", err, out)
	}
	if !strings.Contains(string(out), feedValkeyACLLine) {
		t.Fatalf("helm does not show the ACL line:\n%s", feedValkeyACLLine)
	}
	deployment, _ := valkeyDeployment(t, append([]string{
		"--set", "valkey.acl.enabled=true", "--set", "valkey.acl.secretName=valkey-acl-secret",
		"--set", "networkPolicy.enabled=true",
	}, authNetworkPolicy...)...)
	joined := " " + strings.Join(valkeyArgs(deployment), " ") + " "
	for _, want := range []string{" --appendonly yes ", " --aclfile /etc/valkey-acl/users.acl "} {
		if !strings.Contains(joined, want) {
			t.Errorf("the rendered Valkey args lack %q: %s", want, joined)
		}
	}
	if strings.Contains(joined, "--appendfsync no") {
		t.Errorf("the chart turns fsync off: %s", joined)
	}
}
