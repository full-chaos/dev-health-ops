package ratelimitvalkey

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The revision feed (CHAOS-10983) runs under the same login: the one script reads and writes one hash under the existing
// auth_svc_ key prefix and publishes on one channel under auth_svc_, and the subscriber subscribes to it. The rule is
// pinned as a whole so a change is a reviewed contract change (acp PLAN GO 10983 ruling 7).
const feedACLRules = "resetkeys ~dho:rl:auth_svc_* resetchannels &auth_svc_* -@all +hello +select +ping +get +eval +incr +pexpire +hget +hset +publish +subscribe +client|tracking (~dho:rl:auth_svc_rev_hwm +hgetall)"

func TestTheAclRuleIsTheReviewedLineWithTheFeedChannelAndCommands(t *testing.T) {
	if ACLRules != feedACLRules {
		t.Fatalf("ACLRules =\n%s\nwant\n%s", ACLRules, feedACLRules)
	}
}

func TestTheAclRuleScopesTheFeedToTheExistingPrefixes(t *testing.T) {
	if !strings.Contains(ACLRules, "resetchannels &auth_svc_*") {
		t.Fatal("the channel pattern is not reset and scoped to &auth_svc_*")
	}
	if strings.Count(ACLRules, "~") != 2 || strings.Count(ACLRules, "&") != 1 {
		t.Fatalf("the rule names more than one key or channel pattern: %s", ACLRules)
	}
	// HGETALL is granted by a selector scoped to the ONE hash, never by the root permissions (which cover every auth_svc_ key).
	root, selector, ok := strings.Cut(ACLRules, " (")
	if !ok || selector != "~dho:rl:auth_svc_rev_hwm +hgetall)" || strings.Contains(root, "hgetall") {
		t.Fatalf("hgetall is not confined to a selector on the hwm hash: %s", ACLRules)
	}
	for _, forbidden := range []string{"&*", "allchannels", "+psubscribe", "+hscan", "+info", "+config", "+hdel", "+del", "+unlink", "+expire", "+config", "+@pubsub", "+@hash", "+evalsha", "+script"} {
		if strings.Contains(ACLRules, forbidden) {
			t.Errorf("the rule grants %s", forbidden)
		}
	}
}

func TestTheChartValuesAndTheDocsCarryTheFeedAclLine(t *testing.T) {
	for _, rel := range []string{"docs/reference/auth/configuration.md", "deploy/helm/dev-health/values.yaml", "docs/contribute/architecture/auth-control-plane-audit-outbox.md"} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "..", filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), feedACLRules) {
			t.Errorf("%s does not carry the ACL rule %q", rel, feedACLRules)
		}
	}
}
