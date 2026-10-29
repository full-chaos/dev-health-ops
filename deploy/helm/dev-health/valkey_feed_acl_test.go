package devhealth_test

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-10983 PR B: the chart documents, in values.yaml, the ACL line of the auth-service Valkey user including the
// revision feed's channel and commands (acp PLAN GO ruling 7: scope the channel pattern to the existing prefix, keep the
// database index, pin the line).
const feedValkeyACLLine = "user auth-service-ratelimit on #<sha256 hex of the password> resetkeys ~dho:rl:auth_svc_* resetchannels &auth_svc_* -@all +hello +select +ping +get +eval +incr +pexpire +hget +hset +publish +subscribe +client|tracking"

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
	for _, forbidden := range []string{"&* ", "~* &auth"} {
		if strings.Contains(feedValkeyACLLine, forbidden) {
			t.Fatalf("the pinned line contains %q", forbidden)
		}
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
