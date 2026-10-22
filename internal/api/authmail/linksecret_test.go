package authmail

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
)

func valid(t *testing.T, c Config, token string) bool {
	t.Helper()
	ok, err := signedtoken.ValidAny(token, c.AcceptSecrets())
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func TestLinkSecretUnsetKeepsTheLegacyChain(t *testing.T) {
	c := Config{TokenSecret: "access-key"}
	if got := c.Secret(); got != "access-key" {
		t.Fatalf("signing secret %q, want the legacy chain secret", got)
	}
	token, _ := signedtoken.Build(uuid.New(), c.Secret())
	if !valid(t, c, token) {
		t.Fatal("a token signed with the legacy secret is refused with no link secret set")
	}
	if got := (Config{}).AcceptSecrets(); len(got) != 1 || got[0] != signedtoken.DevFallbackSecret {
		t.Fatalf("zero Config accepts %v, want only the last-resort default", got)
	}
}

func TestLinkSecretSignsAndKeepsPreRollTokensValid(t *testing.T) {
	id := uuid.New()
	preRoll, _ := signedtoken.Build(id, "access-key")

	rolled := Config{TokenSecret: "access-key", LinkSecret: "link-key"}
	if got := rolled.Secret(); got != "link-key" {
		t.Fatalf("signing secret %q, want the link secret", got)
	}
	if !valid(t, rolled, preRoll) {
		t.Fatal("a link issued before the roll is refused after it")
	}
	fresh, _ := signedtoken.Build(uuid.New(), rolled.Secret())
	if !valid(t, rolled, fresh) {
		t.Fatal("a link issued after the roll is refused")
	}
	if ok, _ := signedtoken.ValidAny(fresh, []string{"access-key"}); ok {
		t.Fatal("a post-roll link validates under the access key alone")
	}
}

func TestLinkSecretRotationAcceptsThePreviousAndDropsTheLegacyKeyWhenRemoved(t *testing.T) {
	id := uuid.New()
	underOld, _ := signedtoken.Build(id, "link-old")
	rotated := Config{LinkSecret: "link-new", PreviousLinkSecret: "link-old"}
	if !valid(t, rotated, underOld) {
		t.Fatal("a link signed with the previous secret is refused during the overlap")
	}
	if got := rotated.AcceptSecrets(); len(got) != 2 {
		t.Fatalf("accepts %v, want the link secret and the previous one only (no last-resort default)", got)
	}
	overlapEnded := Config{LinkSecret: "link-new"}
	if valid(t, overlapEnded, underOld) {
		t.Fatal("a link signed with the previous secret validates after the overlap ended")
	}
	unrelated, _ := signedtoken.Build(id, "someone-else")
	if valid(t, rotated, unrelated) {
		t.Fatal("a link signed with an unrelated secret validates")
	}
}

func TestLinkSecretAcceptListHasNoDuplicatesOrBlanks(t *testing.T) {
	c := Config{TokenSecret: "same", LinkSecret: "same", PreviousLinkSecret: "same"}
	if got := c.AcceptSecrets(); len(got) != 1 || got[0] != "same" {
		t.Fatalf("accepts %v, want one entry", got)
	}
}
