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

func TestLinkSecretSignsAndOnlyItsOwnKeysAccept(t *testing.T) {
	id := uuid.New()
	formerChain, _ := signedtoken.Build(id, "access-key")

	rolled := Config{LinkSecret: "link-key"}
	if got := rolled.Secret(); got != "link-key" {
		t.Fatalf("signing secret %q, want the link secret", got)
	}
	if valid(t, rolled, formerChain) {
		t.Fatal("a link signed with a key outside the link secrets is accepted")
	}
	fresh, _ := signedtoken.Build(uuid.New(), rolled.Secret())
	if !valid(t, rolled, fresh) {
		t.Fatal("a link signed with the link secret is refused")
	}
}

func TestLinkSecretRotationAcceptsThePreviousAndDropsItWhenRemoved(t *testing.T) {
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
	c := Config{LinkSecret: "same", PreviousLinkSecret: "same"}
	if got := c.AcceptSecrets(); len(got) != 1 || got[0] != "same" {
		t.Fatalf("accepts %v, want one entry", got)
	}
}
