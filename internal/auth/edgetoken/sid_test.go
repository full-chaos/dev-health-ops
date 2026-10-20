package edgetoken

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func payloadOf(t *testing.T, token string) string {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d segments", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// The sid claim is additive: without a SessionID the bytes are the go-api
// bytes, with one the claim is appended last and every reader still verifies
// the token.
func TestSessionIDClaimIsAppendedLastAndOnlyWhenSet(t *testing.T) {
	key := syntheticKey()
	signer, err := NewSigner(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	base := AccessClaims{UserID: "u", Email: "e@example.com", OrgID: "", Role: "member", TokenVersion: 3}

	without, err := signer.Access(base, now, "jti-1")
	if err != nil {
		t.Fatal(err)
	}
	empty := ""
	withEmpty := base
	withEmpty.SessionID = &empty
	sameAsWithout, err := signer.Access(withEmpty, now, "jti-1")
	if err != nil {
		t.Fatal(err)
	}
	if sameAsWithout != without {
		t.Fatal("an empty SessionID changed the token")
	}
	if strings.Contains(payloadOf(t, without), `"sid"`) {
		t.Fatal("a token without SessionID carries sid")
	}

	sid := "ses_AAAAAAAAAAAAAAAAAAAAAA"
	with := base
	with.SessionID = &sid
	token, err := signer.Access(with, now, "jti-1")
	if err != nil {
		t.Fatal(err)
	}
	payload := payloadOf(t, token)
	if want := strings.TrimSuffix(payloadOf(t, without), "}") + `,"sid":"` + sid + `"}`; payload != want {
		t.Fatalf("payload = %s, want %s", payload, want)
	}
	claims, err := verifier.Verify(token)
	if err != nil {
		t.Fatalf("Verify refused a token with sid: %v", err)
	}
	if claims["sid"] != sid {
		t.Fatalf("sid = %v", claims["sid"])
	}
}
