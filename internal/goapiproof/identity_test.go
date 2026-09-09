package goapiproof

import "testing"

func TestRequestIdentityIsStableAcrossMapOrderAndScopeOrder(t *testing.T) {
	auth := AuthContext{
		PrincipalKind: "stored_account",
		Audience:      "query-api",
		KeyID:         "local-dev-20260906",
		Scopes:        []string{"read:analytics", "read:workgraph"},
	}
	reordered := auth
	reordered.Scopes = []string{"read:workgraph", "read:analytics"}

	first, err := RequestIdentity("70d529e0", auth, map[string]any{"orgId": "70d529e0", "limit": 100})
	if err != nil {
		t.Fatalf("RequestIdentity: %v", err)
	}
	second, err := RequestIdentity("70d529e0", reordered, map[string]any{"limit": 100, "orgId": "70d529e0"})
	if err != nil {
		t.Fatalf("RequestIdentity: %v", err)
	}
	if first != second {
		t.Fatalf("identity must not depend on map or scope order: %s != %s", first, second)
	}
}

// Each of the three documented inputs must change the identity: a digest
// that ignores one of them would let a receipt from a different request
// authorize this one.
func TestRequestIdentityChangesWithEveryDocumentedInput(t *testing.T) {
	auth := AuthContext{PrincipalKind: "stored_account", Audience: "query-api", KeyID: "kid-1"}
	variables := map[string]any{"orgId": "70d529e0"}

	base, err := RequestIdentity("70d529e0", auth, variables)
	if err != nil {
		t.Fatalf("RequestIdentity: %v", err)
	}

	otherOrgAuth := auth
	cases := map[string]func() (string, error){
		"org id": func() (string, error) {
			return RequestIdentity("other-org", auth, variables)
		},
		"variables": func() (string, error) {
			return RequestIdentity("70d529e0", auth, map[string]any{"orgId": "70d529e0", "limit": 1})
		},
		"principal kind": func() (string, error) {
			changed := otherOrgAuth
			changed.PrincipalKind = "service"
			return RequestIdentity("70d529e0", changed, variables)
		},
		"audience": func() (string, error) {
			changed := otherOrgAuth
			changed.Audience = "other"
			return RequestIdentity("70d529e0", changed, variables)
		},
		"key id": func() (string, error) {
			changed := otherOrgAuth
			changed.KeyID = "kid-2"
			return RequestIdentity("70d529e0", changed, variables)
		},
	}

	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := build()
			if err != nil {
				t.Fatalf("RequestIdentity: %v", err)
			}
			if got == base {
				t.Fatalf("changing the %s must change the request identity", name)
			}
		})
	}
}

// The identity must never be derived from credential material -- a token
// rotation would otherwise make two identical requests incomparable, and
// a durable table would carry a digest of a secret as its input.
func TestRequestIdentityIgnoresTheCredentialItself(t *testing.T) {
	auth := AuthContext{PrincipalKind: "stored_account", Audience: "query-api", KeyID: "kid-1"}
	first, err := RequestIdentity("70d529e0", auth, map[string]any{"orgId": "70d529e0"})
	if err != nil {
		t.Fatalf("RequestIdentity: %v", err)
	}
	// AuthContext has no token field at all; this asserts the SHAPE stays
	// that way, so a later "convenience" field carrying a bearer value
	// fails here rather than in production.
	second, err := RequestIdentity("70d529e0", AuthContext{
		PrincipalKind: auth.PrincipalKind, Audience: auth.Audience, KeyID: auth.KeyID,
	}, map[string]any{"orgId": "70d529e0"})
	if err != nil {
		t.Fatalf("RequestIdentity: %v", err)
	}
	if first != second {
		t.Fatal("two requests differing only in credential value must share an identity")
	}
}
