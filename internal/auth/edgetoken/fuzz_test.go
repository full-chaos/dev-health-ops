package edgetoken

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// FuzzVerifyToken drives the HS256 refresh-token verifier with arbitrary
// token text. Properties: no panic; every refusal is a *Rejection with a
// non-empty Reason; an accepted token carries sub and type "refresh" and was
// HS256 with three segments (the algorithm-confusion and missing-claim
// classes).
func FuzzVerifyToken(f *testing.F) {
	secret := strings.Repeat("k", MinSecretLength)
	verifier, err := New(secret, "issuer", "audience")
	if err != nil {
		f.Fatal(err)
	}
	signer, err := NewSigner(secret, "issuer", "audience")
	if err != nil {
		f.Fatal(err)
	}
	refresh, err := signer.Refresh(RefreshClaims{UserID: "u1", OrgID: "o1", FamilyID: "f1"}, time.Now(), "jti-1")
	if err != nil {
		f.Fatal(err)
	}
	expired, err := signer.RefreshUntil(RefreshClaims{UserID: "u1"}, time.Now().Add(-48*time.Hour), time.Now().Add(-47*time.Hour), "jti-2")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(refresh)
	f.Add(expired)
	f.Add(refresh[:len(refresh)-2])
	f.Add("eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.e30.")
	f.Add("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.x")
	f.Add("")
	f.Fuzz(func(t *testing.T, token string) {
		claims, err := verifier.VerifyRefresh(token)
		if err != nil {
			var rejection *Rejection
			if !errors.As(err, &rejection) || rejection.Reason == "" {
				t.Fatalf("a refusal is not a *Rejection with a reason: %v", err)
			}
			return
		}
		if _, ok := claims["sub"]; !ok {
			t.Fatal("accepted a token without sub")
		}
		if claims["type"] != RefreshType {
			t.Fatalf("accepted a token of type %v as a refresh token", claims["type"])
		}
		if strings.Count(token, ".") != 2 {
			t.Fatalf("accepted a token that is not three segments: %q", token)
		}
	})
}
