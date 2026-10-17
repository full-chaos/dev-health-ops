package principal

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"log/slog"
	"strings"
	"testing"
)

func verifyWith(t *testing.T, mutate func(*Claims)) *Claims {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	c := baseClaims()
	mutate(&c)
	v := mustVerifier(t, writeJWKS(t, pub, testKID), testIssuer, testAudience)
	got, err := v.Verify(context.Background(), signEnvelope(t, priv, testKID, c))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	return got
}

func TestVerify_SuperuserDiscipline(t *testing.T) {
	impersonator := "22222222-2222-4222-8222-222222222222"
	empty := ""
	cases := []struct {
		name           string
		mutate         func(*Claims)
		wantSuperuser  bool
		wantStripLogOf string
	}{
		{"hint only, not verified", func(c *Claims) { c.IsSuperuser = true }, false, "superuser_unverified"},
		{"verified without hint", func(c *Claims) { c.IsSuperuserVerified = true }, false, ""},
		{"both flags", func(c *Claims) { c.IsSuperuser, c.IsSuperuserVerified = true, true }, true, ""},
		{"both flags while impersonation active", func(c *Claims) {
			c.IsSuperuser, c.IsSuperuserVerified, c.ImpersonationActive = true, true, true
		}, false, "superuser_while_impersonating"},
		{"both flags with an impersonator named", func(c *Claims) {
			c.IsSuperuser, c.IsSuperuserVerified, c.ImpersonatedBy = true, true, &impersonator
		}, false, "superuser_while_impersonating"},
		{"both flags with an empty impersonator", func(c *Claims) {
			c.IsSuperuser, c.IsSuperuserVerified, c.ImpersonatedBy = true, true, &empty
		}, true, ""},
		{"hint only while impersonating", func(c *Claims) {
			c.IsSuperuser, c.ImpersonationActive = true, true
		}, false, "superuser_while_impersonating"},
		{"no superuser claims", func(*Claims) {}, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			old := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			defer slog.SetDefault(old)

			got := verifyWith(t, tc.mutate)
			if got.IsSuperuser != tc.wantSuperuser {
				t.Errorf("IsSuperuser = %v, want %v", got.IsSuperuser, tc.wantSuperuser)
			}
			if !tc.wantSuperuser && got.IsSuperuser != got.IsSuperuserVerified && got.IsSuperuser {
				t.Errorf("IsSuperuserVerified survived without a superuser grant")
			}
			if tc.name != "verified without hint" && !tc.wantSuperuser && got.IsSuperuserVerified {
				t.Errorf("IsSuperuserVerified survived without a superuser grant")
			}
			if tc.wantSuperuser && !got.IsSuperuserVerified {
				t.Errorf("a granted superuser lost its verified flag")
			}
			stripped := strings.Contains(logs.String(), "superuser_stripped")
			if (tc.wantStripLogOf != "") != stripped || (stripped && !strings.Contains(logs.String(), tc.wantStripLogOf)) {
				t.Errorf("strip log = %q, want reason %q", logs.String(), tc.wantStripLogOf)
			}
		})
	}
}
