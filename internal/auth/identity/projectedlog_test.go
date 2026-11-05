package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

const projectedRow = "0b7e1d3c-5a2f-4c8e-9f10-2d3a4b5c6d7e"

func attrMap(t *testing.T, attrs []any) map[string]string {
	t.Helper()
	if len(attrs)%2 != 0 {
		t.Fatalf("odd attr list %v", attrs)
	}
	out := map[string]string{}
	for i := 0; i < len(attrs); i += 2 {
		out[attrs[i].(string)] = fmt.Sprint(attrs[i+1])
	}
	return out
}

func TestProjectedLogAttrs_NamesRowAndProtocol(t *testing.T) {
	sum := sha256.Sum256([]byte(projectedRow))
	wantRef := hex.EncodeToString(sum[:])[:12]
	sso := ExternalKey{Issuer: "https://idp.example", Provider: Provider(projectedRow), Subject: "s-1"}
	cases := []struct {
		name     string
		key      ExternalKey
		protocol string
		want     map[string]string
	}{
		{"sso oidc", sso, "oidc", map[string]string{"provider": "sso", "provider_ref": wantRef, "protocol": "oidc"}},
		{"sso saml", sso, "saml", map[string]string{"provider": "sso", "provider_ref": wantRef, "protocol": "saml"}},
		{"sso unknown type", sso, "weird type!", map[string]string{"provider": "sso", "provider_ref": wantRef, "protocol": "other"}},
		{"sso backfill", sso, "", map[string]string{"provider": "sso", "provider_ref": wantRef, "protocol": "unknown"}},
		{"social", ExternalKey{Issuer: "https://github.com", Provider: ProviderGitHub, Subject: "42"}, "github",
			map[string]string{"provider": "github", "protocol": "github"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := attrMap(t, projectedLogAttrs(tc.key, tc.protocol))
			if len(got) != len(tc.want) {
				t.Fatalf("attrs = %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("attr %s = %q, want %q (all: %v)", k, got[k], v, got)
				}
			}
		})
	}
}

func TestProjectedLogAttrs_NoSubjectNoUserID(t *testing.T) {
	key := ExternalKey{Issuer: "https://idp.example", Provider: Provider(projectedRow), Subject: "subject-secret-77"}
	for _, protocol := range []string{"oidc", "", "subject-secret-77"} {
		attrs := projectedLogAttrs(key, protocol)
		for _, v := range attrs {
			s := fmt.Sprint(v)
			if strings.Contains(s, "subject-secret-77") || strings.Contains(s, projectedRow) || uuidLike.MatchString(s) {
				t.Errorf("attr value %q leaks a subject, row id or uuid (protocol %q)", s, protocol)
			}
		}
		got := attrMap(t, attrs)
		for k := range got {
			if k != "provider" && k != "provider_ref" && k != "protocol" {
				t.Errorf("unexpected attr key %q", k)
			}
		}
	}
}

var uuidLike = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
