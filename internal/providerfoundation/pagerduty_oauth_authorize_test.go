package providerfoundation

import (
	"crypto/sha256"
	"encoding/base64"
	"net/url"
	"strings"
	"testing"
)

func TestBuildPagerDutyAuthorizationRequest(t *testing.T) {
	config := PagerDutyRevokeConfig{ClientID: "client", RedirectURI: "https://app.example/callback", AuthorizationURL: "https://idp.example/authorize"}
	request, err := BuildPagerDutyAuthorizationRequest(config)
	if err != nil {
		t.Fatal(err)
	}
	if len(request.CodeVerifier) != 86 || len(request.State) != 43 || len(request.Nonce) != 43 {
		t.Fatalf("token lengths verifier=%d state=%d nonce=%d, want 86/43/43", len(request.CodeVerifier), len(request.State), len(request.Nonce))
	}
	parsed, err := url.Parse(request.URL)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Scheme+"://"+parsed.Host+parsed.Path != "https://idp.example/authorize" {
		t.Fatalf("endpoint %q", request.URL)
	}
	query := parsed.Query()
	sum := sha256.Sum256([]byte(request.CodeVerifier))
	for key, want := range map[string]string{
		"response_type": "code", "client_id": "client", "redirect_uri": "https://app.example/callback",
		"scope": "escalation_policies.read incidents.read oncalls.read schedules.read services.read teams.read users.read",
		"state": request.State, "nonce": request.Nonce,
		"code_challenge":        strings.TrimRight(base64.URLEncoding.EncodeToString(sum[:]), "="),
		"code_challenge_method": "S256",
	} {
		if query.Get(key) != want {
			t.Errorf("%s = %q, want %q", key, query.Get(key), want)
		}
	}
	other, _ := BuildPagerDutyAuthorizationRequest(config)
	if other.State == request.State || other.CodeVerifier == request.CodeVerifier {
		t.Fatal("two requests share a random value")
	}
	if !strings.HasPrefix((PagerDutyRevokeConfig{}).authorizationURL(), "https://identity.pagerduty.com/oauth/authorize") {
		t.Fatal("default authorization URL")
	}
}
