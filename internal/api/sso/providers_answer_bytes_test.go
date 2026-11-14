package sso

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/ssoprovider"
)

// auth-service's provider config is byte-equal to the config member go-api's providerResponse renders from the same stored text
// (product rule 1: the answer after the move is the answer before it).
func TestAuthServiceProviderConfigIsByteEqualToProviderResponse(t *testing.T) {
	long := strings.Repeat("Z", 80)
	for name, stored := range map[string]string{
		"non-alphabetical, legacy spacing": `{"scopes": ["openid", "profile"], "client_id": "c", "issuer": "https://i.example", "claim_mapping": {"z": "1", "a": "2"}, "client_secret": "plain"}`,
		"cut certificate":                  `{"entity_id": "e", "sso_url": "https://s", "certificate": "` + long + `", "slo_url": null}`,
		"50 and 51 characters":             `{"certificate": "` + strings.Repeat("Y", 51) + `", "x": "` + strings.Repeat("Y", 50) + `"}`,
		"unicode and html":                 `{"issuer": "https://\u00e9.example", "note": "a<b>&c\u2028\ud83d\ude00"}`,
		"nested and numbers":               `{"b": 1.50, "a": [1, 2.0, {"d": null, "c": true}], "n": 12345678901234567890}`,
		"empty":                            `{}`,
	} {
		stored := stored
		row := &providerRow{ID: "i", OrgID: "o", Name: "n", Protocol: "oidc", Status: "active", DefaultRole: "member", Config: &stored,
			CreatedAt: time.Unix(0, 0).UTC(), UpdatedAt: time.Unix(0, 0).UTC()}
		out, err := providerResponse(row)
		if err != nil {
			t.Fatalf("%s: providerResponse: %v", name, err)
		}
		whole, err := pyjson.MarshalModel(out)
		if err != nil {
			t.Fatal(err)
		}
		var members map[string]json.RawMessage
		if err := json.Unmarshal(whole, &members); err != nil {
			t.Fatal(err)
		}
		got, err := ssoprovider.MaskedConfig(&stored)
		if err != nil {
			t.Fatalf("%s: MaskedConfig: %v", name, err)
		}
		if string(got) != string(members["config"]) {
			t.Errorf("%s:\n auth-service %s\n go-api       %s", name, got, members["config"])
		}
	}
}
