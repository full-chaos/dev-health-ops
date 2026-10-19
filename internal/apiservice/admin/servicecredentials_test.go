package admin

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestServiceTokenShapeAndHash(t *testing.T) {
	previous := serviceCredentialRandom
	defer func() { serviceCredentialRandom = previous }()

	serviceCredentialRandom = bytes.NewReader(make([]byte, 32))
	token, err := generateServiceToken(ServiceACR)
	if err != nil {
		t.Fatal(err)
	}
	// 32 zero bytes are 43 'A's in unpadded base64url, which is token_urlsafe(32)'s form.
	if want := "svc_acr_" + strings.Repeat("A", 43); token != want {
		t.Fatalf("token %q, want %q", token, want)
	}
	serviceCredentialRandom = bytes.NewReader(bytes.Repeat([]byte{0xff}, 32))
	worker, err := generateServiceToken(ServiceWorkerOperator)
	if err != nil {
		t.Fatal(err)
	}
	if want := "svc_worker_" + strings.Repeat("_", 42) + "8"; worker != want {
		t.Fatalf("worker token %q, want %q", worker, want)
	}
	if got := HashServiceToken("abc"); got != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("hash %s", got)
	}
	serviceCredentialRandom = bytes.NewReader(make([]byte, 5))
	if _, err := generateServiceToken(ServiceACR); err == nil {
		t.Fatal("a short random source was accepted")
	}
}

func TestServiceCredentialScopes(t *testing.T) {
	got, err := ServiceCredentialScopes(ServiceWorkerOperator, []string{"workers:read", "workers:operate", "workers:read"})
	if err != nil || strings.Join(got, ",") != "workers:operate,workers:read" {
		t.Fatalf("scopes %v err %v", got, err)
	}
	for name, tc := range map[string]struct {
		service string
		scopes  []string
	}{
		"none":              {ServiceACR, nil},
		"unknown":           {ServiceACR, []string{"nope"}},
		"another service's": {ServiceACR, []string{"workers:read"}},
		"one bad of two":    {ServiceWorkerOperator, []string{"workers:read", "entitlements:read"}},
		"unknown service":   {"bogus", []string{"entitlements:read"}},
	} {
		if _, err := ServiceCredentialScopes(tc.service, tc.scopes); err == nil {
			t.Errorf("%s: accepted", name)
		} else {
			var refusal *ServiceCredentialError
			if !errors.As(err, &refusal) || refusal.Message != "unsupported internal service credential scope" {
				t.Errorf("%s: %v", name, err)
			}
		}
	}
	if !equalStrings(ServiceNames(), []string{"acr", "worker-llm-config", "worker-operator"}) {
		t.Fatalf("service names %v", ServiceNames())
	}
}

func equalStrings(a, b []string) bool { return strings.Join(a, "\x00") == strings.Join(b, "\x00") }

func TestServiceCredentialExpiry(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	text := func(s string) *string { return &s }
	if got, err := ParseServiceCredentialExpiry(nil, now); got != nil || err != nil {
		t.Fatalf("no expiry: %v %v", got, err)
	}
	got, err := ParseServiceCredentialExpiry(text("2026-09-26T18:30:00+05:30"), now)
	if err != nil || !got.Equal(time.Date(2026, 9, 26, 13, 0, 0, 0, time.UTC)) || got.Location() != time.UTC {
		t.Fatalf("offset expiry %v %v", got, err)
	}
	for name, raw := range map[string]string{
		"naive":       "2026-09-27T00:00:00",
		"past":        "2026-09-26T11:59:59+00:00",
		"exactly now": "2026-09-26T12:00:00+00:00",
		"garbage":     "tomorrow",
		"empty":       "",
	} {
		if _, err := ParseServiceCredentialExpiry(text(raw), now); err == nil {
			t.Errorf("%s: %q accepted", name, raw)
		}
	}
	_, err = ParseServiceCredentialExpiry(text("2026-09-27T00:00:00"), now)
	if err == nil || err.Error() != "--expires-at must include a timezone" {
		t.Fatalf("naive message %v", err)
	}
	_, err = ParseServiceCredentialExpiry(text("2001-01-01T00:00:00+00:00"), now)
	if err == nil || err.Error() != "--expires-at must be in the future" {
		t.Fatalf("past message %v", err)
	}
}

func TestServiceCredentialIDs(t *testing.T) {
	if got, err := ParseServiceCredentialCreator(""); got != nil || err != nil {
		t.Fatalf("empty creator: %v %v", got, err)
	}
	for _, raw := range []string{"00000000-0000-4000-8000-0000000000aa", "{00000000-0000-4000-8000-0000000000AA}", "urn:uuid:00000000-0000-4000-8000-0000000000aa", "00000000000040008000-0000000000aa"} {
		if got, err := ParseServiceCredentialCreator(raw); err != nil || got.String() != "00000000-0000-4000-8000-0000000000aa" {
			t.Errorf("%q: %v %v", raw, got, err)
		}
		if id, err := ParseServiceCredentialID(raw); err != nil || id.String() != "00000000-0000-4000-8000-0000000000aa" {
			t.Errorf("id %q: %v %v", raw, id, err)
		}
	}
	for _, raw := range []string{"bad", "0000", "00000000-0000-4000-8000-0000000000zz"} {
		if _, err := ParseServiceCredentialID(raw); err == nil || err.Error() != "badly formed hexadecimal UUID string" {
			t.Errorf("%q: %v", raw, err)
		}
	}
}

func TestMetadataDocumentIsPublicMetadata(t *testing.T) {
	at := time.Date(2099, 1, 2, 3, 4, 5, 123456000, time.UTC)
	document := ServiceCredentialMetadata{ID: "i", ServiceName: "acr", TokenPrefix: "svc_acr_abcdefgh", Scopes: []string{"entitlements:read"}, ExpiresAt: &at}.Document(time.Date(2098, 1, 1, 0, 0, 0, 0, time.UTC))
	if document["expires_at"] != "2099-01-02T03:04:05.123456+00:00" || document["revoked_at"] != nil || document["last_used_at"] != nil {
		t.Fatalf("document %v", document)
	}
	if len(document) != 8 {
		t.Fatalf("public_metadata has 8 keys, got %d: %v", len(document), document)
	}
	if document["is_valid"] != true {
		t.Fatalf("is_valid %v", document["is_valid"])
	}
	for _, secret := range []string{"token_hash", "token"} {
		if _, ok := document[secret]; ok {
			t.Fatalf("the document carries %s", secret)
		}
	}
}

func TestMetadataIsValidPerState(t *testing.T) {
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	past, future := now.Add(-time.Second), now.Add(time.Hour)
	for name, tc := range map[string]struct {
		item ServiceCredentialMetadata
		want bool
	}{
		"no expiry":        {ServiceCredentialMetadata{}, true},
		"future expiry":    {ServiceCredentialMetadata{ExpiresAt: &future}, true},
		"expires exactly":  {ServiceCredentialMetadata{ExpiresAt: &now}, true},
		"expired":          {ServiceCredentialMetadata{ExpiresAt: &past}, false},
		"revoked":          {ServiceCredentialMetadata{RevokedAt: &past}, false},
		"revoked, in date": {ServiceCredentialMetadata{RevokedAt: &past, ExpiresAt: &future}, false},
	} {
		if got := tc.item.IsValid(now); got != tc.want {
			t.Errorf("%s: IsValid = %v, want %v", name, got, tc.want)
		}
		if got := tc.item.Document(now)["is_valid"]; got != tc.want {
			t.Errorf("%s: document is_valid = %v, want %v", name, got, tc.want)
		}
	}
}

// worker-llm-config holds llm_config:read and nothing else, and its tokens are
// told apart from the retired worker-operator ones by prefix.
func TestWorkerLLMConfigServiceCredential(t *testing.T) {
	if got, err := ServiceCredentialScopes(ServiceWorkerLLMConfig, []string{"llm_config:read"}); err != nil || len(got) != 1 {
		t.Fatalf("llm_config:read: %v %v", got, err)
	}
	for _, scope := range []string{"workers:operate", "entitlements:read"} {
		if _, err := ServiceCredentialScopes(ServiceWorkerLLMConfig, []string{scope}); err == nil {
			t.Errorf("%s accepted", scope)
		}
	}
	if _, err := ServiceCredentialScopes(ServiceWorkerOperator, []string{"llm_config:read"}); err == nil {
		t.Error("the retired worker-operator service gained llm_config:read")
	}
	token, err := generateServiceToken(ServiceWorkerLLMConfig)
	if err != nil || !strings.HasPrefix(token, "svc_wllm_") {
		t.Fatalf("token prefix: %v", err)
	}
}
