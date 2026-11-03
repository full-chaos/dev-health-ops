package authapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

func installRefreshCounter(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	counter, err := accessverify.NewRefreshCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"), AccessService)
	if err != nil {
		t.Fatal(err)
	}
	SetRefreshAccepted(counter)
	t.Cleanup(func() { SetRefreshAccepted(nil) })
	return reader
}

// rawRefreshToken signs an HS256 refresh JWT whose sub, org_id and jti are the given JSON values.
func rawRefreshToken(t *testing.T, f *compatFixture, claims map[string]any) string {
	t.Helper()
	body := map[string]any{
		"sub": f.principal.String(), "org_id": f.org.String(), "family_id": "family-1", "type": "refresh",
		"iss": testIssuer, "aud": testAudience, "exp": time.Now().Add(time.Hour).Unix(), "iat": time.Now().Unix(), "jti": "legacy-jti-0001",
	}
	for k, v := range claims {
		if v == nil {
			delete(body, k)
			continue
		}
		body[k] = v
	}
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	input := header + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write([]byte(input))
	return input + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestCompatRefreshRefusedForItsClaimShapeIsNotCounted(t *testing.T) {
	cases := map[string]map[string]any{
		"invalid sub":       {"sub": "not-a-uuid"},
		"missing jti":       {"jti": nil},
		"non-string jti":    {"jti": 12345},
		"invalid org_id":    {"org_id": "not-a-uuid"},
		"non-string org_id": {"org_id": 12345},
	}
	for name, claims := range cases {
		t.Run(name, func(t *testing.T) {
			f := newCompatFixture(t)
			reader := installRefreshCounter(t)
			f.sessions.legacyResult = accepted(f.record)
			response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+rawRefreshToken(t, f, claims)+`"}`, nil)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status %d: %s, want 401", response.Code, response.Body)
			}
			if got := legacyRefreshTotal(t, reader); got != 0 {
				t.Fatalf("a refused refresh token moved the counter to %d, want 0", got)
			}
		})
	}
}

func TestCompatRefreshRefusedByTheProjectorIsNotCounted(t *testing.T) {
	f := newCompatFixture(t)
	reader := installRefreshCounter(t)
	f.sessions.legacyResult = accepted(f.record)
	f.projector.gone[f.principal] = true
	response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+legacyToken(t, f, f.org.String(), testSecret)+`"}`, nil)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status %d: %s, want 401", response.Code, response.Body)
	}
	if got := legacyRefreshTotal(t, reader); got != 0 {
		t.Fatalf("a refresh the projector refused moved the counter to %d, want 0", got)
	}
}

func TestCompatLogoutWithAValidGoAPIRefreshTokenIsNotCountedAsARefresh(t *testing.T) {
	f := newCompatFixture(t)
	reader := installRefreshCounter(t)
	response := f.do(http.MethodPost, PatternCompatLogout, `{"refresh_token":"`+legacyToken(t, f, f.org.String(), testSecret)+`"}`, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if got := legacyRefreshTotal(t, reader); got != 0 {
		t.Fatalf("a logout moved the refresh counter to %d, want 0", got)
	}
}
