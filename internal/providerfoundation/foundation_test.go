package providerfoundation

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"golang.org/x/crypto/pbkdf2"
)

func TestFernetDecryptorMatchesPythonV1Contract(t *testing.T) {
	t.Parallel()
	key := secrets.NewValue("test-master-key")
	decryptor, err := NewFernetDecryptor(key, "salt")
	if err != nil {
		t.Fatal(err)
	}
	cipherText := encryptForTest(t, []byte(`{"token":"secret","base_url":"https://example.test"}`), key.Reveal(), "salt")
	plain, err := decryptor.Decrypt(secrets.NewValue("v1:" + cipherText))
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != `{"token":"secret","base_url":"https://example.test"}` {
		t.Fatalf("plaintext mismatch")
	}
}
func TestCredentialResolverRequiresLeaseAndDoesNotExposeSecret(t *testing.T) {
	t.Parallel()
	key := secrets.NewValue("test-master-key")
	decryptor, _ := NewFernetDecryptor(key, "salt")
	resolver := CredentialResolver{Repository: testRepository{cipherText: "v1:" + encryptForTest(t, []byte(`{"token":"secret"}`), key.Reveal(), "salt")}, Decryptor: decryptor}
	credential, err := resolver.Resolve(context.Background(), LeaseGuardFunc(func(context.Context) error { return nil }), TenantScope{OrgID: "org", Provider: "gitlab", IntegrationID: "integration"})
	if err != nil {
		t.Fatal(err)
	}
	if rendered := fmt.Sprint(credential.SafeAttributes()); strings.Contains(rendered, "secret") {
		t.Fatalf("safe attributes leaked secret: %s", rendered)
	}
	token, _ := credential.Secret("token")
	if token.Reveal() != "secret" {
		t.Fatal("wrong secret")
	}
}

func TestCredentialResolverRejectsDifferentCredentialThanFrozenClaim(t *testing.T) {
	t.Parallel()
	key := secrets.NewValue("test-master-key")
	decryptor, _ := NewFernetDecryptor(key, "salt")
	resolver := CredentialResolver{
		Repository: testRepository{cipherText: "v1:" + encryptForTest(t, []byte(`{"token":"secret"}`), key.Reveal(), "salt")},
		Decryptor:  decryptor,
	}
	_, err := resolver.Resolve(
		context.Background(),
		LeaseGuardFunc(func(context.Context) error { return nil }),
		TenantScope{
			OrgID: "org", Provider: "gitlab", IntegrationID: "integration",
			CredentialID: "frozen-credential",
		},
	)
	if !errors.Is(err, ErrCredentialInvalid) {
		t.Fatalf("error=%v", err)
	}
}

func TestDecodeConfigRejectsMalformedStoredJSON(t *testing.T) {
	t.Parallel()
	target := map[string]string{}
	if err := decodeConfig([]byte(`{"base_url":`), target); err == nil {
		t.Fatal("malformed credential config was accepted")
	}
	if len(target) != 0 {
		t.Fatalf("target mutated after malformed config: %v", target)
	}
}

func TestCredentialEphemeralSecretDoesNotMutateDescriptor(t *testing.T) {
	t.Parallel()
	descriptor := Credential{
		Provider: "pagerduty",
		fields: map[string]secrets.Value{
			"auth_mode":             secrets.NewValue("oauth"),
			"oauth_credential_name": secrets.NewValue("default"),
		},
	}
	hydrated, err := descriptor.WithEphemeralSecret("access_token", secrets.NewValue("ephemeral"))
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := descriptor.Secret("access_token"); exists {
		t.Fatal("persisted descriptor was mutated")
	}
	token, exists := hydrated.Secret("access_token")
	if !exists || token.Reveal() != "ephemeral" {
		t.Fatal("hydrated credential is missing its ephemeral token")
	}
}
func TestHTTPClassificationAndPaginationFixtures(t *testing.T) {
	t.Parallel()
	cases := []struct {
		provider string
		status   int
		headers  http.Header
		want     ErrorClass
	}{{"github", 403, http.Header{"X-RateLimit-Remaining": []string{"0"}}, ErrorRateLimited}, {"github", 403, http.Header{}, ErrorAuthentication}, {"gitlab", 401, http.Header{}, ErrorAuthentication}, {"jira", 404, http.Header{}, ErrorNotFound}, {"linear", 409, http.Header{}, ErrorConflict}, {"gitlab", 429, http.Header{"Retry-After": []string{"2"}}, ErrorRateLimited}, {"gitlab", 503, http.Header{}, ErrorTransient}}
	for _, test := range cases {
		if got := ClassifyHTTP(test.provider, test.status, test.headers); got == nil || got.Class != test.want {
			t.Fatalf("%s/%d=%v want %s", test.provider, test.status, got, test.want)
		}
	}
	calls := 0
	values, err := CollectPages(context.Background(), 2, func(context.Context, string) ([]int, string, error) {
		calls++
		if calls == 1 {
			return []int{1}, "next", nil
		}
		return []int{2}, "", nil
	})
	if err != nil || fmt.Sprint(values) != "[1 2]" {
		t.Fatalf("pagination=%v,%v", values, err)
	}
}

func TestProviderParityFixture(t *testing.T) {
	t.Parallel()
	content, err := os.ReadFile("testdata/provider_parity.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		SchemaVersion string `json:"schema_version"`
		Cases         []struct {
			ID             string            `json:"id"`
			Provider       string            `json:"provider"`
			Status         int               `json:"status"`
			Headers        map[string]string `json:"headers"`
			Message        string            `json:"message"`
			Classification ErrorClass        `json:"classification"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(content, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.SchemaVersion != "v1" || len(fixture.Cases) == 0 {
		t.Fatal("invalid parity fixture")
	}
	for _, item := range fixture.Cases {
		headers := http.Header{}
		for key, value := range item.Headers {
			headers.Set(key, value)
		}
		got := ClassifyHTTPWithMessage(item.Provider, item.Status, headers, item.Message)
		if got == nil || got.Class != item.Classification {
			t.Fatalf("%s: got %v, want %s", item.ID, got, item.Classification)
		}
	}
}

func TestGitHubSecondaryLimitBodyClassification(t *testing.T) {
	t.Parallel()
	got := ClassifyHTTPWithMessage("github", http.StatusForbidden, http.Header{}, `{"message":"You have triggered a secondary rate limit"}`)
	if got == nil || got.Class != ErrorRateLimited {
		t.Fatalf("classification=%v", got)
	}
}

func TestHTTPClientRetriesOnlyWithinConfiguredBudget(t *testing.T) {
	t.Parallel()
	doer := &sequenceDoer{statuses: []int{http.StatusServiceUnavailable, http.StatusOK}}
	client, err := NewHTTPClient("gitlab", "https://gitlab.example", doer, TokenAuth("Authorization", "Bearer ", secrets.NewValue("token")), RetryPolicy{MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}, LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(context.Background(), http.MethodGet, "/api/v4/projects", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if doer.calls != 2 {
		t.Fatalf("calls=%d, want 2", doer.calls)
	}
}

func TestSharedBackoffKeyMatchesPythonContract(t *testing.T) {
	t.Parallel()
	gate := ValkeyBackoffGate{Provider: "github", OrgID: "", Host: ""}
	if got, want := gate.key(), "rate_limit:github:_:_"; got != want {
		t.Fatalf("key=%q, want %q", got, want)
	}
}

func TestGitHubAppClientMintsInstallationTokenWithoutGlobalState(t *testing.T) {
	t.Parallel()
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	privateKey := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	credential := Credential{Provider: "github", fields: map[string]secrets.Value{"app_id": secrets.NewValue("1"), "private_key": secrets.NewValue(privateKey), "installation_id": secrets.NewValue("2")}}
	doer := &githubAppDoer{}
	client, err := NewGitHubClient(credential, doer, RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}, LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(context.Background(), http.MethodGet, "/repos/o/r", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if doer.calls != 2 || doer.providerAuthorization != "Bearer installation-token" {
		t.Fatalf("app calls=%d authorization=%q", doer.calls, doer.providerAuthorization)
	}
}
func TestEnvelopeDedupeRejectsConflictingContent(t *testing.T) {
	t.Parallel()
	now := time.Now()
	first := NormalizedEnvelope{SchemaVersion: "v1", Provider: "github", OrgID: "org", IntegrationID: "i", EntityType: "issue", SourceID: "1", DedupeKey: "same", ObservedAt: now, Provenance: Provenance{Source: "native", Confidence: "1.0"}}
	changed := first
	changed.SourceID = "2"
	if err := validateBatch([]NormalizedEnvelope{first, changed}); err != ErrSinkDuplicate {
		t.Fatalf("error=%v", err)
	}
}

func TestClickHouseGenerationTokenIsStableAndOpaque(t *testing.T) {
	t.Parallel()
	envelope := testGenerationEnvelope("record-1")
	blocks, err := BuildGenerationBlocks(
		"sync-unit:11111111-1111-4111-8111-111111111111",
		"provider_records",
		[]NormalizedEnvelope{envelope},
	)
	if err != nil || len(blocks) != 1 {
		t.Fatalf("blocks=%d error=%v", len(blocks), err)
	}
	first, err := clickHouseGenerationContext(context.Background(), blocks[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := clickHouseGenerationContext(context.Background(), blocks[0])
	if err != nil {
		t.Fatal(err)
	}
	token := clickHouseGenerationToken(first)
	if token == "" || token != clickHouseGenerationToken(second) ||
		strings.Contains(token, "11111111") || len(token) != sha256.Size*2 {
		t.Fatalf("generation token=%q", token)
	}
}

func TestClickHouseGenerationRejectsUnboundedOrEmptyKeys(t *testing.T) {
	t.Parallel()
	for _, generation := range []string{"", " ", strings.Repeat("x", 257)} {
		if _, err := BuildGenerationBlocks(generation, "provider_records", []NormalizedEnvelope{testGenerationEnvelope("record-1")}); !errors.Is(err, ErrSinkGenerationUnsafe) {
			t.Fatalf("generation length=%d error=%v", len(generation), err)
		}
	}
}

func TestGenerationBlocksAreBoundedDeterministicAndDestinationScoped(t *testing.T) {
	t.Parallel()
	input := []NormalizedEnvelope{
		testGenerationEnvelope("record-3"),
		testGenerationEnvelope("record-1"),
		testGenerationEnvelope("record-2"),
	}
	first, err := buildGenerationBlocks("sync-unit:one", "provider_records", input, 2, maxGenerationBlockBytes)
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildGenerationBlocks("sync-unit:one", "provider_records", input, 2, maxGenerationBlockBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].Index() != 0 || first[1].Index() != 1 ||
		first[0].ContentDigest() != second[0].ContentDigest() ||
		first[0].Batch()[0].SourceID != "record-1" ||
		first[1].Batch()[0].SourceID != "record-3" {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	firstContext, _ := clickHouseGenerationContext(context.Background(), first[0])
	secondContext, _ := clickHouseGenerationContext(context.Background(), first[1])
	if clickHouseGenerationToken(firstContext) == clickHouseGenerationToken(secondContext) {
		t.Fatal("distinct blocks reused a ClickHouse token")
	}
}

func TestGenerationReplayGuardRejectsConflictingContent(t *testing.T) {
	t.Parallel()
	first, _ := buildGenerationBlocks(
		"sync-unit:one", "provider_records",
		[]NormalizedEnvelope{testGenerationEnvelope("record-1")}, 1, maxGenerationBlockBytes,
	)
	conflicting, _ := buildGenerationBlocks(
		"sync-unit:one", "provider_records",
		[]NormalizedEnvelope{testGenerationEnvelope("record-2")}, 1, maxGenerationBlockBytes,
	)
	guard := NewGenerationReplayGuard()
	if err := guard.Remember(first[0]); err != nil {
		t.Fatal(err)
	}
	if err := guard.Remember(first[0]); err != nil {
		t.Fatalf("identical replay failed: %v", err)
	}
	if err := guard.Remember(conflicting[0]); !errors.Is(err, ErrSinkReplayConflict) {
		t.Fatalf("conflicting replay error=%v", err)
	}
}

func TestGenerationReplayGuardEvictsDeterministicallyAtCapacity(t *testing.T) {
	t.Parallel()
	guard, err := NewGenerationReplayGuardWithCapacity(2)
	if err != nil {
		t.Fatal(err)
	}
	block := func(generation, source string) GenerationBlock {
		blocks, buildErr := buildGenerationBlocks(
			generation, "provider_records",
			[]NormalizedEnvelope{testGenerationEnvelope(source)}, 1, maxGenerationBlockBytes,
		)
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		return blocks[0]
	}
	first := block("sync-unit:one", "record-1")
	second := block("sync-unit:two", "record-2")
	third := block("sync-unit:three", "record-3")
	for _, value := range []GenerationBlock{first, second, third} {
		if err := guard.Remember(value); err != nil {
			t.Fatal(err)
		}
	}
	if guard.Size() != 2 {
		t.Fatalf("guard size=%d", guard.Size())
	}
	secondConflict := block("sync-unit:two", "changed-while-retained")
	if err := guard.Remember(secondConflict); !errors.Is(err, ErrSinkReplayConflict) {
		t.Fatalf("retained conflict error=%v", err)
	}
	firstConflict := block("sync-unit:one", "changed-after-eviction")
	if err := guard.Remember(firstConflict); err != nil {
		t.Fatalf("oldest key was not evicted: %v", err)
	}
}

func TestGenerationReplayGuardIsRaceSafeAndBounded(t *testing.T) {
	t.Parallel()
	guard, err := NewGenerationReplayGuardWithCapacity(8)
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for index := 0; index < 64; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			blocks, buildErr := buildGenerationBlocks(
				fmt.Sprintf("sync-unit:%d", index), "provider_records",
				[]NormalizedEnvelope{testGenerationEnvelope(fmt.Sprintf("record-%d", index))},
				1, maxGenerationBlockBytes,
			)
			if buildErr != nil {
				t.Errorf("build block: %v", buildErr)
				return
			}
			if rememberErr := guard.Remember(blocks[0]); rememberErr != nil {
				t.Errorf("remember block: %v", rememberErr)
			}
		}()
	}
	wait.Wait()
	if guard.Size() != 8 {
		t.Fatalf("guard size=%d", guard.Size())
	}
}

func testGenerationEnvelope(sourceID string) NormalizedEnvelope {
	return NormalizedEnvelope{
		SchemaVersion: "v1", Provider: "github", OrgID: "org",
		IntegrationID: "integration", EntityType: "repository",
		SourceID: sourceID, DedupeKey: "github:repository:" + sourceID,
		ObservedAt: time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
		Provenance: Provenance{Source: "native", Confidence: "1.0"},
		Attributes: map[string]string{"name": sourceID},
	}
}

type testRepository struct{ cipherText string }

func (r testRepository) ResolveEncrypted(context.Context, TenantScope) (EncryptedCredential, error) {
	return EncryptedCredential{ID: "id", Provider: "gitlab", Name: "default", Active: true, Ciphertext: secrets.NewValue(r.cipherText)}, nil
}

type sequenceDoer struct {
	statuses []int
	calls    int
}

func (d *sequenceDoer) Do(request *http.Request) (*http.Response, error) {
	status := d.statuses[d.calls]
	d.calls++
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
}

type githubAppDoer struct {
	calls                 int
	providerAuthorization string
}

func (d *githubAppDoer) Do(request *http.Request) (*http.Response, error) {
	d.calls++
	if strings.Contains(request.URL.Path, "/access_tokens") {
		if !strings.HasPrefix(request.Header.Get("Authorization"), "Bearer ") {
			return nil, fmt.Errorf("missing app jwt")
		}
		return &http.Response{StatusCode: http.StatusCreated, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"token":"installation-token","expires_at":"2099-01-01T00:00:00Z"}`)), Request: request}, nil
	}
	d.providerAuthorization = request.Header.Get("Authorization")
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("{}")), Request: request}, nil
}
func encryptForTest(t *testing.T, plain []byte, secret, salt string) string {
	t.Helper()
	key := pbkdf2.Key([]byte(secret), []byte(salt), 600000, 32, sha256.New)
	padded := append([]byte(nil), plain...)
	n := aes.BlockSize - len(padded)%aes.BlockSize
	padded = append(padded, bytes.Repeat([]byte{byte(n)}, n)...)
	block, err := aes.NewCipher(key[16:])
	if err != nil {
		t.Fatal(err)
	}
	payload := append([]byte{fernetVersion}, make([]byte, 8)...)
	iv := bytes.Repeat([]byte{7}, aes.BlockSize)
	payload = append(payload, iv...)
	encrypted := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(encrypted, padded)
	payload = append(payload, encrypted...)
	mac := hmac.New(sha256.New, key[:16])
	_, _ = mac.Write(payload)
	payload = append(payload, mac.Sum(nil)...)
	return base64.RawURLEncoding.EncodeToString(payload)
}
