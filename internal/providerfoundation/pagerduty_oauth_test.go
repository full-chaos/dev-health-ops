package providerfoundation

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func TestCredentialResolverHydratesTokenlessPagerDutyOAuthBinding(t *testing.T) {
	t.Parallel()
	key := secrets.NewValue("test-master-key")
	decryptor, err := NewFernetDecryptor(key, "salt")
	if err != nil {
		t.Fatal(err)
	}
	descriptor := `{"account_id":"account-1","auth_mode":"oauth","oauth_binding_id":"binding-1","oauth_credential_name":"operations","region":"us","subdomain":"acme"}`
	tokens := `{"access_token":"ephemeral-access-token","refresh_token":"refresh-token","expires_at":"2036-08-12T00:00:00Z","granted_scopes":["escalation_policies.read","incidents.read","oncalls.read","schedules.read","services.read","teams.read","users.read"]}`
	oauthRepository := &fakePagerDutyOAuthTokenRepository{
		record: PagerDutyOAuthTokenRecord{
			Ciphertext: secrets.NewValue("v1:" + encryptForTest(t, []byte(tokens), key.Reveal(), "salt")),
			Version:    7,
			BindingID:  "binding-1",
		},
	}
	resolver := CredentialResolver{
		Repository: fakeEncryptedCredentialRepository{
			record: EncryptedCredential{
				ID: "credential-1", Provider: "pagerduty", Name: "default", Active: true,
				Ciphertext: secrets.NewValue("v1:" + encryptForTest(t, []byte(descriptor), key.Reveal(), "salt")),
			},
		},
		Decryptor: decryptor,
		Hydrator: PagerDutyOAuthHydrator{
			Repository: oauthRepository,
			Cipher:     decryptor,
			Now:        func() time.Time { return time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC) },
		},
	}
	scope := TenantScope{
		OrgID: "org-1", Provider: "pagerduty", IntegrationID: "integration-1",
	}
	credential, err := resolver.Resolve(
		context.Background(),
		LeaseGuardFunc(func(context.Context) error { return nil }),
		scope,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateCredentialShape(credential); err != nil {
		t.Fatalf("hydrated credential shape: %v", err)
	}
	accessToken, exists := credential.Secret("access_token")
	if !exists || accessToken.Reveal() != "ephemeral-access-token" {
		t.Fatal("resolved credential did not carry the OAuth access token")
	}
	if oauthRepository.loadedOrgID != "org-1" || oauthRepository.loadedName != "operations" {
		t.Fatalf(
			"OAuth lookup scope=(%q,%q), want (org-1,operations)",
			oauthRepository.loadedOrgID,
			oauthRepository.loadedName,
		)
	}
}

func TestPagerDutyOAuthHydratorPreservesEmbeddedAccessTokenCompatibility(t *testing.T) {
	t.Parallel()
	credential := testCredential("pagerduty", map[string]string{
		"auth_mode": "oauth", "access_token": "direct-access-token", "region": "us",
	})
	hydrated, err := (PagerDutyOAuthHydrator{}).Hydrate(
		context.Background(),
		LeaseGuardFunc(func(context.Context) error { return nil }),
		TenantScope{OrgID: "org-1", Provider: "pagerduty", IntegrationID: "integration-1"},
		credential,
	)
	if err != nil {
		t.Fatal(err)
	}
	token, exists := hydrated.Secret("access_token")
	if !exists || token.Reveal() != "direct-access-token" {
		t.Fatal("embedded PagerDuty access token compatibility was not preserved")
	}
}

func TestPagerDutyOAuthHydratorRefreshesAndRotatesEncryptedToken(t *testing.T) {
	t.Parallel()
	key := secrets.NewValue("test-master-key")
	cipher, err := NewFernetDecryptor(key, "salt")
	if err != nil {
		t.Fatal(err)
	}
	refreshToken := "refresh-token"
	stored := pagerDutyOAuthTokens{
		AccessToken: "expired-token", RefreshToken: &refreshToken,
		ExpiresAt:     time.Date(2026, 8, 12, 0, 1, 0, 0, time.UTC),
		GrantedScopes: append([]string(nil), pagerDutyOperationalReadScopes...),
	}
	repository := &fakePagerDutyOAuthTokenRepository{record: PagerDutyOAuthTokenRecord{
		Ciphertext: secrets.NewValue("v1:" + encryptForTest(t, mustJSON(t, stored), key.Reveal(), "salt")),
		Version:    7, BindingID: "binding-1",
	}}
	doer := &pagerDutyRefreshDoer{}
	hydrated, err := (PagerDutyOAuthHydrator{
		Repository: repository, Cipher: cipher, Doer: doer,
		AppClientID:     secrets.NewValue("client-id"),
		AppClientSecret: secrets.NewValue("client-secret"),
		Now:             func() time.Time { return time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC) },
	}).Hydrate(
		context.Background(),
		LeaseGuardFunc(func(context.Context) error { return nil }),
		TenantScope{OrgID: "org-1", Provider: "pagerduty", IntegrationID: "integration-1"},
		testCredential("pagerduty", map[string]string{
			"auth_mode": "oauth", "oauth_credential_name": "operations",
			"oauth_binding_id": "binding-1",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	token, exists := hydrated.Secret("access_token")
	if !exists || token.Reveal() != "refreshed-access-token" {
		t.Fatal("refreshed access token was not hydrated")
	}
	if doer.calls != 1 || !repository.rotated {
		t.Fatalf("refresh calls=%d rotated=%t", doer.calls, repository.rotated)
	}
	if repository.rotation.ExpectedVersion != 7 ||
		repository.rotation.ExpectedBindingID != "binding-1" ||
		!repository.rotation.Ciphertext.Configured() {
		t.Fatalf("unexpected optimistic rotation metadata: %+v", repository.rotation)
	}
	plaintext, err := cipher.Decrypt(repository.rotation.Ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plaintext), "expired-token") ||
		!strings.Contains(string(plaintext), "refreshed-access-token") {
		t.Fatal("rotation did not persist only the refreshed token payload")
	}
}

func TestPagerDutyOAuthHydratorRejectsBindingAndScopeDrift(t *testing.T) {
	t.Parallel()
	key := secrets.NewValue("test-master-key")
	cipher, _ := NewFernetDecryptor(key, "salt")
	for name, mutate := range map[string]func(*PagerDutyOAuthTokenRecord, *pagerDutyOAuthTokens){
		"binding": func(record *PagerDutyOAuthTokenRecord, _ *pagerDutyOAuthTokens) {
			record.BindingID = "reconnected-binding"
		},
		"scope": func(_ *PagerDutyOAuthTokenRecord, tokens *pagerDutyOAuthTokens) {
			tokens.GrantedScopes = []string{"services.read"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tokens := pagerDutyOAuthTokens{
				AccessToken:   "access-token",
				ExpiresAt:     time.Date(2036, 8, 12, 0, 0, 0, 0, time.UTC),
				GrantedScopes: append([]string(nil), pagerDutyOperationalReadScopes...),
			}
			record := PagerDutyOAuthTokenRecord{Version: 7, BindingID: "binding-1"}
			mutate(&record, &tokens)
			record.Ciphertext = secrets.NewValue("v1:" + encryptForTest(t, mustJSON(t, tokens), key.Reveal(), "salt"))
			_, err := (PagerDutyOAuthHydrator{
				Repository: &fakePagerDutyOAuthTokenRepository{record: record},
				Cipher:     cipher,
			}).Hydrate(
				context.Background(),
				LeaseGuardFunc(func(context.Context) error { return nil }),
				TenantScope{OrgID: "org-1", Provider: "pagerduty", IntegrationID: "integration-1"},
				testCredential("pagerduty", map[string]string{
					"auth_mode": "oauth", "oauth_credential_name": "operations",
					"oauth_binding_id": "binding-1",
				}),
			)
			if err == nil {
				t.Fatal("drifted OAuth binding was accepted")
			}
		})
	}
}

func TestPagerDutyOAuthHydratorSerializesConcurrentRefresh(t *testing.T) {
	t.Parallel()
	key := secrets.NewValue("test-master-key")
	cipher, err := NewFernetDecryptor(key, "salt")
	if err != nil {
		t.Fatal(err)
	}
	refreshToken := "single-use-refresh-token"
	stored := pagerDutyOAuthTokens{
		AccessToken: "expiring-token", RefreshToken: &refreshToken,
		ExpiresAt:     time.Date(2026, 8, 12, 0, 1, 0, 0, time.UTC),
		GrantedScopes: append([]string(nil), pagerDutyOperationalReadScopes...),
	}
	repository := newConcurrentPagerDutyOAuthTokenRepository(PagerDutyOAuthTokenRecord{
		Ciphertext: secrets.NewValue("v1:" + encryptForTest(t, mustJSON(t, stored), key.Reveal(), "salt")),
		Version:    7,
		BindingID:  "binding-1",
	})
	doer := &singleUsePagerDutyRefreshDoer{}
	hydrator := PagerDutyOAuthHydrator{
		Repository: repository, Cipher: cipher, Doer: doer,
		AppClientID:     secrets.NewValue("client-id"),
		AppClientSecret: secrets.NewValue("client-secret"),
		Now:             func() time.Time { return time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC) },
	}
	credential := testCredential("pagerduty", map[string]string{
		"auth_mode": "oauth", "oauth_credential_name": "operations",
		"oauth_binding_id": "binding-1",
	})
	scope := TenantScope{OrgID: "org-1", Provider: "pagerduty", IntegrationID: "integration-1"}

	start := make(chan struct{})
	errors := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			_, hydrateErr := hydrator.Hydrate(
				context.Background(),
				LeaseGuardFunc(func(context.Context) error { return nil }),
				scope,
				credential,
			)
			errors <- hydrateErr
		}()
	}
	close(start)
	for range 2 {
		if hydrateErr := <-errors; hydrateErr != nil {
			t.Errorf("concurrent hydration failed: %v", hydrateErr)
		}
	}
	if calls := doer.Calls(); calls != 1 {
		t.Fatalf("remote refresh calls=%d, want one serialized rotation", calls)
	}
}

type fakeEncryptedCredentialRepository struct{ record EncryptedCredential }

func (r fakeEncryptedCredentialRepository) ResolveEncrypted(
	context.Context,
	TenantScope,
) (EncryptedCredential, error) {
	return r.record, nil
}

type fakePagerDutyOAuthTokenRepository struct {
	record      PagerDutyOAuthTokenRecord
	loadedOrgID string
	loadedName  string
	rotated     bool
	rotation    PagerDutyOAuthTokenRotation
}

func (r *fakePagerDutyOAuthTokenRepository) Load(
	_ context.Context,
	orgID string,
	credentialName string,
) (PagerDutyOAuthTokenRecord, error) {
	r.loadedOrgID = orgID
	r.loadedName = credentialName
	return r.record, nil
}

func (r *fakePagerDutyOAuthTokenRepository) WithRefreshLock(
	_ context.Context,
	_ string,
	_ string,
	refresh func(PagerDutyOAuthTokenRecord) (*PagerDutyOAuthTokenRotation, error),
) error {
	rotation, err := refresh(r.record)
	if err != nil {
		return err
	}
	if rotation == nil {
		return nil
	}
	r.rotated = true
	r.rotation = *rotation
	r.record.Ciphertext = rotation.Ciphertext
	r.record.Version++
	return nil
}

type pagerDutyRefreshDoer struct{ calls int }

func (d *pagerDutyRefreshDoer) Do(request *http.Request) (*http.Response, error) {
	d.calls++
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	values, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, err
	}
	if request.URL.String() != pagerDutyTokenURL ||
		values.Get("grant_type") != "refresh_token" ||
		values.Get("client_id") != "client-id" ||
		values.Get("client_secret") != "client-secret" ||
		values.Get("refresh_token") != "refresh-token" {
		return nil, ErrCredentialInvalid
	}
	return testHTTPResponse(request, http.StatusOK, nil,
		`{"access_token":"refreshed-access-token","expires_in":3600}`), nil
}

type concurrentPagerDutyOAuthTokenRepository struct {
	mu          sync.Mutex
	record      PagerDutyOAuthTokenRecord
	initialLoad sync.WaitGroup
}

func newConcurrentPagerDutyOAuthTokenRepository(
	record PagerDutyOAuthTokenRecord,
) *concurrentPagerDutyOAuthTokenRepository {
	repository := &concurrentPagerDutyOAuthTokenRepository{record: record}
	repository.initialLoad.Add(2)
	return repository
}

func (r *concurrentPagerDutyOAuthTokenRepository) Load(
	_ context.Context,
	_ string,
	_ string,
) (PagerDutyOAuthTokenRecord, error) {
	r.mu.Lock()
	record := r.record
	r.mu.Unlock()
	r.initialLoad.Done()
	r.initialLoad.Wait()
	return record, nil
}

func (r *concurrentPagerDutyOAuthTokenRepository) WithRefreshLock(
	_ context.Context,
	_ string,
	_ string,
	refresh func(PagerDutyOAuthTokenRecord) (*PagerDutyOAuthTokenRotation, error),
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rotation, err := refresh(r.record)
	if err != nil {
		return err
	}
	if rotation == nil {
		return nil
	}
	if r.record.Version != rotation.ExpectedVersion ||
		r.record.BindingID != rotation.ExpectedBindingID {
		return ErrCredentialInvalid
	}
	r.record.Ciphertext = rotation.Ciphertext
	r.record.Version++
	return nil
}

type singleUsePagerDutyRefreshDoer struct {
	mu    sync.Mutex
	calls int
}

func (d *singleUsePagerDutyRefreshDoer) Do(request *http.Request) (*http.Response, error) {
	d.mu.Lock()
	d.calls++
	call := d.calls
	d.mu.Unlock()
	if call > 1 {
		return testHTTPResponse(request, http.StatusBadRequest, nil, `{}`), nil
	}
	return testHTTPResponse(request, http.StatusOK, nil,
		`{"access_token":"refreshed-access-token","refresh_token":"rotated-refresh-token","expires_in":3600}`), nil
}

func (d *singleUsePagerDutyRefreshDoer) Calls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
