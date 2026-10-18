package acr

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

const (
	entToken      = "svc_acr_entitlements-token-0123456789"
	entOnlyScopes = "entitlements:read"
)

type auditRow struct{ credentialID, orgID, outcome string }

// fakeCredentials is a hand-controlled CredentialStore that records every
// MarkUsed and Audit call.
type fakeCredentials struct {
	mu          sync.Mutex
	creds       map[string]Credential
	findErr     error
	markErr     error
	auditErr    error
	audits      []auditRow
	used        []string
	findCalls   int
	lastUsedAts []time.Time
}

func newFakeCredentials() *fakeCredentials {
	return &fakeCredentials{creds: map[string]Credential{
		entToken: {ID: "cred-ent", ServiceName: "acr", Scopes: []string{entOnlyScopes}},
	}}
}

func (f *fakeCredentials) Find(_ context.Context, token string) (Credential, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.findCalls++
	if f.findErr != nil {
		return Credential{}, false, f.findErr
	}
	cred, ok := f.creds[token]
	return cred, ok, nil
}

func (f *fakeCredentials) MarkUsed(_ context.Context, id string, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.markErr != nil {
		return f.markErr
	}
	f.used = append(f.used, id)
	f.lastUsedAts = append(f.lastUsedAts, at)
	return nil
}

func (f *fakeCredentials) Audit(_ context.Context, id, orgID, outcome string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.auditErr != nil {
		return f.auditErr
	}
	f.audits = append(f.audits, auditRow{id, orgID, outcome})
	return nil
}

var errBoom = errors.New("boom")

// getAuthed GETs url with the valid entitlements credential.
func getAuthed(url string) (*http.Response, error) {
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+entToken)
	return http.DefaultClient.Do(request)
}
