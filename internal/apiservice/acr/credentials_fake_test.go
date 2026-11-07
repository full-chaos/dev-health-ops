package acr

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
)

// testSigner, testWorkload and entToken are the acr-api workload identity the
// route tests present: entToken is a valid workload token for testWorkload at
// wlNow, the only credential the internal acr routes accept.
var (
	testSigner   = newPackageSigner()
	testWorkload = newPackageWorkload(testSigner)
	entToken     = testSigner.sign(wlHeader(), wlClaims())
)

func newPackageSigner() wlSigner {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return wlSigner{pub: pub, priv: priv}
}

func newPackageWorkload(signer wlSigner) *WorkloadAuth {
	verifier, err := authverify.NewWorkloadVerifier(authverify.WorkloadVerifierConfig{
		Issuer: wlIssuer, Audience: "dev-health-api", Keys: authverify.StaticKeys{wlKid: signer.pub},
	})
	if err != nil {
		panic(err)
	}
	return &WorkloadAuth{Verifier: verifier, Subject: wlSubject, Now: func() time.Time { return wlNow }}
}

type auditRow struct{ credentialID, orgID, outcome string }

// fakeCredentials is a hand-controlled CredentialStore that records every
// Audit call.
type fakeCredentials struct {
	mu         sync.Mutex
	auditErr   error
	audits     []auditRow
	principals []string
}

func newFakeCredentials() *fakeCredentials { return &fakeCredentials{} }

func (f *fakeCredentials) Audit(_ context.Context, id, principalID, orgID, outcome string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.auditErr != nil {
		return f.auditErr
	}
	f.audits = append(f.audits, auditRow{id, orgID, outcome})
	f.principals = append(f.principals, principalID)
	return nil
}

var errBoom = errors.New("boom")

// getAuthed GETs url with the valid acr-api workload token.
func getAuthed(url string) (*http.Response, error) {
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+entToken)
	return http.DefaultClient.Do(request)
}
