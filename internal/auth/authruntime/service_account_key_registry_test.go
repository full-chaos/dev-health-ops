package authruntime

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadexchange"
)

type fakeKeyResolver struct {
	key    serviceaccount.ResolvedKey
	err    error
	digest [sha256.Size]byte
	now    time.Time
}

func (f *fakeKeyResolver) ResolveKey(_ context.Context, digest [sha256.Size]byte, now time.Time) (serviceaccount.ResolvedKey, error) {
	f.digest, f.now = digest, now
	return f.key, f.err
}

// The exchange ignores key audiences and scopes: whatever the key holds, empty
// included, the registration carries the key row, the account, the org, the
// expiry and the revision, and nothing else.
func TestTheKeyAdapterIgnoresKeyAudiencesAndScopes(t *testing.T) {
	id, principal, org := uuid.New(), uuid.New(), uuid.New()
	expires := time.Unix(1_900_100_000, 0).UTC()
	for name, held := range map[string]struct{ audiences, scopes []string }{
		"empty":             {[]string{}, []string{}},
		"another audience":  {[]string{"acr-api"}, []string{"acr.context.read"}},
		"the same audience": {[]string{"dev-health-api"}, []string{"x"}},
		"a long scope list": {[]string{"dev-health-api", "auth-service"}, []string{"a", "b", "c"}},
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeKeyResolver{key: serviceaccount.ResolvedKey{CredentialID: id, Principal: principal, OrgID: org, ExpiresAt: expires,
				Revision: 7, Audiences: held.audiences, Scopes: held.scopes}}
			digest := sha256.Sum256([]byte("value"))
			now := time.Unix(1_900_000_000, 0)
			got, err := serviceAccountKeyRegistry{resolver: f}.ResolveServiceAccountKey(context.Background(), digest, now)
			if err != nil {
				t.Fatalf("a live key was refused: %v", err)
			}
			want := workloadexchange.ServiceAccountKeyRegistration{CredentialID: id, PrincipalID: principal, OrgID: org, ExpiresAt: expires, Revision: 7}
			if got != want {
				t.Fatalf("registration %+v, want %+v", got, want)
			}
			if f.digest != digest || !f.now.Equal(now) {
				t.Fatal("the resolver was not asked for the digest at now")
			}
		})
	}
	if n := reflect.TypeOf(workloadexchange.ServiceAccountKeyRegistration{}).NumField(); n != 5 {
		t.Fatalf("the registration has %d fields: it carries no audience and no scope", n)
	}
}

// A key refusal keeps its reason; anything else (a store fault) stays an error
// the exchange cannot read as a refusal.
func TestTheKeyAdapterMapsRefusalsAndKeepsStoreFaultsApart(t *testing.T) {
	for _, reason := range []string{"unknown", "wrong_class", "not_platform", "wrong_kind", "revoked", "expired", "account_disabled", "principal_disabled", "org_suspended"} {
		f := &fakeKeyResolver{err: &serviceaccount.KeyRefusal{Reason: reason}}
		_, err := serviceAccountKeyRegistry{resolver: f}.ResolveServiceAccountKey(context.Background(), [sha256.Size]byte{}, time.Now())
		var refusal *workloadexchange.KeyRefusal
		if !errors.As(err, &refusal) || refusal.Reason != reason {
			t.Errorf("reason %s: got %v", reason, err)
		}
	}
	fault := &fakeKeyResolver{err: errors.Join(serviceaccount.ErrStore, errors.New("down"))}
	_, err := serviceAccountKeyRegistry{resolver: fault}.ResolveServiceAccountKey(context.Background(), [sha256.Size]byte{}, time.Now())
	var refusal *workloadexchange.KeyRefusal
	if err == nil || errors.As(err, &refusal) {
		t.Fatalf("a store fault read as a refusal or as success: %v", err)
	}
}
