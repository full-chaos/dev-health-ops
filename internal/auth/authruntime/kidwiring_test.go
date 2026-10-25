package authruntime

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
)

type recordingSigner struct{}

func (recordingSigner) Sign(context.Context, string, []byte) (string, string, error) {
	return "compact", "kid-1", nil
}

type kidSpy struct {
	sessions []uuid.UUID
	kids     []string
}

func (s *kidSpy) RecordSigningKid(_ context.Context, id uuid.UUID, kid string) error {
	s.sessions, s.kids = append(s.sessions, id), append(s.kids, kid)
	return nil
}

// The production builder wires the kid recorder for both classes: a mint
// through it records on the session (hs256_compat: the empty kid).
func TestTheProductionIssuerBuilderRecordsTheSigningKid(t *testing.T) {
	for class, wantKid := range map[string]string{"hs256_compat": "", "eddsa": "kid-1"} {
		t.Run(class, func(t *testing.T) {
			spy := &kidSpy{}
			issuer, err := newRecordingIssuer(classConfig(t, class), recordingSigner{}, spy)
			if err != nil {
				t.Fatal(err)
			}
			grant := authapi.Grant{PrincipalID: uuid.New(), SessionID: uuid.New()}
			if token, err := issuer.AccessContext(context.Background(), grant); err != nil || token == "" {
				t.Fatalf("mint: %q, %v", token, err)
			}
			if len(spy.kids) != 1 || spy.kids[0] != wantKid || spy.sessions[0] != grant.SessionID {
				t.Fatalf("recorded %v on %v; want %q on %s (the builder lost its kid recorder)", spy.kids, spy.sessions, wantKid, grant.SessionID)
			}
		})
	}
}
