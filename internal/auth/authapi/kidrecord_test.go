package authapi

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

type recordedKid struct {
	session uuid.UUID
	kid     string
}

type fakeKidRecorder struct {
	calls []recordedKid
	err   error
}

func (f *fakeKidRecorder) RecordSigningKid(_ context.Context, sessionID uuid.UUID, kid string) error {
	f.calls = append(f.calls, recordedKid{sessionID, kid})
	return f.err
}

func kidGrant(sessionID uuid.UUID) Grant {
	return Grant{PrincipalID: uuid.New(), SessionID: sessionID, Role: "member", User: UserState{Email: "a@example.com"}}
}

func TestEdDSAMintRecordsTheSigningKidOnTheSession(t *testing.T) {
	signer, _, _ := fileSigner(t)
	issuer := newEdDSAIssuer(t, signer, testAudience)
	recorder := &fakeKidRecorder{}
	issuer.WithKidRecorder(recorder)
	sid := uuid.New()
	if _, err := issuer.AccessContext(context.Background(), kidGrant(sid)); err != nil {
		t.Fatal(err)
	}
	if len(recorder.calls) != 1 || recorder.calls[0] != (recordedKid{sid, "k1"}) {
		t.Fatalf("recorded %+v, want one record of k1 on %s", recorder.calls, sid)
	}
}

func TestHS256MintRecordsTheEmptyKidSet(t *testing.T) {
	issuer, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &fakeKidRecorder{}
	issuer.WithKidRecorder(recorder)
	sid := uuid.New()
	if _, err := issuer.AccessContext(context.Background(), kidGrant(sid)); err != nil {
		t.Fatal(err)
	}
	if len(recorder.calls) != 1 || recorder.calls[0] != (recordedKid{sid, ""}) {
		t.Fatalf("recorded %+v, want one empty-kid record on %s", recorder.calls, sid)
	}
}

func TestAMintWhoseRecordFailsReturnsNoToken(t *testing.T) {
	signer, _, _ := fileSigner(t)
	eddsa := newEdDSAIssuer(t, signer, testAudience)
	hs, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	for name, issuer := range map[string]*Issuer{"eddsa": eddsa, "hs256_compat": hs} {
		t.Run(name, func(t *testing.T) {
			lost := errors.New("session row not found")
			issuer.WithKidRecorder(&fakeKidRecorder{err: lost})
			compact, err := issuer.AccessContext(context.Background(), kidGrant(uuid.New()))
			if compact != "" || !errors.Is(err, errIssue) || !errors.Is(err, lost) {
				t.Fatalf("got token %q, err %v; want no token and errIssue wrapping the record failure", compact, err)
			}
		})
	}
}

func TestAMintWithoutARecorderStillMints(t *testing.T) {
	signer, _, _ := fileSigner(t)
	issuer := newEdDSAIssuer(t, signer, testAudience)
	if compact, err := issuer.AccessContext(context.Background(), kidGrant(uuid.New())); err != nil || compact == "" {
		t.Fatalf("got %q, %v", compact, err)
	}
}
