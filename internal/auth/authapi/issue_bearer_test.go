package authapi

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// testIssueKeyID is the kid of the one key the default session issue workload path trusts.
const testIssueKeyID = "issue-workload-test"

// testIssue is the default go-api workload of the session issue routes in this package's tests: one in-memory
// signing key and one workload principal, shared by every fixture so issuerBearer needs none.
var testIssue struct {
	once    sync.Once
	private ed25519.PrivateKey
	keys    staticKeySet
	subject string
}

// staticKeySet is a KeySetSource that always answers the same set.
type staticKeySet struct{ set token.KeySet }

func (s staticKeySet) KeySet(context.Context) (token.KeySet, error) { return s.set, nil }

func testIssueSetup() {
	testIssue.once.Do(func() {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			panic(err)
		}
		set, err := token.NewKeySet([]token.Key{{KeyID: testIssueKeyID, State: token.StateActive, Algorithm: token.Algorithm, PublicKey: public}})
		if err != nil {
			panic(err)
		}
		testIssue.private, testIssue.keys, testIssue.subject = private, staticKeySet{set: set}, session.WirePrincipalID(uuid.New())
	})
}

// testIssueWorkload is the default workload path: issuerBearer's tokens pass it.
func testIssueWorkload() *SessionIssueWorkload {
	testIssueSetup()
	workload, err := NewSessionIssueWorkload(SessionIssueWorkloadConfig{IssuerURL: goldenIssuer, Keys: testIssue.keys, Subject: testIssue.subject})
	if err != nil {
		panic(err)
	}
	return workload
}

// testIssueSubjectID is the default workload principal, as a uuid.
func testIssueSubjectID() uuid.UUID {
	testIssueSetup()
	id, ok := parseWirePrincipal(testIssue.subject)
	if !ok {
		panic("the test issue subject is not a principal id")
	}
	return id
}

// issuerBearer is the Authorization header of a fresh go-api workload token the default workload path accepts: the
// one credential the session issue, state and delegated routes take.
func issuerBearer() map[string]string {
	testIssueSetup()
	now := time.Now()
	claims, err := json.Marshal(contracts.WorkloadTokenClaims{
		Iss: goldenIssuer, Aud: issueWorkloadAudience, Sub: testIssue.subject, PrincipalType: "workload", Jti: uuid.NewString(),
		Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 300,
	})
	if err != nil {
		panic(err)
	}
	header, err := json.Marshal(map[string]string{"alg": token.Algorithm, "kid": testIssueKeyID, "typ": contracts.WorkloadTokenTyp})
	if err != nil {
		panic(err)
	}
	enc := base64.RawURLEncoding
	input := enc.EncodeToString(header) + "." + enc.EncodeToString(claims)
	return map[string]string{"Authorization": "Bearer " + input + "." + enc.EncodeToString(ed25519.Sign(testIssue.private, []byte(input)))}
}
