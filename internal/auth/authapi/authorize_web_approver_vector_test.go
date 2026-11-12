package authapi

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// CHAOS-11207 (B-2): web's real signers made testdata/web-approver.v1.json
// (web src/lib/acr/__tests__/approver-vector.test.ts fails in web CI when the
// signers emit other bytes). This runs the REAL resolver and the REAL
// authorize route on it. web-approver.v1.source names the web commit and the
// file's sha256.

type approverVector struct {
	Clock     int64           `json:"clock"`
	Issuer    string          `json:"issuer"`
	JWKS      json.RawMessage `json:"jwks"`
	Principal struct {
		OrgID       string `json:"org_id"`
		SessionUUID string `json:"session_uuid"`
		SID         string `json:"sid"`
		UserID      string `json:"user_id"`
	} `json:"principal"`
	Cases []struct {
		Name              string `json:"name"`
		WebAssertion      string `json:"web_assertion"`
		ApproverReference string `json:"approver_reference"`
		Expect            struct {
			PDPReference string `json:"pdp_reference"`
		} `json:"expect"`
	} `json:"cases"`
}

func readApproverVector(t *testing.T) approverVector {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "web-approver.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	source, err := os.Open(filepath.Join("testdata", "web-approver.v1.source"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	recorded := ""
	for scanner := bufio.NewScanner(source); scanner.Scan(); {
		if value, ok := strings.CutPrefix(scanner.Text(), "sha256 "); ok {
			recorded = value
		}
	}
	if recorded != hex.EncodeToString(digest[:]) {
		t.Fatalf("vector sha256 %x; the .source file records %q", digest, recorded)
	}
	var v approverVector
	if err := json.Unmarshal(raw, &v); err != nil || len(v.Cases) == 0 {
		t.Fatalf("vector: %v", err)
	}
	return v
}

// C-3: the vector's sub and sid are the forms the real ops builders give: the
// sign-in answer's user.id (userInfo) and the session.v1 wire session id.
func TestWebApproverVector_subAndSidAreTheOpsBuilderForms(t *testing.T) {
	v := readApproverVector(t)
	principal := uuid.MustParse(v.Principal.UserID)
	id, _ := userInfo(principal, UserState{}, nil).Get("id")
	if id != v.Principal.UserID {
		t.Fatalf("userInfo id %v; the vector's sub is %q", id, v.Principal.UserID)
	}
	if wire := session.WireSessionID(uuid.MustParse(v.Principal.SessionUUID)); wire != v.Principal.SID {
		t.Fatalf("WireSessionID %q; the vector's sid is %q", wire, v.Principal.SID)
	}
}

func approverVectorReferences(t *testing.T, v approverVector, caller string, at time.Time) *PrincipalReferences {
	t.Helper()
	jwks := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(jwks, v.JWKS, 0o600); err != nil {
		t.Fatal(err)
	}
	approver, err := NewWebApproverReferences(WebApproverReferencesConfig{Issuer: v.Issuer, JWKSFile: jwks, Caller: caller, Now: func() time.Time { return at }})
	if err != nil {
		t.Fatal(err)
	}
	f := newAuthorizeFixture(t)
	references, err := NewPrincipalReferences(PrincipalReferencesConfig{IssuerURL: goldenIssuer, Keys: f.keys, Now: func() time.Time { return at }, WebApprover: approver})
	if err != nil {
		t.Fatal(err)
	}
	return references
}

func TestWebApproverVector_realSignerReferencesResolve(t *testing.T) {
	v := readApproverVector(t)
	caller := session.WirePrincipalID(uuid.New())
	references := approverVectorReferences(t, v, caller, time.Unix(v.Clock, 0).UTC())
	for _, c := range v.Cases {
		t.Run(c.Name, func(t *testing.T) {
			p, why, err := references.ResolveFor(c.ApproverReference, caller)
			if err != nil {
				t.Fatal(err)
			}
			if accepted := why == ""; accepted != (c.Expect.PDPReference == "accept") {
				t.Fatalf("resolved %v (why %q); the vector expects %s", accepted, why, c.Expect.PDPReference)
			}
			if why == "" && (p.Kind != ReferenceWebApprover || p.ID.String() != v.Principal.UserID || p.SessionID != v.Principal.SID || p.OrgID != v.Principal.OrgID) {
				t.Fatalf("principal %+v; want the vector's user, sid and organization", p)
			}
		})
	}
	late := approverVectorReferences(t, v, caller, time.Unix(v.Clock, 0).UTC().Add(40*time.Second))
	if _, why, _ := late.ResolveFor(v.Cases[0].ApproverReference, caller); why == "" {
		t.Fatal("V1 resolved 40 s after it was signed")
	}
}

// K1 on the real web form: the route allows V1 over a decision state that
// holds the vector's live session and an admin membership.
func TestWebApproverVector_K1AllowsTheRealWebForm(t *testing.T) {
	v := readApproverVector(t)
	a := newApproverFixture(t)
	a.now = time.Unix(v.Clock, 0).UTC()
	a.issuer = v.Issuer
	a.principal = uuid.MustParse(v.Principal.UserID)
	a.org = v.Principal.OrgID
	a.sid = uuid.MustParse(v.Principal.SessionUUID)
	a.state.sessions[a.sid] = DecisionSession{Found: true, PrincipalID: a.principal, Live: true}
	a.state.snapshots[snapKey{a.principal, a.org}] = &fakeSnapshot{principal: authorization.StatusActive, org: a.org, orgStatus: authorization.StatusActive,
		membership: &authorization.Membership{OrgID: a.org, Role: "admin", Status: authorization.StatusActive}, now: authorizeNow}
	if err := os.WriteFile(a.jwksFile, v.JWKS, 0o600); err != nil {
		t.Fatal(err)
	}
	a.mount(t)

	status, d := a.ask(a.approver, a.issueQuestion(v.Cases[0].ApproverReference, a.org))

	if status != http.StatusOK || !d.Allowed {
		t.Fatalf("status %d decision %+v; want 200 allow", status, d)
	}
}

// The vector's key is a public test key (its seed is in web's vector test). It
// may be named only in test files and testdata: no deployed verifier may trust
// it. Every verifier test above builds its key set from the vector in-test.
func TestWebApproverVector_keyIsNamedOnlyInTests(t *testing.T) {
	var vector struct {
		JWKS struct {
			Keys []struct {
				X string `json:"x"`
			} `json:"keys"`
		} `json:"jwks"`
	}
	raw, err := os.ReadFile(filepath.Join("testdata", "web-approver.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &vector); err != nil || len(vector.JWKS.Keys) != 1 || vector.JWKS.Keys[0].X == "" {
		t.Fatalf("vector key: %v", err)
	}
	key := []byte(vector.JWKS.Keys[0].X)
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "vendor", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(content, key) {
			t.Errorf("%s names the vector's test key outside tests and testdata", path)
		}
		found++
		return nil
	})
	if err != nil || found == 0 {
		t.Fatalf("walk: %v (%d files)", err, found)
	}
}
