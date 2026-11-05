package authapi

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

type readWorkloadFixture struct {
	*provFixture
	w *workloadIssueFixture
}

func newReadWorkloadFixture(t *testing.T, rows []byte) *readWorkloadFixture {
	t.Helper()
	w := newWorkloadIssueFixture(t)
	if rows == nil {
		rows = docWithRows(t, append(readPatterns(), PatternOrgSuspend, PatternOrgRename)...)
	}
	f := newProvFixtureFor(t, rows, w.workload)
	seedReads(f)
	return &readWorkloadFixture{provFixture: f, w: w}
}

func TestReadRoutesAcceptTheGoAPIWorkloadToken(t *testing.T) {
	for _, c := range readCases() {
		f := newReadWorkloadFixture(t, nil)
		header, _ := f.w.mint(contracts.WorkloadTokenTyp, nil)
		got := f.post(c.path, c.body(f.provFixture), header)
		if got.Code != http.StatusOK {
			t.Errorf("%s with the go-api workload token: %d %s", c.op, got.Code, got.Body)
		}
		if len(f.creds.scopes) != 0 {
			t.Errorf("%s: a workload token reached the service credential check: %v", c.op, f.creds.scopes)
		}
		if f.storeCalls() != 0 || f.users.reads != 0 {
			t.Errorf("%s wrote or read an actor", c.op)
		}
	}
}

func TestReadRoutesRefuseOtherWorkloadTokens(t *testing.T) {
	refused := map[string]func(*contracts.WorkloadTokenClaims){
		"foreign subject": func(c *contracts.WorkloadTokenClaims) { c.Sub = "prn_00000000-0000-4000-8000-000000000001" },
		"wrong audience":  func(c *contracts.WorkloadTokenClaims) { c.Aud = "dev-health-api" },
		"expired":         func(c *contracts.WorkloadTokenClaims) { c.Iat -= 600; c.Nbf -= 600; c.Exp -= 600 },
	}
	for name, mutate := range refused {
		for _, c := range readCases() {
			f := newReadWorkloadFixture(t, nil)
			header, _ := f.w.mint(contracts.WorkloadTokenTyp, mutate)
			expectRefusal(t, f.post(c.path, c.body(f.provFixture), header), http.StatusUnauthorized, reasonInvalidCredentials)
			if f.reads.calls+f.grantReads.calls != 0 {
				t.Errorf("%s, %s: a refused workload token reached a store", c.op, name)
			}
		}
	}
	f := newReadWorkloadFixture(t, nil)
	header := map[string]string{"Authorization": "Bearer e30.e30.c2ln"}
	for _, c := range readCases() {
		expectRefusal(t, f.post(c.path, c.body(f.provFixture), header), http.StatusUnauthorized, reasonInvalidCredentials)
	}
}

func TestReadRoutesRefuseAWorkloadTokenWithoutTheWorkloadPath(t *testing.T) {
	f := newReadFixture(t)
	w := newWorkloadIssueFixture(t)
	header, _ := w.mint(contracts.WorkloadTokenTyp, nil)
	for _, c := range readCases() {
		expectRefusal(t, f.post(c.path, c.body(f), header), http.StatusUnauthorized, reasonInvalidCredentials)
	}
}

func TestReadRoutesServiceCredentialUnchangedWithWorkloadPath(t *testing.T) {
	for _, c := range readCases() {
		f := newReadWorkloadFixture(t, nil)
		f.creds.held = []string{c.scope}
		if got := f.post(c.path, c.body(f.provFixture), nil); got.Code != http.StatusOK {
			t.Errorf("%s with its scope: %d %s", c.op, got.Code, got.Body)
		}
		other := newReadWorkloadFixture(t, nil)
		other.creds.held = []string{"memberships:write"}
		expectRefusal(t, other.post(c.path, c.body(other.provFixture), nil), http.StatusUnauthorized, reasonInvalidCredentials)
		if !slices.Equal(other.creds.scopes, []string{c.scope}) {
			t.Errorf("%s asked for scopes %v", c.op, other.creds.scopes)
		}
	}
}

func TestReadRoutesStayDarkUnlessTheRowListsBothClasses(t *testing.T) {
	for _, classes := range [][]string{{serviceSessionIssuerClass}, {workloadTokenClass}} {
		for _, c := range readCases() {
			f := newReadWorkloadFixture(t, plantReadClasses(t, c.path, classes...))
			header, _ := f.w.mint(contracts.WorkloadTokenTyp, nil)
			if got := f.post(c.path, c.body(f.provFixture), header); got.Code != http.StatusNotFound {
				t.Errorf("%s with a row listing %v: %d, want 404", c.op, classes, got.Code)
			}
		}
	}
}

func plantReadClasses(t *testing.T, route string, classes ...string) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(docWithRows(t, append(readPatterns(), PatternOrgSuspend, PatternOrgRename)...), &document); err != nil {
		t.Fatal(err)
	}
	for _, item := range document["rows"].([]any) {
		row := item.(map[string]any)
		if row["route"] == route && row["service"] == AccessService && row["method"] == "POST" {
			list := make([]any, 0, len(classes))
			for _, class := range classes {
				list = append(list, class)
			}
			row["accepted_credential_classes"] = list
		}
	}
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
