package authapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
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
	f := newProvFixtureFor(t, rows, w)
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
	f := newProvFixtureOff(t, docWithRows(t, append(readPatterns(), PatternOrgSuspend, PatternOrgRename)...))
	seedReads(f)
	w := newWorkloadIssueFixture(t)
	header, _ := w.mint(contracts.WorkloadTokenTyp, nil)
	for _, c := range readCases() {
		expectRefusal(t, f.post(c.path, c.body(f), header), http.StatusUnauthorized, reasonInvalidCredentials)
	}
}

func TestReadRoutesStayDarkUnlessTheRowListsExactlyTheWorkloadToken(t *testing.T) {
	for _, classes := range [][]string{{serviceSessionIssuerClass}, {serviceSessionIssuerClass, workloadTokenClass}, {workloadTokenClass, serviceSessionIssuerClass}} {
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

func TestReadRoutesRefuseOtherKeyClassesOnTheWorkloadPath(t *testing.T) {
	for _, typ := range []string{"at+jwt", "jwt", contracts.ServiceAccountTokenTyp, "workload+jwt-x"} {
		for _, c := range readCases() {
			f := newReadWorkloadFixture(t, nil)
			header, _ := f.w.mint(typ, nil)
			expectRefusal(t, f.post(c.path, c.body(f.provFixture), header), http.StatusUnauthorized, reasonInvalidCredentials)
			if f.reads.calls+f.grantReads.calls != 0 {
				t.Errorf("%s, typ %s: a refused token reached a store", c.op, typ)
			}
		}
	}
}

func TestReadRoutesRefuseEveryWorkloadTokenWithoutAConfiguredSubject(t *testing.T) {
	for _, c := range readCases() {
		w := newWorkloadIssueFixture(t)
		unset := &SessionIssueWorkload{verifier: w.workload.verifier, now: w.workload.now}
		rows := docWithRows(t, append(readPatterns(), PatternOrgSuspend, PatternOrgRename)...)
		unsetFixture := *w
		unsetFixture.workload = unset
		f := newProvFixtureFor(t, rows, &unsetFixture)
		seedReads(f)
		for _, mutate := range []func(*contracts.WorkloadTokenClaims){nil, func(cl *contracts.WorkloadTokenClaims) { cl.Sub = "" }} {
			header, _ := w.mint(contracts.WorkloadTokenTyp, mutate)
			expectRefusal(t, f.post(c.path, c.body(f), header), http.StatusUnauthorized, reasonInvalidCredentials)
		}
		if f.reads.calls+f.grantReads.calls != 0 {
			t.Errorf("%s: a token reached a store with no subject configured", c.op)
		}
	}
}

func TestReadRoutesRefuseTheSubjectAnotherRouteAllows(t *testing.T) {
	for _, c := range readCases() {
		f := newReadWorkloadFixture(t, nil)
		otherSubject := session.WirePrincipalID(uuid.New())
		otherRoute, err := NewSessionIssueWorkload(SessionIssueWorkloadConfig{IssuerURL: goldenIssuer, Keys: f.w.keys, Subject: otherSubject})
		if err != nil {
			t.Fatal(err)
		}
		header, _ := f.w.mint(contracts.WorkloadTokenTyp, func(cl *contracts.WorkloadTokenClaims) { cl.Sub = otherSubject })
		if _, why, err := otherRoute.authorize(strings.TrimPrefix(header["Authorization"], "Bearer ")); why != "" || err != nil {
			t.Fatalf("the other route must allow its subject: %q %v", why, err)
		}
		expectRefusal(t, f.post(c.path, c.body(f.provFixture), header), http.StatusUnauthorized, reasonInvalidCredentials)
		if f.reads.calls+f.grantReads.calls != 0 {
			t.Errorf("%s: a token for another route's subject reached a store", c.op)
		}
	}
}
