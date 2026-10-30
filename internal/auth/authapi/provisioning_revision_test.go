package authapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
)

const stateRoute = "/internal/v1/organizations/state"

// revisionRoute is one of the two verbs that take an optional if_revision.
type revisionRoute struct {
	name, path string
	calls      func(f *provFixture) int
	got        func(f *provFixture) *int64
}

func revisionRoutes() []revisionRoute {
	return []revisionRoute{
		{"reactivate", PatternOrgReactivate, func(f *provFixture) int { return f.reactivate.calls }, func(f *provFixture) *int64 { return f.reactivate.ifRevision }},
		{"suspend", PatternOrgSuspend, func(f *provFixture) int { return f.reactivate.suspends }, func(f *provFixture) *int64 { return f.reactivate.sIfRevision }},
	}
}

func (f *provFixture) bodyWith(ifRevision string) string {
	text := fmt.Sprintf(`{"organization_id":%q,"actor_principal_id":%q`, f.org, f.super)
	if ifRevision != "" {
		text += `,"if_revision":` + ifRevision
	}
	return text + "}"
}

// if_revision is optional on both routes: absent and null are no check (nil to the verb); an integer from 0 up is passed
// to the verb as it is; any other value is a 400 before the verb.
func TestIfRevisionDecodeDomainOnBothRoutes(t *testing.T) {
	zero, seven, top := int64(0), int64(7), int64(9223372036854775807)
	for _, route := range revisionRoutes() {
		for name, tc := range map[string]struct {
			raw  string
			want *int64
		}{
			"absent": {"", nil}, "null": {"null", nil}, "zero": {"0", &zero}, "seven": {"7", &seven}, "max": {"9223372036854775807", &top},
		} {
			f := newProvFixture(t)
			if got := f.post(route.path, f.bodyWith(tc.raw), nil); got.Code != http.StatusOK {
				t.Fatalf("%s %s: %d %s, want 200", route.name, name, got.Code, got.Body)
			}
			got := route.got(f)
			if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
				t.Fatalf("%s %s: the verb got %v, want %v", route.name, name, got, tc.want)
			}
		}
		for name, raw := range map[string]string{"negative": "-1", "fraction": "1.5", "string": `"1"`, "overflow": "9223372036854775808",
			"exponent": "1e2", "bool": "true", "array": "[]", "object": "{}", "leading zero": "01"} {
			f := newProvFixture(t)
			expectRefusal(t, f.post(route.path, f.bodyWith(raw), nil), http.StatusBadRequest, reasonMalformed)
			if route.calls(f) != 0 {
				t.Fatalf("%s %s: a malformed if_revision reached the verb", route.name, name)
			}
		}
		f := newProvFixture(t)
		expectRefusal(t, f.post(route.path, strings.Replace(f.bodyWith("3"), `"if_revision":3`, `"if_revision":3,"if_revision":4`, 1), nil),
			http.StatusBadRequest, reasonMalformed)
	}
}

// A stale if_revision is the verb's ErrRevisionConflict: 409 state_changed on both routes, whether the organization is at
// the target status (a no-op) or not. The verb is given the revision; the route answers a refusal, never a 200.
func TestStaleIfRevisionAnswers409StateChangedOnBothRoutes(t *testing.T) {
	for _, route := range revisionRoutes() {
		for _, noop := range []bool{false, true} {
			f := newProvFixture(t)
			f.reactivate.err = org.ErrRevisionConflict
			f.reactivate.noop = noop
			expectRefusal(t, f.post(route.path, f.bodyWith("4"), nil), http.StatusConflict, reasonStateChanged)
			if got := route.got(f); route.calls(f) != 1 || got == nil || *got != 4 {
				t.Fatalf("%s noop=%v: the verb was called %d times with %v, want once with 4", route.name, noop, route.calls(f), got)
			}
		}
	}
}

// With no if_revision a conflict cannot happen, and any other store fault stays the fail-closed 503.
func TestIfRevisionRouteFaultsStayFailClosed(t *testing.T) {
	for _, route := range revisionRoutes() {
		f := newProvFixture(t)
		f.reactivate.err = fmt.Errorf("%w: down", org.ErrStore)
		expectRefusal(t, f.post(route.path, f.bodyWith("4"), nil), http.StatusServiceUnavailable, reasonStoreUnavailable)
	}
}

// docWithExtraRow adds a copy of the deprovision row for route to a document that may already hold others.
func docWithExtraRow(t *testing.T, doc []byte, route string) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(doc, &document); err != nil {
		t.Fatal(err)
	}
	rows := document["rows"].([]any)
	for _, item := range rows {
		row := item.(map[string]any)
		if row["route"] == PatternOrgDeprovision && row["service"] == AccessService {
			clone := map[string]any{}
			for k, v := range row {
				clone[k] = v
			}
			clone["route"] = route
			clone["id"] = "POST " + route + " [dev-health-auth-service @api]"
			document["rows"] = append(rows, clone)
			raw, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}
	}
	t.Fatal("no deprovision row")
	return nil
}

func (f *provFixture) stateBody() string { return fmt.Sprintf(`{"organization_id":%q}`, f.org) }

// The state route answers the organization's status and revision, read once (one statement: the pair is never made of
// two reads), with the organization id; it writes nothing and reads no actor.
func TestOrgStateAnswersStatusAndRevisionFromOneRead(t *testing.T) {
	for status, want := range map[org.Status]string{org.StatusActive: "active", org.StatusSuspended: "suspended"} {
		f := newProvFixtureWith(t, docWithRow(t, stateRoute))
		f.reactivate.status, f.reactivate.revision, f.reactivate.revStep = status, 7, 1
		response := f.post(stateRoute, f.stateBody(), nil)
		if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("state: %d (Cache-Control %q) %s", response.Code, response.Header().Get("Cache-Control"), response.Body)
		}
		var got map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 || got["organization_id"] != f.org.String() || got["status"] != want || got["revision"] != float64(7) {
			t.Fatalf("state answer = %v, want organization_id, status %s and revision 7 only", got, want)
		}
		if f.reactivate.gets != 1 || f.reactivate.gid != f.org {
			t.Fatalf("the route read the organization %d times (%s), want one read", f.reactivate.gets, f.reactivate.gid)
		}
		if f.reactivate.calls+f.reactivate.suspends != 0 || f.users.reads != 0 || f.users.roleReads != 0 {
			t.Fatalf("the state route wrote or read an actor (verbs %d, users %d)", f.reactivate.calls+f.reactivate.suspends, f.users.reads)
		}
	}
}

func TestOrgStateRefusalSet(t *testing.T) {
	for name, tc := range map[string]struct {
		err    error
		status int
		reason string
	}{
		"unknown organization": {org.ErrNotFound, http.StatusNotFound, reasonOrganizationNotFound},
		"store down":           {fmt.Errorf("%w: read: down", org.ErrStore), http.StatusServiceUnavailable, reasonStoreUnavailable},
		"unclassified":         {errors.New("boom"), http.StatusServiceUnavailable, reasonStoreUnavailable},
	} {
		f := newProvFixtureWith(t, docWithRow(t, stateRoute))
		f.reactivate.getErr = tc.err
		expectRefusal(t, f.post(stateRoute, f.stateBody(), nil), tc.status, tc.reason)
		if f.reactivate.gets != 1 {
			t.Fatalf("%s: %d reads", name, f.reactivate.gets)
		}
	}
}

// The body is one strict object of organization_id alone.
func TestOrgStateBodyIsStrict(t *testing.T) {
	f := newProvFixtureWith(t, docWithRow(t, stateRoute))
	for name, body := range map[string]string{
		"empty":         `{}`,
		"not a uuid":    `{"organization_id":"nope"}`,
		"upper case":    fmt.Sprintf(`{"organization_id":%q}`, strings.ToUpper(f.org.String())),
		"extra member":  fmt.Sprintf(`{"organization_id":%q,"actor_principal_id":%q}`, f.org, f.super),
		"duplicate":     fmt.Sprintf(`{"organization_id":%q,"organization_id":%q}`, f.org, f.org),
		"trailing data": f.stateBody() + `{}`,
		"not an object": `[]`,
		"null id":       `{"organization_id":null}`,
	} {
		expectRefusal(t, f.post(stateRoute, body, nil), http.StatusBadRequest, reasonMalformed)
		if f.reactivate.gets != 0 {
			t.Fatalf("%s: a malformed body reached the store", name)
		}
	}
}

// Only the session issuer service credential reaches the route: no bearer, a refused or unknown credential, and the
// go-api workload token (the route is outside the workload per-op set) are the one 401, before any read.
func TestOrgStateTakesTheIssuerServiceCredentialOnly(t *testing.T) {
	f := newProvFixtureWith(t, docWithRow(t, stateRoute))
	expectRefusal(t, f.post(stateRoute, f.stateBody(), map[string]string{}), http.StatusUnauthorized, reasonInvalidCredentials)
	f.creds.refusal = servicecred.RefusedRevoked
	expectRefusal(t, f.post(stateRoute, f.stateBody(), nil), http.StatusUnauthorized, reasonInvalidCredentials)
	if f.reactivate.gets != 0 {
		t.Fatal("a refused credential reached the store")
	}

	w := newWorkloadIssueFixture(t)
	g := newProvFixtureFor(t, docWithRow(t, stateRoute), w.workload)
	header, _ := w.mint("workload+jwt", nil)
	expectRefusal(t, g.post(stateRoute, g.stateBody(), header), http.StatusUnauthorized, reasonInvalidCredentials)
	if g.reactivate.gets != 0 || g.storeCalls() != 0 {
		t.Fatal("a workload token reached the state read")
	}
	if got := g.post(stateRoute, g.stateBody(), nil); got.Code != http.StatusOK {
		t.Fatalf("the issuer credential on the state route: %d %s", got.Code, got.Body)
	}
}

// Dark: with no endpoint-profile row for the route it answers 404 and one startup line names it; the suspend route's
// row does not mount it and its row does not mount the suspend route.
func TestOrgStateIsDarkWithoutItsProfileRow(t *testing.T) {
	for name, doc := range map[string][]byte{"document without the row": docWithoutRow(t, nil, stateRoute),
		"suspend row only": docWithoutRow(t, docWithRow(t, PatternOrgSuspend), stateRoute)} {
		f := newProvFixtureWith(t, doc)
		if got := f.post(stateRoute, f.stateBody(), nil); got.Code != http.StatusNotFound {
			t.Fatalf("%s: state without its row: %d %s, want 404", name, got.Code, got.Body)
		}
		if f.reactivate.gets != 0 {
			t.Fatalf("%s: the store ran for a route that is not mounted", name)
		}
		lines := 0
		for _, line := range strings.Split(strings.TrimSpace(f.logs.String()), "\n") {
			if strings.Contains(line, stateRoute) {
				lines++
				if !strings.Contains(line, "not mounted") {
					t.Fatalf("%s: the startup line does not say the route is not mounted: %s", name, line)
				}
			}
		}
		if lines != 1 {
			t.Fatalf("%s: %d startup lines name the state route, want 1: %s", name, lines, f.logs.String())
		}
	}
	f := newProvFixtureWith(t, docWithExtraRow(t, docWithoutRow(t, PatternOrgSuspend), stateRoute))
	if got := f.post(PatternOrgSuspend, f.bodyWith(""), nil); got.Code != http.StatusNotFound {
		t.Fatalf("suspend with only the state row: %d, want 404", got.Code)
	}
	both := newProvFixtureWith(t, docWithExtraRow(t, docWithRow(t, PatternOrgSuspend), stateRoute))
	if got := both.post(stateRoute, both.stateBody(), nil); got.Code != http.StatusOK {
		t.Fatalf("state with its row: %d %s, want 200", got.Code, got.Body)
	}
	if strings.Contains(both.logs.String(), "not mounted") {
		t.Fatalf("a not-mounted line with both rows present: %s", both.logs.String())
	}
}

func TestOrgStateNeedsAnAuthServicePostRowAcceptingTheIssuerCredential(t *testing.T) {
	bare := docWithoutRow(t, nil, stateRoute)
	for name, edit := range map[string]func(row map[string]any){
		"another service": func(row map[string]any) { row["service"] = "dev-health-ops-api" },
		"another method":  func(row map[string]any) { row["method"] = "GET" },
		"another class":   func(row map[string]any) { row["accepted_credential_classes"] = []any{"workload_token"} },
	} {
		var document map[string]any
		if err := json.Unmarshal(docWithExtraRow(t, bare, stateRoute), &document); err != nil {
			t.Fatal(err)
		}
		rows := document["rows"].([]any)
		edit(rows[len(rows)-1].(map[string]any))
		raw, err := json.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		f := newProvFixtureWith(t, raw)
		if got := f.post(stateRoute, f.stateBody(), nil); got.Code != http.StatusNotFound {
			t.Fatalf("%s: state %d, want 404", name, got.Code)
		}
	}
}

// docWithoutRow is doc (nil is the embedded document) with every row of route removed.
func docWithoutRow(t *testing.T, doc []byte, route string) []byte {
	t.Helper()
	if doc == nil {
		doc = authv1.AuthServiceEndpointProfiles
	}
	var document map[string]any
	if err := json.Unmarshal(doc, &document); err != nil {
		t.Fatal(err)
	}
	kept := []any{}
	for _, item := range document["rows"].([]any) {
		if item.(map[string]any)["route"] != route {
			kept = append(kept, item)
		}
	}
	document["rows"] = kept
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// The embedded endpoint-profile document holds the state route's row: the route is served with it, no startup line says
// it is not mounted, and its answer is the one the route gives with a planted row.
func TestOrgStateIsServedWithTheEmbeddedDocument(t *testing.T) {
	f := newProvFixtureWith(t, nil)
	f.reactivate.status, f.reactivate.revision = org.StatusActive, 3
	if got := f.post(stateRoute, f.stateBody(), nil); got.Code != http.StatusOK {
		t.Fatalf("state on the embedded document: %d %s, want 200", got.Code, got.Body)
	}
	if strings.Contains(f.logs.String(), stateRoute) {
		t.Fatalf("a startup line names the state route: %s", f.logs.String())
	}
}
