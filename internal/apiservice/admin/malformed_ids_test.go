package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

const validUUID = "782e33f1-da29-5013-9368-6c2888baef54"

func answerWith(handler http.HandlerFunc, method string, setup func(*http.Request) *http.Request) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/api/v1/admin/x", nil)
	req = req.WithContext(policy.WithUser(req.Context(), &policy.User{IsSuperuser: true, UserID: validUUID}))
	if setup != nil {
		req = setup(req)
	}
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

func withBody(raw string) func(*http.Request) *http.Request {
	return func(req *http.Request) *http.Request {
		value, err := pyjson.DecodeString(raw)
		if err != nil {
			panic(err)
		}
		return req.WithContext(policy.WithBody(req.Context(), pybody.Body{Value: value}))
	}
}

func pathValues(pairs ...string) func(*http.Request) *http.Request {
	return func(req *http.Request) *http.Request {
		for i := 0; i < len(pairs); i += 2 {
			req.SetPathValue(pairs[i], pairs[i+1])
		}
		return req
	}
}

func compose(steps ...func(*http.Request) *http.Request) func(*http.Request) *http.Request {
	return func(req *http.Request) *http.Request {
		for _, step := range steps {
			req = step(req)
		}
		return req
	}
}

func expect400(t *testing.T, rec *httptest.ResponseRecorder, detail string) {
	t.Helper()
	if want := `{"detail":"` + detail + `"}`; rec.Code != http.StatusBadRequest || rec.Body.String() != want {
		t.Fatalf("status = %d body = %s, want 400 %s", rec.Code, rec.Body.String(), want)
	}
}

// TestMalformedUserIDIsANamed400OnEveryUserRoute pins CHAOS-10593: the four
// /admin/users/{user_id} handlers answered 500 for a value that is no UUID.
func TestMalformedUserIDIsANamed400OnEveryUserRoute(t *testing.T) {
	h := &handlers{}
	routes := map[string]struct {
		handler http.HandlerFunc
		method  string
		setup   func(*http.Request) *http.Request
	}{
		"get user":     {h.getUser, http.MethodGet, nil},
		"update user":  {h.updateUser, http.MethodPatch, nil},
		"set password": {h.setUserPassword, http.MethodPost, nil},
		"delete user":  {h.deleteUser, http.MethodDelete, nil},
	}
	for name, route := range routes {
		for _, value := range []string{"me", "current", "not-a-uuid", ""} {
			t.Run(name+"/"+value, func(t *testing.T) {
				rec := answerWith(route.handler, route.method, pathValues("user_id", value))
				expect400(t, rec, "Invalid user id")
			})
		}
	}
}

// TestMalformedXOrgIDHeaderIsANamed400 pins the list-users header: a value
// that is no UUID answered 500.
func TestMalformedXOrgIDHeaderIsANamed400(t *testing.T) {
	h := &handlers{}
	for _, value := range []string{"current", "not-a-uuid", "123"} {
		rec := answerWith(h.listUsers, http.MethodGet, func(req *http.Request) *http.Request {
			req.Header.Set("X-Org-Id", value)
			return req
		})
		expect400(t, rec, "Invalid organization id")
	}
}

// TestMalformedFeatureOverrideIDsAreNamed400 pins CHAOS-10593: the four
// feature-override routes answered 500 for a malformed org or override id.
func TestMalformedFeatureOverrideIDsAreNamed400(t *testing.T) {
	h := &handlers{}
	validBody := withBody(`{"feature_id":"` + validUUID + `"}`)
	type route struct {
		handler http.HandlerFunc
		method  string
		setup   func(...string) func(*http.Request) *http.Request
	}
	plain := func(pairs ...string) func(*http.Request) *http.Request { return pathValues(pairs...) }
	bodied := func(pairs ...string) func(*http.Request) *http.Request {
		return compose(validBody, pathValues(pairs...))
	}
	routes := map[string]route{
		"list":   {h.listFeatureOverrides, http.MethodGet, plain},
		"create": {h.createFeatureOverride, http.MethodPost, bodied},
		"patch": {h.updateFeatureOverride, http.MethodPatch, func(pairs ...string) func(*http.Request) *http.Request {
			return compose(withBody(`{}`), pathValues(pairs...))
		}},
		"delete": {h.deleteFeatureOverride, http.MethodDelete, plain},
	}
	for name, r := range routes {
		for _, bad := range []string{"not-a-uuid", "nope", "current"} {
			t.Run(name+"/org/"+bad, func(t *testing.T) {
				rec := answerWith(r.handler, r.method, r.setup("org_id", bad, "override_id", validUUID))
				expect400(t, rec, "Invalid organization id")
			})
			if name == "patch" || name == "delete" {
				t.Run(name+"/override/"+bad, func(t *testing.T) {
					rec := answerWith(r.handler, r.method, r.setup("org_id", validUUID, "override_id", bad))
					expect400(t, rec, "Invalid override id")
				})
			}
		}
	}
}
