package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRouteTemplatesMatchTheRegisteredKey(t *testing.T) {
	none := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	templates, err := NewRouteTemplates([]Route{
		{Method: http.MethodGet, Pattern: "/a/{id}", Handler: none},
		{Method: http.MethodGet, Pattern: "/a/literal", Handler: none},
		{Method: http.MethodPost, Pattern: "/a/{id}", Handler: none},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path, want string
		ok                 bool
	}{
		{http.MethodGet, "/a/7", "GET /a/{id}", true},
		{http.MethodGet, "/a/literal", "GET /a/literal", true},
		{http.MethodPost, "/a/literal", "POST /a/{id}", true},
		{http.MethodHead, "/a/7", "GET /a/{id}", true},
		{http.MethodDelete, "/a/7", "", false},
		{http.MethodGet, "/a/7/", "", false},
		{http.MethodGet, "/b", "", false},
	} {
		got, ok := templates.Match(httptest.NewRequest(tc.method, tc.path, nil))
		if got != tc.want || ok != tc.ok {
			t.Errorf("%s %s: %q %v, want %q %v", tc.method, tc.path, got, ok, tc.want, tc.ok)
		}
	}
}

func TestRouteTemplatesRefuseARouteTheMuxRefuses(t *testing.T) {
	none := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	for name, routes := range map[string][]Route{
		"twice":    {{Method: http.MethodGet, Pattern: "/a", Handler: none}, {Method: http.MethodGet, Pattern: "/a", Handler: none}},
		"conflict": {{Method: http.MethodGet, Pattern: "/{x}/b", Handler: none}, {Method: http.MethodGet, Pattern: "/a/{y}", Handler: none}},
		"unrooted": {{Method: http.MethodGet, Pattern: "a", Handler: none}},
	} {
		if _, err := NewRouteTemplates(routes); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
