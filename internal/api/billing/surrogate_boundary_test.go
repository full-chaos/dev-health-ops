package billing

import (
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"
)

// A lone surrogate in any string position of a billing body is a 422 at the
// body boundary, before the handler's database read; the route rig has no pool,
// so a request that is not refused answers 500.
func TestBillingBodiesRefuseALoneSurrogateAtTheBoundary(t *testing.T) {
	rig := newRouteRig(t)
	p := rig.impersonator(t, nil, "admin", false, false)
	post := func(body string) (int, string) {
		status, text, _ := rig.send(t, billingCall{"create plan", http.MethodPost, prefix + "/plans", body}, p.bearer, nil)
		return status, text
	}
	cases := []struct{ name, body, loc string }{
		{"str key", `{"key":"k\ud800","name":"n","tier":"team"}`, `["body","key"]`},
		{"str name", `{"key":"k","name":"a\udc00b","tier":"team"}`, `["body","name"]`},
		{"str description", `{"key":"k","name":"n","description":"d\ud800","tier":"team"}`, `["body","description"]`},
		{"list item bundle_ids", `{"key":"k","name":"n","tier":"team","bundle_ids":["\ud800"]}`, `["body","bundle_ids",0]`},
		{"dict value metadata", `{"key":"k","name":"n","tier":"team","metadata":{"x":"\ud800"}}`, `["body","metadata","x"]`},
		{"dict key metadata", `{"key":"k","name":"n","tier":"team","metadata":{"\ud800":"x"}}`, `["body","metadata","[key]"]`},
		{"int field display_order", `{"key":"k","name":"n","tier":"team","display_order":"\ud800"}`, `["body","display_order"]`},
		{"enum field tier", `{"key":"k","name":"n","tier":"\ud800"}`, `["body","tier"]`},
		{"uuid-typed list item bundle_ids", `{"key":"k","name":"n","tier":"team","bundle_ids":["\udc00"]}`, `["body","bundle_ids",0]`},
	}
	for _, c := range cases {
		status, text := post(c.body)
		lower := strings.ToLower(text)
		if status != http.StatusUnprocessableEntity || !strings.Contains(text, `"string_unicode"`) || !strings.Contains(strings.ReplaceAll(text, " ", ""), c.loc) {
			t.Errorf("%s: %d %s, want 422 string_unicode at %s", c.name, status, text, c.loc)
		}
		if !utf8.ValidString(text) || strings.Contains(lower, "ud800") || strings.Contains(lower, "udc00") || strings.Contains(lower, `"input"`) {
			t.Errorf("%s: the refusal echoes the input: %s", c.name, text)
		}
	}
	if status, text := post(`{"key":"k","name":"n","tier":"team"}`); status == http.StatusUnprocessableEntity {
		t.Errorf("a clean body was refused: %d %s", status, text)
	}
}
