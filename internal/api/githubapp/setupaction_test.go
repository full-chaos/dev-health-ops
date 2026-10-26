package githubapp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

// setup_action is written into the credential's json config, which stores a
// lone surrogate as an escape instead of refusing it, so it is refused here.
func TestInstallCallbackBodyRefusesALoneSurrogateSetupAction(t *testing.T) {
	read := func(raw string) pybody.Body {
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(raw))
		request.Header.Set("Content-Type", "application/json")
		body, outcome, _, err := pybody.Read(request)
		if err != nil || outcome != pybody.Ready {
			t.Fatalf("read: %v %v", outcome, err)
		}
		return body
	}
	for _, escape := range []string{`\ud800`, `\udbff`, `\udc00`, `\udfff`} {
		_, problems := parseCallbackBody(read(`{"installation_id":1,"state":"s","setup_action":"a` + escape + `"}`))
		if len(problems) != 1 || problems[0].Type != "string_unicode" || len(problems[0].Loc) != 2 || problems[0].Loc[1] != "setup_action" {
			t.Errorf("%s: %+v", escape, problems)
		}
	}
	callback, problems := parseCallbackBody(read(`{"installation_id":1,"state":"s","setup_action":"install😀"}`))
	if len(problems) != 0 || callback.setupAction == nil {
		t.Errorf("a surrogate pair: %+v", problems)
	}
}
