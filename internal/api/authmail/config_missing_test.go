package authmail

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

func TestLinkSecretIsTheOnlySigningAndAcceptKey(t *testing.T) {
	zero := Config{}
	if zero.Configured() {
		t.Fatal("a zero Config is configured")
	}
	if got := zero.Secret(); got != "" {
		t.Fatalf("zero Config signs with a %d byte key, want none", len(got))
	}
	if got := zero.AcceptSecrets(); len(got) != 0 {
		t.Fatalf("zero Config accepts %d keys, want none", len(got))
	}
	onlyPrevious := Config{PreviousLinkSecret: "link-old"}
	if onlyPrevious.Configured() || len(onlyPrevious.AcceptSecrets()) != 0 {
		t.Fatal("a previous key alone configures link tokens")
	}
	c := Config{LinkSecret: "link-new", PreviousLinkSecret: "link-old"}
	if !c.Configured() || !slices.Equal(c.AcceptSecrets(), []string{"link-new", "link-old"}) {
		t.Fatalf("accept list %v, want [link-new link-old]", c.AcceptSecrets())
	}
}

func TestRefuseUnconfiguredAnswers503ConfigMissing(t *testing.T) {
	for name, c := range map[string]Config{"zero": {}, "previous only": {PreviousLinkSecret: "link-old"}} {
		recorder := httptest.NewRecorder()
		if !c.RefuseUnconfigured(recorder) {
			t.Fatalf("%s: RefuseUnconfigured = false", name)
		}
		if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"detail":"config_missing: LINK_TOKEN_SECRET"`) {
			t.Fatalf("%s: status %d body %q, want 503 config_missing", name, recorder.Code, recorder.Body.String())
		}
	}
	recorder := httptest.NewRecorder()
	if (Config{LinkSecret: "link-new"}).RefuseUnconfigured(recorder) || recorder.Body.Len() != 0 {
		t.Fatal("a configured Config refused")
	}
}
