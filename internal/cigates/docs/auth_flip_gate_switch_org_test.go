package docs_test

import (
	"strings"
	"testing"
)

const authSessionsFlipRunbook = "docs/operate/runbooks/auth-sessions-flip-and-rollback.md"

// Under the eddsa access token class, switch-org takes only the session_access_token (audience auth-service), which
// go-api's own guard refuses. The runbook's eddsa section must carry a Gate line that puts AUTH_PROXY_SESSIONS on
// before the flip and names the route, as it does for change-password and AUTH_PROXY_CREDENTIALS.
func TestTheEdDSAFlipGatesNameSwitchOrgUnderTheSessionsAggregate(t *testing.T) {
	text, err := realTree(t).read(authSessionsFlipRunbook)
	if err != nil {
		t.Fatal(err)
	}
	section, ok := sectionOf(text, "## Access token class eddsa (W3 D1)")
	if !ok {
		t.Fatal("no eddsa section in " + authSessionsFlipRunbook)
	}
	var gate string
	for _, line := range strings.Split(section, "\n") {
		if strings.HasPrefix(line, "Gate (acp):") && strings.Contains(line, "POST /api/v1/auth/switch-org") {
			gate = line
		}
	}
	if gate == "" {
		t.Fatal("no Gate (acp) line names POST /api/v1/auth/switch-org")
	}
	for _, want := range []string{"AUTH_PROXY_SESSIONS", "before eddsa is set", "session_access_token"} {
		if !strings.Contains(gate, want) {
			t.Errorf("the switch-org gate line lacks %q: %s", want, gate)
		}
	}
}
