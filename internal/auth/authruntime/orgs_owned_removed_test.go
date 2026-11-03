package authruntime

import (
	"strings"
	"testing"
)

// A set AUTH_ORGS_OWNED does not stop auth-service: it starts, and the log carries one ERROR line that names the key
// and not its value.
func TestExecuteIgnoresAndReportsTheRemovedOrgsOwnedKey(t *testing.T) {
	for _, value := range []string{"true", "false", "zz-not-a-switch-4471"} {
		env := brokenEnvironment(t, anyAPIAddress, anyOperatorAddress)
		env["AUTH_ORGS_OWNED"] = value
		logs, stop, _, _ := startOnAnyPort(t, env)
		if code := stop(); code != 0 {
			t.Errorf("%q: Execute exited %d, want 0 after a clean shutdown", value, code)
		}
		errorLines := 0
		for _, line := range strings.Split(logs.String(), "\n") {
			if strings.Contains(line, `"level":"ERROR"`) && strings.Contains(line, "AUTH_ORGS_OWNED") {
				errorLines++
				if strings.Contains(line, "zz-not-a-switch") {
					t.Errorf("%q: the value reached the log", value)
				}
			}
		}
		if errorLines != 1 {
			t.Errorf("%q: %d ERROR lines name the key, want 1", value, errorLines)
		}
	}
}
