package authruntime

import (
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
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

// The gauge reaches the operator /metrics of auth-service, whatever the value of the key (a present key with an empty
// value counts). It runs in a fresh process: the process-wide meter provider is installed once, so an earlier test in
// this package would hide a gauge recorded before the install.
func TestTheRemovedOrgsOwnedGaugeReachesTheOperatorMetrics(t *testing.T) {
	if os.Getenv("AUTHRUNTIME_GAUGE_HELPER") == "1" {
		for _, value := range []string{"true", ""} {
			env := brokenEnvironment(t, anyAPIAddress, anyOperatorAddress)
			env["AUTH_ORGS_OWNED"] = value
			_, stop, _, operatorAddress := startOnAnyPort(t, env)
			client := serviceClient(10 * time.Second)
			scrape := awaitResponse(t, client, "http://"+operatorAddress+"/metrics")
			body, err := io.ReadAll(scrape.Body)
			_ = scrape.Body.Close()
			client.CloseIdleConnections()
			if err != nil {
				t.Fatalf("read /metrics: %v", err)
			}
			if !regexp.MustCompile(`(?m)^dev_health_auth_orgs_owned_ignored(\{[^}]*\})? 1$`).Match(body) {
				t.Errorf("%q: operator /metrics lacks dev_health_auth_orgs_owned_ignored 1\n%s", value, body)
			}
			if code := stop(); code != 0 {
				t.Errorf("%q: Execute exited %d, want 0", value, code)
			}
		}
		return
	}
	command := exec.Command(os.Args[0], "-test.run=^TestTheRemovedOrgsOwnedGaugeReachesTheOperatorMetrics$", "-test.v", "-test.count=1")
	command.Env = append(os.Environ(), "AUTHRUNTIME_GAUGE_HELPER=1")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("helper process failed: %v\n%s", err, output)
	}
}
