package authapi

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
)

// CHAOS-13356: auth-service reads go-api's AUTH_REGISTER_LIMIT ("3/hour" form, httpapi.ParseLimit). Unset is
// 3 per hour; a value set and not parseable never refuses the start: one ERROR line names the key, and the
// route runs at 3 per hour.

const registerLimitKey = "AUTH_REGISTER_LIMIT"

const goAPIRateLimited = `{"detail":{"message":"Rate limit exceeded. Please try again later."}}`

// setRegisterLimit sets AUTH_REGISTER_LIMIT for the test, or unsets it when set is false.
func setRegisterLimit(t *testing.T, value string, set bool) {
	t.Helper()
	t.Setenv(registerLimitKey, value)
	if !set {
		if err := os.Unsetenv(registerLimitKey); err != nil {
			t.Fatal(err)
		}
	}
}

// registerAnswer is one register answer: the status and the body.
type registerAnswer struct {
	code int
	body string
}

// registerFromOneAddress posts n registers from one end client address (through the trusted proxy) and
// returns the answers in order.
func registerFromOneAddress(t *testing.T, c *credFixture, n int) []registerAnswer {
	t.Helper()
	h := trustedProxy(t, c.routes)
	out := make([]registerAnswer, 0, n)
	for i := 0; i < n; i++ {
		r := send(h, proxyPeer, "198.51.100.77", http.MethodPost, PatternRegister, registerBody, nil)
		out = append(out, registerAnswer{r.Code, r.Body.String()})
	}
	return out
}

func assertThreeAnHour(t *testing.T, c *credFixture) {
	t.Helper()
	answers := registerFromOneAddress(t, c, 4)
	for i, a := range answers[:3] {
		if a.code != http.StatusAccepted {
			t.Fatalf("register %d = %d %s, want 202", i+1, a.code, a.body)
		}
	}
	if a := answers[3]; a.code != http.StatusTooManyRequests || a.body != goAPIRateLimited {
		t.Fatalf("register 4 = %d %s, want the limiter's 429 %s", a.code, a.body, goAPIRateLimited)
	}
}

// errorLines are the ERROR records of the fixture's logger.
func errorLines(t *testing.T, c *credFixture) []string {
	t.Helper()
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(c.logs.String()), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line is not JSON: %v: %.120s", err, line)
		}
		if record["level"] == "ERROR" {
			out = append(out, line)
		}
	}
	return out
}

func TestRegisterLimitUnsetIsThreeAnHour(t *testing.T) {
	registerEnv(t, "")
	setRegisterLimit(t, "", false)
	c := newCredFixture(t)
	assertThreeAnHour(t, c)
	if lines := errorLines(t, c); len(lines) != 0 {
		t.Fatalf("unset logged ERROR lines: %q", lines)
	}
}

func TestRegisterLimitFromEnvIsHonoured(t *testing.T) {
	registerEnv(t, "")
	setRegisterLimit(t, "1000/hour", true)
	c := newCredFixture(t)
	for i, a := range registerFromOneAddress(t, c, 10) {
		if a.code != http.StatusAccepted {
			t.Fatalf("AUTH_REGISTER_LIMIT=1000/hour: register %d = %d %s, want 202", i+1, a.code, a.body)
		}
	}
	if lines := errorLines(t, c); len(lines) != 0 {
		t.Fatalf("a valid value logged ERROR lines: %q", lines)
	}
}

func TestRegisterLimitInvalidLogsOneErrorAndKeepsThreeAnHour(t *testing.T) {
	for _, value := range []string{"", "lots", "1000/fortnight", "0/hour", "1/hour;2/day"} {
		t.Run("value "+value, func(t *testing.T) {
			registerEnv(t, "")
			setRegisterLimit(t, value, true)
			c := newCredFixture(t) // a refusal here (CredentialRoutes error) fails the fixture
			lines := errorLines(t, c)
			if len(lines) != 1 {
				t.Fatalf("AUTH_REGISTER_LIMIT=%q: %d ERROR lines, want exactly one: %q", value, len(lines), lines)
			}
			if !strings.Contains(lines[0], registerLimitKey) {
				t.Fatalf("the ERROR line does not name %s: %s", registerLimitKey, lines[0])
			}
			if value != "" && strings.Contains(lines[0], value) {
				t.Fatalf("the ERROR line carries the value: %s", lines[0])
			}
			assertThreeAnHour(t, c)
		})
	}
}
