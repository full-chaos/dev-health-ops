package secrets

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestWithRedactedCauseKeepsTheCauseWithoutCredentials(t *testing.T) {
	sentinel := errors.New("ClickHouse readiness check failed")
	const password = "planted-password-6369"
	dsn := "clickhouse://api:" + password + "@clickhouse.internal:9000/default"
	cause := fmt.Errorf("code: 516, message: api: Authentication failed: password %s is incorrect (dsn %s)", password, dsn)

	err := WithRedactedCause(sentinel, dsn, cause)
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is(err, sentinel) = false for %v", err)
	}
	text := err.Error()
	if strings.Contains(text, password) || strings.Contains(text, dsn) {
		t.Fatalf("credential reached the error text: %s", text)
	}
	if !strings.HasPrefix(text, sentinel.Error()+": ") || !strings.Contains(text, "Authentication failed") {
		t.Fatalf("error text %q does not carry the sentinel and the cause", text)
	}
	if errors.Is(err, cause) || errors.Unwrap(err) != sentinel {
		t.Fatalf("the raw cause is reachable through the chain: %#v", err)
	}
	if WithRedactedCause(sentinel, dsn, nil) != sentinel {
		t.Fatal("a nil cause must return the sentinel itself")
	}
}
