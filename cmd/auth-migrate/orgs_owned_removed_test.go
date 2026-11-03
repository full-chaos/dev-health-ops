package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// A set AUTH_ORGS_OWNED is ignored and reported with one ERROR line on stderr, before the database step; the run
// then stops on the missing DSN as it does without the key.
func TestExecuteReportsTheRemovedOrgsOwnedKey(t *testing.T) {
	for _, value := range []string{"true", "false", "zz-not-a-switch-4471"} {
		lookup := func(key string) (string, bool) {
			if key == "AUTH_ORGS_OWNED" {
				return value, true
			}
			return "", false
		}
		var stdout, stderr bytes.Buffer
		if code := execute(context.Background(), nil, lookup, &stdout, &stderr); code != 1 {
			t.Fatalf("%q: exit = %d, want 1; stderr: %s", value, code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "is required") {
			t.Errorf("%q: stderr = %q, want the missing DSN step", value, stderr.String())
		}
		errorLines := 0
		for _, line := range strings.Split(stderr.String(), "\n") {
			if strings.Contains(line, `"level":"ERROR"`) && strings.Contains(line, "AUTH_ORGS_OWNED") {
				errorLines++
				if strings.Contains(line, "zz-not-a-switch") {
					t.Errorf("%q: the value reached the log", value)
				}
			}
		}
		if errorLines != 1 {
			t.Errorf("%q: %d ERROR lines name the key, want 1; stderr: %s", value, errorLines, stderr.String())
		}
	}
}
