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

// Every verb reports the key, because the observation runs before the dispatch. The run then stops on its own step (a
// missing DSN, a refused flag), never before the line.
func TestEveryVerbReportsTheRemovedOrgsOwnedKey(t *testing.T) {
	verbs := map[string][]string{
		"migrate":                     nil,
		"grants":                      {"grants"},
		"check":                       {"-check"},
		"version":                     {"-version"},
		"keys":                        {"keys", "list"},
		"restore-fence":               {"restore-fence", "status"},
		"sessions":                    {"sessions", "count-unpinned"},
		"workloads":                   {"workloads", "seed"},
		"workload-assertion-key-set":  {"workload-assertion-key-set"},
		"workload-assertion-key-list": {"workload-assertion-key-list"},
		"platform-role-bootstrap":     {"platform-role-bootstrap"},
		"owner-move":                  {"owner-move"},
		"platform-owner-grant":        {"platform-owner-grant"},
		"platform-owner-revoke":       {"platform-owner-revoke"},
	}
	for name, args := range verbs {
		for _, value := range []string{"", " \t", "true", "false", "zz-not-a-switch-4471"} {
			lookup := func(key string) (string, bool) {
				if key == "AUTH_ORGS_OWNED" {
					return value, true
				}
				return "", false
			}
			var stdout, stderr bytes.Buffer
			execute(context.Background(), args, lookup, &stdout, &stderr)
			errorLines := 0
			for _, line := range strings.Split(stderr.String(), "\n") {
				if strings.Contains(line, `"level":"ERROR"`) && strings.Contains(line, "AUTH_ORGS_OWNED") {
					errorLines++
					if strings.Contains(line, "zz-not-a-switch") {
						t.Errorf("%s %q: the value reached the log", name, value)
					}
				}
			}
			if errorLines != 1 {
				t.Errorf("%s %q: %d ERROR lines name the key, want 1; stderr: %s", name, value, errorLines, stderr.String())
			}
		}
	}
}
