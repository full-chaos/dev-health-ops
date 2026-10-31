package main

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

func goodSetArgs() []string {
	return []string{"-workload", "web", "-issuer", "https://issuer.example", "-kid", "key-2026-10", "-status", "active",
		"-rotated-at", "2026-10-31T12:00:00Z", "-confirm"}
}

func without(args []string, flag string) []string {
	out := []string{}
	for i := 0; i < len(args); i++ {
		if args[i] == flag {
			if flag != "-confirm" {
				i++
			}
			continue
		}
		out = append(out, args[i])
	}
	return out
}

func TestParseAssertionKeySetArgs(t *testing.T) {
	got, err := parseAssertionKeySetArgs(goodSetArgs(), io.Discard)
	if err != nil {
		t.Fatalf("a valid command line: %v", err)
	}
	if got.name != "web" || got.spec.Issuer != "https://issuer.example" || got.spec.KID != "key-2026-10" ||
		got.spec.Status != workload.AssertionKeyActive || !got.spec.RotatedAt.Equal(time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("parsed = %+v", got)
	}
	retiring, err := parseAssertionKeySetArgs(append(without(without(goodSetArgs(), "-status"), "-confirm"),
		"-status", "retiring", "-retire-after", "2026-11-01T12:00:00Z", "-confirm"), io.Discard)
	if err != nil || retiring.spec.RetireAfter.IsZero() {
		t.Fatalf("a retiring key: %+v %v", retiring, err)
	}
	rotation, err := parseAssertionKeySetArgs(append(goodSetArgs(), "-retire-previous-after", "24h"), io.Discard)
	if err != nil || rotation.spec.RetirePreviousAfter != 24*time.Hour {
		t.Fatalf("a rotation: %+v %v", rotation, err)
	}
	for name, args := range map[string][]string{
		"no confirm":             without(goodSetArgs(), "-confirm"),
		"no workload":            without(goodSetArgs(), "-workload"),
		"bad workload":           append(without(goodSetArgs(), "-workload"), "-workload", "Web!"),
		"no issuer":              without(goodSetArgs(), "-issuer"),
		"bad issuer":             append(without(goodSetArgs(), "-issuer"), "-issuer", "http://plain.example"),
		"no kid":                 without(goodSetArgs(), "-kid"),
		"bad kid":                append(without(goodSetArgs(), "-kid"), "-kid", "has space"),
		"no status":              without(goodSetArgs(), "-status"),
		"bad status":             append(without(goodSetArgs(), "-status"), "-status", "paused"),
		"no rotated-at":          without(goodSetArgs(), "-rotated-at"),
		"date without a time":    append(without(goodSetArgs(), "-rotated-at"), "-rotated-at", "2026-10-31"),
		"time without a zone":    append(without(goodSetArgs(), "-rotated-at"), "-rotated-at", "2026-10-31T12:00:00"),
		"retire-after on active": append(goodSetArgs(), "-retire-after", "2026-11-01T12:00:00Z"),
		"retiring without date":  append(without(goodSetArgs(), "-status"), "-status", "retiring"),
		"retire-previous 25h":    append(goodSetArgs(), "-retire-previous-after", "25h"),
		"retire-previous 0":      append(goodSetArgs(), "-retire-previous-after", "0s"),
		"retire-previous -1h":    append(goodSetArgs(), "-retire-previous-after", "-1h"),
		"positional argument":    append(goodSetArgs(), "extra"),
		"unknown flag apply":     append(goodSetArgs(), "-apply"),
		"a key flag":             append(goodSetArgs(), "-key", "AAAA"),
		"a private flag":         append(goodSetArgs(), "-private-key-file", "/x"),
	} {
		if _, err := parseAssertionKeySetArgs(args, io.Discard); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestParseAssertionKeyListArgs(t *testing.T) {
	if name, err := parseAssertionKeyListArgs(nil, io.Discard); err != nil || name != "" {
		t.Fatalf("no flags: %q %v", name, err)
	}
	if name, err := parseAssertionKeyListArgs([]string{"-workload", "web"}, io.Discard); err != nil || name != "web" {
		t.Fatalf("one workload: %q %v", name, err)
	}
	for name, args := range map[string][]string{
		"bad workload":        {"-workload", "Web!"},
		"positional argument": {"web"},
		"confirm":             {"-confirm"},
		"kid":                 {"-kid", "k"},
	} {
		if _, err := parseAssertionKeyListArgs(args, io.Discard); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestAssertionKeyVerbsRefuseBeforeReadingTheEnvironment(t *testing.T) {
	lookup := func(string) (string, bool) {
		t.Fatal("the environment was read for a bad command line")
		return "", false
	}
	for _, args := range [][]string{
		{"workload-assertion-key-set", "-workload", "web"},
		{"workload-assertion-key-set"},
		{"workload-assertion-key-list", "-confirm"},
	} {
		if code := execute(t.Context(), args, lookup, io.Discard, io.Discard); code != 2 {
			t.Errorf("%v: exit = %d, want 2", args, code)
		}
	}
}

func TestAssertionKeyVerbsNeedTheMigrationEndpoint(t *testing.T) {
	lookup := func(string) (string, bool) { return "", false }
	for _, args := range [][]string{
		append([]string{"workload-assertion-key-set"}, goodSetArgs()...),
		{"workload-assertion-key-list"},
	} {
		var stderr strings.Builder
		code := execute(t.Context(), args, lookup, io.Discard, &stderr)
		if code != 1 || !strings.Contains(stderr.String(), "is required") {
			t.Errorf("%v: exit/stderr = %d / %q", args[0], code, stderr.String())
		}
	}
}
