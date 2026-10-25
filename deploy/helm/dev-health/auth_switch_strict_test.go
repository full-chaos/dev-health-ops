package devhealth_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Helm reads the non-empty string "false" as true. Every authService switch this chart reads is refused unless it is a
// YAML boolean, and the refusal names the key.
func TestAuthSwitchesRefuseNonBooleans(t *testing.T) {
	for _, key := range []string{"authService.orgsOwned", "authService.usersOwned", "authService.backfill.enabled", "authService.backfill.dryRun"} {
		for name, flag := range map[string][]string{
			"string false": {"--set-string", key + "=false"},
			"string true":  {"--set-string", key + "=true"},
			"null":         {"--set-json", key + "=null"},
			"number":       {"--set-json", key + "=1"},
			"empty string": {"--set-string", key + "="},
			"list":         {"--set-json", key + "=[]"},
			"map":          {"--set-json", key + "={}"},
		} {
			t.Run(key+"/"+name, func(t *testing.T) {
				_, out, err := renderAuth(t, append(backfillOnWithout(key), flag...)...)
				if err == nil || !strings.Contains(out, key) {
					t.Fatalf("%s %s rendered or the refusal does not name the key: err=%v out=%.300s", key, name, err, out)
				}
			})
		}
		t.Run(key+"/bool passes", func(t *testing.T) {
			for _, value := range []string{"true", "false"} {
				if key == "authService.backfill.enabled" && value == "false" {
					continue
				}
				if _, out, err := renderAuth(t, withSets(backfillOn, key+"="+value)...); err != nil {
					t.Fatalf("%s=%s refused: %v\n%.300s", key, value, err, out)
				}
			}
		})
	}
}

// With the Job off, a bad authService.backfill.enabled string still fails: the check does not hide behind the gate.
func TestAuthBackfillEnabledStringFailsWhileOff(t *testing.T) {
	_, out, err := renderAuth(t, append(append([]string{}, authBase...), "--set-string", "authService.backfill.enabled=false")...)
	if err == nil || !strings.Contains(out, "authService.backfill.enabled") {
		t.Fatalf("string false rendered: err=%v out=%.300s", err, out)
	}
}

func TestAuthBackfillRunIdMustBeANonEmptyString(t *testing.T) {
	for name, flag := range map[string][]string{
		"null":   {"--set-json", "authService.backfill.runId=null"},
		"number": {"--set-json", "authService.backfill.runId=7"},
		"bool":   {"--set-json", "authService.backfill.runId=true"},
		"empty":  {"--set-string", "authService.backfill.runId="},
	} {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, append(backfillOnWithout("authService.backfill.runId"), flag...)...)
			if err == nil || !strings.Contains(out, "authService.backfill.runId") {
				t.Fatalf("runId %s rendered: err=%v out=%.300s", name, err, out)
			}
		})
	}
}

func TestAuthBackfillBatchMustBeAWholeNumberInRange(t *testing.T) {
	for name, flag := range map[string][]string{
		"string":   {"--set-string", "authService.backfill.batch=100"},
		"null":     {"--set-json", "authService.backfill.batch=null"},
		"fraction": {"--set-json", "authService.backfill.batch=100.5"},
		"zero":     {"--set-json", "authService.backfill.batch=0"},
		"high":     {"--set-json", "authService.backfill.batch=10001"},
		"bool":     {"--set-json", "authService.backfill.batch=true"},
	} {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, append(append([]string{}, backfillOn...), flag...)...)
			if err == nil || !strings.Contains(out, "authService.backfill.batch") {
				t.Fatalf("batch %s rendered: err=%v out=%.300s", name, err, out)
			}
		})
	}
	for _, ok := range []string{"1", "100", "10000"} {
		if _, out, err := renderAuth(t, withSets(backfillOn, "authService.backfill.batch="+ok)...); err != nil {
			t.Errorf("batch %s refused: %v\n%.300s", ok, err, out)
		}
	}
	if _, out, err := renderAuth(t, append(append([]string{}, backfillOn...), "--set-json", "authService.backfill.batch=250.0")...); err != nil {
		t.Errorf("batch 250.0 (a whole float64) refused: %v\n%.300s", err, out)
	}
}

// Each template that reads a switch reads it through dev-health.authBool: a bare `if .Values.authService.usersOwned`
// would let a string pass in that one consumer.
func TestAuthSwitchesAreOnlyReadThroughTheStrictHelper(t *testing.T) {
	read := regexp.MustCompile(`(\.usersOwned|\.orgsOwned|\$b\.enabled|\$b\.dryRun|backfill\.(enabled|dryRun))\b`)
	files, err := filepath.Glob("templates/*.yaml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no templates: %v", err)
	}
	reads := 0
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for number, line := range strings.Split(string(raw), "\n") {
			trimmed := strings.TrimSpace(line)
			if !strings.Contains(trimmed, "{{") || !read.MatchString(line) || strings.Contains(trimmed, "{{- fail") || strings.Contains(trimmed, `{{- fail (printf`) || strings.HasPrefix(trimmed, "{{- /*") {
				continue
			}
			if !strings.Contains(line, "dev-health.authBool") {
				if strings.Contains(line, "fail") {
					continue
				}
				t.Errorf("%s:%d reads a switch without dev-health.authBool: %s", file, number+1, trimmed)
				continue
			}
			reads++
		}
	}
	if want := 9; reads < want {
		t.Errorf("found %d strict reads, want at least %d (auth-service x2, migrate, grants, go-api x2, Job x4 incl. enabled and dryRun)", reads, want)
	}
	_ = fmt.Sprint
}

// backfillOnWithout is backfillOn minus the --set pairs for the named keys, so a --set-json value for that key is the
// only one Helm sees (Helm applies --set after --set-json).
func backfillOnWithout(keys ...string) []string {
	var out []string
	for i := 0; i < len(backfillOn); i++ {
		if backfillOn[i] == "--set" && i+1 < len(backfillOn) {
			drop := false
			for _, key := range keys {
				drop = drop || strings.HasPrefix(backfillOn[i+1], key+"=")
			}
			if drop {
				i++
				continue
			}
		}
		out = append(out, backfillOn[i])
	}
	return out
}
