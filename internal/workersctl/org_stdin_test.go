package workersctl

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// CHAOS-8892: `--org-stdin` on the two recompute verbs.

const orgStdinTestOrg = "5d8e2c1a-7b3f-4a90-b6d4-0e9f1a2b3c4d"

// orgStdinVerbs are the verbs that accept --org-stdin, with the other flags
// that make a request valid.
func orgStdinVerbs() map[string][]string {
	return map[string][]string{
		"daily-start": {"daily-start", "--day", "2026-08-01"},
		"partition-recompute": {"partition-recompute", "--from", "2026-08-01", "--to", "2026-08-01",
			"--family", "repo_user_commit", "--review-evidence", "testing"},
	}
}

func runOrgStdinVerb(runtime *operatorRuntime, base []string, extra ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	args := append(append(append([]string{}, base...), auditFlags()...), extra...)
	code := dispatchMetrics(context.Background(), runtime, args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestOrgStdinRejectsBadInputWithoutEchoingIt(t *testing.T) {
	long := strings.Repeat("a", orgStdinMaxBytes+1)
	cases := map[string]struct {
		stdin  string
		extra  []string
		detail string
	}{
		"empty":       {stdin: "", detail: "org_stdin_empty"},
		"newline":     {stdin: "\n", detail: "org_stdin_empty"},
		"two lines":   {stdin: orgStdinTestOrg + "\n" + orgStdinTestOrg + "\n", detail: "org_stdin_not_one_line"},
		"too long":    {stdin: long, detail: "org_stdin_too_long"},
		"malformed":   {stdin: "plainwordorg\n", detail: "org_stdin_malformed"},
		"with --org":  {stdin: orgStdinTestOrg + "\n", extra: []string{"--org", orgStdinTestOrg}},
		"trailing sp": {stdin: orgStdinTestOrg + " \n", detail: "org_stdin_malformed"},
	}
	for verb, base := range orgStdinVerbs() {
		for name, tc := range cases {
			t.Run(verb+"/"+name, func(t *testing.T) {
				runtime := &operatorRuntime{stdin: strings.NewReader(tc.stdin)}
				code, stdout, stderr := runOrgStdinVerb(runtime, base, append([]string{"--org-stdin"}, tc.extra...)...)
				if code != 2 || stdout != "" {
					t.Fatalf("code=%d stdout=%q stderr=%q, want usage error", code, stdout, stderr)
				}
				if tc.detail == "" {
					if stderr != invalidRequestJSON {
						t.Fatalf("stderr=%q", stderr)
					}
				} else if !strings.Contains(stderr, tc.detail) || !strings.Contains(stderr, `"length":`) {
					t.Fatalf("stderr=%q, want %s and a length", stderr, tc.detail)
				}
				for _, secret := range []string{orgStdinTestOrg, "plainwordorg", "aaaaaaaa"} {
					if strings.Contains(stderr, secret) {
						t.Fatalf("stderr repeats the stdin value: %q", stderr)
					}
				}
			})
		}
	}
}

func TestOrgStdinNilStdinIsEmpty(t *testing.T) {
	for verb, base := range orgStdinVerbs() {
		code, _, stderr := runOrgStdinVerb(&operatorRuntime{}, base, "--org-stdin")
		if code != 2 || !strings.Contains(stderr, "org_stdin_empty") {
			t.Fatalf("%s: code=%d stderr=%q", verb, code, stderr)
		}
	}
}

// Without the flag nothing changes: stdin is not read and --org still rules.
func TestOrgStdinFlagAbsentLeavesOrgPathAlone(t *testing.T) {
	for verb, base := range orgStdinVerbs() {
		reader := &countingReader{}
		code, _, stderr := runOrgStdinVerb(&operatorRuntime{stdin: reader}, base)
		if code != 2 || stderr != invalidRequestJSON {
			t.Fatalf("%s: missing --org: code=%d stderr=%q", verb, code, stderr)
		}
		code, _, stderr = runOrgStdinVerb(&operatorRuntime{stdin: reader}, base, "--org", orgStdinTestOrg)
		if code != 1 || !strings.Contains(stderr, "operator_backend_unavailable") {
			t.Fatalf("%s: --org path: code=%d stderr=%q", verb, code, stderr)
		}
		if reader.reads != 0 {
			t.Fatalf("%s: stdin read %d times without --org-stdin", verb, reader.reads)
		}
	}
}

type countingReader struct{ reads int }

func (r *countingReader) Read([]byte) (int, error) { r.reads++; return 0, nil }

// The value from stdin reaches the same audit request the --org path builds,
// and neither stdout, stderr nor any log line holds it.
func TestOrgStdinReachesSameRequestAndStaysOutOfOutput(t *testing.T) {
	for verb, base := range orgStdinVerbs() {
		audit := func(extra []string, stdin string) (*refusingAuditor, string, string, string) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			defer slog.SetDefault(previous)
			auditor := &refusingAuditor{}
			runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
			runtime.stdin = strings.NewReader(stdin)
			code, stdout, stderr := runOrgStdinVerb(runtime, base, extra...)
			if code != 1 || !strings.Contains(stderr, "audit_unavailable") {
				t.Fatalf("%s: code=%d stderr=%q", verb, code, stderr)
			}
			return auditor, stdout, stderr, logs.String()
		}
		viaOrg, _, _, orgLogs := audit([]string{"--org", orgStdinTestOrg}, "")
		viaStdin, stdout, stderr, stdinLogs := audit([]string{"--org-stdin"}, strings.ToUpper(orgStdinTestOrg)+"\r\n")
		if len(viaOrg.events) == 1 && len(viaStdin.events) == 1 {
			viaOrg.events[0].CreatedAt, viaStdin.events[0].CreatedAt = time.Time{}, time.Time{}
		}
		if len(viaOrg.events) != 1 || len(viaStdin.events) != 1 || viaOrg.events[0] != viaStdin.events[0] {
			t.Fatalf("%s: audit requests differ: --org %+v, stdin %+v", verb, viaOrg.events, viaStdin.events)
		}
		if !strings.Contains(orgLogs, orgStdinTestOrg) {
			t.Fatalf("%s: control: the --org path logs no org id, so the log check below proves nothing: %q", verb, orgLogs)
		}
		for name, text := range map[string]string{"stdout": stdout, "stderr": stderr, "logs": stdinLogs} {
			if strings.Contains(strings.ToLower(text), orgStdinTestOrg) {
				t.Fatalf("%s: %s holds the org id: %q", verb, name, text)
			}
		}
	}
}

// --dry-run with stdin: nothing printed holds the id either.
func TestOrgStdinDryRunPrintsNoOrg(t *testing.T) {
	runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, &refusingAuditor{})
	runtime.stdin = strings.NewReader(orgStdinTestOrg + "\n")
	code, stdout, stderr := runOrgStdinVerb(runtime, orgStdinVerbs()["partition-recompute"], "--org-stdin", "--dry-run")
	if code == 2 || strings.Contains(stdout+stderr, orgStdinTestOrg) {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
}
