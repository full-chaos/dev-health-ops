package workersctl

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// CHAOS-8892: `--org-stdin` on the two recompute verbs; CHAOS-8888: on
// `providersync retire-jira-project-as-team`, which takes its organization
// from stdin only.

const orgStdinTestOrg = "5d8e2c1a-7b3f-4a90-b6d4-0e9f1a2b3c4d"

// orgStdinVerbs are the verbs that accept --org-stdin (group, name and the
// other flags that make a request valid). The set is orgStdinVerbNames.
func orgStdinVerbs() map[string][]string {
	return map[string][]string{
		"metrics daily-start": {"metrics", "daily-start", "--day", "2026-08-01"},
		"metrics partition-recompute": {"metrics", "partition-recompute", "--from", "2026-08-01", "--to", "2026-08-01",
			"--family", "repo_user_commit", "--review-evidence", "testing"},
		"providersync retire-jira-project-as-team": {"providersync", "retire-jira-project-as-team"},
		"providersync carry-team-ids":              {"providersync", "carry-team-ids"},
	}
}

// orgStdinOnlyVerbs have no --org flag: the organization comes from stdin only.
var orgStdinOnlyVerbs = map[string]bool{"providersync retire-jira-project-as-team": true, "providersync carry-team-ids": true}

// The test table and the preflight set name the same verbs.
func TestOrgStdinVerbsAreThePreflightSet(t *testing.T) {
	if len(orgStdinVerbs()) != len(orgStdinVerbNames) {
		t.Fatalf("test verbs %d, orgStdinVerbNames %d", len(orgStdinVerbs()), len(orgStdinVerbNames))
	}
	for name, base := range orgStdinVerbs() {
		if !orgStdinVerbNames[name] {
			t.Errorf("%s is not in orgStdinVerbNames", name)
		}
		if hasFlag, _ := orgStdinVerb(append(append([]string{}, base...), "--org-stdin")); !hasFlag {
			t.Errorf("%s: orgStdinVerb does not see --org-stdin", name)
		}
	}
}

func runOrgStdinVerb(runtime *operatorRuntime, base []string, extra ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	args := append(append(append([]string{}, base...), auditFlags()...), extra...)
	code := dispatch(context.Background(), runtime, args, &stdout, &stderr)
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
// A stdin-only verb refuses both a missing --org-stdin and an --org.
func TestOrgStdinFlagAbsentLeavesOrgPathAlone(t *testing.T) {
	for verb, base := range orgStdinVerbs() {
		reader := &countingReader{}
		code, _, stderr := runOrgStdinVerb(&operatorRuntime{stdin: reader}, base)
		if code != 2 || stderr != invalidRequestJSON {
			t.Fatalf("%s: missing --org: code=%d stderr=%q", verb, code, stderr)
		}
		code, _, stderr = runOrgStdinVerb(&operatorRuntime{stdin: reader}, base, "--org", orgStdinTestOrg)
		if orgStdinOnlyVerbs[verb] {
			if code != 2 || stderr != invalidRequestJSON {
				t.Fatalf("%s: --org on a stdin-only verb: code=%d stderr=%q, want invalid_request", verb, code, stderr)
			}
		} else if code != 1 || !strings.Contains(stderr, "operator_backend_unavailable") {
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
		viaStdin, stdout, stderr, stdinLogs := audit([]string{"--org-stdin"}, strings.ToUpper(orgStdinTestOrg)+"\r\n")
		if orgStdinOnlyVerbs[verb] {
			// No --org path to compare with: the audit request holds the
			// canonical id, and the refused-write log line exists but holds
			// no id.
			if len(viaStdin.events) != 1 || viaStdin.events[0].ResourceType != "organization" || viaStdin.events[0].ResourceID != orgStdinTestOrg {
				t.Fatalf("%s: audit requests %+v, want one for the organization from stdin", verb, viaStdin.events)
			}
			if !strings.Contains(stdinLogs, "audited write refused before it ran") {
				t.Fatalf("%s: control: no refused-write log line, so the log check below proves nothing: %q", verb, stdinLogs)
			}
			for name, text := range map[string]string{"stdout": stdout, "stderr": stderr, "logs": stdinLogs} {
				if strings.Contains(strings.ToLower(text), orgStdinTestOrg) {
					t.Fatalf("%s: %s holds the org id: %q", verb, name, text)
				}
			}
			continue
		}
		viaOrg, _, _, orgLogs := audit([]string{"--org", orgStdinTestOrg}, "")
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
	for _, verb := range []string{"metrics partition-recompute", "providersync retire-jira-project-as-team", "providersync carry-team-ids"} {
		runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, &refusingAuditor{})
		runtime.stdin = strings.NewReader(orgStdinTestOrg + "\n")
		code, stdout, stderr := runOrgStdinVerb(runtime, orgStdinVerbs()[verb], "--org-stdin", "--dry-run")
		if code == 2 || strings.Contains(stdout+stderr, orgStdinTestOrg) {
			t.Fatalf("%s: code=%d stdout=%q stderr=%q", verb, code, stdout, stderr)
		}
	}
}

// A stdin that never closes (a terminal, a pipe nobody writes) ends in a
// clear refusal after orgStdinTimeout and not in a hang.
func TestOrgStdinNeverClosingPipeTimesOut(t *testing.T) {
	previous := orgStdinTimeout
	orgStdinTimeout = 50 * time.Millisecond
	defer func() { orgStdinTimeout = previous }()
	reader, writer := io.Pipe()
	defer writer.Close()
	for verb, base := range orgStdinVerbs() {
		finished := make(chan struct{})
		var code int
		var stderr string
		go func() {
			defer close(finished)
			code, _, stderr = runOrgStdinVerb(&operatorRuntime{stdin: reader}, base, "--org-stdin")
		}()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: hung on a stdin that never closes", verb)
		}
		if code != 2 || !strings.Contains(stderr, "org_stdin_timeout") {
			t.Fatalf("%s: code=%d stderr=%q", verb, code, stderr)
		}
	}
}

// The refusal comes BEFORE any runtime is built: with no database settings at
// all, a bad stdin is still a usage error and not a configuration error.
func TestExecuteRefusesBadOrgStdinBeforeBuildingARuntime(t *testing.T) {
	empty := func(string) (string, bool) { return "", false }
	for verb, base := range orgStdinVerbs() {
		args := append(append([]string{}, base...), auditFlags("--org-stdin")...)
		for name, stdin := range map[string]string{"empty": "", "two lines": orgStdinTestOrg + "\n" + orgStdinTestOrg + "\n"} {
			var stdout, stderr bytes.Buffer
			code := executeWithStdin(context.Background(), args, empty, strings.NewReader(stdin), &stdout, &stderr)
			if code != 2 || !strings.Contains(stderr.String(), "org_stdin_") || strings.Contains(stderr.String(), orgStdinTestOrg) {
				t.Fatalf("%s/%s: code=%d stderr=%q", verb, name, code, stderr.String())
			}
		}
		var stdout, stderr bytes.Buffer
		code := executeWithStdin(context.Background(), args, empty, strings.NewReader(orgStdinTestOrg+"\n"), &stdout, &stderr)
		if code != 1 || !strings.Contains(stderr.String(), "configuration_error") || strings.Contains(stderr.String(), orgStdinTestOrg) {
			t.Fatalf("%s: valid id: code=%d stderr=%q, want to pass validation and stop at configuration", verb, code, stderr.String())
		}
	}
}
