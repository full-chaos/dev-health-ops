package devhealth_test

import (
	"strings"
	"testing"
)

// CHAOS-12402 (pm 11-03): AUTH_ACR_WORKLOAD_SUBJECT is a workload `sub`, which is the session wire form (prn_ and 22
// base64url characters). A UUID-text form starts go-api and never matches a token, so the chart refuses it at render
// time, with go-api on and off. go-api itself does not stop (D6465).
func TestGoAPIACRWorkloadSubjectChartRefusesTheUUIDForm(t *testing.T) {
	for name, value := range map[string]string{
		"uuid text":      "prn_00000000-0000-4000-8000-000000000000",
		"uuid upper":     "prn_00000000-0000-4000-8000-00000000ABCD",
		"too short":      "prn_AAAAAAAAAAAAAAAAAAAAA",
		"too long":       "prn_AAAAAAAAAAAAAAAAAAAAAAA",
		"no prefix":      "AAAAAAAAAAAAAAAAAAAAAA",
		"padded literal": " prn_00000000-0000-4000-8000-000000000000 ",
	} {
		for mode, render := range map[string]func(...string) []string{"go-api on": goAPIWithAuth, "go-api off": goAPIDisabled} {
			args := []string{"--set", "goApi.extraEnv[0].name=AUTH_ACR_WORKLOAD_SUBJECT", "--set-string", "goApi.extraEnv[0].value=" + value}
			_, out, err := renderAuth(t, render(args...)...)
			if err == nil {
				t.Errorf("%s, %s: the render accepted AUTH_ACR_WORKLOAD_SUBJECT=%q", name, mode, value)
				continue
			}
			if !strings.Contains(out+err.Error(), "AUTH_ACR_WORKLOAD_SUBJECT") {
				t.Errorf("%s, %s: the refusal does not name the variable: %v %.300s", name, mode, err, out)
			}
		}
	}
}

func TestGoAPIACRWorkloadSubjectChartAcceptsTheSessionFormAndASecretReference(t *testing.T) {
	for name, args := range map[string][]string{
		"session form": {"--set", "goApi.extraEnv[0].name=AUTH_ACR_WORKLOAD_SUBJECT", "--set-string", "goApi.extraEnv[0].value=prn_hh9Wy-4Ok0qnOs2Wk8bOEg"},
		"secret ref": {"--set", "goApi.extraEnv[0].name=AUTH_ACR_WORKLOAD_SUBJECT",
			"--set", "goApi.extraEnv[0].valueFrom.configMapKeyRef.name=acr", "--set", "goApi.extraEnv[0].valueFrom.configMapKeyRef.key=subject"},
		"unrelated variable": {"--set", "goApi.extraEnv[0].name=SOME_OTHER", "--set-string", "goApi.extraEnv[0].value=prn_00000000-0000-4000-8000-000000000000"},
	} {
		if _, out, err := renderAuth(t, goAPIWithAuth(args...)...); err != nil {
			t.Errorf("%s: the render failed: %v %.400s", name, err, out)
		}
	}
}
