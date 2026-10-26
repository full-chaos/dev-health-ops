package devhealth_test

import (
	"reflect"
	"strings"
	"testing"
)

func jobArgs(t *testing.T, doc map[string]any) []string {
	t.Helper()
	if doc == nil {
		t.Fatal("job not rendered")
	}
	containers, _ := at(doc, "spec", "template", "spec", "containers").([]any)
	if len(containers) != 1 {
		t.Fatalf("want one container, got %d", len(containers))
	}
	var out []string
	raw, _ := at(containers[0], "args").([]any)
	for _, a := range raw {
		out = append(out, a.(string))
	}
	return out
}

const ackDigest = "df0950bc72ed8889"

// The grant-narrowing acknowledgement reaches both auth-migrate Jobs as exactly
// one flag, and only when the value is set.
func TestGrantNarrowingAcknowledgementRendersOnBothJobsOnlyWhenSet(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	if got := jobArgs(t, authDoc(docs, "Job", "a-dev-health-auth-migrate")); len(got) != 0 {
		t.Errorf("default auth-migrate args = %v, want none", got)
	}
	if got := jobArgs(t, authDoc(docs, "Job", "a-dev-health-auth-grants")); !reflect.DeepEqual(got, []string{"grants"}) {
		t.Errorf("default auth-grants args = %v, want [grants]", got)
	}

	docs, out, err = renderAuth(t, append(append([]string{}, authBase...),
		"--set", "authService.migrate.acknowledgeGrantNarrowing="+ackDigest)...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	flag := "-ack-grant-narrowing=" + ackDigest
	if got := jobArgs(t, authDoc(docs, "Job", "a-dev-health-auth-migrate")); !reflect.DeepEqual(got, []string{flag, "-defer-narrowing"}) {
		t.Errorf("auth-migrate args = %v, want [%s -defer-narrowing]", got, flag)
	}
	if got := jobArgs(t, authDoc(docs, "Job", "a-dev-health-auth-grants")); !reflect.DeepEqual(got, []string{"grants", flag, "-defer-narrowing"}) {
		t.Errorf("auth-grants args = %v, want [grants %s -defer-narrowing]", got, flag)
	}
}

// A value that is not a 16-hex digest fails the render and names the setting.
func TestGrantNarrowingAcknowledgementMustBeADigest(t *testing.T) {
	for _, bad := range []string{"DF0950BC72ED8889", "df0950bc72ed888", "df0950bc72ed88890", "not-a-digest", "df0950bc72ed8889 --x"} {
		_, out, err := renderAuth(t, append(append([]string{}, authBase...),
			"--set", "authService.migrate.acknowledgeGrantNarrowing="+bad)...)
		if err == nil {
			t.Errorf("value %q rendered; want a refusal", bad)
			continue
		}
		if !strings.Contains(out, "authService.migrate.acknowledgeGrantNarrowing") {
			t.Errorf("value %q: refusal does not name the setting: %.300s", bad, out)
		}
	}
}

// Rolling back phase 4 to phase 3 narrows the runtime role's grants, so the auth-migrate hook needs the digest the
// ratchet prints on refusal. Phase 4 alone carries no acknowledgement; the phase 3 render with the key set carries it
// as the one flag on both Jobs and renders no users-owned switch anywhere.
func TestPhase4RollbackCarriesTheGrantAcknowledgement(t *testing.T) {
	docs := renderPhase(t, 4)
	for _, job := range []string{"a-dev-health-auth-migrate", "a-dev-health-auth-grants"} {
		for _, arg := range jobArgs(t, authDoc(docs, "Job", job)) {
			if strings.HasPrefix(arg, "-ack-grant-narrowing") {
				t.Errorf("phase 4 without the key: %s carries %s", job, arg)
			}
		}
	}

	docs = renderPhase(t, 3, "--set", "authService.migrate.acknowledgeGrantNarrowing="+ackDigest)
	flag := "-ack-grant-narrowing=" + ackDigest
	if got := jobArgs(t, authDoc(docs, "Job", "a-dev-health-auth-migrate")); !reflect.DeepEqual(got, []string{flag}) {
		t.Errorf("phase 3 rollback: auth-migrate args = %v, want [%s]", got, flag)
	}
	if got := jobArgs(t, authDoc(docs, "Job", "a-dev-health-auth-grants")); !reflect.DeepEqual(got, []string{"grants", flag}) {
		t.Errorf("phase 3 rollback: auth-grants args = %v, want [grants %s]", got, flag)
	}
	for _, target := range []struct{ kind, name string }{
		{"Deployment", "a-dev-health-auth-service"}, {"Deployment", "a-dev-health-go-api"},
		{"Job", "a-dev-health-auth-migrate"}, {"Job", "a-dev-health-auth-grants"},
	} {
		if value, set := containerEnvValue(t, authDoc(docs, target.kind, target.name), "AUTH_USERS_OWNED"); set {
			t.Errorf("phase 3 rollback: %s sets AUTH_USERS_OWNED=%q", target.name, value)
		}
	}
}
