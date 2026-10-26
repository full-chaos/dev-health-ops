package devhealth_test

import (
	"reflect"
	"strings"
	"testing"
)

// ackJobs returns the args of every rendered Job that passes the grant-narrowing
// acknowledgement, by Job name.
func ackJobs(t *testing.T, docs []map[string]any) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, doc := range docs {
		if doc["kind"] != "Job" {
			continue
		}
		args := jobArgs(t, doc)
		for _, a := range args {
			if strings.HasPrefix(a, "-ack-grant-narrowing=") {
				name, _ := at(doc, "metadata", "name").(string)
				out[name] = args
				break
			}
		}
	}
	return out
}

// The 4->3 flip-back release renders phase 3 with the acknowledgement. Old
// owned auth-service pods still serve while its pre-upgrade hooks run, so no
// Job of that release may narrow: each one that passes the acknowledgement
// also passes -defer-narrowing.
func TestTheFlipBackReleaseNeverNarrows(t *testing.T) {
	docs := renderPhase(t, 3, "--set", "authService.migrate.acknowledgeGrantNarrowing="+ackDigest)
	flag := "-ack-grant-narrowing=" + ackDigest
	want := map[string][]string{
		"a-dev-health-auth-migrate": {flag, "-defer-narrowing"},
		"a-dev-health-auth-grants":  {"grants", flag, "-defer-narrowing"},
	}
	got := ackJobs(t, docs)
	for name, args := range got {
		if !reflect.DeepEqual(args, want[name]) {
			t.Errorf("Job %s args = %v, want %v (a Job of the flip-back release must not narrow)", name, args, want[name])
		}
	}
	for name := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("Job %s with the acknowledgement not rendered", name)
		}
	}
}

// The narrowing is a later release: with authService.migrate.narrowAfterFlipBack
// the same two Jobs pass the acknowledgement without -defer-narrowing.
func TestTheNarrowRendersOnlyWithTheFlag(t *testing.T) {
	docs, out, err := renderAuth(t, phaseArgs(3,
		"--set", "authService.migrate.acknowledgeGrantNarrowing="+ackDigest,
		"--set", "authService.migrate.narrowAfterFlipBack=true")...)
	if err != nil {
		t.Fatalf("render with narrowAfterFlipBack: %v\n%.600s", err, out)
	}
	flag := "-ack-grant-narrowing=" + ackDigest
	want := map[string][]string{
		"a-dev-health-auth-migrate": {flag},
		"a-dev-health-auth-grants":  {"grants", flag},
	}
	if got := ackJobs(t, docs); !reflect.DeepEqual(got, want) {
		t.Errorf("Jobs with the acknowledgement = %v, want %v", got, want)
	}
}

// narrowAfterFlipBack without the acknowledgement fails the render and names
// both settings.
func TestNarrowAfterFlipBackIsRefusedWithoutTheAck(t *testing.T) {
	_, out, err := renderAuth(t, phaseArgs(3, "--set", "authService.migrate.narrowAfterFlipBack=true")...)
	if err == nil {
		t.Fatal("render with narrowAfterFlipBack and no acknowledgement succeeded")
	}
	for _, name := range []string{"narrowAfterFlipBack", "acknowledgeGrantNarrowing"} {
		if !strings.Contains(out, name) {
			t.Errorf("refusal does not name %s:\n%.600s", name, out)
		}
	}
}
