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

// The narrowing is a later release: only with
// authService.migrate.narrowAfterFlipBack do the same two Jobs pass the
// acknowledgement without -defer-narrowing; without it, none does.
func TestTheNarrowRendersOnlyWithTheFlag(t *testing.T) {
	for name, args := range ackJobs(t, renderPhase(t, 3, "--set", "authService.migrate.acknowledgeGrantNarrowing="+ackDigest)) {
		if !strings.Contains(strings.Join(args, " "), "-defer-narrowing") {
			t.Errorf("without narrowAfterFlipBack, Job %s narrows: args = %v", name, args)
		}
	}
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
// both settings, with or without the grants hook.
func TestNarrowAfterFlipBackIsRefusedWithoutTheAck(t *testing.T) {
	for _, extra := range [][]string{nil, {"--set", "authService.migrate.grantsHook.enabled=false"}} {
		_, out, err := renderAuth(t, phaseArgs(3, append([]string{"--set", "authService.migrate.narrowAfterFlipBack=true"}, extra...)...)...)
		if err == nil {
			t.Fatalf("%v: render with narrowAfterFlipBack and no acknowledgement succeeded", extra)
		}
		for _, name := range []string{"narrowAfterFlipBack", "acknowledgeGrantNarrowing"} {
			if !strings.Contains(out, name) {
				t.Errorf("%v: refusal does not name %s:\n%.600s", extra, name, out)
			}
		}
	}
}

// The refusal does not depend on an auth Job rendering: with auth-migrate
// off, narrowAfterFlipBack without a valid acknowledgement still fails the
// render, with the same acknowledgement check as the Jobs.
func TestNarrowAfterFlipBackIsRefusedWithAuthMigrateOff(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra []string
		names []string
	}{
		{"no acknowledgement", nil, []string{"narrowAfterFlipBack", "acknowledgeGrantNarrowing"}},
		{"acknowledgement not a digest", []string{"--set", "authService.migrate.acknowledgeGrantNarrowing=not-a-digest"}, []string{"acknowledgeGrantNarrowing"}},
	} {
		args := append([]string{
			"--set", "authService.migrate.enabled=false",
			"--set", "authService.migrate.narrowAfterFlipBack=true"}, tc.extra...)
		_, out, err := renderAuth(t, phaseArgs(3, args...)...)
		if err == nil {
			t.Errorf("%s: render with authService.migrate.enabled=false and narrowAfterFlipBack succeeded", tc.name)
			continue
		}
		for _, name := range tc.names {
			if !strings.Contains(out, name) {
				t.Errorf("%s: refusal does not name %s:\n%.600s", tc.name, name, out)
			}
		}
	}
}
