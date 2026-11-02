package devhealth_test

import (
	"strings"
	"testing"
)

const revisionFeedEnv = "AUTH_REVISION_FEED"

var feedValkey = []string{"--set", "authService.valkey.secretName=auth-valkey",
	"--set-json", `authService.networkPolicy.egress.valkey.to=[{"podSelector":{"matchLabels":{"app":"auth-valkey"}}}]`}

func feedArgs(phase int, extra ...string) []string {
	args := phaseArgs(phase)
	if phase < 2 {
		args = append(args, feedValkey...)
	}
	return append(args, extra...)
}

// Default and false render the feed nowhere.
func TestRevisionFeedOffRendersNothing(t *testing.T) {
	for _, phase := range []int{1, 2} {
		for _, extra := range [][]string{nil, {"--set", "authService.revisionFeed.enabled=false"}} {
			docs, out, err := renderAuth(t, feedArgs(phase, extra...)...)
			if err != nil {
				t.Fatalf("phase %d: render: %v\n%.400s", phase, err, out)
			}
			if doc := authDoc(docs, "Deployment", "a-dev-health-auth-service"); doc == nil {
				t.Fatalf("phase %d: no auth-service Deployment in the render", phase)
			}
			if strings.Contains(out, revisionFeedEnv) {
				t.Errorf("phase %d %v: the render carries %s; want it nowhere", phase, extra, revisionFeedEnv)
			}
		}
	}
}

// True renders AUTH_REVISION_FEED=on on auth-service, and on nothing else.
func TestRevisionFeedOnRendersOnAuthServiceOnly(t *testing.T) {
	for _, phase := range []int{1, 2} {
		docs, out, err := renderAuth(t, feedArgs(phase, "--set", "authService.revisionFeed.enabled=true")...)
		if err != nil {
			t.Fatalf("phase %d: render: %v\n%.400s", phase, err, out)
		}
		doc := authDoc(docs, "Deployment", "a-dev-health-auth-service")
		if value, set := containerEnvValue(t, doc, revisionFeedEnv); !set || value != "on" {
			t.Errorf("phase %d: auth-service %s=%q set=%v; want on", phase, revisionFeedEnv, value, set)
		}
		if n := strings.Count(out, "name: "+revisionFeedEnv); n != 1 {
			t.Errorf("phase %d: %d env lines name %s; want 1 (auth-service only)", phase, n, revisionFeedEnv)
		}
	}
}

// The feed writes to the rate-limit store: true without its Secret is refused at render time.
func TestRevisionFeedNeedsTheValkeySecret(t *testing.T) {
	_, out, err := renderAuth(t, phaseArgs(1, "--set", "authService.revisionFeed.enabled=true")...)
	if err == nil || !strings.Contains(out, "authService.revisionFeed.enabled needs authService.valkey.secretName") {
		t.Errorf("rendered without the store Secret: err=%v out=%.300s", err, out)
	}
}

// The schema or the template refuses anything but a bool under enabled.
func TestRevisionFeedMustBeABool(t *testing.T) {
	for name, set := range map[string][]string{
		"a string": {"--set-string", "authService.revisionFeed.enabled=on"},
		"null":     {"--set-json", "authService.revisionFeed.enabled=null"},
		"a typo":   {"--set", "authService.revisionFeed.enable=true"},
	} {
		_, out, err := renderAuth(t, feedArgs(1, set...)...)
		if err == nil || !strings.Contains(out, "revisionFeed") {
			t.Errorf("%s: rendered: err=%v out=%.300s", name, err, out)
		}
	}
}

// The feed has one source: extraEnv cannot set it past the store check.
func TestRevisionFeedCannotBeSetThroughExtraEnv(t *testing.T) {
	_, out, err := renderAuth(t, feedArgs(1, "--set-json", `authService.extraEnv=[{"name":"AUTH_REVISION_FEED","value":"on"}]`)...)
	if err == nil || !strings.Contains(out, "authService.extraEnv must not set AUTH_REVISION_FEED") {
		t.Errorf("extraEnv rendered: err=%v out=%.300s", err, out)
	}
}
