package devhealth_test

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

// CHAOS-12532: phase 3 rolls in two stages. Stage authService: auth-service mounts the login route (the lockout key and
// the social client env) while go-api still logs in locally. Stage goApi: go-api forwards login and social. Phase 3
// needs a stage and the stage exists only at phase 3 (the usersOwnedStage rules at phase 4).

func loginStageArgs(phase, stage string, extra ...string) []string {
	sets := []string{"--set", "authService.phase=" + phase}
	if stage != "" {
		sets = append(sets, "--set", "authService.loginStage="+stage)
	}
	return append(goAPIWithAuth(append(sets, phase3Secrets...)...), extra...)
}

var socialEnvNames = []string{
	"SOCIAL_GITHUB_CLIENT_ID", "SOCIAL_GITHUB_CLIENT_SECRET", "SOCIAL_GITLAB_CLIENT_ID",
	"SOCIAL_GITLAB_CLIENT_SECRET", "SOCIAL_GOOGLE_CLIENT_ID", "SOCIAL_GOOGLE_CLIENT_SECRET",
}

func authServiceServesLogin(t *testing.T, docs []map[string]any) (login, social bool) {
	t.Helper()
	svc := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	if svc == nil {
		t.Fatal("no auth-service Deployment in the render")
	}
	_, login = containerEnvValue(t, svc, "AUTH_LOGIN_LOCKOUT_KEY")
	_, social = containerEnvValue(t, svc, socialEnvNames[0])
	return login, social
}

func TestLoginStageAuthServiceRendersOnlyTheAuthServiceHalf(t *testing.T) {
	docs, out, err := renderAuth(t, loginStageArgs("3", "authService")...)
	if err != nil {
		t.Fatalf("stage authService: render: %v\n%.400s", err, out)
	}
	login, social := authServiceServesLogin(t, docs)
	if !login || !social {
		t.Errorf("stage authService: auth-service lockout env=%v social env=%v; want both", login, social)
	}
	flags := proxyEnv(t, docs)
	for _, name := range []string{"AUTH_PROXY_LOCAL_LOGIN", "AUTH_PROXY_SOCIAL"} {
		if flags[name] != "false" {
			t.Errorf("stage authService: go-api %s=%q; want false (go-api still logs in locally)", name, flags[name])
		}
	}
	if flags["AUTH_PROXY_SESSIONS"] != "true" {
		t.Errorf("stage authService: go-api AUTH_PROXY_SESSIONS=%q; want true (phase 2 stays)", flags["AUTH_PROXY_SESSIONS"])
	}
}

func TestLoginStageGoAPIForwardsLoginAndSocial(t *testing.T) {
	docs, out, err := renderAuth(t, loginStageArgs("3", "goApi")...)
	if err != nil {
		t.Fatalf("stage goApi: render: %v\n%.400s", err, out)
	}
	login, social := authServiceServesLogin(t, docs)
	if !login || !social {
		t.Errorf("stage goApi: auth-service lockout env=%v social env=%v; want both", login, social)
	}
	flags := proxyEnv(t, docs)
	for _, name := range []string{"AUTH_PROXY_LOCAL_LOGIN", "AUTH_PROXY_SOCIAL"} {
		if flags[name] != "true" {
			t.Errorf("stage goApi: go-api %s=%q; want true", name, flags[name])
		}
	}
}

// Phase 3 without a stage is refused, naming both values and their order (a silent default would let the race back in).
func TestPhase3NeedsALoginStage(t *testing.T) {
	_, out, err := renderAuthRaw(t, loginStageArgs("3", "")...)
	if err == nil {
		t.Fatalf("phase 3 without a stage rendered: %.300s", out)
	}
	for _, want := range []string{"authService.loginStage", "authService first", "then goApi"} {
		if !strings.Contains(out, want) {
			t.Errorf("the refusal does not say %q: %.400s", want, out)
		}
	}
}

func TestLoginStageIsRefusedOutsidePhase3(t *testing.T) {
	for _, phase := range []string{"0", "1", "2", "4"} {
		for _, stage := range []string{"authService", "goApi"} {
			extra := []string{}
			if phase == "4" {
				extra = append(extra, "--set", "authService.usersOwnedStage=goApi")
			}
			_, out, err := renderAuthRaw(t, loginStageArgs(phase, stage, extra...)...)
			if err == nil || !strings.Contains(out, "authService.loginStage") {
				t.Errorf("phase %s with loginStage=%s rendered: err=%v out=%.300s", phase, stage, err, out)
			}
		}
	}
}

func TestLoginStageValueIsClosed(t *testing.T) {
	for _, bad := range []string{"GoApi", "auth-service", "both", "true"} {
		_, out, err := renderAuthRaw(t, loginStageArgs("3", bad)...)
		if err == nil || !strings.Contains(out, "loginStage") {
			t.Errorf("loginStage %q rendered: err=%v out=%.300s", bad, err, out)
		}
	}
}

// go-api never forwards login to an auth-service that does not mount it, in any phase or stage.
func TestNoRenderForwardsLoginToAnAuthServiceWithoutTheLoginRoute(t *testing.T) {
	cases := []struct {
		phase, stage string
		extra        []string
	}{
		{"2", "", nil}, {"3", "authService", nil}, {"3", "goApi", nil},
		{"4", "", []string{"--set", "authService.usersOwnedStage=goApi"}},
		{"4", "", []string{"--set", "authService.usersOwnedStage=owned"}},
	}
	for _, c := range cases {
		docs, out, err := renderAuth(t, loginStageArgs(c.phase, c.stage, c.extra...)...)
		if err != nil {
			t.Fatalf("phase %s stage %q: render: %v\n%.300s", c.phase, c.stage, err, out)
		}
		login, social := authServiceServesLogin(t, docs)
		flags := proxyEnv(t, docs)
		if flags["AUTH_PROXY_LOCAL_LOGIN"] == "true" && !login {
			t.Errorf("phase %s stage %q: go-api forwards login, auth-service has no lockout key", c.phase, c.stage)
		}
		if flags["AUTH_PROXY_SOCIAL"] == "true" && !social {
			t.Errorf("phase %s stage %q: go-api forwards social, auth-service has no social client env", c.phase, c.stage)
		}
	}
}

// The stage is the only difference between the two stage renders: Secret references and every other document are shared.
func TestLoginStagesDifferOnlyInTheGoAPIFlags(t *testing.T) {
	docsA, _, err := renderAuth(t, loginStageArgs("3", "authService")...)
	if err != nil {
		t.Fatalf("stage authService: render: %v", err)
	}
	docsG, _, err := renderAuth(t, loginStageArgs("3", "goApi")...)
	if err != nil {
		t.Fatalf("stage goApi: render: %v", err)
	}
	for _, doc := range docsG {
		meta, _ := doc["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		kind, _ := doc["kind"].(string)
		if kind == "Deployment" && name == "a-dev-health-go-api" {
			continue
		}
		other := authDoc(docsA, kind, name)
		if other == nil {
			t.Errorf("%s %s is missing from the stage authService render", kind, name)
			continue
		}
		if !reflect.DeepEqual(other, doc) {
			t.Errorf("%s %s differs between the stages", kind, name)
		}
	}
}

func TestLoginStageIsInTheSchemaAndTheValues(t *testing.T) {
	schema, err := os.ReadFile("values.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(schema), `"loginStage"`) {
		t.Error("values.schema.json has no loginStage")
	}
	values, err := os.ReadFile("values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(values), "\n  loginStage:") {
		t.Error("values.yaml has no authService.loginStage default")
	}
}

func TestLoginStageRunbookNamesBothRollsAndTheReverseRollback(t *testing.T) {
	doc, err := os.ReadFile("../../../docs/contribute/architecture/auth-control-plane-deployment.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(doc)
	for _, want := range []string{"authService.loginStage", "stage `authService`", "stage `goApi`", "reverse order"} {
		if !strings.Contains(text, want) {
			t.Errorf("auth-control-plane-deployment.md does not mention %q", want)
		}
	}
}
