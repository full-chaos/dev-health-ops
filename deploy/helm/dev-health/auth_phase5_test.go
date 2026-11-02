package devhealth_test

import (
	"fmt"
	"strings"
	"testing"
)

// Phase 5 (orgs owned) renders once go-api sends organization changes to auth-service.
var orgProvisioningOnNoInviteKey = append([]string{"--set", "goApi.authProxy.orgProvisioning=true"}, membershipRoutesOn...)

// The invite signing keys auth-service's membership routes need, by Secret reference (the chart creates no Secret).
var orgProvisioningOn = append(append([]string{}, orgProvisioningOnNoInviteKey...), "--set", "authService.sessionKeys.inviteToken.secretName=auth-invite")

func phase5Args(extra ...string) []string {
	return phaseArgs(5, append(append([]string{}, orgProvisioningOn...), extra...)...)
}

func TestPhase5Renders(t *testing.T) {
	docs, out, err := renderAuth(t, phase5Args()...)
	if err != nil {
		t.Fatalf("phase 5: render failed: %v\n%.600s", err, out)
	}
	service := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	if service == nil {
		t.Fatal("no auth-service Deployment at phase 5")
	}
	for _, name := range []string{"AUTH_USERS_OWNED", "AUTH_ORGS_OWNED"} {
		if value, set := containerEnvValue(t, service, name); !set || value != "true" {
			t.Errorf("auth-service %s = %q (set %v), want true", name, value, set)
		}
	}
}

func TestPhase5NeedsOrgProvisioning(t *testing.T) {
	for name, args := range map[string][]string{
		"absent":         phaseArgs(5, membershipRoutesOn...),
		"explicit false": phaseArgs(5, append([]string{"--set", "goApi.authProxy.orgProvisioning=false"}, membershipRoutesOn...)...),
		"no routes":      phaseArgs(5, "--set", "goApi.authProxy.orgProvisioning=true"),
	} {
		_, out, err := renderAuth(t, args...)
		if err == nil {
			t.Errorf("%s: phase 5 rendered", name)
			continue
		}
		if name != "no routes" && !strings.Contains(out, "authService.phase 5 needs goApi.authProxy.orgProvisioning=true") {
			t.Errorf("%s: the refusal does not name the missing switch: %.400s", name, out)
		}
	}
}

func TestPhase5RefusesAUsersOwnedStage(t *testing.T) {
	for _, stage := range []string{"goApi", "owned"} {
		_, out, err := renderAuth(t, phase5Args("--set", "authService.usersOwnedStage="+stage)...)
		if err == nil || !strings.Contains(out, "authService.usersOwnedStage is set but authService.phase is 5") {
			t.Errorf("stage %s at phase 5: err=%v out=%.300s", stage, err, out)
		}
	}
}

// One switch, every reader: auth-service, go-api, the schema migrate Job and the River hook read the
// same AUTH_ORGS_OWNED, so the api role's REVOKE and go-api's readiness cannot disagree.
func TestOrgsOwnedEnvReachesEveryReaderFromPhase5(t *testing.T) {
	readers := []struct{ kind, name string }{
		{"Deployment", "a-dev-health-auth-service"},
		{"Deployment", "a-dev-health-go-api"},
		{"Job", "a-dev-health-migrate"},
		{"Job", "a-dev-health-river-migrate"},
	}
	for phase := 0; phase <= 5; phase++ {
		args := phaseArgs(phase)
		if phase == 5 {
			args = phase5Args()
		}
		docs, out, err := renderAuth(t, args...)
		if err != nil {
			t.Fatalf("phase %d: render failed: %v\n%.400s", phase, err, out)
		}
		for _, reader := range readers {
			if phase == 0 && reader.name == "a-dev-health-auth-service" {
				continue
			}
			entry, set := sessionsContainerEnv(t, docs, reader.kind, reader.name)["AUTH_ORGS_OWNED"]
			if want := phase == 5; set != want || (set && at(entry, "value") != "true") {
				t.Errorf("phase %d: %s %s AUTH_ORGS_OWNED = %v (set %v), want set=%v value true", phase, reader.kind, reader.name, entry, set, want)
			}
		}
	}
}

func TestGoAPIExtraEnvRefusesOrgsOwned(t *testing.T) {
	for _, name := range []string{"AUTH_ORGS_OWNED", "auth_orgs_owned"} {
		for label, args := range map[string][]string{
			"phase 4": phaseArgs(4, "--set-json", `goApi.extraEnv=[{"name":"`+name+`","value":"true"}]`),
			"phase 5": phase5Args("--set-json", `goApi.extraEnv=[{"name":"`+name+`","value":"true"}]`),
		} {
			_, out, err := renderAuth(t, args...)
			if err == nil || !strings.Contains(out, "goApi.extraEnv must not set AUTH_ORGS_OWNED") {
				t.Errorf("%s %s: err=%v out=%.300s", label, name, err, out)
			}
		}
	}
}

// Platform roles move with orgs: the switch needs orgs owned, not only users owned.
func TestPlatformRolesOwnedNeedsOrgsOwned(t *testing.T) {
	const on = "authService.platformRolesOwned=true"
	_, out, err := renderAuth(t, phaseArgs(4, "--set", on)...)
	if err == nil || !strings.Contains(out, "authService.platformRolesOwned needs orgs owned") {
		t.Errorf("phase 4 owned: err=%v out=%.300s", err, out)
	}
	docs, out, err := renderAuth(t, phase5Args("--set", on)...)
	if err != nil {
		t.Fatalf("phase 5: render failed: %v\n%.400s", err, out)
	}
	for _, target := range []struct{ kind, name string }{{"Deployment", "a-dev-health-auth-service"}, {"Deployment", "a-dev-health-go-api"}, {"Deployment", "a-dev-health-query-api"}} {
		entry := sessionsContainerEnv(t, docs, target.kind, target.name)["AUTH_PLATFORM_ROLES_OWNED"]
		if at(entry, "value") != "true" {
			t.Errorf("%s: AUTH_PLATFORM_ROLES_OWNED = %v, want true", target.name, entry)
		}
	}
}

// The federated sign-in join renders on auth-service only, and only at phase 5.
func TestSSOJITRendersOnAuthServiceOnlyAtPhase5(t *testing.T) {
	docs, out, err := renderAuth(t, phase5Args("--set", "authService.ssoJit=true")...)
	if err != nil {
		t.Fatalf("phase 5: render failed: %v\n%.400s", err, out)
	}
	carriers := 0
	for _, doc := range docs {
		for _, container := range podContainers(doc) {
			env, _ := at(container, "env").([]any)
			for _, entry := range env {
				if at(entry, "name") == ssoJITEnv {
					carriers++
					if at(doc, "metadata", "name") != "a-dev-health-auth-service" || at(entry, "value") != "true" {
						t.Errorf("%v carries %s=%v", at(doc, "metadata", "name"), ssoJITEnv, at(entry, "value"))
					}
				}
			}
		}
	}
	if carriers != 1 {
		t.Fatalf("%d containers carry %s, want 1", carriers, ssoJITEnv)
	}
}

func inviteRef(t *testing.T, docs []map[string]any, kind, name, env string) (secret, key string, present bool) {
	t.Helper()
	entry, ok := sessionsContainerEnv(t, docs, kind, name)[env]
	if !ok {
		return "", "", false
	}
	if at(entry, "value") != nil {
		t.Errorf("%s %s carries a value for %s", kind, name, env)
	}
	return fmt.Sprint(at(entry, "valueFrom", "secretKeyRef", "name")), fmt.Sprint(at(entry, "valueFrom", "secretKeyRef", "key")), true
}

// Phase 5 mounts the membership routes, which refuse to start without both invite keys: the chart renders them from
// authService.sessionKeys.inviteToken by secretKeyRef, at phase 5 only, on auth-service only.
func TestInviteKeysRenderAtPhase5Only(t *testing.T) {
	docs, out, err := renderAuth(t, phase5Args()...)
	if err != nil {
		t.Fatalf("phase 5: render failed: %v\n%.400s", err, out)
	}
	for env, key := range map[string]string{"AUTH_INVITE_TOKEN_SECRET": "AUTH_INVITE_TOKEN_SECRET", "AUTH_INVITE_TOKEN_SECRET_PREVIOUS": "AUTH_INVITE_TOKEN_SECRET_PREVIOUS"} {
		secret, gotKey, present := inviteRef(t, docs, "Deployment", "a-dev-health-auth-service", env)
		if !present || secret != "auth-invite" || gotKey != key {
			t.Errorf("auth-service %s = secret %q key %q (present %v), want auth-invite / %s", env, secret, gotKey, present, key)
		}
	}
	docs, out, err = renderAuth(t, phase5Args("--set", "authService.sessionKeys.inviteToken.secretKey=cur-key", "--set", "authService.sessionKeys.inviteToken.previousSecretKey=prev-key")...)
	if err != nil {
		t.Fatalf("custom keys: render failed: %v\n%.400s", err, out)
	}
	if _, key, _ := inviteRef(t, docs, "Deployment", "a-dev-health-auth-service", "AUTH_INVITE_TOKEN_SECRET"); key != "cur-key" {
		t.Errorf("current key = %q, want cur-key", key)
	}
	if _, key, _ := inviteRef(t, docs, "Deployment", "a-dev-health-auth-service", "AUTH_INVITE_TOKEN_SECRET_PREVIOUS"); key != "prev-key" {
		t.Errorf("previous key = %q, want prev-key", key)
	}
	for _, doc := range docs {
		if at(doc, "metadata", "name") == "a-dev-health-auth-service" && doc["kind"] == "Deployment" {
			continue
		}
		for _, container := range podContainers(doc) {
			env, _ := at(container, "env").([]any)
			for _, entry := range env {
				if name, _ := at(entry, "name").(string); strings.HasPrefix(name, "AUTH_INVITE_TOKEN_SECRET") {
					t.Errorf("%v %v carries %s", doc["kind"], at(doc, "metadata", "name"), name)
				}
			}
		}
	}
	// Below phase 5 the key is inert, with the routes on and the Secret named.
	below, out, err := renderAuth(t, phaseArgs(4, orgProvisioningOn...)...)
	if err != nil {
		t.Fatalf("phase 4: render failed: %v\n%.400s", err, out)
	}
	for _, env := range []string{"AUTH_INVITE_TOKEN_SECRET", "AUTH_INVITE_TOKEN_SECRET_PREVIOUS"} {
		if _, _, present := inviteRef(t, below, "Deployment", "a-dev-health-auth-service", env); present {
			t.Errorf("phase 4 renders %s", env)
		}
	}
}

func TestPhase5NeedsTheInviteKeySecret(t *testing.T) {
	_, out, err := renderAuth(t, phaseArgs(5, orgProvisioningOnNoInviteKey...)...)
	if err == nil || !strings.Contains(out, "authService.phase 5 needs authService.sessionKeys.inviteToken.secretName") {
		t.Errorf("phase 5 without the invite Secret: err=%v out=%.300s", err, out)
	}
}

func TestInviteKeyNamesMustBeSecretKeys(t *testing.T) {
	for _, key := range []string{"secretKey", "previousSecretKey"} {
		_, out, err := renderAuth(t, phase5Args("--set", "authService.sessionKeys.inviteToken."+key+"=bad key!")...)
		if err == nil || !strings.Contains(out, "authService.sessionKeys.inviteToken."+key+" must be a Secret key") {
			t.Errorf("%s: err=%v out=%.300s", key, err, out)
		}
	}
}

func TestInviteEnvCannotBeSetThroughExtraEnv(t *testing.T) {
	for _, name := range []string{"AUTH_INVITE_TOKEN_SECRET", "auth_invite_token_secret_previous"} {
		entries := `[{"name":"AUTH_MEMBERSHIP_ROUTES","value":"true"},{"name":"` + name + `","valueFrom":{"secretKeyRef":{"name":"x","key":"y"}}}]`
		_, out, err := renderAuth(t, phase5Args("--set-json", "authService.extraEnv="+entries)...)
		if err == nil || !strings.Contains(out, "authService.extraEnv must not set") {
			t.Errorf("%s: err=%v out=%.300s", name, err, out)
		}
	}
}
