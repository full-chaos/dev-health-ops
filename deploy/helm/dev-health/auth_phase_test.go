package devhealth_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// authService.phase is the one auth cutover key. These tests pin the exact auth environment each rendered phase gives
// every workload (goldens under testdata/auth_phase), the refusal of the keys it replaces and of phases 4 and 5, and
// that go-api's and auth-service's own start-up parsers accept what the chart renders, with every route go-api
// forwards mounted by auth-service.

var phase3Secrets = []string{
	"--set", "authService.sessionKeys.loginLockout.secretName=auth-lockout",
	"--set", "authService.social.secretName=auth-social",
}

// phaseArgs renders go-api, query-api, the migrate Job and the River hooks beside auth-service, with every
// precondition the phase needs.
func phaseArgs(phase int, extra ...string) []string {
	out := append([]string{}, authBase...)
	out = append(out, queryAPIRoleBase...)
	out = append(out, riverHookSets...)
	out = append(out, roleEnv("devhealth_query")...)
	out = append(out, "--set", fmt.Sprintf("authService.phase=%d", phase))
	if phase >= 2 {
		out = append(out, "--set", "authService.migrate.sessionReaders.queryApi=devhealth_query")
		out = append(out, sessionSuccessor...)
		out = append(out, authTrusted...)
		out = append(out, "--set", "authService.valkey.secretName=auth-valkey",
			"--set-json", `authService.networkPolicy.egress.valkey.to=[{"podSelector":{"matchLabels":{"app":"auth-valkey"}}}]`)
	}
	if phase >= 3 {
		out = append(out, phase3Secrets...)
	}
	return append(out, extra...)
}

// withoutPhase drops every --set of authService.phase, so a --set-json of it is the value Helm sees (Helm applies
// --set after --set-json, whatever the order on the command line).
func withoutPhase(args []string, extra ...string) []string {
	var out []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--set" && i+1 < len(args) && strings.HasPrefix(args[i+1], "authService.phase=") {
			i++
			continue
		}
		out = append(out, args[i])
	}
	return append(out, extra...)
}

func podContainers(doc map[string]any) []any {
	spec := at(doc, "spec", "template", "spec")
	if spec == nil {
		return nil
	}
	var out []any
	for _, field := range []string{"initContainers", "containers"} {
		list, _ := at(spec, field).([]any)
		out = append(out, list...)
	}
	return out
}

func envEntryValue(entry any) string {
	if ref := at(entry, "valueFrom", "secretKeyRef"); ref != nil {
		optional := ""
		if at(ref, "optional") == true {
			optional = "?"
		}
		return fmt.Sprintf("secret:%v/%v%s", at(ref, "name"), at(ref, "key"), optional)
	}
	if ref := at(entry, "valueFrom", "configMapKeyRef"); ref != nil {
		return fmt.Sprintf("configmap:%v/%v", at(ref, "name"), at(ref, "key"))
	}
	return fmt.Sprintf("value:%v", at(entry, "value"))
}

// authSnapshot is the auth surface of a render: every AUTH_* and SOCIAL_* variable of every container, the auth
// documents rendered, and the peers the auth-service API port admits.
func authSnapshot(t *testing.T, docs []map[string]any) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, doc := range docs {
		kind, _ := doc["kind"].(string)
		name, _ := at(doc, "metadata", "name").(string)
		if strings.Contains(name, "auth-") {
			out["doc/"+kind+"/"+name] = "rendered"
		}
		for _, container := range podContainers(doc) {
			cname, _ := at(container, "name").(string)
			env, _ := at(container, "env").([]any)
			for _, entry := range env {
				ename, _ := at(entry, "name").(string)
				if strings.HasPrefix(ename, "AUTH_") || strings.HasPrefix(ename, "SOCIAL_") {
					out[kind+"/"+name+"/"+cname+"/"+ename] = envEntryValue(entry)
				}
			}
		}
	}
	if authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service") != nil {
		var from []string
		for _, peer := range authServiceAPIFrom(t, docs) {
			from = append(from, fmt.Sprint(at(peer, "podSelector", "matchLabels", "app.kubernetes.io/component")))
		}
		sort.Strings(from)
		out["NetworkPolicy/a-dev-health-auth-service/api-port-from"] = strings.Join(from, ",")
	}
	return out
}

func renderPhase(t *testing.T, phase int, extra ...string) []map[string]any {
	t.Helper()
	docs, out, err := renderAuth(t, phaseArgs(phase, extra...)...)
	if err != nil {
		t.Fatalf("phase %d: render failed: %v\n%.600s", phase, err, out)
	}
	return docs
}

func TestAuthPhaseRendersExactFlagSet(t *testing.T) {
	for phase := 0; phase <= 3; phase++ {
		t.Run(fmt.Sprintf("phase %d", phase), func(t *testing.T) {
			got := authSnapshot(t, renderPhase(t, phase))
			path := filepath.Join("testdata", "auth_phase", fmt.Sprintf("phase%d.golden.json", phase))
			raw, err := json.MarshalIndent(got, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			raw = append(raw, '\n')
			if os.Getenv("AUTH_PHASE_UPDATE_GOLDEN") == "1" {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, raw, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden (AUTH_PHASE_UPDATE_GOLDEN=1 writes it): %v", err)
			}
			if string(want) != string(raw) {
				t.Fatalf("phase %d auth surface differs from %s:\n got %s\nwant %s", phase, path, raw, want)
			}
		})
	}
}

// The ladder itself, stated once more without the goldens: which AUTH_PROXY_* flags go-api carries per phase.
func TestAuthPhaseLadder(t *testing.T) {
	ladder := map[int][]string{
		0: nil,
		1: nil,
		2: {"AUTH_PROXY_SESSIONS", "AUTH_PROXY_URL"},
		3: {"AUTH_PROXY_LOCAL_LOGIN", "AUTH_PROXY_SESSIONS", "AUTH_PROXY_SOCIAL", "AUTH_PROXY_URL"},
	}
	for phase, want := range ladder {
		docs := renderPhase(t, phase)
		var got []string
		for name := range proxyEnv(t, docs) {
			got = append(got, name)
		}
		sort.Strings(got)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("phase %d: go-api AUTH_PROXY_* = %v, want %v", phase, got, want)
		}
		if service := authDoc(docs, "Deployment", "a-dev-health-auth-service"); (service != nil) != (phase >= 1) {
			t.Errorf("phase %d: auth-service rendered = %v", phase, service != nil)
		}
	}
}

// Phase 0 is the chart default: setting it changes nothing.
func TestAuthPhaseZeroIsTheDefaultRender(t *testing.T) {
	_, plain, err := renderAuth(t, "--set", "goApi.enabled=true")
	if err != nil {
		t.Fatalf("default render: %v\n%.400s", err, plain)
	}
	_, zero, err := renderAuth(t, "--set", "goApi.enabled=true", "--set", "authService.phase=0")
	if err != nil {
		t.Fatalf("phase 0 render: %v\n%.400s", err, zero)
	}
	if plain != zero {
		t.Fatal("authService.phase=0 renders differently from the default")
	}
}

func TestAuthPhase45Refused(t *testing.T) {
	for _, phase := range []int{4, 5} {
		for name, args := range map[string][]string{
			"alone":             append(append([]string{}, authBase...), "--set", fmt.Sprintf("authService.phase=%d", phase)),
			"with every input":  phaseArgs(3, "--set", fmt.Sprintf("authService.phase=%d", phase)),
			"without a service": {"--set", "goApi.enabled=true", "--set", fmt.Sprintf("authService.phase=%d", phase)},
		} {
			t.Run(fmt.Sprintf("phase %d %s", phase, name), func(t *testing.T) {
				_, out, err := renderAuth(t, args...)
				if err == nil {
					t.Fatal("rendered; phases 4 and 5 must be refused")
				}
				if !strings.Contains(out, fmt.Sprintf("authService.phase %d: phases 4 and 5 need the public-table writer decision (D6358)", phase)) {
					t.Fatalf("the refusal does not name the decision: %.400s", out)
				}
			})
		}
	}
}

func TestAuthPhaseRefusesHandSetFlags(t *testing.T) {
	cases := map[string]struct {
		args []string
		want string
	}{
		"authProxy empty map":     {phaseArgs(1, "--set-json", "goApi.authProxy={}"), "goApi.authProxy is replaced by authService.phase"},
		"authProxy without goApi": {append(append([]string{}, authBase...), "--set", "goApi.authProxy.sessions=false"), "goApi.authProxy is replaced by authService.phase"},
		"phase -1":                {phaseArgs(1, "--set", "authService.phase=-1"), "/authService/phase"},
		"phase 6":                 {phaseArgs(1, "--set", "authService.phase=6"), "/authService/phase"},
		"phase string":            {phaseArgs(1, "--set-string", "authService.phase=1"), "/authService/phase"},
		"phase fraction":          {withoutPhase(phaseArgs(1), "--set-json", "authService.phase=1.5"), "/authService/phase"},
		"phase null":              {withoutPhase(phaseArgs(1), "--set-json", "authService.phase=null"), "authService.phase must be a whole number from 0 to 5"},
		"phase 3 without lockout": {phaseArgs(3, "--set", "authService.sessionKeys.loginLockout.secretName="), "needs authService.sessionKeys.loginLockout.secretName"},
		"phase 3 without social":  {phaseArgs(3, "--set", "authService.social.secretName="), "needs authService.social.secretName"},
		"token class below 2":     {phaseArgs(1, "--set", "authService.accessToken.class=hs256_compat"), "authService.accessToken.class needs authService.phase >= 2"},
		"token class unknown":     {phaseArgs(2, "--set", "authService.accessToken.class=rs256"), "must be empty, hs256_compat or eddsa"},
		"token class not string":  {phaseArgs(2, "--set-json", "authService.accessToken.class=true"), "must be strings"},
		"eddsa without issuer":    {phaseArgs(2, "--set", "authService.accessToken.class=eddsa"), "needs authService.accessToken.issuerURL"},
		"eddsa http issuer":       {phaseArgs(2, "--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL=http://auth.example"), "needs authService.accessToken.issuerURL"},
		"eddsa trailing slash":    {phaseArgs(2, "--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL=https://auth.example/"), "needs authService.accessToken.issuerURL"},
		"eddsa upper-case host":   {phaseArgs(2, "--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL=https://Auth.example"), "needs authService.accessToken.issuerURL"},
		"issuer without eddsa":    {phaseArgs(2, "--set", "authService.accessToken.issuerURL=https://auth.example"), "is set but authService.accessToken.class is not eddsa"},
		"link routes below 2":     {phaseArgs(1, "--set", "authService.socialLinkRoutes=true"), "authService.socialLinkRoutes needs authService.phase >= 2"},
		"oidc on":                 {phaseArgs(3, "--set", "authService.sso.oidc=true"), "authService.sso.oidc=true is refused"},
		"saml on":                 {phaseArgs(3, "--set", "authService.sso.saml=true"), "authService.sso.saml=true is refused"},
	}
	for _, key := range []string{"authService.enabled", "authService.usersOwned", "authService.orgsOwned"} {
		for _, value := range []string{"true", "false"} {
			cases[key+"="+value] = struct {
				args []string
				want string
			}{phaseArgs(1, "--set", key+"="+value), key + " is replaced by authService.phase"}
		}
		cases[key+" string"] = struct {
			args []string
			want string
		}{phaseArgs(1, "--set-string", key+"=false"), key + " is replaced by authService.phase"}
	}
	for _, name := range []string{"AUTH_ACCESS_TOKEN_CLASS", "AUTH_TOKEN_ISSUER_URL", "AUTH_JWT_AUDIENCE", "AUTH_LOGIN_LOCKOUT_KEY",
		"AUTH_SOCIAL_LINK_ROUTES", "AUTH_ORGS_OWNED", "AUTH_USERS_OWNED", "auth_access_token_class"} {
		cases["auth-service extraEnv "+name] = struct {
			args []string
			want string
		}{phaseArgs(1, "--set-json", fmt.Sprintf(`authService.extraEnv=[{"name":%q,"value":"x"}]`, name)), "authService.extraEnv must not set"}
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, tc.args...)
			if err == nil {
				t.Fatal("rendered; it must be refused")
			}
			if !strings.Contains(out, tc.want) {
				t.Fatalf("the refusal does not say %q: %.500s", tc.want, out)
			}
		})
	}
}

// The named keys beside the ladder render where the plan puts them, and nowhere else.
func TestAuthPhaseNamedKeys(t *testing.T) {
	envOf := func(docs []map[string]any, kind, name, env string) (string, bool) {
		return containerEnvValue(t, authDoc(docs, kind, name), env)
	}
	docs := renderPhase(t, 2, "--set", "authService.socialLinkRoutes=true", "--set", "authService.accessToken.class=hs256_compat")
	if v, _ := envOf(docs, "Deployment", "a-dev-health-auth-service", "AUTH_SOCIAL_LINK_ROUTES"); v != "true" {
		t.Errorf("AUTH_SOCIAL_LINK_ROUTES = %q, want true", v)
	}
	if v, _ := envOf(docs, "Deployment", "a-dev-health-auth-service", "AUTH_ACCESS_TOKEN_CLASS"); v != "hs256_compat" {
		t.Errorf("AUTH_ACCESS_TOKEN_CLASS = %q, want hs256_compat", v)
	}
	if _, set := envOf(docs, "Deployment", "a-dev-health-go-api", "AUTH_ACCESS_TOKEN_ISSUER"); set {
		t.Error("hs256_compat renders go-api AUTH_ACCESS_TOKEN_ISSUER")
	}
	issuer := "https://auth.example/v1"
	docs = renderPhase(t, 2, "--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL="+issuer)
	for _, want := range []struct{ kind, name, env, value string }{
		{"Deployment", "a-dev-health-auth-service", "AUTH_ACCESS_TOKEN_CLASS", "eddsa"},
		{"Deployment", "a-dev-health-auth-service", "AUTH_TOKEN_ISSUER_URL", issuer},
		{"Deployment", "a-dev-health-auth-service", "AUTH_JWT_AUDIENCE", "dev-health-api"},
		{"Deployment", "a-dev-health-go-api", "AUTH_ACCESS_TOKEN_ISSUER", issuer},
	} {
		if v, _ := envOf(docs, want.kind, want.name, want.env); v != want.value {
			t.Errorf("%s %s = %q, want %q", want.name, want.env, v, want.value)
		}
	}
}

// fakeSecret is a value of the shape the service checks, for each Secret key the chart references.
func fakeSecret(t *testing.T, ref string) string {
	t.Helper()
	key := ref[strings.LastIndex(ref, "/")+1:]
	key = strings.TrimSuffix(key, "?")
	key32 := base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("k", 32)))
	switch {
	case key == "AUTH_DATABASE_URI":
		return "postgres://auth:pw@db:5432/devhealth"
	case key == "JWT_SECRET_KEY":
		return strings.Repeat("j", 48)
	case key == "AUTH_SESSION_SUCCESSOR_KEY", key == "AUTH_LOGIN_LOCKOUT_KEY":
		return key32
	case key == "MIGRATION_URI", key == "AUTH_MIGRATION_DATABASE_URI":
		return "postgres://migrate:pw@db:5432/devhealth"
	case key == "AUTH_SESSION_ISSUE_TOKEN":
		return "svc_fake_session_issuer_token"
	case key == "AUTH_VALKEY_URI":
		return "redis://auth:pw@valkey:6379/1"
	case strings.HasPrefix(key, "SOCIAL_"):
		return "social-" + strings.ToLower(key)
	}
	t.Fatalf("no fake value for the Secret reference %s: add one so the start-up check covers it", ref)
	return ""
}

// containerLookup is the process environment a container starts with: plain values as rendered, Secret references
// as shaped fakes. Config map references stay unset.
func containerLookup(t *testing.T, doc map[string]any, container string) func(string) (string, bool) {
	t.Helper()
	env := map[string]string{}
	for _, c := range podContainers(doc) {
		if at(c, "name") != container {
			continue
		}
		entries, _ := at(c, "env").([]any)
		for _, entry := range entries {
			name, _ := at(entry, "name").(string)
			value := envEntryValue(entry)
			switch {
			case strings.HasPrefix(value, "value:"):
				env[name] = strings.TrimPrefix(value, "value:")
			case strings.HasPrefix(value, "secret:") && (strings.HasPrefix(name, "AUTH_") || strings.HasPrefix(name, "SOCIAL_")):
				env[name] = fakeSecret(t, value)
			}
		}
	}
	return func(name string) (string, bool) { v, ok := env[name]; return v, ok }
}

// authServiceOverrides maps the rendered auth-service args onto the settings they shadow, as the service's own flag
// parser does (authconfig.Options); an unknown flag fails here as it fails the process.
func authServiceOverrides(t *testing.T, doc map[string]any) map[string]string {
	t.Helper()
	byFlag := map[string]string{}
	for _, option := range authconfig.Options() {
		if option.Flag != "" && !option.Secret {
			byFlag[option.Flag] = option.Env
		}
	}
	out := map[string]string{}
	for _, c := range podContainers(doc) {
		if at(c, "name") != "auth-service" {
			continue
		}
		args, _ := at(c, "args").([]any)
		for _, arg := range args {
			flag, value, ok := strings.Cut(strings.TrimLeft(fmt.Sprint(arg), "-"), "=")
			env, known := byFlag[flag]
			if !ok || !known {
				t.Fatalf("auth-service arg %v is not a known --flag=value", arg)
			}
			out[env] = value
		}
	}
	return out
}

func TestAuthPhaseStartupAgrees(t *testing.T) {
	type variant struct {
		phase int
		extra []string
	}
	variants := map[string]variant{
		"phase 0": {0, nil}, "phase 1": {1, nil}, "phase 2": {2, nil}, "phase 3": {3, nil},
		"phase 2 eddsa":       {2, []string{"--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL=https://auth.example"}},
		"phase 3 link routes": {3, []string{"--set", "authService.socialLinkRoutes=true"}},
	}
	for name, v := range variants {
		t.Run(name, func(t *testing.T) {
			docs := renderPhase(t, v.phase, v.extra...)
			goAPI := authDoc(docs, "Deployment", "a-dev-health-go-api")
			if goAPI == nil {
				t.Fatal("no go-api Deployment")
			}
			goLookup := containerLookup(t, goAPI, "go-api")
			proxy, err := apiservice.ParseAuthProxyConfig(goLookup)
			if err != nil {
				t.Fatalf("go-api refuses the rendered proxy settings: %v", err)
			}
			goUsersOwned, err := apiservice.ParseUsersOwned(goLookup, proxy)
			if err != nil {
				t.Fatalf("go-api refuses the rendered users-owned setting: %v", err)
			}
			service := authDoc(docs, "Deployment", "a-dev-health-auth-service")
			if service == nil {
				for _, aggregate := range apiservice.AuthAggregates {
					if proxy.Enabled(aggregate) {
						t.Errorf("no auth-service, but go-api forwards %s", aggregate)
					}
				}
				return
			}
			serviceLookup := containerLookup(t, service, "auth-service")
			cfg, err := authconfig.Load(authconfig.Spec{Overrides: authServiceOverrides(t, service), LookupEnv: serviceLookup})
			if err != nil {
				t.Fatalf("auth-service refuses the rendered settings: %v", err)
			}
			socialProviders := 0
			for _, client := range cfg.Social {
				if client.ID != "" {
					socialProviders++
				}
			}
			mounted := map[apiservice.AuthAggregate]bool{
				apiservice.AuthSessions:    cfg.Session.Enabled,
				apiservice.AuthLocalLogin:  cfg.Session.Enabled && cfg.Session.LoginEnabled,
				apiservice.AuthCredentials: cfg.Session.Enabled && cfg.UsersOwned,
				apiservice.AuthSocial:      cfg.Session.Enabled && socialProviders == len(authconfig.SocialProviders),
				apiservice.AuthOIDC:        cfg.Session.Enabled && cfg.SSO.OIDCEnabled(),
				apiservice.AuthSAML:        cfg.Session.Enabled && cfg.SSO.SAMLEnabled(),
			}
			for _, aggregate := range apiservice.AuthAggregates {
				if proxy.Enabled(aggregate) && !mounted[aggregate] {
					t.Errorf("go-api forwards %s, but auth-service does not mount it", aggregate)
				}
			}
			for _, aggregate := range []apiservice.AuthAggregate{apiservice.AuthSessions, apiservice.AuthLocalLogin} {
				if mounted[aggregate] && !proxy.Enabled(aggregate) {
					t.Errorf("auth-service mounts %s, but go-api does not forward it", aggregate)
				}
			}
			if cfg.UsersOwned || cfg.OrgsOwned || goUsersOwned {
				t.Errorf("a rendered phase sets an owned switch: auth-service users=%v orgs=%v, go-api users=%v", cfg.UsersOwned, cfg.OrgsOwned, goUsersOwned)
			}
			for _, job := range []string{"a-dev-health-auth-migrate", "a-dev-health-auth-grants"} {
				doc := authDoc(docs, "Job", job)
				if doc == nil {
					continue
				}
				lookup := containerLookup(t, doc, fmt.Sprint(at(podContainers(doc)[len(podContainers(doc))-1], "name")))
				users, err := authconfig.UsersOwnedFromLookup(lookup)
				if err != nil || users != cfg.UsersOwned {
					t.Errorf("%s: AUTH_USERS_OWNED reads %v (err %v), auth-service %v", job, users, err, cfg.UsersOwned)
				}
			}
			issuer, _ := goLookup("AUTH_ACCESS_TOKEN_ISSUER")
			if (cfg.Session.AccessClass == authconfig.AccessClassEdDSA) != (issuer != "") || (issuer != "" && issuer != cfg.TokenIssuerURL) {
				t.Errorf("token class %s with issuer %q, go-api verifies issuer %q", cfg.Session.AccessClass, cfg.TokenIssuerURL, issuer)
			}
		})
	}
}
