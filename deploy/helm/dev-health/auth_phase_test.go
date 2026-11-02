package devhealth_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// authService.phase is the one auth cutover key. These tests pin the exact auth environment each rendered phase gives
// every workload (goldens under testdata/auth_phase), the refusal of the keys it replaces and of phase 5, and
// that go-api's and auth-service's own start-up parsers accept what the chart renders, with every route go-api
// forwards mounted by auth-service.

var phase3Secrets = append([]string{
	"--set", "authService.sessionKeys.loginLockout.secretName=auth-lockout",
	"--set", "authService.social.secretName=auth-social",
}, mailSets()...) // the mail setting renders only from phase 4 and is inert below it

// phaseArgs renders go-api, query-api, the migrate Job and the River hooks beside auth-service, with every
// precondition the phase needs.
func phaseArgs(phase int, extra ...string) []string {
	out := append([]string{}, authBase...)
	out = append(out, queryAPIRoleBase...)
	out = append(out, riverHookSets...)
	out = append(out, roleEnv("devhealth_query")...)
	out = append(out, "--set", fmt.Sprintf("authService.phase=%d", phase))
	if phase == 3 {
		out = append(out, "--set", "authService.loginStage=goApi")
	}
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
	if phase == 4 && !slices.ContainsFunc(extra, func(s string) bool { return strings.HasPrefix(s, "authService.usersOwnedStage=") }) {
		out = append(out, "--set", "authService.usersOwnedStage=owned")
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
	for phase := 0; phase <= 4; phase++ {
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

// The ladder itself, stated once more without the goldens: which AUTH_PROXY_* flags go-api carries "true" per phase.
func TestAuthPhaseLadder(t *testing.T) {
	ladder := map[int][]string{
		0: nil,
		1: nil,
		2: {"AUTH_PROXY_SESSIONS"},
		3: {"AUTH_PROXY_LOCAL_LOGIN", "AUTH_PROXY_SESSIONS", "AUTH_PROXY_SOCIAL"},
		4: {"AUTH_PROXY_CREDENTIALS", "AUTH_PROXY_LOCAL_LOGIN", "AUTH_PROXY_SESSIONS", "AUTH_PROXY_SOCIAL"},
	}
	for phase, want := range ladder {
		docs := renderPhase(t, phase)
		var got []string
		for name, value := range proxyEnv(t, docs) {
			if name != "AUTH_PROXY_URL" && value == "true" {
				got = append(got, name)
			}
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

func TestAuthPhase5Refused(t *testing.T) {
	for _, phase := range []int{5} {
		for name, args := range map[string][]string{
			"alone":             append(append([]string{}, authBase...), "--set", fmt.Sprintf("authService.phase=%d", phase)),
			"with every input":  phaseArgs(3, "--set", fmt.Sprintf("authService.phase=%d", phase)),
			"without a service": {"--set", "goApi.enabled=true", "--set", fmt.Sprintf("authService.phase=%d", phase)},
		} {
			t.Run(fmt.Sprintf("phase %d %s", phase, name), func(t *testing.T) {
				_, out, err := renderAuth(t, args...)
				if err == nil {
					t.Fatal("rendered; phase 5 must be refused")
				}
				if !strings.Contains(out, fmt.Sprintf("authService.phase %d: phase 5 (orgs owned) needs its own public-table writer leaf (D6358)", phase)) {
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
		"eddsa class below 2":     {phaseArgs(1, "--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL=https://auth.example"), "authService.accessToken.class needs authService.phase >= 2"},
		"token class unknown":     {phaseArgs(2, "--set", "authService.accessToken.class=rs256"), "must be empty, hs256_compat or eddsa"},
		"token class not string":  {phaseArgs(2, "--set-json", "authService.accessToken.class=true"), "must be strings"},
		"token class false":       {phaseArgs(2, "--set-json", "authService.accessToken.class=false"), "must be strings"},
		"token class zero":        {phaseArgs(2, "--set-json", "authService.accessToken.class=0"), "must be strings"},
		"issuer false":            {phaseArgs(2, "--set-json", "authService.accessToken.issuerURL=false"), "must be strings"},
		"issuer zero":             {phaseArgs(2, "--set-json", "authService.accessToken.issuerURL=0"), "must be strings"},
		"eddsa without issuer":    {phaseArgs(2, "--set", "authService.accessToken.class=eddsa"), "needs authService.accessToken.issuerURL"},
		"eddsa http issuer":       {phaseArgs(2, "--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL=http://auth.example"), "needs authService.accessToken.issuerURL"},
		"eddsa trailing slash":    {phaseArgs(2, "--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL=https://auth.example/"), "needs authService.accessToken.issuerURL"},
		"eddsa upper-case host":   {phaseArgs(2, "--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL=https://Auth.example"), "needs authService.accessToken.issuerURL"},
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
		"phase 0": {0, nil}, "phase 1": {1, nil}, "phase 2": {2, nil}, "phase 3": {3, nil}, "phase 4": {4, nil},
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
			if wantUsers := v.phase >= 4; cfg.UsersOwned != wantUsers || goUsersOwned != wantUsers || cfg.OrgsOwned {
				t.Errorf("%s: owned switches auth-service users=%v orgs=%v, go-api users=%v; want users=%v and orgs=false", name, cfg.UsersOwned, cfg.OrgsOwned, goUsersOwned, wantUsers)
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

// From phase 2 go-api carries every AUTH_PROXY_* flag as an explicit "true" or "false" value: an env entry beats a
// same-named variable from goApi.envFrom, so no envFrom source can forward a route the phase does not move.
// Phases 0 and 1 render none (the prod render stays byte-identical).
func TestGoAPIAuthProxyFlagsAreExplicitFromPhase2(t *testing.T) {
	all := []string{"AUTH_PROXY_SESSIONS", "AUTH_PROXY_LOCAL_LOGIN", "AUTH_PROXY_CREDENTIALS", "AUTH_PROXY_SOCIAL", "AUTH_PROXY_OIDC", "AUTH_PROXY_SAML"}
	on := map[int]map[string]bool{
		2: {"AUTH_PROXY_SESSIONS": true},
		3: {"AUTH_PROXY_SESSIONS": true, "AUTH_PROXY_LOCAL_LOGIN": true, "AUTH_PROXY_SOCIAL": true},
	}
	for phase := 0; phase <= 3; phase++ {
		goAPI := authDoc(renderPhase(t, phase), "Deployment", "a-dev-health-go-api")
		for _, name := range all {
			value, set := containerEnvValue(t, goAPI, name)
			if phase < 2 {
				if set {
					t.Errorf("phase %d renders %s=%q; phases 0 and 1 render no AUTH_PROXY_* flag", phase, name, value)
				}
				continue
			}
			want := "false"
			if on[phase][name] {
				want = "true"
			}
			if !set || value != want {
				t.Errorf("phase %d: %s = %q (set %v), want the explicit value %q", phase, name, value, set, want)
			}
		}
	}
}

// Every env name the chart renders for auth-service (Secret references and phase-derived settings) is refused in
// authService.extraEnv, in any case: a later entry with the same name would shadow the rendered one, a Secret
// reference included, and put a literal in the manifest.
func TestAuthServiceExtraEnvRefusesEveryRenderedName(t *testing.T) {
	configs := map[string][]string{
		"phase 3 with link routes":  phaseArgs(3, "--set", "authService.socialLinkRoutes=true"),
		"phase 2 with eddsa tokens": phaseArgs(2, "--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL=https://auth.example"),
	}
	names := map[string][]string{}
	for label, args := range configs {
		docs, out, err := renderAuth(t, args...)
		if err != nil {
			t.Fatalf("%s: render failed: %v\n%.400s", label, err, out)
		}
		service := authDoc(docs, "Deployment", "a-dev-health-auth-service")
		for _, container := range podContainers(service) {
			env, _ := at(container, "env").([]any)
			for _, entry := range env {
				name, _ := at(entry, "name").(string)
				names[name] = args
			}
		}
	}
	// AUTH_USERS_OWNED renders from phase 4 and AUTH_ORGS_OWNED from phase 5 (refused at render), so the renders above show neither.
	for _, name := range []string{"AUTH_USERS_OWNED", "AUTH_ORGS_OWNED"} {
		names[name] = configs["phase 3 with link routes"]
	}
	if len(names) < 18 {
		t.Fatalf("only %d auth-service env names rendered: %v", len(names), names)
	}
	for name, args := range names {
		for _, spelling := range []string{name, strings.ToLower(name)} {
			_, out, err := renderAuth(t, append(append([]string{}, args...),
				"--set-json", fmt.Sprintf(`authService.extraEnv=[{"name":%q,"value":"literal"}]`, spelling))...)
			if err == nil {
				t.Errorf("authService.extraEnv %s rendered: a literal would shadow the chart's %s", spelling, name)
				continue
			}
			if !strings.Contains(out, "authService.extraEnv must not set") {
				t.Errorf("authService.extraEnv %s: the refusal does not name extraEnv: %.300s", spelling, out)
			}
		}
	}
}

// authService.extraEnv refuses a literal value under any name that looks like a secret, rendered by the chart or not,
// in any case; a valueFrom entry (secretKeyRef) under such a name stays allowed.
func TestAuthServiceExtraEnvRefusesSecretLikeLiterals(t *testing.T) {
	refused := map[string]string{
		"collision with a rendered name": `[{"name":"SOCIAL_GITHUB_CLIENT_SECRET","value":"literal"}]`,
		"unknown social client secret":   `[{"name":"SOCIAL_X_CLIENT_SECRET","value":"literal"}]`,
		"mixed case":                     `[{"name":"Partner_Api_Token","value":"literal"}]`,
		"password":                       `[{"name":"SMTP_PASSWORD","value":"literal"}]`,
		"key":                            `[{"name":"WEBHOOK_SIGNING_KEY","value":"literal"}]`,
		"dsn":                            `[{"name":"REPORTING_DSN","value":"literal"}]`,
		"credentials":                    `[{"name":"partner_credentials","value":"literal"}]`,
		"empty literal":                  `[{"name":"OTHER_SECRET","value":""}]`,
	}
	for name, env := range refused {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, phaseArgs(3, "--set-json", "authService.extraEnv="+env)...)
			if err == nil {
				t.Fatalf("%s rendered a literal under a secret-like name", env)
			}
			if !strings.Contains(out, "authService.extraEnv must not set") {
				t.Fatalf("the refusal does not name extraEnv: %.300s", out)
			}
		})
	}
	control := `[{"name":"PARTNER_API_TOKEN","valueFrom":{"secretKeyRef":{"name":"partner","key":"token"}}},{"name":"LOG_FORMAT","value":"json"}]`
	docs, out, err := renderAuth(t, phaseArgs(3, "--set-json", "authService.extraEnv="+control)...)
	if err != nil {
		t.Fatalf("a secretKeyRef under a secret-like name and a plain literal were refused: %v\n%.400s", err, out)
	}
	service := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	if ref := envEntryValue(func() any {
		for _, container := range podContainers(service) {
			env, _ := at(container, "env").([]any)
			for _, entry := range env {
				if at(entry, "name") == "PARTNER_API_TOKEN" {
					return entry
				}
			}
		}
		return nil
	}()); ref != "secret:partner/token" {
		t.Errorf("PARTNER_API_TOKEN = %s, want the Secret reference", ref)
	}
}

// An empty access-token class and issuer are the defaults: they render, and set nothing.
func TestAuthPhaseEmptyTokenClassIsTheDefault(t *testing.T) {
	docs := renderPhase(t, 2, "--set-string", "authService.accessToken.class=", "--set-string", "authService.accessToken.issuerURL=")
	for _, env := range []string{"AUTH_ACCESS_TOKEN_CLASS", "AUTH_TOKEN_ISSUER_URL", "AUTH_JWT_AUDIENCE"} {
		if value, set := containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-auth-service"), env); set {
			t.Errorf("an empty class renders %s=%q", env, value)
		}
	}
}

// go-api: every env name the chart renders for go-api is refused in goApi.extraEnv, in any case (a later entry would
// shadow the rendered one, a Secret reference included).
func TestGoAPIExtraEnvRefusesEveryRenderedName(t *testing.T) {
	configs := map[string][]string{
		"phase 3":                   phaseArgs(3),
		"phase 2 with eddsa tokens": phaseArgs(2, "--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL=https://auth.example"),
	}
	names := map[string][]string{}
	for label, args := range configs {
		docs, out, err := renderAuth(t, args...)
		if err != nil {
			t.Fatalf("%s: render failed: %v\n%.400s", label, err, out)
		}
		for _, container := range podContainers(authDoc(docs, "Deployment", "a-dev-health-go-api")) {
			env, _ := at(container, "env").([]any)
			for _, entry := range env {
				name, _ := at(entry, "name").(string)
				names[name] = args
			}
		}
	}
	// AUTH_USERS_OWNED renders only from phase 4, which the renders above do not use.
	names["AUTH_USERS_OWNED"] = configs["phase 3"]
	if len(names) < 17 {
		t.Fatalf("only %d go-api env names rendered: %v", len(names), names)
	}
	for name, args := range names {
		for _, spelling := range []string{name, strings.ToLower(name)} {
			_, out, err := renderAuth(t, append(append([]string{}, args...),
				"--set-json", fmt.Sprintf(`goApi.extraEnv=[{"name":%q,"value":"literal"}]`, spelling))...)
			if err == nil {
				t.Errorf("goApi.extraEnv %s rendered: a literal would shadow the chart's %s", spelling, name)
				continue
			}
			if !strings.Contains(out, "goApi.extraEnv must not set") {
				t.Errorf("goApi.extraEnv %s: the refusal does not name extraEnv: %.300s", spelling, out)
			}
		}
	}
}

// go-api: a literal value under any secret-like name is refused in goApi.extraEnv, in any case; valueFrom stays allowed.
func TestGoAPIExtraEnvRefusesSecretLikeLiterals(t *testing.T) {
	refused := map[string]string{
		"collision JWT_SECRET_KEY":           `[{"name":"JWT_SECRET_KEY","value":"literal"}]`,
		"collision API_DATABASE_URI":         `[{"name":"API_DATABASE_URI","value":"literal"}]`,
		"collision AUTH_SESSION_ISSUE_TOKEN": `[{"name":"AUTH_SESSION_ISSUE_TOKEN","value":"literal"}]`,
		"unknown social client secret":       `[{"name":"SOCIAL_X_CLIENT_SECRET","value":"literal"}]`,
		"mixed case":                         `[{"name":"Partner_Api_Token","value":"literal"}]`,
		"password":                           `[{"name":"SMTP_PASSWORD","value":"literal"}]`,
		"dsn":                                `[{"name":"REPORTING_DSN","value":"literal"}]`,
		"credentials":                        `[{"name":"partner_credentials","value":"literal"}]`,
	}
	for name, env := range refused {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, phaseArgs(2, "--set-json", "goApi.extraEnv="+env)...)
			if err == nil {
				t.Fatalf("%s rendered a literal under a secret-like or rendered name", env)
			}
			if !strings.Contains(out, "goApi.extraEnv must not set") {
				t.Fatalf("the refusal does not name extraEnv: %.300s", out)
			}
		})
	}
	control := `[{"name":"PARTNER_API_TOKEN","valueFrom":{"secretKeyRef":{"name":"partner","key":"token"}}},{"name":"LOG_FORMAT","value":"json"}]`
	docs, out, err := renderAuth(t, phaseArgs(2, "--set-json", "goApi.extraEnv="+control)...)
	if err != nil {
		t.Fatalf("a secretKeyRef under a secret-like name and a plain literal were refused: %v\n%.400s", err, out)
	}
	found := false
	for _, container := range podContainers(authDoc(docs, "Deployment", "a-dev-health-go-api")) {
		env, _ := at(container, "env").([]any)
		for _, entry := range env {
			if at(entry, "name") == "PARTNER_API_TOKEN" {
				found = envEntryValue(entry) == "secret:partner/token"
			}
		}
	}
	if !found {
		t.Error("PARTNER_API_TOKEN did not render as the Secret reference")
	}
}

// The phase 2 preconditions (both session readers, the successor key) hold whether or not go-api renders: query-api
// and the River hooks carry AUTH_PROXY_SESSIONS from phase 2 even with go-api off.
func TestAuthPhase2PreconditionsHoldWithoutGoAPI(t *testing.T) {
	base := append(append([]string{}, authBase...),
		"--set", "goApi.enabled=false", "--set", "queryApi.enabled=true", "--set", "authService.phase=2",
		"--set", "authService.valkey.secretName=auth-valkey",
		"--set-json", `authService.networkPolicy.egress.valkey.to=[{"podSelector":{"matchLabels":{"app":"auth-valkey"}}}]`)
	base = append(base, roleEnv("devhealth_query")...)
	readers := []string{"--set", "authService.migrate.sessionReaders.goApi=devhealth_api", "--set", "authService.migrate.sessionReaders.queryApi=devhealth_query"}
	cases := map[string]struct {
		args []string
		want string
	}{
		"no readers":       {append(append([]string{}, base...), sessionSuccessor...), "authService.migrate.sessionReaders.goApi"},
		"no query reader":  {append(append(append([]string{}, base...), sessionSuccessor...), "--set", "authService.migrate.sessionReaders.goApi=devhealth_api"), "authService.migrate.sessionReaders.queryApi"},
		"no successor key": {append(append([]string{}, base...), readers...), "authService.sessionKeys.successor.secretName"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, tc.args...)
			if err == nil {
				t.Fatal("phase 2 rendered without its preconditions while go-api is off")
			}
			if !strings.Contains(out, "authService.phase >= 2 (sessions) needs") || !strings.Contains(out, tc.want) {
				t.Fatalf("the refusal does not name %s: %.400s", tc.want, out)
			}
		})
	}
	docs, out, err := renderAuth(t, append(append(append([]string{}, base...), readers...), sessionSuccessor...)...)
	if err != nil {
		t.Fatalf("phase 2 with every precondition and go-api off was refused: %v\n%.400s", err, out)
	}
	if v, _ := containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-query-api"), "AUTH_PROXY_SESSIONS"); v != "true" {
		t.Errorf("query-api AUTH_PROXY_SESSIONS = %q, want true", v)
	}
}

// The session Secret keys must be valid Kubernetes Secret keys wherever the phase needs them: the successor key from
// phase 2, the login lockout key from phase 3.
func TestAuthPhaseSessionSecretKeysAreValidKeys(t *testing.T) {
	for _, tc := range []struct {
		phase int
		key   string
	}{{2, "authService.sessionKeys.successor.secretKey"}, {3, "authService.sessionKeys.loginLockout.secretKey"}} {
		for _, bad := range []string{"", "a/b", "a b"} {
			t.Run(fmt.Sprintf("%s=%q", tc.key, bad), func(t *testing.T) {
				_, out, err := renderAuth(t, phaseArgs(tc.phase, "--set-string", tc.key+"="+bad)...)
				if err == nil {
					t.Fatalf("%s=%q rendered", tc.key, bad)
				}
				if !strings.Contains(out, tc.key) {
					t.Fatalf("the refusal does not name %s: %.400s", tc.key, out)
				}
			})
		}
		docs := renderPhase(t, tc.phase, "--set-string", tc.key+"=Key-1.x_y")
		env := "AUTH_SESSION_SUCCESSOR_KEY"
		if tc.phase == 3 {
			env = "AUTH_LOGIN_LOCKOUT_KEY"
		}
		var ref string
		for _, container := range podContainers(authDoc(docs, "Deployment", "a-dev-health-auth-service")) {
			entries, _ := at(container, "env").([]any)
			for _, entry := range entries {
				if at(entry, "name") == env {
					ref = envEntryValue(entry)
				}
			}
		}
		if !strings.HasSuffix(ref, "/Key-1.x_y") {
			t.Errorf("%s with a valid key renders %q", env, ref)
		}
	}
}

// The phase 2 preconditions hold whatever each enable flag says: every service flag off in turn, the render still
// refuses phase 2 without the reader grants or without the successor key.
func TestAuthPhase2PreconditionsHoldWithEachServiceFlagOff(t *testing.T) {
	// authService.enabled is not a flag any more: a hand-set value is refused (TestAuthPhaseRefusesHandSetFlags).
	for _, flag := range []string{"goApi.enabled", "queryApi.enabled", "web.enabled", "migrations.hook.enabled",
		"migrations.hook.riverMigrate.enabled", "authService.migrate.enabled", "goApi.enabled,queryApi.enabled"} {
		full := phaseArgs(2)
		for _, f := range strings.Split(flag, ",") {
			full = append(full, "--set", f+"=false")
		}
		for name, drop := range map[string][]string{
			"no readers":       {"authService.migrate.sessionReaders.goApi=", "authService.migrate.sessionReaders.queryApi="},
			"no successor key": {"authService.sessionKeys.successor.secretName="},
		} {
			t.Run(flag+"=false/"+name, func(t *testing.T) {
				args := append([]string{}, full...)
				for _, d := range drop {
					args = append(args, "--set", d)
				}
				if strings.Contains(name, "readers") {
					args = append(args, "--set-json", `queryApi.extraEnv=[]`)
				}
				_, out, err := renderAuth(t, args...)
				if err == nil {
					t.Fatalf("phase 2 with %s=false rendered without its preconditions (%s)", flag, name)
				}
				if !strings.Contains(out, "authService.phase >= 2 (sessions) needs") {
					t.Fatalf("the refusal is not the phase 2 precondition: %.400s", out)
				}
			})
		}
		t.Run(flag+"=false/all set", func(t *testing.T) {
			if _, out, err := renderAuth(t, full...); err != nil {
				t.Fatalf("phase 2 with %s=false and every precondition was refused: %v\n%.400s", flag, err, out)
			}
		})
	}
}

// Class rule: every Secret key the auth values read is a valid Kubernetes Secret key, in every render.
func TestAuthSecretKeysAreValidSecretKeys(t *testing.T) {
	keys := []string{"authService.database.secretKey", "authService.migrate.secretKey", "authService.signingKey.secretKey",
		"authService.valkey.secretKey", "authService.sessionKeys.successor.secretKey",
		"authService.sessionKeys.loginLockout.secretKey", "goApi.sessionIssue.secretKey"}
	for _, key := range keys {
		for _, bad := range []string{"", "a b", "a/b"} {
			t.Run(fmt.Sprintf("%s=%q", key, bad), func(t *testing.T) {
				_, out, err := renderAuth(t, phaseArgs(3, "--set-string", key+"="+bad)...)
				if err == nil {
					t.Fatalf("%s=%q rendered", key, bad)
				}
				// goApi.sessionIssue is also bounded by values.schema.json, whose refusal names the JSON path.
				if !strings.Contains(out, key) && !strings.Contains(out, "/"+strings.ReplaceAll(key, ".", "/")) {
					t.Fatalf("the refusal does not name %s: %.400s", key, out)
				}
			})
		}
		t.Run(key+" valid", func(t *testing.T) {
			if _, out, err := renderAuth(t, phaseArgs(3, "--set-string", key+"=Key-1.x_y")...); err != nil {
				t.Fatalf("%s=Key-1.x_y refused: %v\n%.400s", key, err, out)
			}
		})
	}
	for _, bad := range []string{"a b", "a/b"} {
		keys := strings.Replace(twoKeys, `"secretName":"key-b","secretKey":"signing-key.pem"`, `"secretName":"key-b","secretKey":"`+bad+`"`, 1)
		if _, out, err := renderAuth(t, keySetArgs(keys)...); err == nil || !strings.Contains(out, "authService.keySet.keys[1].secretKey") {
			t.Errorf("keySet secretKey %q: err=%v out=%.300s", bad, err, out)
		}
	}
	if _, out, err := renderAuth(t, keySetArgs(twoKeys)...); err != nil {
		t.Errorf("a valid keySet was refused: %v\n%.300s", err, out)
	}
}
