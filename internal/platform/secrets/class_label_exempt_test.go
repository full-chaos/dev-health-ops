package secrets

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// authconfigSource is the auth-service config file that declares every AUTH_* setting name.
const authconfigSource = "../../auth/authconfig/config.go"

// Non-secret auth settings whose names match secretNamePattern (CHAOS-12804), each with its consumer.
//
//	AUTH_ACCESS_TOKEN_CLASS    -> authconfig accessTokenClass: a switch over hs256_compat / eddsa.
//	AUTH_ACCESS_TOKEN_LIFETIME -> authconfig accessTokenLifetime: time.ParseDuration, whole minutes 5m..60m.
var authNonSecretByName = map[string]string{
	"AUTH_ACCESS_TOKEN_CLASS":    "accessTokenClass",
	"AUTH_ACCESS_TOKEN_LIFETIME": "accessTokenLifetime",
}

// Every other auth setting that matches secretNamePattern stays registered as a secret or is a
// known candidate left alone on purpose (a URL or a file path; allowing it widens the scope).
var authSecretLooking = []string{
	"AUTH_JWT_SECRET_KEY", "AUTH_INVITE_TOKEN_SECRET", "AUTH_INVITE_TOKEN_SECRET_PREVIOUS",
	"AUTH_SIGNING_KEY", "AUTH_SESSION_SUCCESSOR_KEY", "AUTH_LOGIN_LOCKOUT_KEY", "AUTH_RATE_LIMIT_KEY",
	"AUTH_SSO_STATE_KEY", "AUTH_LOADTEST_WORKLOAD_TOKEN", "SMTP_PASSWORD", "SENTINEL_PASSWORD",
	"SOCIAL_GITHUB_CLIENT_SECRET", "SOCIAL_GITLAB_CLIENT_SECRET", "SOCIAL_GOOGLE_CLIENT_SECRET",
	"SETTINGS_ENCRYPTION_KEY", "RESEND_API_KEY", "EMAIL_API_KEY", "USER_ACCESS_TOKEN",
}
var authLeftAlone = []string{"AUTH_TOKEN_ISSUER_URL", "AUTH_WORKLOAD_EXCHANGE_REVIEWER_TOKEN_FILE"}

func envConstsOf(t *testing.T) (map[string]string, map[string]*ast.FuncDecl) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), authconfigSource, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	consts := map[string]string{}
	funcs := map[string]*ast.FuncDecl{}
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			funcs[d.Name.Name] = d
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, n := range vs.Names {
					if i >= len(vs.Values) || !strings.HasPrefix(n.Name, "Env") {
						continue
					}
					if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						v, _ := strconv.Unquote(lit.Value)
						consts[n.Name] = v
					}
				}
			}
		}
	}
	return consts, funcs
}

func TestAccessTokenClassAndLifetimeAreNotRegisteredAsSecrets(t *testing.T) {
	for name, value := range map[string]string{
		"AUTH_ACCESS_TOKEN_CLASS": "eddsa", "AUTH_ACCESS_TOKEN_LIFETIME": "60m",
	} {
		if LooksSecret(name) {
			t.Errorf("%s: LooksSecret must be false", name)
		}
		if out := lookupWarnings(t, name, value); out != "" {
			t.Errorf("%s: want no warning, got: %s", name, out)
		}
	}
}

func TestNamesNextToTheAccessTokenSettingsStillRegisterAndWarn(t *testing.T) {
	for _, name := range []string{
		"AUTH_ACCESS_TOKEN", "AUTH_ACCESS_TOKEN_SECRET", "AUTH_ACCESS_TOKEN_CLASS_KEY", "AUTH_ACCESS_TOKEN_CLASS_X",
		"X_AUTH_ACCESS_TOKEN_CLASS", "AUTH_ACCESS_TOKEN_LIFETIME_X", "X_AUTH_ACCESS_TOKEN_LIFETIME",
		"auth_access_token_class", "AUTH_ACCESS_TOKEN_CLASS ", "AUTH_REFRESH_TOKEN_CLASS",
	} {
		if !LooksSecret(name) {
			t.Errorf("%s must still look secret", name)
		}
		out := lookupWarnings(t, name, "eddsa")
		if strings.Count(out, "secret_shorter_than_minimum_not_redactable_by_value") != 1 {
			t.Errorf("%s: want one short-secret warning, got: %s", name, out)
		}
	}
}

func TestEveryCredentialShapedAuthKeyStaysRedactedByValue(t *testing.T) {
	value := "a-long-enough-secret-value-0123"
	for _, name := range authSecretLooking {
		if !LooksSecret(name) {
			t.Errorf("%s must look secret", name)
			continue
		}
		if out := lookupWarnings(t, name, value); out != "" {
			t.Errorf("%s: a long secret must not warn: %s", name, out)
		}
		if got := RedactRegistered("x " + value + " y"); strings.Contains(got, value) {
			t.Errorf("%s: a registered secret leaked: %q", name, got)
		}
	}
	for _, name := range authLeftAlone {
		if !LooksSecret(name) {
			t.Errorf("%s is not allowed by CHAOS-12804 and must still look secret", name)
		}
	}
}

// Census: every authconfig Env name that matches secretNamePattern is either one of the two exact
// non-secret names (each backed by its consumer), or listed as secret-looking or left alone.
func TestAuthConfigNamesMatchingTheSecretPatternAreAllClassified(t *testing.T) {
	consts, funcs := envConstsOf(t)
	known := map[string]bool{}
	for n := range authNonSecretByName {
		known[n] = true
	}
	for _, n := range authSecretLooking {
		known[n] = true
	}
	for _, n := range authLeftAlone {
		known[n] = true
	}
	seen := map[string]bool{}
	for constName, value := range consts {
		seen[value] = true
		if !secretNamePattern.MatchString(value) {
			continue
		}
		if !known[value] && !strings.HasSuffix(value, "_FILE") {
			t.Errorf("%s (%s) matches the secret name pattern and is not classified: add it to authSecretLooking, authLeftAlone or authNonSecretByName", constName, value)
		}
	}
	for name, consumer := range authNonSecretByName {
		if !seen[name] {
			t.Errorf("%s is not declared in %s", name, authconfigSource)
		}
		if !nonSecretNames[name] {
			t.Errorf("%s is classified non-secret but is not in nonSecretNames", name)
		}
		fn := funcs[consumer]
		if fn == nil {
			t.Errorf("%s: consumer %s not found", name, consumer)
			continue
		}
		var parsesDuration, switches bool
		ast.Inspect(fn, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.SelectorExpr:
				if id, ok := x.X.(*ast.Ident); ok && id.Name == "time" && x.Sel.Name == "ParseDuration" {
					parsesDuration = true
				}
			case *ast.SwitchStmt:
				switches = true
			}
			return true
		})
		if consumer == "accessTokenLifetime" && !parsesDuration {
			t.Errorf("%s: consumer %s must parse a duration", name, consumer)
		}
		if consumer == "accessTokenClass" && !switches {
			t.Errorf("%s: consumer %s must switch over class labels", name, consumer)
		}
	}
	for name := range nonSecretNames {
		if strings.HasPrefix(name, "AUTH_ACCESS_TOKEN") {
			if _, ok := authNonSecretByName[name]; !ok {
				t.Errorf("%s is in nonSecretNames with no consumer cell", name)
			}
		}
	}
}
