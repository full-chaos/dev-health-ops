package apiservice

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

func stateConfig(t *testing.T, jwt, sso, github string) config.Config {
	t.Helper()
	values := map[string]string{"POSTGRES_URI": "postgresql://app:app@db.internal:5432/appdb", "JWT_SECRET_KEY": jwt}
	if sso != "" {
		values["SSO_STATE_SECRET"] = sso
	}
	if github != "" {
		values["GITHUB_APP_STATE_SECRET"] = github
	}
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(name string) (string, bool) {
		v, ok := values[name]
		return v, ok
	}})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return cfg
}

func TestStatePurposeSecretsUseTheirOwnSuccessorKey(t *testing.T) {
	t.Parallel()
	jwt, sso, gh := strings.Repeat("j", 40), strings.Repeat("s", 40), strings.Repeat("g", 40)
	gotSSO, gotGH := statePurposeSecrets(stateConfig(t, jwt, sso, gh))
	if gotSSO != sso {
		t.Fatal("the SSO state key is not SSO_STATE_SECRET when it is set")
	}
	if gotGH.Secret != gh {
		t.Fatal("the GitHub install state key is not GITHUB_APP_STATE_SECRET when it is set")
	}
	if gotSSO == gotGH.Secret || gotSSO == jwt || gotGH.Secret == jwt {
		t.Fatal("two purposes share a key")
	}
}
