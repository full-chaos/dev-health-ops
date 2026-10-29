package authruntime

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadexchange"
)

// perRequestFields are the error envelope members two requests never share
// (the request id and the time); the census compares answers without them.
var perRequestFields = regexp.MustCompile(`"(request_id|occurred_at)":"[^"]*"`)

// TestABootstrapCredentialOpensNoAuthRoute is G-36 on auth-service: the Compose
// bootstrap credential is accepted only as the exchange's subject_token. As an
// Authorization bearer it is not a credential anywhere: every route
// auth-service serves (the endpoint-profile probe's set, both exchange
// subjects on) answers a request carrying it exactly as the same request with
// no credential, so a route that needs a bearer answers it with its 401.
func TestABootstrapCredentialOpensNoAuthRoute(t *testing.T) {
	env := map[string]string{
		authconfig.EnvDatabaseURI:        "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:     "/nonexistent",
		authconfig.EnvSigningKeyID:       "census-test",
		authconfig.EnvRateLimitPerSecond: "100000",
		authconfig.EnvRateLimitBurst:     "100000",
		authconfig.EnvAPIAddress:         "127.0.0.1:0",
		authconfig.EnvOperatorAddress:    "127.0.0.1:1",
		authconfig.EnvSocialLinkRoutes:   "true",
		authconfig.EnvUsersOwned:         "true",
		authconfig.EnvTokenIssuerURL:     wellKnownIssuer,
	}
	exchangeEnvironment(t, env)
	api, err := configuredAPI(t, ssoEnvironment(compatEnvironment(env)))
	if err != nil {
		t.Fatal(err)
	}
	value := workload.BootstrapPrefix + "CENSUSb00tstrapVALUE" + strings.Repeat("Q", 43-len("CENSUSb00tstrapVALUE"))
	if !workload.ValidBootstrapShape(value) {
		t.Fatal("the census value is not of the bootstrap shape")
	}
	answer := func(method, path, authorization string) (int, string) {
		req := httptest.NewRequest(method, path, strings.NewReader("{}"))
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, req)
		return rec.Code, perRequestFields.ReplaceAllString(rec.Body.String(), `"$1":""`)
	}
	var keys []string
	for key := range servedAuthRoutes(t) {
		if strings.HasSuffix(key, "@api") {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	checked, unauthorized := 0, 0
	exchanged := false
	for _, key := range keys {
		method, rest, _ := strings.Cut(key, " ")
		path := strings.TrimSuffix(rest, "@api")
		baseCode, baseBody := answer(method, path, "")
		for _, authorization := range []string{"Bearer " + value, "bearer " + value} {
			code, body := answer(method, path, authorization)
			if code != baseCode || body != baseBody {
				t.Errorf("%s with the bootstrap credential as a bearer: %d %q, want the no-credential answer %d %q",
					key, code, body, baseCode, baseBody)
			}
			if strings.Contains(body, value) {
				t.Errorf("%s echoes the bootstrap credential", key)
			}
		}
		if baseCode == http.StatusUnauthorized {
			unauthorized++
		}
		if path == workloadexchange.Pattern {
			exchanged = true
		}
		checked++
	}
	if checked < 30 || unauthorized == 0 || !exchanged {
		t.Fatalf("checked %d api routes, %d answered 401, exchange probed %v: the census measured too little", checked, unauthorized, exchanged)
	}
	t.Logf("BOOTSTRAP_BEARER_CENSUS api routes=%d 401=%d", checked, unauthorized)
}
