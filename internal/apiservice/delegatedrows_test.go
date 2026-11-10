package apiservice

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

const actionsCatalog = "../../contracts/auth/v1/vocabulary/actions.v1.json"

// delegatedRefusal is the body of the Guard's refusal of a delegated bearer.
const delegatedRefusal = `{"detail":"Not allowed in an impersonation session"}`

// guardAnchor is the anchor note of a row whose validator is the Guard.
const guardAnchor = "func (g *Guard) Wrap"

func catalogImpersonation(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(actionsCatalog)
	if err != nil {
		t.Fatal(err)
	}
	var catalog struct {
		Actions []struct {
			Action        string `json:"action"`
			Impersonation string `json:"impersonation"`
		} `json:"actions"`
	}
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, action := range catalog.Actions {
		out[action.Action] = action.Impersonation
	}
	return out
}

// outsideGuardDenyRows are the rows whose handler reads the user bearer
// outside the Guard and applies the Guard's delegated refusal to it
// (Guard.RefuseDelegated; the policy census holds the call). POST
// /api/v1/auth/validate reads a token in the body, writes nothing and carries
// no value.
var outsideGuardDenyRows = map[string]bool{"POST /api/v1/auth/logout": true}

// guardBearerRow reports whether row takes a user bearer at the Guard's
// delegated refusal: the rows the Guard gates, and outsideGuardDenyRows.
func guardBearerRow(row map[string]any) bool {
	if outsideGuardDenyRows[profileOf(row)] {
		return true
	}
	classes, _ := row["accepted_credential_classes"].([]any)
	user := false
	for _, class := range classes {
		if class == "user_access_token" || class == "ops_access_token_hs256" {
			user = true
		}
	}
	validator, _ := row["primary_validator"].(map[string]any)
	anchor, _ := validator["anchor"].(map[string]any)
	return user && anchor["note"] == guardAnchor
}

func profileOf(row map[string]any) string {
	method, _ := row["method"].(string)
	route, _ := row["route"].(string)
	return method + " " + route
}

func underAdmin(route string) bool {
	return route == "/api/v1/admin" || strings.HasPrefix(route, "/api/v1/admin/")
}

// delegatedRule is the deny table's rule (PLAN-10991 G-51, acp): a row admits
// a delegated bearer only outside /api/v1/admin, never with a catalog-denied
// action, and only for a GET or HEAD or a catalog-allowed action. A write with
// no action stays denied: a support need comes as a row with a catalog action.
func delegatedRule(row map[string]any, catalog map[string]string) string {
	method, _ := row["method"].(string)
	route, _ := row["route"].(string)
	action, _ := row["action"].(string)
	verdict := catalog[action]
	read := method == http.MethodGet || method == http.MethodHead
	if !underAdmin(route) && verdict != "denied" && (read || verdict == "allowed") {
		return "allowed"
	}
	return "denied"
}

// TestDelegatedBearerRowsFollowTheRule is the contract of delegated_bearer on
// the go-api rows: every row the Guard's delegated refusal reaches for a user
// bearer has a value, no other row has one, and each value is the rule's. An
// allowed row is never under /api/v1/admin and never carries a catalog-denied
// action; POST /api/v1/auth/logout is denied.
func TestDelegatedBearerRowsFollowTheRule(t *testing.T) {
	catalog := catalogImpersonation(t)
	counts := map[string]int{}
	outside := map[string]string{}
	for _, row := range loadGoAPIRows(t) {
		key := rowKey(t, row)
		if profile := profileOf(row); outsideGuardDenyRows[profile] {
			outside[profile], _ = row["delegated_bearer"].(string)
		}
		value, has := row["delegated_bearer"].(string)
		_, present := row["delegated_bearer"]
		gated := guardBearerRow(row)
		switch {
		case gated && !has:
			t.Errorf("%s: the Guard's delegated refusal reaches this row for a user bearer and it has no delegated_bearer", key)
			continue
		case !gated && present:
			t.Errorf("%s: the Guard's delegated refusal does not reach this row for a user bearer and it has delegated_bearer %v", key, row["delegated_bearer"])
			continue
		case !gated:
			continue
		}
		counts[value]++
		if want := delegatedRule(row, catalog); value != want {
			t.Errorf("%s: delegated_bearer %q, the rule says %q", key, value, want)
		}
		route, _ := row["route"].(string)
		action, _ := row["action"].(string)
		if value == "allowed" && (underAdmin(route) || catalog[action] == "denied") {
			t.Errorf("%s: allowed under /api/v1/admin or with the catalog-denied action %q", key, action)
		}
	}
	if counts["allowed"] == 0 || counts["denied"] < 150 {
		t.Fatalf("delegated_bearer values %v: the table stopped being read", counts)
	}
	for profile := range outsideGuardDenyRows {
		if outside[profile] != "denied" {
			t.Errorf("%s: delegated_bearer %q, want denied (its handler applies the Guard's refusal)", profile, outside[profile])
		}
	}
}

// TestNewProtectionReadsTheDelegatedRows: the authenticator go-api serves
// with reads the embedded rows, so the Guard's deny follows them.
func TestNewProtectionReadsTheDelegatedRows(t *testing.T) {
	protected, err := newProtection(apiTestConfig(), nil, false, AuthProxyConfig{}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	allowed, err := parseDelegatedRows(mustReadGoAPIRows(t))
	if err != nil {
		t.Fatal(err)
	}
	seenAllowed, seenDenied := 0, 0
	for _, row := range loadGoAPIRows(t) {
		value, has := row["delegated_bearer"].(string)
		if !has {
			continue
		}
		method, _ := row["method"].(string)
		route, _ := row["route"].(string)
		profile := method + " " + route
		got := protected.auth.DelegatedAllowed(profile)
		if got != (value == "allowed") || got != allowed[profile] {
			t.Errorf("%s: the served authenticator admits a delegated bearer = %v, the row says %q", profile, got, value)
		}
		if got {
			seenAllowed++
		} else {
			seenDenied++
		}
	}
	if seenAllowed == 0 || seenDenied == 0 {
		t.Fatalf("allowed %d denied %d: the rows were not read", seenAllowed, seenDenied)
	}
	if protected.auth.DelegatedAllowed("GET /no/such/route") {
		t.Fatal("a route with no row admits a delegated bearer")
	}
}

func TestParseDelegatedRowsRefusesABadValue(t *testing.T) {
	for name, raw := range map[string]string{
		"unknown value": `{"rows":[{"service":"dev-health-ops-api","method":"GET","route":"/x","delegated_bearer":"maybe"}]}`,
		"two rows":      `{"rows":[{"service":"dev-health-ops-api","method":"GET","route":"/x","delegated_bearer":"allowed"},{"service":"dev-health-ops-api","method":"GET","route":"/x","delegated_bearer":"denied"}]}`,
		"no method":     `{"rows":[{"service":"dev-health-ops-api","route":"/x","delegated_bearer":"allowed"}]}`,
		"not json":      `{`,
	} {
		if _, err := parseDelegatedRows([]byte(raw)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}

func mustReadGoAPIRows(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(goAPIRows)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestEveryGuardRowAnswersADelegatedBearerByItsRow drives the real mounted
// handler of every row the Guard gates, at its own route template: a
// delegated session's bearer is refused with the deny body on a denied row
// and is not on an allowed one; the same caller without the claim never meets
// the deny.
func TestEveryGuardRowAnswersADelegatedBearerByItsRow(t *testing.T) {
	rows, err := goAPIDelegatedRows()
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]map[string]any{}
	for _, row := range loadGoAPIRows(t) {
		byKey[rowKey(t, row)] = row
	}
	actor := uuid.New()
	plainToken := mountedTokens.MustMint(t, edtokens.Claims{UserID: mountedUser, OrgID: mountedOrg.String()})
	delegatedToken := mountedTokens.MustMint(t, edtokens.Claims{UserID: mountedUser, OrgID: mountedOrg.String(), Actor: &actor})
	params := regexp.MustCompile(`\{([^}]*)\}`)
	probed := map[string]int{}
	for _, route := range mountedRoutesWith(t, func(a *policy.Authenticator) { a.DelegatedRows(rows) }) {
		row := byKey[route.key()]
		value, has := row["delegated_bearer"].(string)
		if row == nil || !has {
			continue
		}
		serve := func(token string) *httptest.ResponseRecorder {
			values := dispatchProbeValues[route.Method+" "+route.Pattern]
			target := params.ReplaceAllStringFunc(route.Pattern, func(match string) string {
				if v, ok := values[match[1:len(match)-1]]; ok {
					return v
				}
				return "x"
			})
			request := httptest.NewRequest(route.Method, target, strings.NewReader(`{}`)).WithContext(context.Background())
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer "+token)
			request.Pattern = route.Method + " " + route.Pattern
			for _, match := range params.FindAllStringSubmatch(route.Pattern, -1) {
				v := "x"
				if got, ok := values[match[1]]; ok {
					v = got
				}
				request.SetPathValue(match[1], v)
			}
			recorder := httptest.NewRecorder()
			route.Handler.ServeHTTP(recorder, request)
			return recorder
		}
		if plain := serve(plainToken); plain.Body.String() == delegatedRefusal {
			t.Errorf("%s: the plain bearer met the delegated deny", route.key())
		}
		got := serve(delegatedToken)
		denied := got.Code == http.StatusForbidden && got.Body.String() == delegatedRefusal
		switch {
		case value == "denied" && !denied && !bodyFirstAnswer(got.Code):
			t.Errorf("%s: denied row answered the delegated bearer %d %s, want 403 %s", route.key(), got.Code, got.Body.String(), delegatedRefusal)
		case value == "allowed" && denied:
			t.Errorf("%s: allowed row refused the delegated bearer", route.key())
		}
		if denied || value == "allowed" {
			probed[value]++
		} else {
			probed["body first"]++
		}
	}
	if probed["denied"] < 150 || probed["allowed"] == 0 {
		t.Fatalf("probed %v: the mounted rows were not reached", probed)
	}
	t.Logf("probed %v", probed)
}

// bodyFirstAnswer is a status a body-first route answers before the Guard
// runs when the probe body does not decode for it.
func bodyFirstAnswer(code int) bool {
	return slices.Contains([]int{http.StatusUnprocessableEntity, http.StatusBadRequest}, code)
}

// TestRoutedAccessVerifyCarriesTheActor: the act claim of a delegated
// session's access token reaches policy as the claims' Actor; a token with no
// act has none.
func TestRoutedAccessVerifyCarriesTheActor(t *testing.T) {
	minter := newEdDSAMinter(t)
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: probeIssuer, Audience: accessTokenAudienceAPI, Keys: minter.keys})
	if err != nil {
		t.Fatal(err)
	}
	const profile = "GET /p"
	validator, err := accessverify.New(accessverify.Config{
		Rows:  []accessverify.Row{{Service: goAPIAccessService, Profile: profile, AcceptsUser: true}},
		EdDSA: verifier, Meter: sdkmetric.NewMeterProvider().Meter("test"), Now: func() time.Time { return probeNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()
	adapter := routedAccessVerify{validator}
	result, err := adapter.Validate(context.Background(), goAPIAccessService, profile, minter.mint(probeUser, map[string]any{"act": map[string]any{"sub": wirePrincipal(actor)}}))
	if err != nil || result.Claims == nil || result.Claims.Actor == nil || *result.Claims.Actor != wirePrincipal(actor) {
		t.Fatalf("delegated token: %+v %v, want Actor %s", result, err, wirePrincipal(actor))
	}
	result, err = adapter.Validate(context.Background(), goAPIAccessService, profile, minter.mint(probeUser, nil))
	if err != nil || result.Claims == nil || result.Claims.Actor != nil {
		t.Fatalf("plain token: %+v %v, want no Actor", result, err)
	}
}
