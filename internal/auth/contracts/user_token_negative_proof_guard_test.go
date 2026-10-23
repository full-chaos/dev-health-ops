package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The negative proof (CHAOS-10636): every protected endpoint-profile row that does not take a user access
// token as a credential must have an executed test showing that a user token opens nothing, or an
// exemption with a reason. The rows are DERIVED from the three profiles, so a new such row without a proof
// or an exemption turns this test red (it fails closed); a proof naming a test that does not exist, or a row
// that no longer exists, fails too. The counts are pinned.

// userAccessClasses are the credential classes that ARE user access tokens (or a session standing for one):
// a row accepting any of them is a user surface, outside this proof.
var userAccessClasses = map[string]bool{
	"ops_access_token_hs256": true, "user_access_token": true, "impersonation_session": true, "auth_js_browser_session": true,
}

const (
	proofPush      = "internal/api/externalingest/user_tokens_refused_test.go"
	proofLegacy    = "internal/api/legacyingest/user_tokens_refused_test.go"
	proofACR       = "internal/apiservice/acr/user_tokens_refused_test.go"
	proofStripe    = "internal/api/billing/user_tokens_refused_test.go"
	proofWebhooks  = "internal/api/webhookintake/user_tokens_refused_test.go"
	proofMCP       = "internal/queryapi/server/user_tokens_refused_test.go"
	proofIssue     = "internal/auth/authapi/issue_user_tokens_test.go"
	pushRoutes     = "TestExternalPushAuthenticatedRoutesRefuseUserTokens"
	legacyTest     = "TestLegacyIngestRefusesUserTokens"
	acrTest        = "TestInternalACRRoutesRefuseUserTokens"
	stripeTest     = "TestStripeWebhookRefusesUserTokens"
	webhookTest    = "TestWebhookRoutesRefuseUserTokens"
	mcpTest        = "TestMCPRefusesUserTokensAsAuthorization"
	buildInfoTest  = "TestBuildInfoRefusesUserTokens"
	issueTest      = "TestIssueSessionRefusesUserTokens"
	exemptRefresh  = "the route's credential IS the user's own refresh credential, presented by design; it is a user flow, outside this leaf (workload, External Push, webhook paths)"
	exemptPublic   = "pending the schema-route ruling: the handler performs no credential check (public schema discovery, rate limited by address only); a user token changes nothing, which TestExternalPushSchemaDiscoveryIgnoresUserTokens proves (answer byte-identical to the anonymous one, no lookup)"
	exemptExchange = "the credential is the social provider's own token in the request body, sent to the provider; no bearer of ours is read; a user flow, outside this leaf"
)

type negativeProof struct {
	file, test string // proven by this test in this file
	exempt     string // or exempt, with the reason
}

var negativeProofs = map[string]negativeProof{
	"go-api|GET /api/v1/external-ingest/availability [dev-health-ops-api @api]":               {file: proofPush, test: pushRoutes},
	"go-api|GET /api/v1/external-ingest/batches [dev-health-ops-api @api]":                    {file: proofPush, test: pushRoutes},
	"go-api|GET /api/v1/external-ingest/batches/{ingestion_id} [dev-health-ops-api @api]":     {file: proofPush, test: pushRoutes},
	"go-api|POST /api/v1/external-ingest/batches [dev-health-ops-api @api]":                   {file: proofPush, test: pushRoutes},
	"go-api|POST /api/v1/external-ingest/validate [dev-health-ops-api @api]":                  {file: proofPush, test: pushRoutes},
	"go-api|GET /api/v1/external-ingest/schemas [dev-health-ops-api @api]":                    {exempt: exemptPublic},
	"go-api|GET /api/v1/external-ingest/schemas/{schema_version} [dev-health-ops-api @api]":   {exempt: exemptPublic},
	"go-api|GET /api/v1/internal/acr/entitlements/{org_id} [dev-health-ops-api @internal]":    {file: proofACR, test: acrTest},
	"go-api|GET /api/v1/internal/acr/health [dev-health-ops-api @internal]":                   {file: proofACR, test: acrTest},
	"go-api|GET /api/v1/internal/acr/orgs/{org_id}/llm-config [dev-health-ops-api @internal]": {file: proofACR, test: acrTest},
	"go-api|POST /api/v1/billing/webhooks/stripe [dev-health-ops-api @api]":                   {file: proofStripe, test: stripeTest},
	"go-api|POST /api/v1/billing/webhooks/stripe [dev-health-ops-billing-edge @billing-edge]": {file: proofStripe, test: stripeTest},
	"go-api|POST /api/v1/ingest/commits [dev-health-ops-api @api]":                            {file: proofLegacy, test: legacyTest},
	"go-api|POST /api/v1/ingest/deployments [dev-health-ops-api @api]":                        {file: proofLegacy, test: legacyTest},
	"go-api|POST /api/v1/ingest/incidents [dev-health-ops-api @api]":                          {file: proofLegacy, test: legacyTest},
	"go-api|POST /api/v1/ingest/pull-requests [dev-health-ops-api @api]":                      {file: proofLegacy, test: legacyTest},
	"go-api|POST /api/v1/ingest/telemetry [dev-health-ops-api @api]":                          {file: proofLegacy, test: legacyTest},
	"go-api|POST /api/v1/ingest/work-items [dev-health-ops-api @api]":                         {file: proofLegacy, test: legacyTest},
	"go-api|POST /api/v1/webhooks/github [dev-health-ops-api @api]":                           {file: proofWebhooks, test: webhookTest},
	"go-api|POST /api/v1/webhooks/pagerduty/{binding_id} [dev-health-ops-api @api]":           {file: proofWebhooks, test: webhookTest},
	"query-api|GET /buildinfo [dev-health-query-api @query,internal]":                         {file: proofMCP, test: buildInfoTest},
	"query-api|POST /query [dev-health-query-api @mcp]":                                       {file: proofMCP, test: mcpTest},
	"auth-service|POST /internal/v1/sessions [dev-health-auth-service @api]":                  {file: proofIssue, test: issueTest},
	"auth-service|POST /v1/token/refresh [dev-health-auth-service @api]":                      {exempt: exemptRefresh},
	"auth-service|POST /api/v1/auth/refresh [dev-health-auth-service @api]":                   {exempt: exemptRefresh},
	"auth-service|POST /api/v1/auth/logout [dev-health-auth-service @api]":                    {exempt: exemptRefresh},
	"auth-service|POST /api/v1/auth/social-login [dev-health-auth-service @api]":              {exempt: exemptExchange},
}

// derivedRows reads the three profiles and returns the key of every protected row that accepts no user
// access class.
func derivedRows(t *testing.T) map[string]bool {
	t.Helper()
	root := testRoot(t)
	rows := map[string]bool{}
	for _, service := range []string{"go-api", "query-api", "auth-service"} {
		raw, err := os.ReadFile(filepath.Join(ContractsDir(root), "endpoint-profiles."+service+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var profile struct {
			Rows []struct {
				ID                string   `json:"id"`
				Classification    string   `json:"classification"`
				AcceptedClassesIn []string `json:"accepted_credential_classes"`
			} `json:"rows"`
		}
		if err := json.Unmarshal(raw, &profile); err != nil {
			t.Fatal(err)
		}
		for _, row := range profile.Rows {
			if row.Classification != "protected" {
				continue
			}
			user := false
			for _, class := range row.AcceptedClassesIn {
				user = user || userAccessClasses[class]
			}
			if !user {
				rows[service+"|"+row.ID] = true
			}
		}
	}
	return rows
}

func TestEveryNonUserProtectedRowHasANegativeProofOrAReasonedExemption(t *testing.T) {
	derived := derivedRows(t)
	var problems []string
	for key := range derived {
		if _, ok := negativeProofs[key]; !ok {
			problems = append(problems, "no proof and no exemption: "+key)
		}
	}
	for key := range negativeProofs {
		if !derived[key] {
			problems = append(problems, "names a row that is not a protected non-user-token row of the profiles: "+key)
		}
	}
	sort.Strings(problems)
	if len(problems) > 0 {
		t.Fatalf("%d problem(s):\n%s", len(problems), strings.Join(problems, "\n"))
	}
}

func TestEveryNegativeProofNamesARealTestThatSendsUserTokens(t *testing.T) {
	root := testRoot(t)
	var problems []string
	for key, proof := range negativeProofs {
		if proof.exempt != "" {
			if proof.file != "" || proof.test != "" || len(strings.TrimSpace(proof.exempt)) < 40 {
				problems = append(problems, "bad exemption (needs a reason of substance and no proof): "+key)
			}
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, proof.file))
		if err != nil {
			problems = append(problems, key+": "+err.Error())
			continue
		}
		text := string(raw)
		start := strings.Index(text, "func "+proof.test+"(")
		if start < 0 {
			problems = append(problems, key+": no func "+proof.test+" in "+proof.file)
			continue
		}
		body := text[start:]
		if next := strings.Index(body[1:], "\nfunc "); next >= 0 {
			body = body[:next+1]
		}
		if !strings.Contains(body, "All(t)") && !strings.Contains(body, "userTokensWithIssuer(t)") {
			problems = append(problems, key+": "+proof.test+" does not mint the user tokens (usertokens All)")
		}
		// The minted tokens must be USED: assigned, then at least twice more (sent, and checked for an
		// echo). A body that mints them and drops them proves nothing.
		if !strings.Contains(body, "tokens :=") || strings.Count(body, "tokens") < 3 {
			problems = append(problems, key+": "+proof.test+" mints the user tokens but does not send them (no use of the tokens it minted)")
		}
		if !strings.Contains(text, "NoLeak(") {
			problems = append(problems, key+": "+proof.file+" does not check that a refusal echoes no token")
		}
	}
	sort.Strings(problems)
	if len(problems) > 0 {
		t.Fatalf("%d problem(s):\n%s", len(problems), strings.Join(problems, "\n"))
	}
}

// The census: 29 derived rows = 23 proven + 6 exempt. A change to either count is a decision, made here.
func TestNegativeProofCensus(t *testing.T) {
	proven, exempt := 0, 0
	for _, proof := range negativeProofs {
		if proof.exempt != "" {
			exempt++
		} else {
			proven++
		}
	}
	if len(derivedRows(t)) != 27 || proven != 21 || exempt != 6 {
		t.Fatalf("derived %d rows, %d proven, %d exempt; want 27, 21, 6", len(derivedRows(t)), proven, exempt)
	}
}
