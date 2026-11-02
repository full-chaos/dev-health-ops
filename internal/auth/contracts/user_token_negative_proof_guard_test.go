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
	proofACR       = "internal/apiservice/acr/user_tokens_refused_test.go"
	proofStripe    = "internal/api/billing/user_tokens_refused_test.go"
	proofWebhooks  = "internal/api/webhookintake/user_tokens_refused_test.go"
	proofMCP       = "internal/queryapi/server/user_tokens_refused_test.go"
	proofIssue     = "internal/auth/authapi/issue_user_tokens_test.go"
	proofRefresh   = "internal/auth/authapi/refresh_user_tokens_test.go"
	proofExchange  = "internal/auth/workloadexchange/user_tokens_refused_test.go"
	proofProvision = "internal/auth/authapi/provisioning_rows_test.go"
	proofSessState = "internal/auth/authapi/session_state_user_tokens_test.go"
	proofDelegated = "internal/auth/authapi/delegated_user_tokens_test.go"
	pushRoutes     = "TestExternalPushAuthenticatedRoutesRefuseUserTokens"
	acrTest        = "TestInternalACRRoutesRefuseUserTokens"
	stripeTest     = "TestStripeWebhookRefusesUserTokens"
	webhookTest    = "TestWebhookRoutesRefuseUserTokens"
	mcpTest        = "TestMCPRefusesUserTokensAsAuthorization"
	buildInfoTest  = "TestBuildInfoRefusesUserTokens"
	issueTest      = "TestIssueSessionRefusesUserTokens"
	refreshTest    = "TestRefreshRowsRefuseUserTokens"
	exchangeTest   = "TestTheExchangeRefusesUserTokens"
	provisionTest  = "TestProvisioningRoutesRefuseUserTokens"
	sessStateTest  = "TestSessionStateRefusesUserTokens"
	delegatedTest  = "TestDelegatedRoutesRefuseUserTokens"
	exemptExchange = "the credential is the social provider's own token in the request body, sent to the provider; no bearer of ours is read; a user flow, outside this leaf"
)

type negativeProof struct {
	file, test string // proven by this test in this file
	exempt     string // or exempt, with the reason
}

var negativeProofs = map[string]negativeProof{
	"go-api|GET /api/v1/external-ingest/availability [dev-health-ops-api @api]":                                {file: proofPush, test: pushRoutes},
	"go-api|GET /api/v1/external-ingest/batches [dev-health-ops-api @api]":                                     {file: proofPush, test: pushRoutes},
	"go-api|GET /api/v1/external-ingest/batches/{ingestion_id} [dev-health-ops-api @api]":                      {file: proofPush, test: pushRoutes},
	"go-api|POST /api/v1/external-ingest/batches [dev-health-ops-api @api]":                                    {file: proofPush, test: pushRoutes},
	"go-api|POST /api/v1/external-ingest/validate [dev-health-ops-api @api]":                                   {file: proofPush, test: pushRoutes},
	"go-api|GET /api/v1/internal/acr/entitlements/{org_id} [dev-health-ops-api @internal]":                     {file: proofACR, test: acrTest},
	"go-api|GET /api/v1/internal/acr/health [dev-health-ops-api @internal]":                                    {file: proofACR, test: acrTest},
	"go-api|GET /api/v1/internal/acr/orgs/{org_id}/llm-config [dev-health-ops-api @internal]":                  {file: proofACR, test: acrTest},
	"go-api|GET /api/v1/internal/acr/orgs/{org_id}/principals/{user_id}/access [dev-health-ops-api @internal]": {file: proofACR, test: acrTest},
	"go-api|POST /api/v1/billing/webhooks/stripe [dev-health-ops-api @api]":                                    {file: proofStripe, test: stripeTest},
	"go-api|POST /api/v1/billing/webhooks/stripe [dev-health-ops-billing-edge @billing-edge]":                  {file: proofStripe, test: stripeTest},
	"go-api|POST /api/v1/webhooks/github [dev-health-ops-api @api]":                                            {file: proofWebhooks, test: webhookTest},
	"go-api|POST /api/v1/webhooks/pagerduty/{binding_id} [dev-health-ops-api @api]":                            {file: proofWebhooks, test: webhookTest},
	"query-api|GET /buildinfo [dev-health-query-api @query,internal]":                                          {file: proofMCP, test: buildInfoTest},
	"query-api|POST /query [dev-health-query-api @mcp]":                                                        {file: proofMCP, test: mcpTest},
	"auth-service|POST /internal/v1/sessions [dev-health-auth-service @api]":                                   {file: proofIssue, test: issueTest},
	"auth-service|POST /internal/v1/token/exchange [dev-health-auth-service @api]":                             {file: proofExchange, test: exchangeTest},
	"auth-service|POST /internal/v1/memberships/add [dev-health-auth-service @api]":                            {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/memberships/role [dev-health-auth-service @api]":                           {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/memberships/remove [dev-health-auth-service @api]":                         {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/memberships/transfer [dev-health-auth-service @api]":                       {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/invites/create [dev-health-auth-service @api]":                             {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/invites/accept [dev-health-auth-service @api]":                             {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/organizations/deprovision [dev-health-auth-service @api]":                  {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/organizations/reactivate [dev-health-auth-service @api]":                   {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/invites/revoke [dev-health-auth-service @api]":                             {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/organizations/suspend [dev-health-auth-service @api]":                      {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/organizations/state [dev-health-auth-service @api]":                        {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/service-accounts/create [dev-health-auth-service @api]":                    {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/service-accounts/disable [dev-health-auth-service @api]":                   {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/service-accounts/enable [dev-health-auth-service @api]":                    {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/service-accounts/owner [dev-health-auth-service @api]":                     {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/service-accounts/keys/issue [dev-health-auth-service @api]":                {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/service-accounts/keys/rotate [dev-health-auth-service @api]":               {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/service-accounts/keys/revoke [dev-health-auth-service @api]":               {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/service-accounts/keys/list [dev-health-auth-service @api]":                 {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/organizations/onboard [dev-health-auth-service @api]":                      {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/organizations/create [dev-health-auth-service @api]":                       {file: proofProvision, test: provisionTest},
	"auth-service|POST /internal/v1/sessions/state [dev-health-auth-service @api]":                             {file: proofSessState, test: sessStateTest},
	"auth-service|POST /internal/v1/sessions/delegated [dev-health-auth-service @api]":                         {file: proofDelegated, test: delegatedTest},
	"auth-service|POST /internal/v1/sessions/delegated/end [dev-health-auth-service @api]":                     {file: proofDelegated, test: delegatedTest},
	"auth-service|POST /v1/token/refresh [dev-health-auth-service @api]":                                       {file: proofRefresh, test: refreshTest},
	"auth-service|POST /api/v1/auth/refresh [dev-health-auth-service @api]":                                    {file: proofRefresh, test: refreshTest},
	"auth-service|POST /api/v1/auth/logout [dev-health-auth-service @api]":                                     {file: proofRefresh, test: refreshTest},
	"auth-service|POST /api/v1/auth/social-login [dev-health-auth-service @api]":                               {exempt: exemptExchange},
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

// The census: 33 derived rows = 32 proven + 1 exempt. A change to either count is a decision, made here.
func TestNegativeProofCensus(t *testing.T) {
	proven, exempt := 0, 0
	for _, proof := range negativeProofs {
		if proof.exempt != "" {
			exempt++
		} else {
			proven++
		}
	}
	// CHAOS-11813 retired the 6 legacy ingest rows (26, 25, 1 before); CHAOS-11798 added the 7 provisioning rows (20, 19, 1 before);
	// CHAOS-11885 added the organization reactivate row, CHAOS-11887 the invite revoke row and the organization suspend row, CHAOS-12043 the organization state row;
	// CHAOS-12128 the principal access read row; CHAOS-12189 the session state row; CHAOS-10992 the organization onboard and create rows.
	// CHAOS-10987 the two delegated session rows; CHAOS-12273 the four service account key rows.
	if len(derivedRows(t)) != 45 || proven != 44 || exempt != 1 {
		t.Fatalf("derived %d rows, %d proven, %d exempt; want 45, 44, 1", len(derivedRows(t)), proven, exempt)
	}
}
