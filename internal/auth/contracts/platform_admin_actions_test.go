package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
)

// The catalog flag platform_admin allowed admits a holder of admin.orgs to an organization action without a
// membership role, as the legacy admin guard (policy.Admin, policy.AdminOrg) admits a holder before it reads the
// membership row, and as the legacy query-api path admits a superuser in any organization. The flag must name exactly
// the organization actions of those go-api rows and the closed query-api list below, no other row may ask a flagged
// action, and a flagged allow must carry nothing the owner's shape leaves out.

// platformAdminLegacyQueryAPIActions maps each query-api action whose legacy path admits a superuser with no
// membership row to its executed legacy cell.
var platformAdminLegacyQueryAPIActions = map[string]string{
	"analytics.read":  "queryapi/server TestPlatformAdminNoMembershipQueryAPIAnswersEqualLegacy GET /api/v1/home",
	"metrics.read":    "queryapi/server TestPlatformAdminNoMembershipQueryAPIAnswersEqualLegacy GET /api/v1/flame",
	"git.read":        "queryapi/server TestPlatformAdminNoMembershipQueryAPIAnswersEqualLegacy GET /api/v1/drilldown/prs",
	"work_items.read": "queryapi/server TestPlatformAdminNoMembershipQueryAPIAnswersEqualLegacy GET /api/v1/drilldown/issues",
	"reports.write":   "acp parity TestParityPDPAlone: OperationOrgGuard and requireMutationOrg read no membership row",
	"org.read":        "queryapi/server TestBuildInfoNeverAsksThePDP GET /buildinfo: the route verifies the internal envelope, admits a platform admin only and never asks the decision point, so the flag changes no answer",
}

// platformAdminLegacyGoAPIActions maps each go-api action whose legacy handlers admit an admin.billing holder (or a
// users.is_superuser user) with no membership row, on every row that asks it, to its executed legacy cell.
var platformAdminLegacyGoAPIActions = map[string]string{
	"billing.read":          "apiservice TestPlatformAdminNoMembershipBillingAndLicenseReadsEqualLegacyWired, the 5 billing.read rows",
	"license.read":          "apiservice TestPlatformAdminNoMembershipBillingAndLicenseReadsEqualLegacyWired, the 2 license.read rows",
	"org.read":              "apiservice TestHandlerDecidedRowsAnswerEveryCallerClassAsLegacy GET /api/v1/orgs/me, platform admin + no membership",
	"settings.read":         "apiservice TestHandlerDecidedRowsAnswerEveryCallerClassAsLegacy GET /api/v1/telemetry/status, platform admin + no membership",
	"acr.credential.revoke": "apiservice TestHandlerDecidedRowsAnswerEveryCallerClassAsLegacy POST /api/v1/acr/credentials/{credential_id}/revoke, platform admin + no membership",
}

// platformAdminLegacyWebActions maps each web action whose legacy route admits a platform admin with no membership
// row in the organization of its session to its executed legacy cell (dev-health-web, run against a real ops stack).
var platformAdminLegacyWebActions = map[string]string{
	"acr.credential.issue": "full-chaos/dev-health-web 440ac1517f1452956ea547162c49a1a7ba58105c tests/acr-legacy/legacy-cells.proof.ts:447 " +
		"device approval and :457 consent approve (platform admin, no membership row), suite acr-legacy " +
		"(vitest.acr-legacy.config.ts, runner ci/acr-legacy/run-legacy-cells.sh), recording run 4 on ops c60f0702: " +
		"200 approved, log 863528210512, record f8aff1566d30",
}

// platformAdminFilteredActions is the closed list of flagged actions that may be repository_scope filtered.
var platformAdminFilteredActions = map[string]bool{"acr.credential.issue": true}

var adminGuardValidators = []string{"policy.Admin:", "policy.AdminOrg:"}

var platformAdminProfiles = []string{"go-api", "query-api", "auth-service"}

type platformAdminProfileRow struct {
	ID               string  `json:"id"`
	Action           *string `json:"action"`
	PrimaryValidator *struct {
		Description string `json:"description"`
	} `json:"primary_validator"`
}

func adminGuardRow(r platformAdminProfileRow) bool {
	if r.PrimaryValidator == nil {
		return false
	}
	for _, p := range adminGuardValidators {
		if strings.HasPrefix(r.PrimaryValidator.Description, p) {
			return true
		}
	}
	return false
}

func TestPlatformAdminFlagIsExactlyTheAdminGuardOrganizationActions(t *testing.T) {
	var catalog struct {
		Actions []struct {
			Action          string `json:"action"`
			Scope           string `json:"scope"`
			RepositoryScope string `json:"repository_scope"`
			PlatformAdmin   string `json:"platform_admin"`
		} `json:"actions"`
	}
	if err := json.Unmarshal(vocabulary.ActionsV1, &catalog); err != nil {
		t.Fatal(err)
	}
	organization, flagged := map[string]bool{}, map[string]bool{}
	for _, a := range catalog.Actions {
		organization[a.Action] = a.Scope == "organization"
		if a.PlatformAdmin == "allowed" {
			flagged[a.Action] = true
		}
	}
	if len(flagged) == 0 {
		t.Fatal("no action is flagged platform_admin allowed; every check below would be vacuous")
	}

	guard := map[string]bool{}
	guardRows := 0
	for _, profile := range platformAdminProfiles {
		raw, err := os.ReadFile(filepath.Join(testRoot(t), "contracts/auth/v1/endpoint-profiles."+profile+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Rows []platformAdminProfileRow `json:"rows"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("%s: %v", profile, err)
		}
		for _, r := range doc.Rows {
			if r.Action == nil || *r.Action == "" {
				continue
			}
			a := *r.Action
			if profile == "go-api" && adminGuardRow(r) {
				guardRows++
				if organization[a] {
					guard[a] = true
				}
				continue
			}
			// Part 2: only an admin-guard row, a query-api row of a listed query-api action, or a go-api row of a
			// listed go-api action may ask a flagged action.
			_, queryAPIListed := platformAdminLegacyQueryAPIActions[a]
			_, goAPIListed := platformAdminLegacyGoAPIActions[a]
			if flagged[a] && !(profile == "query-api" && queryAPIListed) && !(profile == "go-api" && goAPIListed) {
				t.Errorf("%s row %q is not an admin-guard row and asks the flagged action %s", profile, r.ID, a)
			}
		}
	}
	if guardRows == 0 {
		t.Fatal("no go-api admin-guard row; the census reads nothing")
	}

	// Part 1: the flag equals the census, both ways.
	want := map[string]bool{}
	for a := range guard {
		want[a] = true
	}
	for a := range platformAdminLegacyQueryAPIActions {
		if !organization[a] {
			t.Errorf("listed query-api action %s is not an organization action", a)
		}
		want[a] = true
	}
	for a := range platformAdminLegacyGoAPIActions {
		if !organization[a] {
			t.Errorf("listed go-api action %s is not an organization action", a)
		}
		want[a] = true
	}
	for a := range platformAdminLegacyWebActions {
		if !organization[a] {
			t.Errorf("listed web action %s is not an organization action", a)
		}
		want[a] = true
	}
	if got, want := platformAdminSet(flagged), platformAdminSet(want); !slices.Equal(got, want) {
		t.Errorf("platform_admin allowed on %v; the go-api admin-guard organization actions and the query-api and go-api lists are %v", got, want)
	}

	// Part 3: a flagged allow is the owner's shape: no repository scope, no redaction class for any role. One closed
	// exception: acr.credential.issue is repository_scope filtered, because device approval and consent take their
	// repository scopes from this decision; its platform-admin allow is org-wide (repository_scope organization),
	// the shape the membership default gives every role.
	for _, a := range catalog.Actions {
		if flagged[a.Action] && a.RepositoryScope == "filtered" && !platformAdminFilteredActions[a.Action] {
			t.Errorf("flagged action %s is repository_scope filtered", a.Action)
		}
	}
	filtered := map[string]bool{}
	for _, a := range catalog.Actions {
		if a.RepositoryScope == "filtered" {
			filtered[a.Action] = true
		}
	}
	for a := range platformAdminFilteredActions {
		if !flagged[a] || !filtered[a] {
			t.Errorf("listed filtered action %s is not a flagged, repository_scope filtered action", a)
		}
	}
	var redaction struct {
		Rules []struct {
			Role      string   `json:"role"`
			Action    string   `json:"action"`
			Redaction []string `json:"redaction"`
		} `json:"rules"`
	}
	if err := json.Unmarshal(vocabulary.RedactionClassesV1, &redaction); err != nil {
		t.Fatal(err)
	}
	for _, r := range redaction.Rules {
		if flagged[r.Action] {
			t.Errorf("flagged action %s carries redaction %v for role %s", r.Action, r.Redaction, r.Role)
		}
	}
}

func platformAdminSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
