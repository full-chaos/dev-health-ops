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
// membership row. The flag must name exactly the organization actions of those go-api rows, no other profile row
// may ask a flagged action, and a flagged allow must carry nothing the owner's shape leaves out.

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
			// Part 2: only an admin-guard row may ask a flagged action.
			if flagged[a] {
				t.Errorf("%s row %q is not an admin-guard row and asks the flagged action %s", profile, r.ID, a)
			}
		}
	}
	if guardRows == 0 {
		t.Fatal("no go-api admin-guard row; the census reads nothing")
	}

	// Part 1: the flag equals the census, both ways.
	if got, want := platformAdminSet(flagged), platformAdminSet(guard); !slices.Equal(got, want) {
		t.Errorf("platform_admin allowed on %v; the organization actions of the go-api admin-guard rows are %v", got, want)
	}

	// Part 3: a flagged allow is the owner's shape: no repository scope, no redaction class for any role.
	for _, a := range catalog.Actions {
		if flagged[a.Action] && a.RepositoryScope == "filtered" {
			t.Errorf("flagged action %s is repository_scope filtered", a.Action)
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
