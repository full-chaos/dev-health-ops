package writerinventory

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The classes of go-api's public.organizations writers while auth-service owns organizations (AUTH_ORGS_OWNED,
// phase 5): auth-service writes name and is_active in its verbs' commits (internal/auth/org), go-api keeps the rest.
const (
	orgSplit   = "name to auth-service, the rest by go-api, while AUTH_ORGS_OWNED"
	orgOff     = "does not run while AUTH_ORGS_OWNED"
	orgRouted  = "birth routed by AUTH_PROXY_ORG_PROVISIONING (phase 5 needs it)"
	orgColumns = "writes only columns go-api keeps (no name, no is_active)"
)

type organizationSite struct {
	class string
	cells []string
}

const (
	cellAdminPatchOwned = "internal/apiservice/admin/orgs_owned_integration_test.go|TestOrgPatchNameGoesToAuthAndWritesNoNameOrIsActiveWhileOrgsOwned"
	cellAdminPatchQ5    = "internal/apiservice/admin/orgs_owned_integration_test.go|TestOrgPatchRestFailsAfterTheRenameWhileOrgsOwned"
	cellOwnOrgOwned     = "internal/api/orgs/orgs_owned_integration_test.go|TestOwnOrgNameGoesToAuthWhileOrgsOwned"
	cellOwnOrgQ5        = "internal/api/orgs/orgs_owned_integration_test.go|TestOwnOrgRestFailsAfterTheRenameWhileOrgsOwned"
	cellReconcilerOff   = "internal/apiservice/orgstate_reconciler_test.go|TestOrgReconcilerStaysOffWhileOrgsOwned"
)

// Keyed File|Func|Verb on the allowlist's go-api public.organizations rows.
var organizationWriterCensus = map[string]organizationSite{
	"internal/api/orgs/orgs_owned.go|handlers.ownUpdateSQL|UPDATE":                  {orgSplit, []string{cellOwnOrgOwned, cellOwnOrgQ5}},
	"internal/apiservice/admin/orgstore.go|pgStore.updateOrganization|UPDATE":       {orgSplit, []string{cellAdminPatchOwned, cellAdminPatchQ5}},
	"internal/apiservice/admin/orgstate_reconcile.go|OrgStateReconciler.fix|UPDATE": {orgOff, []string{cellReconcilerOff}},
	"internal/api/session/register.go|insertOrganization|INSERT":                    {orgRouted, []string{cellOnboard, cellSessionRefuse}},
	"internal/apiservice/admin/orgstore.go|pgStore.insertOrganizationTx|INSERT":     {orgRouted, []string{cellOrgCreateHTTP, cellOrgCreateCLI}},
	"internal/api/billing/subscription.go|handlers.syncOrgLicense|UPDATE":           {orgColumns, nil},
	"internal/api/billing/subscriptionhooks.go|handlers.revokeLicense|UPDATE":       {orgColumns, nil},
	"internal/api/billing/webhook.go|handlers.persistLicense|UPDATE":                {orgColumns, nil},
	"internal/api/session/onboarding.go|handlers.skipIntegration|UPDATE":            {orgColumns, nil},
}

func goAPIOrganizationWriters() map[string]bool {
	rows := map[string]bool{}
	for _, entry := range Allowlist {
		if entry.Table != "public.organizations" {
			continue
		}
		if !strings.HasPrefix(entry.File, "internal/api/") && !strings.HasPrefix(entry.File, "internal/apiservice/") {
			continue
		}
		rows[entry.File+"|"+entry.Func+"|"+entry.Verb] = true
	}
	return rows
}

// ownedColumnSet matches an assignment to name or is_active in an UPDATE of organizations.
var ownedColumnSet = regexp.MustCompile(`(?is)UPDATE\s+(public\.)?organizations\s+SET\s[^;` + "`" + `]*?\b(name|is_active)\s*=`)

// organizationColumnsProblem is the static check of an orgColumns site: no UPDATE of organizations in its file
// assigns name or is_active.
func organizationColumnsProblem(root, file string) string {
	raw, err := os.ReadFile(filepath.Join(root, file))
	if err != nil {
		return "site file: " + err.Error()
	}
	if ownedColumnSet.Match(raw) {
		return "an UPDATE of organizations in " + file + " assigns name or is_active"
	}
	return ""
}

func organizationCensusProblems(root string, rows map[string]bool) []string {
	var problems []string
	for key := range rows {
		site, ok := organizationWriterCensus[key]
		file, _, _ := strings.Cut(key, "|")
		switch {
		case !ok:
			problems = append(problems, "unclassified go-api public.organizations writer: "+key)
			continue
		case site.class == orgColumns:
			if why := organizationColumnsProblem(root, file); why != "" {
				problems = append(problems, key+": "+why)
			}
		case site.class != orgSplit && site.class != orgOff && site.class != orgRouted:
			problems = append(problems, key+": unknown class "+site.class)
		case len(site.cells) == 0:
			problems = append(problems, key+": no proving cell")
		}
		for _, cell := range site.cells {
			if why := membershipCellProblem(root, cell); why != "" {
				problems = append(problems, key+": "+why)
			}
		}
	}
	for key := range organizationWriterCensus {
		if !rows[key] {
			problems = append(problems, "stale census row (no such allowlist writer): "+key)
		}
	}
	sort.Strings(problems)
	return problems
}

// Every go-api writer of public.organizations has a class for AUTH_ORGS_OWNED, and each class but the static one names
// the cells that prove it.
func TestOrganizationWriterCensus(t *testing.T) {
	root := repoRoot(t)
	rows := goAPIOrganizationWriters()
	if len(rows) == 0 {
		t.Fatal("the allowlist holds no go-api public.organizations writer; the census would pass vacuously")
	}
	for _, problem := range organizationCensusProblems(root, rows) {
		t.Error(problem)
	}
}

// The census fails an unclassified writer, a stale row, a missing cell and a column site that writes name.
func TestOrganizationWriterCensusRefusesItsGaps(t *testing.T) {
	root := repoRoot(t)
	rows := goAPIOrganizationWriters()
	rows["internal/api/orgs/new.go|handlers.rename|UPDATE"] = true
	delete(rows, "internal/api/billing/webhook.go|handlers.persistLicense|UPDATE")
	got := strings.Join(organizationCensusProblems(root, rows), "\n")
	for _, want := range []string{"unclassified go-api public.organizations writer: internal/api/orgs/new.go", "stale census row (no such allowlist writer): internal/api/billing/webhook.go"} {
		if !strings.Contains(got, want) {
			t.Errorf("problems %q do not hold %q", got, want)
		}
	}
	if why := organizationColumnsProblem(root, "internal/apiservice/admin/orgstate_reconcile.go"); why == "" {
		t.Error("the static column check passed a file whose UPDATE assigns is_active")
	}
}
