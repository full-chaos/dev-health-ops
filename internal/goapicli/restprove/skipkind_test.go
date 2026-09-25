package restprove

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

func TestParseFlagsSkipCredentialKindIsRepeatableDedupedAndValidated(t *testing.T) {
	f, err := parseFlags(serviceArgs("-service", "dho-api", "-dho-api-url", "http://127.0.0.1:1",
		"-skip-credential-kind", "org_admin", "-skip-credential-kind", "platform_superadmin", "-skip-credential-kind", "org_admin"))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.skipCredentialKinds) != 2 || f.skipCredentialKinds[0] != goapiproof.RESTCredentialOrgAdmin || f.skipCredentialKinds[1] != goapiproof.RESTCredentialPlatformSuperadmin {
		t.Fatalf("skipCredentialKinds = %v, want [org_admin platform_superadmin]", f.skipCredentialKinds)
	}
	for _, bad := range []string{"", "run", "bogus", "Org_Admin"} {
		if _, err := parseFlags(serviceArgs("-skip-credential-kind", bad)); err == nil {
			t.Errorf("-skip-credential-kind %q accepted, want a refusal (only file-fed kinds can be skipped)", bad)
		}
	}
}

func TestPlanSkipsEntriesOfASkippedKindOnly(t *testing.T) {
	full, err := planRESTRequestsFor(goapiproof.RESTServiceDHOAPI)
	if err != nil {
		t.Fatal(err)
	}
	skipped, err := planRESTRequestsFor(goapiproof.RESTServiceDHOAPI, goapiproof.RESTCredentialPushToken)
	if err != nil {
		t.Fatal(err)
	}
	pushEntries := 0
	for _, p := range full {
		if p.spec.Credential == goapiproof.RESTCredentialPushToken {
			pushEntries++
		}
	}
	if pushEntries == 0 || len(skipped) != len(full)-pushEntries {
		t.Fatalf("plan %d, skipped plan %d, push entries %d: skipping a kind must remove exactly its requests", len(full), len(skipped), pushEntries)
	}
	for _, p := range skipped {
		if p.spec.Credential == goapiproof.RESTCredentialPushToken {
			t.Fatalf("%s still planned", p.operation)
		}
	}
	// Skipping a kind on another service changes nothing there.
	query, _ := planRESTRequestsFor(goapiproof.RESTServiceQueryAPI)
	queryWithSkip, _ := planRESTRequestsFor(goapiproof.RESTServiceQueryAPI, goapiproof.RESTCredentialPushToken)
	if len(query) != len(queryWithSkip) {
		t.Fatal("the query-api plan has no push-token entries; a skip must not change it")
	}
}

func TestSkipCredentialKindWithItsTokenFileIsRefused(t *testing.T) {
	f, err := parseFlags(serviceArgs("-service", "dho-api", "-dho-api-url", "http://127.0.0.1:1",
		"-skip-credential-kind", "push_token", "-push-token-file", "/some/file"))
	if err != nil {
		t.Fatal(err)
	}
	if err := refuseSkipWithTokenFile(f); err == nil || !strings.Contains(err.Error(), "-push-token-file") {
		t.Fatalf("a skip together with that kind's token file must be refused naming the flag, got %v", err)
	}
	f.pushTokenFile = ""
	if err := refuseSkipWithTokenFile(f); err != nil {
		t.Fatalf("a skip without the file is fine: %v", err)
	}
}

// TestRunWithASkippedKindNeedsNoTokenFileForIt: the dho-api run that plans
// push-token entries normally refuses without -push-token-file; with the kind
// skipped that refusal is gone (it goes on to reach the network, which the
// unreachable address then fails, so the error must not be the missing-flag
// one) and NO request reaches a server for a skipped entry.
func TestRunWithASkippedKindNeedsNoTokenFileForIt(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer server.Close()
	f, err := parseFlags(append(serviceArgs("-service", "dho-api", "-dho-api-url", server.URL, "-skip-credential-kind", "push_token"), "-artifact-dir", t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	err = run(f)
	if err != nil && strings.Contains(err.Error(), "-push-token-file") {
		t.Fatalf("a skipped kind must not demand its token file, got %v", err)
	}
	// Without the skip the same run refuses by name before sending anything.
	f2, _ := parseFlags(append(serviceArgs("-service", "dho-api", "-dho-api-url", server.URL), "-artifact-dir", t.TempDir()))
	before := hits.Load()
	if err := run(f2); err == nil || !strings.Contains(err.Error(), "-push-token-file") {
		t.Fatalf("the unskipped run must refuse naming -push-token-file, got %v", err)
	}
	if hits.Load() != before {
		t.Fatal("the refusal must come before any request")
	}
}

func TestSkippedKindsAreStatedInTheReports(t *testing.T) {
	f := flags{service: goapiproof.RESTServiceDHOAPI, skipCredentialKinds: []goapiproof.RESTCredentialKind{goapiproof.RESTCredentialPlatformSuperadmin, goapiproof.RESTCredentialOrgAdmin}}
	report, _ := finalRunReport(f, nil, nil, nil, nil, nil, nil)
	if got := strings.Join(report.SkippedCredentialKinds, ","); got != "org_admin,platform_superadmin" {
		t.Fatalf("report skipped kinds = %q, want the sorted kinds", got)
	}
	none, _ := finalRunReport(flags{}, nil, nil, nil, nil, nil, nil)
	if len(none.SkippedCredentialKinds) != 0 {
		t.Fatalf("a run that skips nothing must report nothing skipped, got %v", none.SkippedCredentialKinds)
	}
}
