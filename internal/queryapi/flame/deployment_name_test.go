package flame

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

func deploymentNameClient(t *testing.T, releaseRefByOrg map[string]string) fakeQueryClient {
	t.Helper()
	return fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "FROM deployments FINAL") {
			t.Fatalf("unexpected query: %s", query)
		}
		var org string
		for _, b := range bindings {
			if b.Name == "org_id" {
				org, _ = b.Value.(string)
			}
		}
		ref, ok := releaseRefByOrg[org]
		if !ok {
			return &fixtureRowScanner{}, nil
		}
		return &fixtureRowScanner{rows: [][]any{{"success", "production",
			day(2024, 1, 12, 1, 0, 0), day(2024, 1, 12, 1, 20, 0), nil, nil, ref}}}, nil
	}}
}

func deploymentEntityName(t *testing.T, client fakeQueryClient, org string) any {
	t.Helper()
	got, err := BuildResponse(context.Background(), client, org, Params{EntityType: "deployment", EntityID: repoID + ":d-1"})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	name, ok := got.Entity.Get("name")
	if !ok {
		t.Fatal("deployment entity has no name key")
	}
	return name
}

func TestDeploymentFlameEntityServesTheReleaseRefAsName(t *testing.T) {
	name := deploymentEntityName(t, deploymentNameClient(t, map[string]string{"org-1": "v1.4.2"}), "org-1")
	if s, ok := name.(string); !ok || s != "v1.4.2" {
		t.Fatalf("name = %#v, want v1.4.2", name)
	}
}

func TestDeploymentFlameEntityNameIsNullWithoutReleaseRef(t *testing.T) {
	for _, ref := range []string{"", "   "} {
		name := deploymentEntityName(t, deploymentNameClient(t, map[string]string{"org-1": ref}), "org-1")
		if name != nil {
			t.Fatalf("release_ref %q: name = %#v, want null (never the deployment id)", ref, name)
		}
	}
}

func TestDeploymentFlameNameNeverCrossesTheOrgBoundary(t *testing.T) {
	client := deploymentNameClient(t, map[string]string{"org-2": "v9-secret"})
	_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":d-1"})
	if err == nil {
		t.Fatal("org-1 read a deployment of org-2")
	}
}

func TestFetchDeploymentQueryReadsReleaseRefInsideTheCallersOrg(t *testing.T) {
	for _, want := range []string{"release_ref", "FROM deployments FINAL\n        WHERE org_id = {org_id:String}"} {
		if !strings.Contains(fetchDeploymentQuery, want) {
			t.Fatalf("deployment query missing %q:\n%s", want, fetchDeploymentQuery)
		}
	}
}
