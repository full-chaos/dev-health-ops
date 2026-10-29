package contracts

import (
	"slices"
	"strings"
	"testing"
)

const sessionIssueRowID = "POST /internal/v1/sessions [dev-health-auth-service @api]"

func sessionIssueRow(t *testing.T) map[string]any {
	t.Helper()
	for _, item := range loadAuthServiceDocument(t)["rows"].([]any) {
		if row := item.(map[string]any); row["id"] == sessionIssueRowID {
			return row
		}
	}
	t.Fatalf("no row %q", sessionIssueRowID)
	return nil
}

// The session issue route accepts the go-api workload token beside the
// svc_authsess_ credential. The workload token is chosen by the JOSE typ of its
// header, a selector only that class owns.
func TestTheSessionIssueRowAcceptsTheWorkloadTokenAndKeepsTheServiceCredential(t *testing.T) {
	row := sessionIssueRow(t)
	var classes []string
	for _, class := range row["accepted_credential_classes"].([]any) {
		classes = append(classes, class.(string))
	}
	slices.Sort(classes)
	if !slices.Equal(classes, []string{"internal_svc_authsess_token", "workload_token"}) {
		t.Fatalf("accepted classes %v", classes)
	}
	selector, _ := row["class_disambiguator"].(map[string]any)
	if selector["kind"] != "jwt_header_typ" || selector["name"] != "workload+jwt" {
		t.Fatalf("class_disambiguator %v, want jwt_header_typ workload+jwt", selector)
	}
	validator := row["primary_validator"].(map[string]any)["anchor"].(map[string]any)
	if validator["path"] != "internal/auth/servicecred/servicecred.go" {
		t.Fatalf("primary validator anchor %v: the compat class keeps its validator", validator)
	}
	if row["source"].(map[string]any)["file"] != "internal/auth/authapi/compat.go" {
		t.Fatalf("source %v", row["source"])
	}
}

// R3: the selector gate itself accepts the row; no gate change is made.
func TestTheSelectorGateAcceptsTheSessionIssueRow(t *testing.T) {
	row := sessionIssueRow(t)
	profile := map[string]any{
		"accepted_credential_classes": row["accepted_credential_classes"],
		"class_disambiguator":         row["class_disambiguator"],
	}
	if err := CheckClassSelector(profile, realRegistry(t)); err != nil {
		t.Fatalf("the selector gate refuses the session issue row: %v", err)
	}
	profile["class_disambiguator"] = map[string]any{"kind": "jwt_header_typ", "name": "at+jwt"}
	if err := CheckClassSelector(profile, realRegistry(t)); err == nil {
		t.Fatal("the gate accepted a typ no accepted class owns")
	}
}

// credential-classes.json names the route as a consumer of both classes at the
// row's source line.
func TestTheRegistryNamesTheSessionIssueRouteAsAConsumerOfBothClasses(t *testing.T) {
	registry := indexRegistry(loadClassRegistryDocument(t))
	line := int(sessionIssueRow(t)["source"].(map[string]any)["line"].(float64))
	key := anchorKey(authHTTPLayer+"compat.go", line)
	for _, class := range []string{"workload_token", "internal_svc_authsess_token"} {
		if !registry.consumers[class][key] {
			t.Errorf("%s has no consumer anchored at %s", class, key)
		}
	}
}

// The workload token is consumed only behind an opt-in (the precedent of
// kubernetes_sa_token_exchange and workload_bootstrap_credential): its status is
// optional_feature, the consumer names the variable, and no gap says it has no
// consumer.
func TestTheWorkloadTokenClassIsAnOptionalFeatureConsumedBehindTheSubjectVariable(t *testing.T) {
	for _, class := range loadClassRegistryDocument(t).Classes {
		if class.ClassID != "workload_token" {
			continue
		}
		if class.Status != "optional_feature" {
			t.Errorf("status %q, want optional_feature", class.Status)
		}
		var named bool
		for _, consumer := range class.Consumers {
			named = named || strings.Contains(consumer.Description, "AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT")
		}
		if !named {
			t.Error("no consumer description names AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT")
		}
		for _, gap := range class.Gaps {
			if strings.HasPrefix(gap, "consumers: none yet") {
				t.Errorf("the gap %q is still listed", gap)
			}
		}
		return
	}
	t.Fatal("no workload_token class")
}
