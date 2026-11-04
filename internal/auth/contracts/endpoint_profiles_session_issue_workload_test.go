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

// The session issue route accepts only the go-api workload token: the
// svc_authsess_ credential is retired there. One class needs no selector, and
// the row's validator is the workload path.
func TestTheSessionIssueRowAcceptsOnlyTheWorkloadToken(t *testing.T) {
	row := sessionIssueRow(t)
	var classes []string
	for _, class := range row["accepted_credential_classes"].([]any) {
		classes = append(classes, class.(string))
	}
	if !slices.Equal(classes, []string{"workload_token"}) {
		t.Fatalf("accepted classes %v, want [workload_token]", classes)
	}
	if selector, has := row["class_disambiguator"]; has {
		t.Fatalf("class_disambiguator %v on a one-class row", selector)
	}
	validator := row["primary_validator"].(map[string]any)["anchor"].(map[string]any)
	if validator["path"] != "internal/auth/authapi/issue_workload.go" || validator["note"] != "func (s *SessionIssueWorkload) authorize" {
		t.Fatalf("primary validator anchor %v, want the workload path", validator)
	}
	if row["source"].(map[string]any)["file"] != "internal/auth/authapi/compat.go" {
		t.Fatalf("source %v", row["source"])
	}
}

// R3: the selector gate itself accepts the one-class row; no gate change is made.
func TestTheSelectorGateAcceptsTheSessionIssueRow(t *testing.T) {
	row := sessionIssueRow(t)
	profile := map[string]any{"accepted_credential_classes": row["accepted_credential_classes"]}
	if err := CheckClassSelector(profile, realRegistry(t)); err != nil {
		t.Fatalf("the selector gate refuses the session issue row: %v", err)
	}
}

// credential-classes.json names the route as a consumer of the workload token
// at the row's source line, and no longer of the retired svc_authsess_ class.
func TestTheRegistryNamesTheSessionIssueRouteAsAConsumerOfTheWorkloadTokenOnly(t *testing.T) {
	registry := indexRegistry(loadClassRegistryDocument(t))
	line := int(sessionIssueRow(t)["source"].(map[string]any)["line"].(float64))
	key := anchorKey(authHTTPLayer+"compat.go", line)
	if !registry.consumers["workload_token"][key] {
		t.Errorf("workload_token has no consumer anchored at %s", key)
	}
	if registry.consumers["internal_svc_authsess_token"][key] {
		t.Errorf("internal_svc_authsess_token still names a consumer at %s", key)
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
