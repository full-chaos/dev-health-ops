package goapiproof

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

func TestEnableRefusesClassRequestsThatBreakTheClassRules(t *testing.T) {
	op, digest := mcpclass.Operation("hotspots"), mcpclass.DocumentDigest()
	base := func() EnableRequest {
		return EnableRequest{
			SchemaDigest: "s", RunningBuild: "b", Operations: []string{op}, OperationKinds: map[string]string{op: OperationKindMCPClass},
			DocumentDigest: map[string]string{op: digest}, Mode: TargetModeCanary, RolloutPercentage: 100,
			RecordedBy: "r", ReviewEvidence: "e", PrincipalID: "p",
		}
	}
	if err := base().validate(); err != nil {
		t.Fatalf("a well-formed class request was refused: %v", err)
	}
	for name, mutate := range map[string]func(*EnableRequest){
		"primary": func(r *EnableRequest) { r.Mode = TargetModePrimary },
		"not allowlisted": func(r *EnableRequest) {
			bad := mcpclass.Operation("dataHealth")
			r.Operations = []string{bad}
			r.OperationKinds = map[string]string{bad: OperationKindMCPClass}
			r.DocumentDigest = map[string]string{bad: digest}
		},
		"wrong digest": func(r *EnableRequest) { r.DocumentDigest = map[string]string{op: "sha256:" + strings.Repeat("1", 64)} },
		"wrong kind":   func(r *EnableRequest) { r.OperationKinds = map[string]string{op: OperationKindQuery} },
		"mixed with a document": func(r *EnableRequest) {
			r.Operations = []string{op, "featureFlags"}
			r.OperationKinds["featureFlags"] = OperationKindQuery
			r.DocumentDigest["featureFlags"] = "sha256:" + strings.Repeat("2", 64)
		},
		"class kind on a document": func(r *EnableRequest) {
			r.Operations = []string{"featureFlags"}
			r.OperationKinds = map[string]string{"featureFlags": OperationKindMCPClass}
			r.DocumentDigest = map[string]string{"featureFlags": "sha256:" + strings.Repeat("2", 64)}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := base()
			mutate(&r)
			if err := r.validate(); err == nil {
				t.Fatal("the request was not refused")
			}
		})
	}
}

func TestEnableNamesAClassKindOnADocumentOperation(t *testing.T) {
	r := EnableRequest{
		SchemaDigest: "s", RunningBuild: "b", Operations: []string{"featureFlags"},
		OperationKinds: map[string]string{"featureFlags": OperationKindMCPClass},
		DocumentDigest: map[string]string{"featureFlags": "sha256:" + strings.Repeat("2", 64)},
		Mode:           TargetModeCanary, RolloutPercentage: 100, RecordedBy: "r", ReviewEvidence: "e", PrincipalID: "p",
	}
	if err := r.validate(); err == nil || !strings.Contains(err.Error(), "is not an MCP class operation") {
		t.Fatalf("err = %v, want the class-kind-on-a-document refusal", err)
	}
}
