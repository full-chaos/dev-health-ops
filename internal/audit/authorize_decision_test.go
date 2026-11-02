package audit

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func highRiskAttrs() map[string]any {
	return map[string]any{
		"decision_id": "dec_" + strings.Repeat("A", 26), "decision_reason_code": "allowed_by_role",
		"action_key": "org.delete", "resource_type": "organization", "policy_revision": int64(3),
		"caller_principal_id": "0b3c1c0e-5a43-4a6e-9f55-0d0e3e1f2a11", "workload_jti": strings.Repeat("a", 22),
	}
}

func TestHighRiskDecisionEventTypeIsAppendOnly(t *testing.T) {
	if !appendOnlyEventTypes[EventAuthorizeHighRiskDecided] {
		t.Fatal("the high-risk decision event type is not append-only")
	}
	m := Mutation{
		Apply:     func(context.Context, TxOps) error { return nil },
		Audit:     AuditEvent{EventType: EventAuthorizeHighRiskDecided, Outcome: OutcomeAllowed},
		OffStream: OffStreamSessionLifecycle,
	}
	if err := m.validate(); !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("Commit accepted an append-only type: %v", err)
	}
}

func TestHighRiskDecisionAttributesAreClosed(t *testing.T) {
	if err := ValidateAttributes(highRiskAttrs()); err != nil {
		t.Fatalf("the row shape was refused: %v", err)
	}
	for name, mut := range map[string]func(map[string]any){
		"decision id with a bad prefix":   func(a map[string]any) { a["decision_id"] = "x_" + strings.Repeat("A", 26) },
		"decision id too short":           func(a map[string]any) { a["decision_id"] = "dec_short" },
		"decision reason outside the set": func(a map[string]any) { a["decision_reason_code"] = "free text" },
		"audit reason as decision reason": func(a map[string]any) { a["decision_reason_code"] = "role_granted" },
		"resource type outside the set":   func(a map[string]any) { a["resource_type"] = "team" },
		"a resource key key":              func(a map[string]any) { a["resource_key"] = "acme" },
		"a principal reference":           func(a map[string]any) { a["principal_reference"] = "eyJ.x.y" },
	} {
		attrs := highRiskAttrs()
		mut(attrs)
		if err := ValidateAttributes(attrs); !errors.Is(err, ErrAttributeNotAllowed) {
			t.Errorf("%s: err = %v, want ErrAttributeNotAllowed", name, err)
		}
	}
	for _, id := range []string{"acme/widget", "*"} {
		a := highRiskAttrs()
		a["resource_type"], a["resource_id"] = "repository", id
		if err := ValidateAttributes(a); err != nil {
			t.Errorf("repository key %q refused: %v", id, err)
		}
	}
}

func TestAppendAllRefusesBeforeItWrites(t *testing.T) {
	ok := AuditEvent{EventType: EventAuthorizeHighRiskDecided, Outcome: OutcomeDenied, Attributes: highRiskAttrs()}
	notAppendOnly := AuditEvent{EventType: "organization.created", Outcome: OutcomeAllowed}
	badAttr := ok
	badAttr.Attributes = map[string]any{"password": "x"}
	schema := authschema.ValidatedIdentifier{}
	ctx := context.Background()
	if err := AppendAll(ctx, nil, schema); !errors.Is(err, ErrMutationFailed) {
		t.Errorf("no events: %v, want a refusal", err)
	}
	if err := AppendAll(ctx, nil, schema, ok, notAppendOnly); !errors.Is(err, ErrNotAppendOnly) {
		t.Errorf("a state-change type in the set: %v, want ErrNotAppendOnly", err)
	}
	if err := AppendAll(ctx, nil, schema, ok, badAttr); !errors.Is(err, ErrAttributeNotAllowed) {
		t.Errorf("a bad attribute in the set: %v, want ErrAttributeNotAllowed", err)
	}
	if err := AppendAll(ctx, nil, schema, ok); err == nil || !strings.Contains(err.Error(), "no connection pool") {
		t.Errorf("a valid set with no pool: %v, want the no-pool refusal", err)
	}
}
