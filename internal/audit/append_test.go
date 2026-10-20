package audit

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func TestAppendWritesOnlyAppendOnlyEventTypes(t *testing.T) {
	for name, event := range map[string]AuditEvent{
		"a state-change event type": {EventType: "organization.created", Outcome: OutcomeAllowed},
		"an unlisted event type":    {EventType: "auth.login.maybe", Outcome: OutcomeDenied},
	} {
		if err := Append(context.Background(), nil, authschema.ValidatedIdentifier{}, event); !errors.Is(err, ErrNotAppendOnly) {
			t.Errorf("%s: err = %v, want ErrNotAppendOnly", name, err)
		}
	}
}

func TestAppendValidatesTheRowAsCommitDoes(t *testing.T) {
	bad := "not-a-uuid"
	for name, event := range map[string]AuditEvent{
		"an outcome outside the set":   {EventType: EventLoginFailed, Outcome: "maybe"},
		"an attribute outside the set": {EventType: EventLoginFailed, Outcome: OutcomeDenied, Attributes: map[string]any{"password": "x"}},
		"a reason outside the set":     {EventType: EventLoginFailed, Outcome: OutcomeDenied, Attributes: map[string]any{"reason_code": "free text"}},
		"a request id that is text":    {EventType: EventLoginFailed, Outcome: OutcomeDenied, RequestID: &bad},
		"no event type":                {Outcome: OutcomeDenied},
	} {
		err := Append(context.Background(), nil, authschema.ValidatedIdentifier{}, event)
		if !errors.Is(err, ErrMutationFailed) || errors.Is(err, ErrNotAppendOnly) {
			t.Errorf("%s: err = %v, want a validation refusal", name, err)
		}
	}
}

func TestCommitRefusesAnAppendOnlyEventType(t *testing.T) {
	m := Mutation{
		Apply:     func(context.Context, TxOps) error { return nil },
		Audit:     AuditEvent{EventType: EventLoginFailed, Outcome: OutcomeDenied},
		OffStream: OffStreamSessionLifecycle,
	}
	if err := m.validate(); !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("validate = %v, want a refusal: an append-only event records no state change", err)
	}
}

func TestTheLoginReasonsAreAuditVocabulary(t *testing.T) {
	for _, reason := range []string{"accepted", "invalid_credentials", "account_locked", "principal_deactivated", "email_unverified", "denied_no_membership"} {
		if err := ValidateAttributes(map[string]any{"reason_code": reason, "provider": "local"}); err != nil {
			t.Errorf("%s: %v", reason, err)
		}
	}
}
