package workload

import (
	"errors"
	"maps"
	"reflect"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
)

func enableFixture() (enableState, EnableInput) {
	st := enableState{principalID: uuid.New(), principalKind: "workload", operatorFound: true}
	return st, EnableInput{Operator: uuid.New()}
}

// TestDecideEnableRefusesEachCaseByName: each refusal is its own error, and
// the operator rules come before the principal rules.
func TestDecideEnableRefusesEachCaseByName(t *testing.T) {
	cases := map[string]struct {
		edit func(*enableState, *EnableInput)
		want error
	}{
		"operator is the workload's principal": {func(st *enableState, in *EnableInput) { in.Operator = st.principalID }, ErrOperatorIsSubject},
		"operator unknown":                     {func(st *enableState, _ *EnableInput) { st.operatorFound = false }, ErrOperatorNotFound},
		"operator disabled":                    {func(st *enableState, _ *EnableInput) { st.operatorDisabled = true }, ErrOperatorDisabled},
		"operator disabled and principal disabled": {func(st *enableState, _ *EnableInput) {
			st.operatorDisabled, st.principalDisabled = true, true
		}, ErrOperatorDisabled},
		"principal disabled, no flag": {func(st *enableState, _ *EnableInput) {
			st.rowDisabled, st.principalDisabled = true, true
		}, ErrBootstrapPrincipalDisabled},
		"principal disabled alone, no flag": {func(st *enableState, _ *EnableInput) { st.principalDisabled = true }, ErrBootstrapPrincipalDisabled},
		"flag on a user principal": {func(st *enableState, in *EnableInput) {
			st.principalDisabled, st.principalKind, in.EnablePrincipal = true, "user", true
		}, ErrPrincipalNotWorkload},
		"flag on a service account principal": {func(st *enableState, in *EnableInput) {
			st.principalDisabled, st.principalKind, in.EnablePrincipal = true, "service_account", true
		}, ErrPrincipalNotWorkload},
		"flag on an enabled user principal": {func(st *enableState, in *EnableInput) {
			st.rowDisabled, st.principalKind, in.EnablePrincipal = true, "user", true
		}, ErrPrincipalNotWorkload},
	}
	for name, c := range cases {
		st, in := enableFixture()
		c.edit(&st, &in)
		plan, err := decideEnable(st, in)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", name, err, c.want)
		}
		if plan.EnableRow || plan.EnablePrincipal {
			t.Errorf("%s: a refused plan enables something: %+v", name, plan)
		}
	}
}

func TestDecideEnablePlansOnlyWhatIsDisabledAndAsked(t *testing.T) {
	cases := map[string]struct {
		row, principal, flag bool
		wantRow, wantPrin    bool
	}{
		"row disabled":                     {row: true, wantRow: true},
		"row disabled, flag, principal on": {row: true, flag: true, wantRow: true},
		"both disabled, flag":              {row: true, principal: true, flag: true, wantRow: true, wantPrin: true},
		"principal disabled alone, flag":   {principal: true, flag: true, wantPrin: true},
		"nothing disabled":                 {},
		"nothing disabled, flag":           {flag: true},
	}
	for name, c := range cases {
		st, in := enableFixture()
		st.rowDisabled, st.principalDisabled, in.EnablePrincipal = c.row, c.principal, c.flag
		plan, err := decideEnable(st, in)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		want := EnablePlan{PrincipalID: st.principalID, RowDisabled: c.row, PrincipalDisabled: c.principal, EnableRow: c.wantRow, EnablePrincipal: c.wantPrin}
		if plan != want {
			t.Errorf("%s: plan %+v, want %+v", name, plan, want)
		}
	}
}

// TestTheEnableAuditRowNamesTheOperatorAndThePrincipalOnlyWithTheFlag: one
// row; principal_enabled and principal_id are present only when the principal
// row changes.
func TestTheEnableAuditRowNamesTheOperatorAndThePrincipalOnlyWithTheFlag(t *testing.T) {
	s := &Store{}
	id, operator := uuid.New(), uuid.New()
	request := uuid.NewString()
	in := EnableInput{Operator: operator, EnablePrincipal: true, RequestID: &request}
	base := map[string]any{"target_kind": "workload", "target_id": id.String(), "reason_code": "workload_enabled"}
	rowFields := map[string]any{"changed_fields": []string{"status", "disabled_at"}, "previous_status": "disabled", "new_status": "active"}
	principal := map[string]any{"principal_enabled": "true", "principal_id": id.String()}
	cases := map[string]struct {
		plan EnablePlan
		want []map[string]any
	}{
		"row only":          {EnablePlan{PrincipalID: id, EnableRow: true}, []map[string]any{base, rowFields}},
		"row and principal": {EnablePlan{PrincipalID: id, EnableRow: true, EnablePrincipal: true}, []map[string]any{base, rowFields, principal}},
		"principal only":    {EnablePlan{PrincipalID: id, EnablePrincipal: true}, []map[string]any{base, principal}},
	}
	for name, c := range cases {
		ev := s.enableAudit(c.plan, in)
		want := map[string]any{}
		for _, part := range c.want {
			maps.Copy(want, part)
		}
		if !reflect.DeepEqual(ev.Attributes, want) {
			t.Errorf("%s: attributes %v, want %v", name, ev.Attributes, want)
		}
		if ev.EventType != "auth.workload.enabled" || ev.Outcome != audit.OutcomeAllowed {
			t.Errorf("%s: event %s %s", name, ev.EventType, ev.Outcome)
		}
		if ev.ActorPrincipalID == nil || *ev.ActorPrincipalID != operator.String() {
			t.Errorf("%s: actor %v, want the operator", name, ev.ActorPrincipalID)
		}
		if ev.SubjectPrincipalID == nil || *ev.SubjectPrincipalID != id.String() || ev.RequestID == nil || *ev.RequestID != request {
			t.Errorf("%s: subject or request id is not the workload's / the caller's", name)
		}
		if err := audit.ValidateAttributes(ev.Attributes); err != nil {
			t.Errorf("%s: the audit allowlist refuses the row: %v", name, err)
		}
	}
}

func TestTheEnableEventsAreOnePerChangedRowAndPassTheProvisioningContract(t *testing.T) {
	id := uuid.New()
	cases := map[string]struct {
		plan EnablePlan
		want []string
	}{
		"row only":          {EnablePlan{PrincipalID: id, EnableRow: true}, []string{"workload.enabled"}},
		"row and principal": {EnablePlan{PrincipalID: id, EnableRow: true, EnablePrincipal: true}, []string{"workload.enabled", "principal.enabled"}},
		"principal only":    {EnablePlan{PrincipalID: id, EnablePrincipal: true}, []string{"principal.enabled"}},
	}
	for name, c := range cases {
		events := enableEvents(c.plan)
		var got []string
		for _, e := range events {
			got = append(got, e.EventType)
			if err := provisioning.CheckEvent(e.AggregateType, e.AggregateID, e.EventType, e.Change); err != nil {
				t.Errorf("%s: %s refused: %v", name, e.EventType, err)
			}
			if e.Change["status"] != "active" {
				t.Errorf("%s: %s change %v", name, e.EventType, e.Change)
			}
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: events %v, want %v", name, got, c.want)
		}
	}
}

func TestEnableRefusalsAreLabelledInvalid(t *testing.T) {
	for _, err := range []error{ErrOperatorNotFound, ErrOperatorDisabled, ErrOperatorIsSubject, ErrPrincipalNotWorkload, ErrEnableStateChanged} {
		if got := outcomeLabel(err); got != "invalid" {
			t.Errorf("outcomeLabel(%v) = %q, want invalid", err, got)
		}
	}
}

// TestDecideEnableRefusesWhileABootstrapValueIsLive (D6505): an enable that
// would change a row is refused while a bootstrap value is live, as the value
// would be accepted again; a workload with nothing disabled stays a no-op.
func TestDecideEnableRefusesWhileABootstrapValueIsLive(t *testing.T) {
	cases := map[string]struct {
		row, principal, flag bool
		live                 int
		want                 error
	}{
		"row disabled, one live":          {row: true, live: 1, want: ErrEnableLiveBootstrap},
		"both disabled, flag, two live":   {row: true, principal: true, flag: true, live: 2, want: ErrEnableLiveBootstrap},
		"principal alone, flag, one live": {principal: true, flag: true, live: 1, want: ErrEnableLiveBootstrap},
		"nothing disabled, one live":      {live: 1},
		"row disabled, none live":         {row: true},
	}
	for name, c := range cases {
		st, in := enableFixture()
		st.rowDisabled, st.principalDisabled, st.liveBootstrap, in.EnablePrincipal = c.row, c.principal, c.live, c.flag
		plan, err := decideEnable(st, in)
		if !errors.Is(err, c.want) || (c.want == nil && err != nil) {
			t.Errorf("%s: err = %v, want %v", name, err, c.want)
		}
		if c.want != nil && (plan.EnableRow || plan.EnablePrincipal) {
			t.Errorf("%s: a refused plan enables something: %+v", name, plan)
		}
	}
	st, in := enableFixture()
	st.rowDisabled, st.liveBootstrap, st.operatorFound = true, 1, false
	if _, err := decideEnable(st, in); !errors.Is(err, ErrOperatorNotFound) {
		t.Errorf("an unknown operator with a live value = %v, want ErrOperatorNotFound first", err)
	}
	if got := outcomeLabel(ErrEnableLiveBootstrap); got != "taken" {
		t.Errorf("outcomeLabel(ErrEnableLiveBootstrap) = %q, want taken", got)
	}
}

// TestAPrincipalOnlyEnableStillWritesItsAuditEvent: when only the principal
// row changes, the one auth.workload.enabled event is still written with its
// actor and action, and it carries no change fields of the workload row.
func TestAPrincipalOnlyEnableStillWritesItsAuditEvent(t *testing.T) {
	s := &Store{}
	id, operator := uuid.New(), uuid.New()
	ev := s.enableAudit(EnablePlan{PrincipalID: id, PrincipalDisabled: true, EnablePrincipal: true}, EnableInput{Operator: operator, EnablePrincipal: true})
	if ev.EventType != "auth.workload.enabled" || ev.Attributes["reason_code"] != "workload_enabled" {
		t.Fatalf("event %q reason %v, want auth.workload.enabled / workload_enabled", ev.EventType, ev.Attributes["reason_code"])
	}
	if ev.ActorPrincipalID == nil || *ev.ActorPrincipalID != operator.String() {
		t.Fatalf("actor %v, want the operator", ev.ActorPrincipalID)
	}
	for _, key := range []string{"changed_fields", "previous_status", "new_status"} {
		if _, ok := ev.Attributes[key]; ok {
			t.Errorf("a principal-only enable carries %s = %v", key, ev.Attributes[key])
		}
	}
	if ev.Attributes["principal_enabled"] != "true" || ev.Attributes["principal_id"] != id.String() {
		t.Fatalf("principal attributes %v", ev.Attributes)
	}
}
