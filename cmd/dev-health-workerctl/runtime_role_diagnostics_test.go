package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// postureRefused builds the error shape CheckRolePosture/
// CheckQueueAuthorization actually return when the database ANSWERED and the
// answer was no, so these tests exercise the real errors.Is chain rather
// than a stand-in sentinel.
func postureRefused(role string) error {
	return fmt.Errorf("%w: %w for role %q", postgresstore.ErrUnavailable, postgresstore.ErrPostureRefused, role)
}

// queryUnavailable builds the other shape: the query never ran at all.
func queryUnavailable() error {
	return fmt.Errorf("%w: querying role posture: %w", postgresstore.ErrUnavailable, errors.New("dial tcp: connection refused"))
}

func passingCheck(label, role string) runtimeRolePostureCheck {
	return runtimeRolePostureCheck{
		label: label,
		role:  role,
		check: func(context.Context) error { return nil },
		diagnose: func(context.Context) ([]postgresstore.PostureGap, error) {
			return nil, errors.New("diagnose must not be called for a passing check")
		},
	}
}

// TestFirstRuntimeRoleRefusalNamesTheFailingCheckAndTheMissingTable is the
// 2026-09-07 route-activate incident in miniature: the domain posture
// refuses because worker_posture_manifest_applied (added to domainPosture()
// by CHAOS-5437 / #2380) does not exist in a database migrated by an older
// go-worker-migrate image. Before this change the operator got exactly
// `{"error":{"code":"runtime_role_unauthorized"}}` -- no role, no table, no
// indication the database had even answered.
func TestFirstRuntimeRoleRefusalNamesTheFailingCheckAndTheMissingTable(t *testing.T) {
	diagnosed := 0
	checks := []runtimeRolePostureCheck{
		{
			label: "domain",
			role:  "devhealth_domain",
			check: func(context.Context) error { return postureRefused("devhealth_domain") },
			diagnose: func(context.Context) ([]postgresstore.PostureGap, error) {
				diagnosed++
				return []postgresstore.PostureGap{
					{TableName: "worker_posture_manifest_applied", TableMissing: true},
				}, nil
			},
		},
		passingCheck("queue", "devhealth_queue"),
		passingCheck("coordinator", "devhealth_coordinator"),
	}

	refusal := firstRuntimeRoleRefusal(context.Background(), checks)
	if refusal == nil {
		t.Fatal("expected a refusal, got nil")
	}
	if refusal.Check != "domain" {
		t.Errorf("check = %q, want %q", refusal.Check, "domain")
	}
	if refusal.Role != "devhealth_domain" {
		t.Errorf("role = %q, want %q", refusal.Role, "devhealth_domain")
	}
	if refusal.Reason != runtimeRoleReasonPostureRefused {
		t.Errorf("reason = %q, want %q", refusal.Reason, runtimeRoleReasonPostureRefused)
	}
	if len(refusal.Gaps) != 1 {
		t.Fatalf("gaps = %v, want exactly one naming the missing table", refusal.Gaps)
	}
	if !strings.Contains(refusal.Gaps[0], "worker_posture_manifest_applied") {
		t.Errorf("gaps = %v, want the missing table named", refusal.Gaps)
	}
	if !strings.Contains(refusal.Gaps[0], "table does not exist") {
		t.Errorf("gaps = %v, want the gap to say the table does not exist", refusal.Gaps)
	}
	if refusal.Note != "" {
		t.Errorf("note = %q, want empty when a concrete gap was found", refusal.Note)
	}
	if diagnosed != 1 {
		t.Errorf("diagnose called %d times, want exactly 1", diagnosed)
	}
}

// TestFirstRuntimeRoleRefusalShortCircuitsAndDoesNotDiagnoseOnASuccessfulRun
// pins the property that made the previous `||` chain cheap: no diagnostic
// work at all when every role is authorized, and no evaluation of a later
// check once an earlier one has refused.
func TestFirstRuntimeRoleRefusalShortCircuitsAndDoesNotDiagnoseOnASuccessfulRun(t *testing.T) {
	if refusal := firstRuntimeRoleRefusal(context.Background(), []runtimeRolePostureCheck{
		passingCheck("domain", "devhealth_domain"),
		passingCheck("queue", "devhealth_queue"),
		passingCheck("coordinator", "devhealth_coordinator"),
	}); refusal != nil {
		t.Fatalf("expected nil for an all-authorized run, got %+v", refusal)
	}

	laterChecked := false
	refusal := firstRuntimeRoleRefusal(context.Background(), []runtimeRolePostureCheck{
		{
			label: "domain",
			role:  "devhealth_domain",
			check: func(context.Context) error { return postureRefused("devhealth_domain") },
			diagnose: func(context.Context) ([]postgresstore.PostureGap, error) {
				return nil, nil
			},
		},
		{
			label: "queue",
			role:  "devhealth_queue",
			check: func(context.Context) error {
				laterChecked = true
				return nil
			},
			diagnose: func(context.Context) ([]postgresstore.PostureGap, error) { return nil, nil },
		},
	})
	if refusal == nil || refusal.Check != "domain" {
		t.Fatalf("refusal = %+v, want the domain check", refusal)
	}
	if laterChecked {
		t.Error("the queue check ran after domain had already refused; short-circuit lost")
	}
}

// TestFirstRuntimeRoleRefusalDistinguishesAnUnansweredQueryFromARefusal is
// the distinction CHAOS-5435 introduced and this CLI previously erased.
// "Fix connectivity" and "fix a grant" are different operator actions, and
// a database that just failed to answer must not be asked to explain
// itself.
func TestFirstRuntimeRoleRefusalDistinguishesAnUnansweredQueryFromARefusal(t *testing.T) {
	diagnosed := false
	refusal := firstRuntimeRoleRefusal(context.Background(), []runtimeRolePostureCheck{
		{
			label: "queue",
			role:  "devhealth_queue",
			check: func(context.Context) error { return queryUnavailable() },
			diagnose: func(context.Context) ([]postgresstore.PostureGap, error) {
				diagnosed = true
				return nil, nil
			},
		},
	})
	if refusal == nil {
		t.Fatal("expected a refusal, got nil")
	}
	if refusal.Reason != runtimeRoleReasonQueryUnavailable {
		t.Errorf("reason = %q, want %q", refusal.Reason, runtimeRoleReasonQueryUnavailable)
	}
	if diagnosed {
		t.Error("diagnose ran against a database that never answered the posture query")
	}
	if len(refusal.Gaps) != 0 {
		t.Errorf("gaps = %v, want none when the query never ran", refusal.Gaps)
	}
}

// TestFirstRuntimeRoleRefusalNotesWhenTheDiagnosticCannotExplainTheRefusal
// guards the CHAOS-4675 false-confirmation shape: DiagnoseRolePosture is
// deliberately narrower than rolePostureQuery, so "0 gaps" after a genuine
// refusal must never be reported as if nothing were wrong.
func TestFirstRuntimeRoleRefusalNotesWhenTheDiagnosticCannotExplainTheRefusal(t *testing.T) {
	refusal := firstRuntimeRoleRefusal(context.Background(), []runtimeRolePostureCheck{
		{
			label:    "coordinator",
			role:     "devhealth_coordinator",
			check:    func(context.Context) error { return postureRefused("devhealth_coordinator") },
			diagnose: func(context.Context) ([]postgresstore.PostureGap, error) { return nil, nil },
		},
	})
	if refusal == nil {
		t.Fatal("expected a refusal, got nil")
	}
	if refusal.Note != runtimeRoleNoteOutsideDiagnosticScope {
		t.Errorf("note = %q, want the outside-diagnostic-scope note", refusal.Note)
	}

	refusal = firstRuntimeRoleRefusal(context.Background(), []runtimeRolePostureCheck{
		{
			label: "coordinator",
			role:  "devhealth_coordinator",
			check: func(context.Context) error { return postureRefused("devhealth_coordinator") },
			diagnose: func(context.Context) ([]postgresstore.PostureGap, error) {
				return nil, postgresstore.ErrUnavailable
			},
		},
	})
	if refusal == nil {
		t.Fatal("expected a refusal, got nil")
	}
	if refusal.Note != runtimeRoleNoteDiagnosticUnavailable {
		t.Errorf("note = %q, want the diagnostic-unavailable note", refusal.Note)
	}
}

// TestWriteRuntimeRoleUnauthorizedKeepsTheErrorCodeContract proves the
// machine-readable contract is unchanged (`.error.code` still reads
// `runtime_role_unauthorized`) while the detail an operator needs is now
// present, and that a nil refusal degrades to the original bytes exactly.
func TestWriteRuntimeRoleUnauthorizedKeepsTheErrorCodeContract(t *testing.T) {
	var stderr bytes.Buffer
	code := writeRuntimeRoleUnauthorized(&stderr, &runtimeRoleRefusal{
		Check:  "domain",
		Role:   "devhealth_domain",
		Reason: runtimeRoleReasonPostureRefused,
		Gaps:   []string{"worker_posture_manifest_applied: table does not exist"},
	})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	var decoded struct {
		Error struct {
			Code   string   `json:"code"`
			Check  string   `json:"check"`
			Role   string   `json:"role"`
			Reason string   `json:"reason"`
			Gaps   []string `json:"gaps"`
			Note   string   `json:"note"`
		} `json:"error"`
	}
	if err := json.Unmarshal(stderr.Bytes(), &decoded); err != nil {
		t.Fatalf("stderr is not valid JSON (%v): %s", err, stderr.String())
	}
	if decoded.Error.Code != "runtime_role_unauthorized" {
		t.Errorf("error.code = %q, want %q", decoded.Error.Code, "runtime_role_unauthorized")
	}
	if decoded.Error.Check != "domain" || decoded.Error.Role != "devhealth_domain" {
		t.Errorf("check/role = %q/%q, want domain/devhealth_domain", decoded.Error.Check, decoded.Error.Role)
	}
	if decoded.Error.Reason != runtimeRoleReasonPostureRefused {
		t.Errorf("error.reason = %q, want %q", decoded.Error.Reason, runtimeRoleReasonPostureRefused)
	}
	if len(decoded.Error.Gaps) != 1 {
		t.Fatalf("error.gaps = %v, want exactly one naming the missing table", decoded.Error.Gaps)
	}
	if !strings.Contains(decoded.Error.Gaps[0], "worker_posture_manifest_applied") {
		t.Errorf("error.gaps = %v, want the missing table named", decoded.Error.Gaps)
	}
	if decoded.Error.Note != "" {
		t.Errorf("error.note = %q, want it omitted when there are concrete gaps", decoded.Error.Note)
	}

	stderr.Reset()
	var original bytes.Buffer
	if got, want := writeRuntimeRoleUnauthorized(&stderr, nil), writeError(&original, "runtime_role_unauthorized"); got != want {
		t.Errorf("nil-refusal exit code = %d, want %d", got, want)
	}
	if stderr.String() != original.String() {
		t.Errorf("nil refusal wrote %q, want the original bytes %q", stderr.String(), original.String())
	}
}

// TestWriteRuntimeRoleUnauthorizedNeverLeaksDriverOrConnectionMaterial
// pins the boundary this file's header states: only a role name, a bounded
// reason, and identifier-only gap text ever reach stderr.
func TestWriteRuntimeRoleUnauthorizedNeverLeaksDriverOrConnectionMaterial(t *testing.T) {
	var stderr bytes.Buffer
	refusal := firstRuntimeRoleRefusal(context.Background(), []runtimeRolePostureCheck{
		{
			label: "domain",
			role:  "devhealth_domain",
			check: func(context.Context) error {
				return fmt.Errorf(
					"%w: querying role posture: %w",
					postgresstore.ErrUnavailable,
					errors.New("failed to connect to `host=db.internal user=devhealth_domain password=hunter2`"),
				)
			},
			diagnose: func(context.Context) ([]postgresstore.PostureGap, error) { return nil, nil },
		},
	})
	writeRuntimeRoleUnauthorized(&stderr, refusal)
	for _, forbidden := range []string{"hunter2", "db.internal", "password=", "host="} {
		if strings.Contains(stderr.String(), forbidden) {
			t.Errorf("stderr leaked %q: %s", forbidden, stderr.String())
		}
	}
}
