package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// This file exists because of the 2026-09-07 route-activate incident. Every
// `dev-health-workerctl` invocation built from ops main after
// b6deeeb98ee2 (CHAOS-5437, #2380) exited 1 with exactly
// `{"error":{"code":"runtime_role_unauthorized"}}` against the shared
// compose stack, and that one string was the ENTIRE diagnostic surface: the
// three posture checks were collapsed into a single `||`, so the output did
// not say which role was refused, whether the database had even answered,
// or what it disagreed about. The actual cause was one table --
// worker_posture_manifest_applied, added to domainPosture() by #2380 and
// created by go-worker-migrate's own migration -- that the live database
// did not have yet, because the migrate image in the stack predated #2380.
// go-worker-migrate's posture gate reported "0 missing / 0 excess" for the
// same reason: it was the OLD binary checking the OLD manifest. Two days of
// a retag workaround, and hours of bisecting, for a fact the refusing
// process already had in hand and threw away.
//
// The bounded error CODE is unchanged (`runtime_role_unauthorized` is a
// compile-time constant and stays the first key of the error object, so
// `jq -r .error.code` keeps working); what is added is the detail the
// operator needs to act, drawn only from values this package already treats
// as safe to log: role names (checked-in runtime identifiers -- config, not
// connection material, per CheckRolePosture's own doc comment), a bounded
// reason vocabulary, and PostureGap.String() output (table/column/privilege
// identifiers validated by validRuntimeIdentifier). No driver error text,
// no DSN, no catalog dump ever reaches the operator's terminal or logs.

// Bounded reason vocabulary for a refused runtime-role posture. Both are
// compile-time constants: neither can carry credential or catalog material.
const (
	// runtimeRoleReasonPostureRefused means the posture query RAN and
	// answered "no" -- the role's live grants disagree with its declared
	// manifest. postgresstore.ErrPostureRefused (CHAOS-5435).
	runtimeRoleReasonPostureRefused = "posture_refused"
	// runtimeRoleReasonQueryUnavailable means the posture query never
	// produced an answer at all: connection refused, auth failure, context
	// deadline, driver fault. A completely different operator action from
	// the case above, and the distinction this CLI previously erased.
	runtimeRoleReasonQueryUnavailable = "posture_query_unavailable"
)

// Bounded note vocabulary. A note explains why the `gaps` list is not the
// whole story, so an operator never reads an empty or absent gap list as
// "nothing is wrong".
const (
	// runtimeRoleNoteOutsideDiagnosticScope is the honest answer when the
	// posture query refused but DiagnoseRolePosture found nothing: the
	// diagnostic is deliberately narrower than rolePostureQuery (see
	// DiagnoseRolePosture's own doc comment). Saying "0 gaps" without this
	// note is exactly the false-confirmation shape CHAOS-4675 hit.
	runtimeRoleNoteOutsideDiagnosticScope = "no declared table, column or sequence requirement is missing or in excess; " +
		"the refusal is in a predicate DiagnoseRolePosture does not cover (role attributes, object ownership, " +
		"database- or schema-level ambient privileges, River-schema grants, or table-wide excess on an undeclared relation)"
	// runtimeRoleNoteDiagnosticUnavailable means the explanatory query
	// itself failed. The refusal still stands; only the explanation is
	// missing, and saying so beats reporting an empty gap list.
	runtimeRoleNoteDiagnosticUnavailable = "posture diagnostic query unavailable; the refusal above still stands"
)

// runtimeRolePostureCheck is one runtime role's readiness check paired with
// the manifest that explains a refusal. The two function fields are bound
// closures rather than a pool plus a method value so this whole path is
// unit-testable without a database -- the same split posture_gate.go made
// for logPostureCheck, for the same reason.
type runtimeRolePostureCheck struct {
	// label is the role's fixed telemetry name: "domain", "queue" or
	// "coordinator". A closed set, never operator-supplied.
	label string
	// role is the configured PostgreSQL role name. Safe to log: a role name
	// is a checked-in runtime identifier, not connection material.
	role string
	// check is CheckDomainAuthorization/CheckQueueAuthorization/
	// CheckCoordinatorAuthorization already bound to its pool, role and
	// schema.
	check func(context.Context) error
	// diagnose is DiagnoseRolePosture already bound to this role's pool,
	// name and posture manifest. Called ONLY after check has already
	// refused -- it re-derives, at higher cost, what the hardened boolean
	// already computed, and must never sit on the readiness hot path.
	diagnose func(context.Context) ([]postgresstore.PostureGap, error)
}

// runtimeRoleRefusal is the machine-readable detail attached to a
// runtime_role_unauthorized exit. Every field is bounded or identifier-only;
// see this file's header for why that matters.
type runtimeRoleRefusal struct {
	// Check is the failing check's label, so an operator knows which of the
	// three refused without re-running anything.
	Check string `json:"check"`
	// Role is the PostgreSQL role that check binds to.
	Role string `json:"role"`
	// Reason distinguishes "the database answered no" from "the database
	// never answered". Bounded vocabulary above.
	Reason string `json:"reason"`
	// Gaps is PostureGap.String() for each expected-vs-actual disagreement
	// DiagnoseRolePosture could name -- e.g.
	// "worker_posture_manifest_applied: table does not exist", which is
	// precisely the line that would have ended the 2026-09-07 incident on
	// its first run.
	Gaps []string `json:"gaps,omitempty"`
	// Note is set whenever Gaps alone would mislead. Bounded vocabulary
	// above.
	Note string `json:"note,omitempty"`
}

// firstRuntimeRoleRefusal runs the checks in order and returns the detail
// for the first one that refuses, or nil when every check passes. Order is
// preserved from the caller (domain, queue, coordinator) and evaluation
// still short-circuits exactly as the previous `||` chain did, so no
// additional database work happens on the success path -- the only new cost
// is one diagnostic pass on a path that was already about to exit 1.
func firstRuntimeRoleRefusal(ctx context.Context, checks []runtimeRolePostureCheck) *runtimeRoleRefusal {
	for _, check := range checks {
		err := check.check(ctx)
		if err == nil {
			continue
		}
		refusal := &runtimeRoleRefusal{Check: check.label, Role: check.role}
		if !errors.Is(err, postgresstore.ErrPostureRefused) {
			// The query never ran. Do not attempt a diagnostic against a
			// database that just failed to answer -- it would fail the same
			// way and add nothing but latency to a failing command.
			refusal.Reason = runtimeRoleReasonQueryUnavailable
			return refusal
		}
		refusal.Reason = runtimeRoleReasonPostureRefused
		gaps, diagnoseErr := check.diagnose(ctx)
		if diagnoseErr != nil {
			refusal.Note = runtimeRoleNoteDiagnosticUnavailable
			return refusal
		}
		for _, gap := range gaps {
			refusal.Gaps = append(refusal.Gaps, gap.String())
		}
		if len(refusal.Gaps) == 0 {
			refusal.Note = runtimeRoleNoteOutsideDiagnosticScope
		}
		return refusal
	}
	return nil
}

// writeRuntimeRoleUnauthorized writes the runtime_role_unauthorized error
// object, enriched with refusal detail, and returns the process exit code.
// `code` stays the first key with its previous value, so any consumer
// reading only `.error.code` is unaffected; a nil refusal degrades to
// exactly writeError's original bytes rather than emitting a half-populated
// object.
func writeRuntimeRoleUnauthorized(stderr io.Writer, refusal *runtimeRoleRefusal) int {
	if refusal == nil {
		return writeError(stderr, "runtime_role_unauthorized")
	}
	payload := map[string]any{
		"code":   "runtime_role_unauthorized",
		"check":  refusal.Check,
		"role":   refusal.Role,
		"reason": refusal.Reason,
	}
	if len(refusal.Gaps) > 0 {
		payload["gaps"] = refusal.Gaps
	}
	if refusal.Note != "" {
		payload["note"] = refusal.Note
	}
	encoded, err := json.Marshal(map[string]any{"error": payload})
	if err != nil {
		// Unreachable for this map (strings and []string only), but a
		// silent half-written error object would be worse than the bounded
		// original, so fall back rather than swallow.
		return writeError(stderr, "runtime_role_unauthorized")
	}
	_, _ = fmt.Fprintf(stderr, "%s\n", encoded)
	return 1
}
