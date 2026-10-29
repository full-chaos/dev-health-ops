//go:build integration

package audit_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// TestDrillAuthAttempt is the second driver of scripts/drill-auth-pitr.sh: it proves that revoked access is unusable by
// an actual authentication attempt, not only by database markers. The attempt goes through the verifier the auth
// service uses for a workload bootstrap credential, workload.Store.ResolveBootstrap, as the unprivileged runtime role:
// it reads the credential, its workload and its principal and refuses a revoked credential and a disabled principal.
// It does nothing unless DRILL_ATTEMPT_PHASE is set; it never starts a container.
//
// Phases: seed (source, before T: one workload per case, each with a live bootstrap credential) | revoke (source,
// after T: revoke the credential of the first, disable the principal of the second) | attempt (a copy: try both).
// The two secrets are written to attempt-secrets.json in the evidence directory (0600, drill-only values, never
// printed, never in an argument).
func TestDrillAuthAttempt(t *testing.T) {
	phase := os.Getenv("DRILL_ATTEMPT_PHASE")
	if phase == "" {
		t.Skip("DRILL_ATTEMPT_PHASE not set: this is a drill driver, run by scripts/drill-auth-pitr.sh")
	}
	dsn, dir := os.Getenv("DRILL_DSN"), os.Getenv("DRILL_EVIDENCE_DIR")
	if dsn == "" || dir == "" {
		t.Fatal("DRILL_DSN and DRILL_EVIDENCE_DIR are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	role := os.Getenv("DRILL_RUNTIME_ROLE")
	if role == "" {
		role = "drill_runtime"
	}
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	defer admin.Close()
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = os.Getenv("DRILL_RUNTIME_PASSWORD")
	rt, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("runtime pool: %v", err)
	}
	defer rt.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store, err := workload.New(workload.Config{Pool: rt, Schema: schema})
	if err != nil {
		t.Fatalf("workload store: %v", err)
	}
	secrets := filepath.Join(dir, "attempt-secrets.json")

	switch phase {
	case "seed":
		drillAttemptSeed(t, ctx, store, secrets)
	case "revoke":
		drillAttemptRevoke(t, ctx, store, rt, schema, secrets)
	case "attempt":
		drillAttemptTry(t, ctx, store, dir, secrets)
	default:
		t.Fatalf("unknown DRILL_ATTEMPT_PHASE %q", phase)
	}
}

type drillAttemptSecret struct {
	Name         string `json:"name"`
	Value        string `json:"value"`
	CredentialID string `json:"credential_id"`
	PrincipalID  string `json:"principal_id"`
}

func drillAttemptSeed(t *testing.T, ctx context.Context, store *workload.Store, path string) {
	t.Helper()
	var out []drillAttemptSecret
	for _, name := range []string{drillAttemptRevokedName, drillAttemptDisabledName} {
		principal, err := store.Register(ctx, workload.Spec{Name: name, Audiences: []string{"auth-service"}}, workload.Actor{})
		if err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
		value, err := workload.MintBootstrap()
		if err != nil {
			t.Fatal(err)
		}
		res, err := store.CreateBootstrap(ctx, name, value, nil, 24*time.Hour, workload.Actor{})
		if err != nil {
			t.Fatalf("create the bootstrap credential of %s: %v", name, err)
		}
		// Sanity: before anything is revoked the verifier accepts it.
		if _, err := store.ResolveBootstrap(ctx, workload.BootstrapDigest(value), time.Now()); err != nil {
			t.Fatalf("the live bootstrap credential of %s is refused on the source: %v", name, err)
		}
		out = append(out, drillAttemptSecret{Name: name, Value: value, CredentialID: res.CredentialID.String(), PrincipalID: principal.String()})
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write the drill secrets: %v", err)
	}
}

func drillAttemptLoad(t *testing.T, path string) (revoked, disabled drillAttemptSecret) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the drill secrets: %v", err)
	}
	var all []drillAttemptSecret
	if err := json.Unmarshal(raw, &all); err != nil || len(all) != 2 {
		t.Fatalf("the drill secrets file is malformed (%d entries)", len(all))
	}
	return all[0], all[1]
}

func drillAttemptRevoke(t *testing.T, ctx context.Context, store *workload.Store, rt *pgxpool.Pool, schema authschema.ValidatedIdentifier, path string) {
	t.Helper()
	revoked, disabled := drillAttemptLoad(t, path)
	if err := store.RevokeBootstrap(ctx, uuid.MustParse(revoked.CredentialID), workload.BootstrapReasonCompromised, workload.Actor{}); err != nil {
		t.Fatalf("revoke the bootstrap credential: %v", err)
	}
	principal := uuid.MustParse(disabled.PrincipalID)
	err := audit.Commit(ctx, rt, schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, principal)
			return err
		},
		Audit: audit.AuditEvent{EventType: "principal.disabled", Outcome: audit.OutcomeAllowed},
		Events: []audit.OutboxEvent{{AggregateType: "organization", AggregateID: "org_" + drillAttemptOrg().String(),
			EventType: "organization.updated"}},
	})
	if err != nil {
		t.Fatalf("disable the workload principal: %v", err)
	}
	// Sanity: on the source both are refused now.
	for _, s := range []drillAttemptSecret{revoked, disabled} {
		if _, err := store.ResolveBootstrap(ctx, workload.BootstrapDigest(s.Value), time.Now()); err == nil {
			t.Fatalf("%s is still accepted on the source after the revocation", s.Name)
		}
	}
}

// drillAttemptOrg is organization 2 of the main driver (the aggregate the revocation events name).
func drillAttemptOrg() uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("drill-11232-org-2"))
}

type drillAttemptResult struct {
	Expect string                `json:"expect"`
	Fenced bool                  `json:"fenced"`
	Tries  []drillAttemptOutcome `json:"attempts"`
	// AcceptedAfterT counts revoked or disabled secrets the verifier still accepts: 0 means revoked access is unusable.
	AcceptedAfterT int `json:"revoked_access_accepted"`
}

func drillAttemptTry(t *testing.T, ctx context.Context, store *workload.Store, dir, path string) {
	t.Helper()
	expect := os.Getenv("DRILL_EXPECT")
	fenced := os.Getenv("DRILL_FENCE") == "1"
	revoked, disabled := drillAttemptLoad(t, path)
	res := drillAttemptResult{Expect: expect, Fenced: fenced}
	for _, s := range []drillAttemptSecret{revoked, disabled} {
		_, err := store.ResolveBootstrap(ctx, workload.BootstrapDigest(s.Value), time.Now())
		out := drillAttemptOutcome{Name: s.Name, Accepted: err == nil}
		if err != nil {
			out.Reason = workload.RefusalReason(err)
			if out.Reason == "" {
				t.Fatalf("the attempt on %s failed without a refusal: %v", s.Name, errors.Unwrap(err))
			}
		} else {
			res.AcceptedAfterT++
		}
		res.Tries = append(res.Tries, out)
	}
	encoded, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "attempt-"+expect+"-"+os.Getenv("DRILL_LABEL")+".json"), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	// The end-of-WAL copy (expect F) and a fenced PITR copy must refuse both; the unfenced PITR copy is the measured gap.
	if (expect == "F" || fenced) && res.AcceptedAfterT != 0 {
		t.Fatalf("revoked access is still usable: %d of 2 attempts accepted (expect=%s fenced=%v)", res.AcceptedAfterT, expect, fenced)
	}
}
