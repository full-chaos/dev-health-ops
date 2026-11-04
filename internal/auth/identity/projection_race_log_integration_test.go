//go:build integration

package identity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// captureAuditLog sends the default slog output (audit.Commit's failure line)
// to a JSON buffer while call runs and returns its parsed lines.
func captureAuditLog(t *testing.T, call func()) []map[string]any {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(logging.NewJSON(&buf, slog.LevelDebug))
	func() {
		defer slog.SetDefault(previous)
		call()
	}()
	var lines []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		rec := map[string]any{}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line: %v: %s", err, line)
		}
		lines = append(lines, rec)
	}
	return lines
}

// loseTheRace writes the row a first projection inserts in an admin
// transaction (triggers off: a test-only writer) and holds it uncommitted
// until call's INSERT into auth.<table> waits on it, then commits. call has
// passed its existence read by then, so it loses the race at its own commit,
// every run.
func (e *env) loseTheRace(t *testing.T, ctx context.Context, table, rowSQL string, args []any, call func()) {
	t.Helper()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, rowSQL, args...); err != nil {
		t.Fatalf("hold the %s row: %v", table, err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		call()
	}()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var waiting int
		if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
WHERE wait_event_type = 'Lock' AND query LIKE 'INSERT INTO %' AND position('.' || $1 || ' (' in query) > 0`, table).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		select {
		case <-done:
			t.Fatalf("the call ended before its INSERT into %s waited on the held row", table)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("no INSERT into %s waited on the held row in 30 s", table)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	<-done
}

// commitLines are the audit.Commit failure lines among lines.
func commitLines(lines []map[string]any) []map[string]any {
	var out []map[string]any
	for _, rec := range lines {
		if msg, _ := rec["msg"].(string); strings.HasPrefix(msg, "audit: Commit ") {
			out = append(out, rec)
		}
	}
	return out
}

// requireOneRefusal: the lost race logs exactly one WARN refusal line of
// eventType naming the concurrentProjection class, and no ERROR line.
func requireOneRefusal(t *testing.T, lines []map[string]any, eventType string) {
	t.Helper()
	got := commitLines(lines)
	if len(got) != 1 {
		t.Fatalf("%d audit.Commit lines, want 1: %v", len(got), got)
	}
	rec := got[0]
	if rec["level"] != "WARN" || rec["msg"] != "audit: Commit refused; its own transaction rolled back" ||
		rec["audit_event_type"] != eventType || rec["refusal"] != "identity.concurrentProjection" {
		t.Fatalf("logged %v, want one WARN refusal of %s with refusal identity.concurrentProjection", rec, eventType)
	}
	for _, line := range lines {
		if line["level"] == "ERROR" {
			t.Fatalf("an ERROR line was logged: %v", line)
		}
	}
}

// C1: a first principal projection that loses the race at its commit is
// success, logged as one WARN refusal, never ERROR.
func TestRaceLostPrincipalProjectionLogsAWarnRefusal(t *testing.T) {
	if raceEnabled {
		t.Logf("%s %s", raceMarker, t.Name())
	}
	e := getEnv(t)
	ctx := context.Background()
	id := uuid.New()
	email := uniqueEmail("lost")
	src := source{email: email, emailLower: strings.ToLower(email), active: true}
	var inserted bool
	var err error
	lines := captureAuditLog(t, func() {
		e.loseTheRace(t, ctx, "principals", `INSERT INTO auth.principals (id, kind) VALUES ($1, 'user')`, []any{id}, func() {
			inserted, err = e.store.insertProjection(ctx, id, src)
		})
	})
	if err != nil || inserted {
		t.Fatalf("lost projection = (%v, %v), want (false, nil)", inserted, err)
	}
	requireOneRefusal(t, lines, "auth.principal.projected")
}

// C2: the same for a first organization projection.
func TestRaceLostOrganizationProjectionLogsAWarnRefusal(t *testing.T) {
	if raceEnabled {
		t.Logf("%s %s", raceMarker, t.Name())
	}
	e := getEnv(t)
	ctx := context.Background()
	id := uuid.New()
	slug := "org-" + id.String()
	var err error
	lines := captureAuditLog(t, func() {
		e.loseTheRace(t, ctx, "organizations", `INSERT INTO auth.organizations (id, slug, name) VALUES ($1, $2, 'race')`, []any{id, slug}, func() {
			err = e.store.insertOrganization(ctx, id, orgState{slug: slug, name: "race", active: true})
		})
	})
	if err != nil {
		t.Fatalf("lost organization projection = %v, want nil", err)
	}
	requireOneRefusal(t, lines, "auth.organization.projected")
}

// C3: a slug another organization holds is a real failure: it stays one ERROR
// line with no refusal class.
func TestOrganizationSlugCollisionStillLogsError(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	slug := "org-" + uuid.NewString()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, slug, name) VALUES ($1, $2, 'holder')`, uuid.New(), slug); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	lines := captureAuditLog(t, func() {
		err = e.store.insertOrganization(ctx, uuid.New(), orgState{slug: slug, name: "second", active: true})
	})
	if !errors.Is(err, ErrOrganizationSlugCollision) {
		t.Fatalf("projection onto a held slug = %v, want ErrOrganizationSlugCollision", err)
	}
	got := commitLines(lines)
	if len(got) != 1 || got[0]["level"] != "ERROR" || got[0]["msg"] != "audit: Commit failed; its own transaction rolled back" ||
		got[0]["audit_event_type"] != "auth.organization.projected" {
		t.Fatalf("logged %v, want one ERROR Commit failed line of auth.organization.projected", got)
	}
	if _, ok := got[0]["refusal"]; ok {
		t.Fatalf("a real failure logged a refusal class: %v", got[0])
	}
}

// C4: a legacy identity projection that loses the race at its commit is
// success, logged as one WARN refusal, never ERROR.
func TestRaceLostIdentityProjectionLogsAWarnRefusal(t *testing.T) {
	if raceEnabled {
		t.Logf("%s %s", raceMarker, t.Name())
	}
	e := getEnv(t)
	ctx := context.Background()
	user := e.seedUser(t, ctx, uniqueEmail("lostid"), true, true)
	e.project(t, ctx, user)
	key := googleKey("lost-" + uuid.NewString())
	var err error
	lines := captureAuditLog(t, func() {
		e.loseTheRace(t, ctx, "external_identities",
			`INSERT INTO auth.external_identities (id, principal_id, issuer, provider, subject) VALUES ($1, $2, $3, $4, $5)`,
			[]any{uuid.New(), user, key.Issuer, string(key.Provider), key.Subject}, func() {
				err = e.store.projectIdentity(ctx, key, user)
			})
	})
	if err != nil {
		t.Fatalf("lost identity projection = %v, want nil", err)
	}
	requireOneRefusal(t, lines, "auth.identity.projected")
}
