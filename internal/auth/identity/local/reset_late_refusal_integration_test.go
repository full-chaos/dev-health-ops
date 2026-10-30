//go:build integration

package local

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/callseq"
)

// lateTracer labels each statement by the table it writes or reads and its verb.
type lateTracer struct{ rec callseq.Recorder }

func (l *lateTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	sql := strings.ToLower(strings.TrimSpace(data.SQL))
	verb, _, _ := strings.Cut(sql, " ")
	label := verb
	switch {
	case strings.Contains(sql, "link_credentials"):
		label = "link:" + verb
	case strings.Contains(sql, "public.users"):
		label = "public.users:" + verb
	case strings.Contains(sql, ".users"):
		label = "auth.users:" + verb
	}
	l.rec.Add(label)
	return ctx
}

func (*lateTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

type lateState struct {
	Hash                                    string
	Version, Audits, Outbox, Consumed, Live int
	SessionLive                             bool
}

// A reset whose link a concurrent actor uses, expires, replaces or unbinds
// after the first read writes nothing, answers FlowInvalidToken, counts
// set_password/link_unusable, logs one WARN line with no link value, and
// takes one statement shape whose last write is the refused consume.
func TestResetLateRefusalRollsBackEveryWriteAndTakesOneStatementShape(t *testing.T) {
	actors := map[string]func(t *testing.T, ctx context.Context, rig *endRig, id uuid.UUID, link string){
		"used-first": func(t *testing.T, ctx context.Context, rig *endRig, _ uuid.UUID, link string) {
			if out, err := rig.flows.Reset(ctx, link, "First-redeem-pass-1"); err != nil || out.Kind != FlowOK {
				t.Fatalf("the first redemption = %+v, %v", out, err)
			}
		},
		"expired": func(t *testing.T, ctx context.Context, rig *endRig, id uuid.UUID, _ string) {
			if _, err := rig.env.admin.Exec(ctx, `UPDATE auth.link_credentials SET expires_at = now() - interval '1 second' WHERE principal_id = $1 AND consumed_at IS NULL`, id); err != nil {
				t.Fatal(err)
			}
		},
		"reissued": func(t *testing.T, ctx context.Context, rig *endRig, id uuid.UUID, link string) {
			if err := rig.links.IssueBound(ctx, IssueRequest{PrincipalID: id, Purpose: PurposePasswordReset, TokenHash: linkHash(link + "-again"), TTL: ResetTTL}); err != nil {
				t.Fatal(err)
			}
		},
		"deactivated": func(t *testing.T, ctx context.Context, rig *endRig, id uuid.UUID, _ string) {
			if _, err := rig.env.admin.Exec(ctx, `UPDATE public.users SET is_active = false, token_version = token_version + 1, updated_at = now() WHERE id = $1`, id); err != nil {
				t.Fatal(err)
			}
		},
	}
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	sequences := map[string][]string{}
	for name, actor := range actors {
		ctx := context.Background()
		rig := newEndRig(t, ctx)
		tracer := &lateTracer{}
		cfg := rig.env.runtime.Config().Copy()
		cfg.ConnConfig.Tracer = tracer
		traced, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(traced.Close)
		reader := sdkmetric.NewManualReader()
		store, err := identity.New(identity.Config{Pool: traced, Schema: schema, UsersOwnedByAuth: true,
			Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")})
		if err != nil {
			t.Fatal(err)
		}
		links := NewPGLinks(traced, schema)

		id := rig.account(t, ctx, "Old-pass-12345")
		session, err := rig.sessions.Create(ctx, id, nil, []string{"pwd"})
		if err != nil {
			t.Fatal(err)
		}
		link := "late-link-" + name
		if err := rig.links.IssueBound(ctx, IssueRequest{PrincipalID: id, Purpose: PurposePasswordReset, TokenHash: linkHash(link), TTL: ResetTTL}); err != nil {
			t.Fatal(err)
		}
		state := func() lateState {
			return lateState{
				Hash:        rig.passwordHash(t, ctx, id),
				Version:     rig.tokenVersion(t, ctx, id),
				Audits:      rig.env.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`),
				Outbox:      rig.env.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events`),
				Consumed:    rig.env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1 AND consumed_at IS NOT NULL`, id),
				Live:        rig.liveLegacyRows(t, ctx, id),
				SessionLive: rig.live(t, ctx, session),
			}
		}
		var afterActor lateState
		flows, err := NewFlows(FlowsConfig{
			Accounts: peekThenWrite{IdentityAccounts: IdentityAccounts{Store: store, Sessions: rig.sessions, Links: links},
				write: func() { actor(t, ctx, rig, id, link); afterActor = state() }},
			Links: links, Sessions: rig.sessions, Mail: &liveMail{}, Locker: links, UsersOwnedByAuth: true, Hash: cheapHash})
		if err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(logging.NewJSON(&buf, slog.LevelDebug))
		before := len(tracer.rec.Names())
		out, err := flows.Reset(ctx, link, "A-new-pass-7788")
		slog.SetDefault(previous)
		sequences[name] = tracer.rec.Names()[before:]

		if err != nil || !reflect.DeepEqual(out, FlowOutcome{Kind: FlowInvalidToken}) {
			t.Fatalf("%s: Reset = %+v, %v; want only %s", name, out, err, FlowInvalidToken)
		}
		if got := state(); got != afterActor {
			t.Errorf("%s: the refused reset changed state: %+v, after the actor %+v", name, got, afterActor)
		}
		lateLog(t, name, buf.String(), link)
		lateCount(t, ctx, name, reader, "set_password/link_unusable")
	}
	callseq.Same(t, "late reset refusals", sequences)
	for name, seq := range sequences {
		consume := slices.Index(seq, "link:update")
		if len(seq) == 0 || seq[0] != "link:select" || consume < 1 || seq[consume-1] != "public.users:update" {
			t.Errorf("%s: statements %v; want the read first and the consume right after the password write", name, seq)
			continue
		}
		for _, label := range seq[consume+1:] {
			if label != "rollback" && label != "commit" {
				t.Errorf("%s: statement %s after the refused consume in %v", name, label, seq)
			}
		}
	}
}

func lateLog(t *testing.T, name, out, link string) {
	t.Helper()
	digest := linkHash(link)
	if strings.Contains(out, link) || strings.Contains(out, hex.EncodeToString(digest[:])) {
		t.Errorf("%s: the link or its digest reached the log: %s", name, out)
	}
	warns := 0
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		var rec struct{ Level, Msg string }
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("%s: %v: %s", name, err, line)
		}
		if rec.Level == "ERROR" {
			t.Errorf("%s: ERROR line: %s", name, line)
		}
		if rec.Level == "WARN" && rec.Msg == "audit: Commit refused; its own transaction rolled back" {
			warns++
		}
	}
	if warns != 1 {
		t.Errorf("%s: %d WARN refusal lines, want 1: %s", name, warns, out)
	}
}

func lateCount(t *testing.T, ctx context.Context, name string, reader *sdkmetric.ManualReader, want string) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok || m.Name != "auth.identity.external_links" {
				continue
			}
			for _, dp := range sum.DataPoints {
				action, _ := dp.Attributes.Value(attribute.Key("action"))
				outcome, _ := dp.Attributes.Value(attribute.Key("outcome"))
				got[action.AsString()+"/"+outcome.AsString()] += dp.Value
			}
		}
	}
	if len(got) != 1 || got[want] != 1 {
		t.Errorf("%s: counted %v, want %s once", name, got, want)
	}
}

type peekVerifyThen struct {
	IdentityAccounts
	write func()
}

func (p peekVerifyThen) PeekVerificationLink(ctx context.Context, tokenHash [32]byte) (uuid.UUID, bool, error) {
	id, ok, err := p.IdentityAccounts.PeekVerificationLink(ctx, tokenHash)
	if ok && err == nil {
		p.write()
	}
	return id, ok, err
}

// A verification link used first or expired after the first read answers
// FlowInvalidToken, leaves the address unverified by this call, counts
// verify_email/link_unusable and logs one WARN line with no link value.
func TestVerifyLateRefusalLogsOneWarnAndCountsLinkUnusable(t *testing.T) {
	actors := map[string]func(t *testing.T, ctx context.Context, rig *endRig, id uuid.UUID, link string){
		"used-first": func(t *testing.T, ctx context.Context, rig *endRig, _ uuid.UUID, link string) {
			if out, err := rig.flows.Verify(ctx, link); err != nil || out.Kind != FlowOK {
				t.Fatalf("the first verification = %+v, %v", out, err)
			}
		},
		"expired": func(t *testing.T, ctx context.Context, rig *endRig, id uuid.UUID, _ string) {
			if _, err := rig.env.admin.Exec(ctx, `UPDATE auth.link_credentials SET expires_at = now() - interval '1 second' WHERE principal_id = $1 AND consumed_at IS NULL`, id); err != nil {
				t.Fatal(err)
			}
		},
	}
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	for name, actor := range actors {
		ctx := context.Background()
		rig := newEndRig(t, ctx)
		reader := sdkmetric.NewManualReader()
		store, err := identity.New(identity.Config{Pool: rig.env.runtime, Schema: schema, UsersOwnedByAuth: true,
			Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")})
		if err != nil {
			t.Fatal(err)
		}
		id := rig.account(t, ctx, "Old-pass-12345")
		link := "late-verify-" + name
		if err := rig.links.IssueBound(ctx, IssueRequest{PrincipalID: id, Purpose: PurposeEmailVerification, TokenHash: linkHash(link), TTL: VerificationTTL}); err != nil {
			t.Fatal(err)
		}
		verified := func() int {
			return rig.env.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND email_verified_at IS NOT NULL`, id)
		}
		afterActor := -1
		flows, err := NewFlows(FlowsConfig{
			Accounts: peekVerifyThen{IdentityAccounts: IdentityAccounts{Store: store, Sessions: rig.sessions, Links: rig.links},
				write: func() { actor(t, ctx, rig, id, link); afterActor = verified() }},
			Links: rig.links, Sessions: rig.sessions, Mail: &liveMail{}, Locker: rig.links, UsersOwnedByAuth: true, Hash: cheapHash})
		if err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(logging.NewJSON(&buf, slog.LevelDebug))
		out, err := flows.Verify(ctx, link)
		slog.SetDefault(previous)

		if err != nil || !reflect.DeepEqual(out, FlowOutcome{Kind: FlowInvalidToken}) {
			t.Fatalf("%s: Verify = %+v, %v; want only %s", name, out, err, FlowInvalidToken)
		}
		if got := verified(); got != afterActor {
			t.Errorf("%s: verified rows %d after the refused call, %d after the actor", name, got, afterActor)
		}
		lateLog(t, name, buf.String(), link)
		lateCount(t, ctx, name, reader, "verify_email/link_unusable")
	}
}
