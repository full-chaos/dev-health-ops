package workload

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func TestMintBootstrapIsCanonicalAndUnique(t *testing.T) {
	seen := map[string]bool{}
	for range 10000 {
		v, err := MintBootstrap()
		if err != nil {
			t.Fatal(err)
		}
		if !ValidBootstrapShape(v) || !CanonicalBootstrap(v) {
			t.Fatalf("minted value is not canonical (len %d)", len(v))
		}
		raw, err := base64.RawURLEncoding.Strict().DecodeString(strings.TrimPrefix(v, BootstrapPrefix))
		if err != nil || len(raw) != 32 {
			t.Fatalf("minted value does not decode to 32 bytes: %d %v", len(raw), err)
		}
		if seen[v] {
			t.Fatal("a minted value repeated")
		}
		seen[v] = true
	}
}

// nonCanonical returns v with its last character replaced by one whose two
// unused low bits are set: the shape still matches, a strict decode fails.
func nonCanonical(t *testing.T, v string) string {
	t.Helper()
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	i := strings.IndexByte(alphabet, v[len(v)-1])
	return v[:len(v)-1] + string(alphabet[(i&^0xF)|0x1])
}

func TestCanonicalBootstrapRefusesEveryOtherForm(t *testing.T) {
	v, err := MintBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(v, BootstrapPrefix))
	bad := map[string]string{
		"stray bits in the last character": nonCanonical(t, v),
		"padded":                           BootstrapPrefix + base64.URLEncoding.EncodeToString(raw),
		"standard alphabet":                BootstrapPrefix + "+/" + v[len(BootstrapPrefix)+2:],
		"31 bytes":                         BootstrapPrefix + base64.RawURLEncoding.EncodeToString(raw[:31]),
		"33 bytes":                         BootstrapPrefix + base64.RawURLEncoding.EncodeToString(append(raw, 0)),
		"no prefix":                        base64.RawURLEncoding.EncodeToString(raw),
		"trailing newline":                 v + "\n",
	}
	if !ValidBootstrapShape(bad["stray bits in the last character"]) {
		t.Fatal("the stray-bits control no longer has the shape; it tests nothing")
	}
	for name, value := range bad {
		if CanonicalBootstrap(value) {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestFaultKeepsOnlyTheSQLState(t *testing.T) {
	v, _ := MintBootstrap()
	d := BootstrapDigest(v)
	digest := hex.EncodeToString(d[:])
	pg := &pgconn.PgError{Code: "23505", Message: "duplicate key value " + v,
		Detail: `Key (secret_hash)=(\x` + digest + `) already exists.`, ConstraintName: "credential_registry_secret_hash_key"}
	for _, in := range []error{pg, fmt.Errorf("insert: %w", pg), errors.New("dial: " + v + " " + digest)} {
		f := fault(in)
		if !errors.Is(f, ErrStore) {
			t.Fatalf("fault(%T) is not ErrStore", in)
		}
		for _, leak := range []string{v, digest, "secret_hash", "duplicate"} {
			if strings.Contains(f.Error(), leak) {
				t.Fatalf("fault text %q holds %q", f.Error(), leak)
			}
		}
	}
	if got := fault(pg).Error(); got != ErrStore.Error()+" (sqlstate 23505)" {
		t.Fatalf("fault text = %q", got)
	}
}

// offlineStore is a Store whose pool points at a closed port: every refusal
// below must come before any store read, and anything that reaches the store
// fails as ErrStore.
func offlineStore(t *testing.T) *Store {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("postgres://nobody@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{Pool: pool, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestBootstrapVerbsRefuseBadInputBeforeTheStoreAndAcceptTheEdges(t *testing.T) {
	s := offlineStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	v, _ := MintBootstrap()
	odd := nonCanonical(t, v)
	type call func() error
	create := func(value string, d time.Duration) call {
		return func() error { _, err := s.CreateBootstrap(ctx, "api", value, nil, d, Actor{}); return err }
	}
	rotate := func(value string, overlap, d time.Duration) call {
		return func() error { _, err := s.RotateBootstrap(ctx, "api", value, overlap, d, Actor{}); return err }
	}
	refused := map[string]struct {
		run  call
		want error
	}{
		"create non-canonical": {create(odd, time.Hour), ErrBootstrapValue},
		"create no expiry":     {create(v, 0), ErrBootstrapExpiry},
		"create 90 d + 1 ns":   {create(v, BootstrapExpiryCeiling+1), ErrBootstrapExpiry},
		"create bad audience": {func() error {
			_, err := s.CreateBootstrap(ctx, "api", v, []string{"A"}, time.Hour, Actor{})
			return err
		}, ErrAudienceInvalid},
		"rotate non-canonical":    {rotate(odd, 0, time.Hour), ErrBootstrapValue},
		"rotate overlap -1 ns":    {rotate(v, -1, time.Hour), ErrBootstrapOverlap},
		"rotate overlap 1 h+1 ns": {rotate(v, BootstrapOverlapCeiling+1, time.Hour), ErrBootstrapOverlap},
		"rotate 90 d + 1 ns":      {rotate(v, 0, BootstrapExpiryCeiling+1), ErrBootstrapExpiry},
		"revoke bad reason":       {func() error { return s.RevokeBootstrap(ctx, uuid.New(), "expired", Actor{}) }, ErrBootstrapReason},
	}
	for name, c := range refused {
		if err := c.run(); !errors.Is(err, c.want) {
			t.Errorf("%s = %v, want %v", name, err, c.want)
		}
	}
	// The edges are accepted: they reach the (offline) store.
	edges := map[string]call{
		"create 90 d":       create(v, BootstrapExpiryCeiling),
		"rotate overlap 0":  rotate(v, 0, BootstrapExpiryCeiling),
		"rotate overlap 1h": rotate(v, BootstrapOverlapCeiling, time.Second),
		"revoke compromised": func() error {
			return s.RevokeBootstrap(ctx, uuid.New(), BootstrapReasonCompromised, Actor{})
		},
		"revoke owner_request": func() error {
			return s.RevokeBootstrap(ctx, uuid.New(), BootstrapReasonOwnerRequest, Actor{})
		},
		"disable": func() error { _, err := s.DisableBootstrap(ctx, "api", Actor{}); return err },
	}
	for name, run := range edges {
		err := run()
		if !errors.Is(err, ErrStore) {
			t.Errorf("%s = %v, want it to reach the store", name, err)
			continue
		}
		if err.Error() != ErrStore.Error() && !strings.HasPrefix(err.Error(), ErrStore.Error()+" (sqlstate ") {
			t.Errorf("%s store error text %q carries more than the class", name, err.Error())
		}
	}
}

// TestBootstrapAuditAndEventsAreInsideTheVocabularies holds the attribute and
// event shapes the four verbs write to the audit allowlist and to
// provisioning-event.v1, without a database.
func TestBootstrapAuditAndEventsAreInsideTheVocabularies(t *testing.T) {
	s := offlineStore(t)
	w, c := uuid.New(), uuid.New()
	for _, ev := range []struct {
		event, reason string
		credential    uuid.UUID
		fields        []string
		next          string
	}{
		{"auth.workload.bootstrap_created", "credential_issued", c, []string{"expires_at", "audiences"}, "active"},
		{"auth.workload.bootstrap_rotated", "credential_rotated", c, []string{"expires_at"}, "active"},
		{"auth.workload.bootstrap_revoked", "credential_revoked", c, []string{"revoked_at"}, "revoked"},
		{"auth.workload.bootstrap_revoked", "compromised", c, []string{"revoked_at"}, "revoked"},
		{"auth.workload.bootstrap_disabled", "compromised", uuid.Nil, []string{"revoked_at"}, "revoked"},
	} {
		a := s.bootstrapAudit(ev.event, ev.reason, w, ev.credential, Actor{}, ev.fields, "", ev.next)
		if err := audit.ValidateAttributes(a.Attributes); err != nil {
			t.Errorf("%s/%s attributes refused: %v", ev.event, ev.reason, err)
		}
		if a.Attributes["credential_class"] != BootstrapClass {
			t.Errorf("%s carries no credential_class", ev.event)
		}
		if err := provisioning.CheckEvent("workload", wire(w), "workload.bootstrap_changed", bootstrapChange(ev.next, ev.credential)); err != nil {
			t.Errorf("%s event refused: %v", ev.event, err)
		}
	}
}
