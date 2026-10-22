package usersbackfill

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

func str(s string) *string { return &s }

func bcryptHash(t *testing.T) *string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte("map-test-material"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return str(string(h))
}

func TestMapHashRules(t *testing.T) {
	good := bcryptHash(t)
	cases := []struct {
		name  string
		hash  *string
		class string
	}{
		{"nil hash maps", nil, ""},
		{"bcrypt maps", good, ""},
		{"empty is refused", str(""), SkipUnknownHashFormat},
		{"other format is refused", str("$arg" + "on2id$" + "v=19"), SkipUnknownHashFormat},
		{"prefix without a valid body is refused", str("$2b$12$short"), SkipUnknownHashFormat},
		{"plain text is refused", str("not-a-hash"), SkipUnknownHashFormat},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target, class, _ := Map(Row{ID: uuid.New(), Email: "a@example.test", PasswordHash: c.hash, Active: true})
			if class != c.class {
				t.Fatalf("class = %q, want %q", class, c.class)
			}
			if class == "" && (target.PasswordHash == nil) != (c.hash == nil) {
				t.Fatal("hash presence not carried")
			}
		})
	}
}

func TestMapProviders(t *testing.T) {
	cases := []struct {
		name       string
		provider   *string
		subject    *string
		wantKey    bool
		class      string
		notCarried bool
	}{
		{"nil provider", nil, nil, false, "", false},
		{"local", str("local"), nil, false, "", false},
		{"github", str("github"), str("12345"), true, "", false},
		{"gitlab", str("gitlab"), str("67890"), true, "", false},
		{"google", str("google"), str("sub-1"), true, "", false},
		{"social without a subject", str("github"), nil, false, SkipInvalidIdentity, false},
		{"sso protocol", str("saml"), str("name-id"), false, "", true},
		{"unknown provider", str("something-else"), str("x"), false, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target, class, notCarried := Map(Row{ID: uuid.New(), Email: "a@example.test", AuthProvider: c.provider, AuthProviderID: c.subject})
			if class != c.class || notCarried != c.notCarried || (target.Identity != nil) != c.wantKey {
				t.Fatalf("class=%q notCarried=%v key=%v", class, notCarried, target.Identity != nil)
			}
			if target.Identity != nil && target.Identity.Validate() != nil {
				t.Fatal("mapped key does not validate")
			}
		})
	}
}

func TestMapCarriesFlagsAndName(t *testing.T) {
	target, class, _ := Map(Row{ID: uuid.New(), Email: "a@example.test", FullName: str("Display"), Active: false, Verified: true})
	if class != "" || target.Active || !target.Verified || target.DisplayName != "Display" {
		t.Fatalf("unexpected target: %+v class=%q", target, class)
	}
	target, _, _ = Map(Row{ID: uuid.New(), Email: "a@example.test"})
	if target.DisplayName != "" {
		t.Fatal("NULL name must map to empty")
	}
}

type pagedSource struct{ rows []Row }

func (s pagedSource) Page(_ context.Context, after *uuid.UUID, limit int) ([]Row, error) {
	var out []Row
	for _, r := range s.rows {
		if after == nil || strings.Compare(r.ID.String(), after.String()) > 0 {
			out = append(out, r)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

type memSink struct {
	seen   map[uuid.UUID]identity.BackfillTarget
	fail   map[uuid.UUID]error
	writes int
}

func (m *memSink) BackfillUser(_ context.Context, t identity.BackfillTarget, dryRun bool) (identity.BackfillResult, error) {
	if err := m.fail[t.ID]; err != nil {
		return identity.BackfillResult{}, err
	}
	prev, ok := m.seen[t.ID]
	var res identity.BackfillResult
	switch {
	case !ok:
		res.Inserted = true
	case prev.Active != t.Active || prev.Verified != t.Verified:
		res.Updated = true
	}
	if !dryRun && (res.Inserted || res.Updated) {
		m.seen[t.ID] = t
		m.writes++
	}
	return res, nil
}

func rowsFor(n int) []Row {
	var rows []Row
	for i := 0; i < n; i++ {
		rows = append(rows, Row{ID: uuid.New(), Email: "u@example.test", Active: true})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID.String() < rows[j].ID.String() })
	return rows
}

func TestRunIsIdempotentAndPagesEveryRow(t *testing.T) {
	src := pagedSource{rows: rowsFor(7)}
	sink := &memSink{seen: map[uuid.UUID]identity.BackfillTarget{}}
	rep, err := Run(context.Background(), src, sink, Options{BatchSize: 3})
	if err != nil || rep.Seen != 7 || rep.Inserted != 7 || rep.SkippedTotal() != 0 {
		t.Fatalf("first run: %+v %v", rep, err)
	}
	writes := sink.writes
	rep, err = Run(context.Background(), src, sink, Options{BatchSize: 3})
	if err != nil || rep.Inserted != 0 || rep.Updated != 0 || rep.Unchanged != 7 || sink.writes != writes {
		t.Fatalf("rerun: %+v %v writes %d->%d", rep, err, writes, sink.writes)
	}
}

func TestRunDryRunWritesNothing(t *testing.T) {
	sink := &memSink{seen: map[uuid.UUID]identity.BackfillTarget{}}
	rep, err := Run(context.Background(), pagedSource{rows: rowsFor(4)}, sink, Options{DryRun: true})
	if err != nil || rep.Inserted != 4 || sink.writes != 0 || len(sink.seen) != 0 {
		t.Fatalf("dry run: %+v %v", rep, err)
	}
}

func TestRunCountsSkipsByClassAndContinues(t *testing.T) {
	rows := rowsFor(5)
	rows[0].PasswordHash = str("$argon2id$bogus")
	sink := &memSink{seen: map[uuid.UUID]identity.BackfillTarget{}, fail: map[uuid.UUID]error{
		rows[1].ID: identity.ErrEmailCollision, rows[2].ID: identity.ErrIdentityConflict, rows[3].ID: errors.New("db down"),
	}}
	rep, err := Run(context.Background(), pagedSource{rows: rows}, sink, Options{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{SkipUnknownHashFormat: 1, SkipEmailCollision: 1, SkipIdentityConflict: 1, SkipStoreError: 1}
	for class, n := range want {
		if rep.Skipped[class] != n {
			t.Fatalf("class %s = %d, want %d (%v)", class, rep.Skipped[class], n, rep.Skipped)
		}
	}
	if rep.Inserted != 1 || rep.SkippedTotal() != 4 {
		t.Fatalf("report: %+v", rep)
	}
}

func TestReportLinesCarryCountsOnly(t *testing.T) {
	secretHash := "$2b$12$" + strings.Repeat("a", 53)
	rows := rowsFor(2)
	rows[0].PasswordHash = &secretHash
	rows[0].Email = "someone@example.test"
	rows[1].PasswordHash = str("$argon2id$bogus")
	sink := &memSink{seen: map[uuid.UUID]identity.BackfillTarget{}}
	rep, _ := Run(context.Background(), pagedSource{rows: rows}, sink, Options{})
	out := strings.Join(rep.Lines(), "\n")
	for _, leak := range []string{secretHash, "argon2", "someone@", rows[0].ID.String(), rows[1].ID.String()} {
		if strings.Contains(out, leak) {
			t.Fatal("report line carries row data")
		}
	}
	if !strings.Contains(out, "platform_role_granted=0") || !strings.Contains(out, "skipped.unknown_hash_format=1") {
		t.Fatalf("report incomplete:\n%s", out)
	}
}

func TestRunCopiesTheAllZeroID(t *testing.T) {
	rows := append([]Row{{ID: uuid.Nil, Email: "z@example.test", Active: true}}, rowsFor(3)...)
	sink := &memSink{seen: map[uuid.UUID]identity.BackfillTarget{}}
	rep, err := Run(context.Background(), pagedSource{rows: rows}, sink, Options{BatchSize: 2})
	if err != nil || rep.Seen != 4 || rep.Inserted != 4 {
		t.Fatalf("report: %+v %v", rep, err)
	}
	if _, ok := sink.seen[uuid.Nil]; !ok {
		t.Fatal("all-zero id row was not written")
	}
}

func TestMapCarriesSuperuserTokenVersionAndLastLogin(t *testing.T) {
	at := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	target, class, _ := Map(Row{ID: uuid.New(), Email: "a@example.test", Active: true, Superuser: true, TokenVersion: 4, LastLoginAt: &at})
	if class != "" || !target.Superuser || target.TokenVersion != 4 || target.LastLoginAt == nil || !target.LastLoginAt.Equal(at) {
		t.Fatalf("not carried: %+v class %q", target, class)
	}
	plain, _, _ := Map(Row{ID: uuid.New(), Email: "b@example.test", Active: true})
	if plain.Superuser || plain.TokenVersion != 0 || plain.LastLoginAt != nil {
		t.Fatalf("defaults wrong: %+v", plain)
	}
}

func TestPlatformAdminRoleNameIsTheCatalogName(t *testing.T) {
	role, err := policycatalog.Default().Role(identity.PlatformAdminRole)
	if err != nil || role.Scope != policycatalog.ScopePlatform {
		t.Fatalf("role key is not a platform role of the catalog: %+v %v", role, err)
	}
}
