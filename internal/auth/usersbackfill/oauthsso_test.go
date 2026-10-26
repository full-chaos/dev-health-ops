package usersbackfill

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func cand(user, row uuid.UUID, provider, sub, base string) OAuthSSOCandidate {
	return OAuthSSOCandidate{UserID: user, AuthProvider: provider, Subject: sub, RowID: row, BaseURL: base}
}

func TestOAuthSSOKeysCarriesRowKeyInSocialNamespace(t *testing.T) {
	row := uuid.New()
	for provider, issuer := range map[string]string{
		"github": "https://github.com", "gitlab": "https://gitlab.com", "google": "https://accounts.google.com"} {
		key, selfHosted := OAuthSSOKey(cand(uuid.New(), row, provider, "42", ""))
		if selfHosted || key == nil {
			t.Fatalf("%s: key=%v selfHosted=%v", provider, key, selfHosted)
		}
		want := identity.ExternalKey{Issuer: issuer, Provider: identity.Provider(row.String()), Subject: "42"}
		if *key != want {
			t.Fatalf("%s: key %+v, want %+v", provider, *key, want)
		}
	}
}

func TestOAuthSSOKeysGitLabComBaseURLIsSocialNamespace(t *testing.T) {
	key, selfHosted := OAuthSSOKey(cand(uuid.New(), uuid.New(), "gitlab", "7", "https://gitlab.com/"))
	if selfHosted || key == nil || key.Issuer != "https://gitlab.com" {
		t.Fatalf("key=%v selfHosted=%v", key, selfHosted)
	}
}

func TestOAuthSSOKeysSelfHostedGitLabNotJoinedToGitLabCom(t *testing.T) {
	key, selfHosted := OAuthSSOKey(cand(uuid.New(), uuid.New(), "gitlab", "123", "https://git.corp.example"))
	if key != nil || !selfHosted {
		t.Fatalf("self-hosted row carried: key=%v selfHosted=%v", key, selfHosted)
	}
}

func TestOAuthSSOKeysOtherProviderIsNotOAuthSSO(t *testing.T) {
	for _, p := range []string{"local", "oidc", "saml", ""} {
		if key, sh := OAuthSSOKey(cand(uuid.New(), uuid.New(), p, "1", "")); key != nil || sh {
			t.Fatalf("%q: key=%v selfHosted=%v", p, key, sh)
		}
	}
}

type fakeCarry struct {
	keys map[uuid.UUID][]identity.ExternalKey
}

func (f *fakeCarry) CarryExtraIdentity(_ context.Context, principal uuid.UUID, key identity.ExternalKey, dryRun bool) (bool, error) {
	if !dryRun {
		f.keys[principal] = append(f.keys[principal], key)
	}
	return true, nil
}

type pagedCands struct{ rows []OAuthSSOCandidate }

func (p pagedCands) Page(_ context.Context, after *OAuthSSOCursor, limit int) ([]OAuthSSOCandidate, error) {
	start := 0
	if after != nil {
		for i, r := range p.rows {
			if r.UserID == after.User && r.RowID == after.Row {
				start = i + 1
			}
		}
	}
	end := start + limit
	if end > len(p.rows) {
		end = len(p.rows)
	}
	return p.rows[start:end], nil
}

func TestRunOAuthSSOTwoSameNamespaceRowsGiveTwoKeys(t *testing.T) {
	u, r1, r2 := uuid.New(), uuid.New(), uuid.New()
	sink := &fakeCarry{keys: map[uuid.UUID][]identity.ExternalKey{}}
	rep, err := RunOAuthSSO(context.Background(),
		pagedCands{rows: []OAuthSSOCandidate{cand(u, r1, "github", "9", ""), cand(u, r2, "github", "9", "")}}, sink, Options{BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.keys[u]) != 2 || rep.Users != 1 || rep.Linked != 2 || rep.SelfHostedUnbindable != 0 {
		t.Fatalf("keys=%d report=%+v", len(sink.keys[u]), rep)
	}
}

func TestRunOAuthSSOSelfHostedWritesNothingAndIsCountedOncePerUser(t *testing.T) {
	u := uuid.New()
	sink := &fakeCarry{keys: map[uuid.UUID][]identity.ExternalKey{}}
	rep, err := RunOAuthSSO(context.Background(),
		pagedCands{rows: []OAuthSSOCandidate{
			cand(u, uuid.New(), "gitlab", "123", "https://git.corp.example"),
			cand(u, uuid.New(), "gitlab", "123", "https://git.other.example")}}, sink, Options{BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.keys) != 0 || rep.Linked != 0 || rep.SelfHostedUnbindable != 1 || rep.Users != 1 {
		t.Fatalf("keys=%v report=%+v", sink.keys, rep)
	}
}

func TestOAuthSSOReportLinesCountsOnly(t *testing.T) {
	rep := OAuthSSOReport{Users: 3, Linked: 2, SelfHostedUnbindable: 1, Skipped: map[string]int{SkipStoreError: 1}}
	got := strings.Join(rep.Lines(), "\n")
	for _, want := range []string{"oauth_sso_users=3", "oauth_sso_keys_linked=2", "oauth_sso_selfhosted_unbindable=1", "skipped.store_error=1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in %q", want, got)
		}
	}
}

func TestOAuthSSOCountLineCountsOnly(t *testing.T) {
	line := identity.OAuthSSOCount{Total: 5, Keyed: 3, Missing: 1, SelfHostedUnbindable: 1}.Line()
	if line != "oauth_sso total=5 keyed=3 missing=1 selfhosted_unbindable=1" {
		t.Fatalf("line %q", line)
	}
	if strings.ContainsAny(line, "@-") {
		t.Fatalf("line carries an id or address: %q", line)
	}
}

func TestOAuthSSOCountSQLSelectsCountsOnly(t *testing.T) {
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	sql := identity.OAuthSSOCountSQL(authschema.Quote(schema))
	for _, bad := range []string{"u.email", "u.id,", "org_id,", "encrypted_secrets"} {
		if strings.Contains(sql[strings.LastIndex(sql, "SELECT count"):], bad) {
			t.Fatalf("final SELECT names %q", bad)
		}
	}
	if !strings.HasPrefix(strings.TrimSpace(sql), "WITH") || strings.Contains(strings.ToUpper(sql), "UPDATE ") || strings.Contains(strings.ToUpper(sql), "INSERT ") {
		t.Fatal("count SQL is not one read-only statement")
	}
}

func TestOAuthSSOReadColumnsWithinRuntimeGrantAndNoSecrets(t *testing.T) {
	grants := map[string]map[string]bool{}
	for _, g := range authschema.PublicReadColumns() {
		grants[g.Table] = map[string]bool{}
		for _, c := range g.Columns {
			grants[g.Table][c] = true
		}
	}
	for table, cols := range identity.OAuthSSOReadColumns() {
		for _, c := range cols {
			if !grants[table][c] {
				t.Fatalf("%s.%s is not granted to the runtime role", table, c)
			}
			if c == "encrypted_secrets" || c == "password_hash" {
				t.Fatalf("%s.%s must not be read", table, c)
			}
		}
	}
	if strings.Contains(oauthSSOPageSQL, "encrypted_secrets") || strings.Contains(oauthSSOPageSQL, "password_hash") {
		t.Fatal("page SQL reads a secret column")
	}
}
