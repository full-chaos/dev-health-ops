package oidc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

type recordedDenials struct {
	rows []DenialRow
	err  error
}

func (r *recordedDenials) Denied(_ context.Context, row DenialRow) error {
	r.rows = append(r.rows, row)
	return r.err
}

func resolveAudited(t *testing.T, protocol string, linked bool, list []string, p profile, denials *recordedDenials) (refusal, *orderIdentities, Provider, *Error) {
	t.Helper()
	ids := &orderIdentities{linked: linked}
	svc := &Service{cfg: Config{Identities: ids, Memberships: resolveMembers{found: true, role: "member"},
		Sessions: &resolveSessions{}, MembershipJIT: &resolveJIT{}, DenialAudit: denials}}
	row := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: protocol, AutoProvision: true, AllowedDomains: list}
	_, failure := svc.resolve(context.Background(), row, validated{profile: p}, nil)
	return refusalOf(t, failure), ids, row, failure
}

// An OAuth domain refusal writes go-api's denial row (recordSSOAuthenticatedDenial, stage domain_check) once, for a
// linked and an unknown key alike, before any identity lookup; the message names the asserted domain (go-api
// oauth.go: the text after the last "@", empty when there is none), sanitized as go-api sanitizes it.
func TestOAuthDomainRefusalWritesOneDenialRow(t *testing.T) {
	want403 := refusal{status: http.StatusForbidden, reason: ReasonDomainNotAllowed, detail: oauthDomainDeniedBody}
	for email, domain := range map[string]string{"u@other.test": "other.test", "u@": "", "u.example.test": "", "a@b@Other.TEST": "Other.TEST"} {
		for _, linked := range []bool{true, false} {
			denials := &recordedDenials{}
			got, ids, row, _ := resolveAudited(t, "oauth_gitlab", linked, []string{"example.test"}, profile{Email: email, EmailVerified: true}, denials)
			if got != want403 || ids.calls != 0 {
				t.Fatalf("%s linked=%v: %+v with %d store calls, want %+v and none", email, linked, got, ids.calls, want403)
			}
			want := DenialRow{OrgID: row.OrgID, ProviderID: row.ID, Protocol: "oauth", Stage: "domain_check",
				ErrorMessage: fmt.Sprintf("Email domain '%s' is not allowed for this provider", domain)}
			if len(denials.rows) != 1 || denials.rows[0] != want {
				t.Fatalf("%s linked=%v: denial rows %+v, want exactly %+v", email, linked, denials.rows, want)
			}
		}
	}
}

// No other answer writes the row: an ID-token OIDC domain refusal (go-api writes none there), an allowed domain,
// no list, a missing or unusable email.
func TestOnlyTheOAuthDomainRefusalWritesADenialRow(t *testing.T) {
	for name, tc := range map[string]struct {
		protocol string
		list     []string
		email    string
	}{
		"oidc off the list":   {"oidc", []string{"example.test"}, "u@other.test"},
		"oauth on the list":   {"oauth_github", []string{"example.test"}, "u@example.test"},
		"oauth no list":       {"oauth_google", nil, "u@other.test"},
		"oauth missing email": {"oauth_gitlab", []string{"example.test"}, ""},
		"oauth unusable":      {"oauth_gitlab", []string{"example.test"}, "u\x00@other.test"},
	} {
		denials := &recordedDenials{}
		resolveAudited(t, tc.protocol, true, tc.list, profile{Email: tc.email, EmailVerified: true}, denials)
		if len(denials.rows) != 0 {
			t.Errorf("%s: denial rows %+v, want none", name, denials.rows)
		}
	}
}

// A failed row write is go-api's 500 (h.fail), never the 403: the refusal answers only after its row is written.
func TestADenialRowWriteFailureIsThe500(t *testing.T) {
	denials := &recordedDenials{err: errors.New("permission denied for table audit_logs")}
	got, ids, _, failure := resolveAudited(t, "oauth_gitlab", true, []string{"example.test"}, profile{Email: "u@other.test", EmailVerified: true}, denials)
	if failure == nil || failure.Status != http.StatusInternalServerError || got.reason != ReasonStoreUnavailable || ids.calls != 0 {
		t.Fatalf("%+v with %d store calls, want a 500 store_unavailable before any lookup", got, ids.calls)
	}
	if strings.Contains(got.detail, "Email domain") || strings.Contains(got.detail, "permission") {
		t.Fatalf("the 500 body names the refusal or the cause: %s", got.detail)
	}
}

type recordedExec struct {
	sql  string
	args []any
}

func (r *recordedExec) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.sql, r.args = sql, args
	return pgconn.CommandTag{}, nil
}

var insertColumns = regexp.MustCompile(`(?s)INSERT INTO\s+\S+\s*\(([^)]*)\)`)

func columnList(t *testing.T, sql string) []string {
	t.Helper()
	m := insertColumns.FindStringSubmatch(sql)
	if m == nil {
		t.Fatalf("no INSERT column list in %q", sql)
	}
	var out []string
	for _, c := range strings.Split(m[1], ",") {
		out = append(out, strings.TrimSpace(c))
	}
	return out
}

// The row is go-api's own: the same columns and the same argument values and Go types that go-api's writer
// (internal/api/audit.PGWriter) gets from recordSSOAuditOnly (internal/api/sso/login.go) for the same refusal, the
// id aside. go-api's entry is built here as login.go builds it, from oauth.go's message text.
func TestPGDenialAuditIsGoAPIsAuditRow(t *testing.T) {
	at := time.Date(2026, 10, 30, 8, 0, 0, 123, time.FixedZone("x", 3600))
	org, provider := uuid.New(), uuid.New()
	for _, domain := range []string{"other.test", "", "Ünïcode.test", strings.Repeat("d", 5000)} {
		message := fmt.Sprintf("Email domain '%s' is not allowed for this provider", domain)

		sanitized := pythonparity.SanitizeErrorTextHardened(message, 4000)
		meta := pyjson.NewObject()
		meta.Set("protocol", "oauth")
		meta.Set("stage", "domain_check")
		metaJSON, err := pyjson.Dumps(meta)
		if err != nil {
			t.Fatal(err)
		}
		goAPI := &recordedExec{}
		if _, err := (audit.PGWriter{Now: func() time.Time { return at }}).Write(context.Background(), goAPI, audit.Entry{
			OrgID: org, Action: audit.ActionSSOLogin, ResourceType: audit.ResourceSSOProvider,
			ResourceID: provider.String(), Status: "failure", ErrorMessage: &sanitized, RequestMetadata: []byte(metaJSON),
		}); err != nil {
			t.Fatal(err)
		}

		denials := &recordedDenials{}
		svc := &Service{cfg: Config{Identities: &orderIdentities{}, DenialAudit: denials}}
		if f := svc.auditDenial(context.Background(), Provider{ID: provider, OrgID: org}, "domain_check", message); f != nil {
			t.Fatal(f)
		}
		auth := &recordedExec{}
		if err := (PGDenialAudit{Exec: auth, Now: func() time.Time { return at }}).Denied(context.Background(), denials.rows[0]); err != nil {
			t.Fatal(err)
		}

		if got, want := columnList(t, auth.sql), columnList(t, goAPI.sql); !reflect.DeepEqual(got, want) {
			t.Fatalf("columns %v, go-api %v", got, want)
		}
		if !strings.Contains(auth.sql, "INSERT INTO public.audit_logs") {
			t.Fatalf("the row is not go-api's public.audit_logs: %q", auth.sql)
		}
		if len(auth.args) != len(goAPI.args) {
			t.Fatalf("%d arguments, go-api %d", len(auth.args), len(goAPI.args))
		}
		for i := 1; i < len(goAPI.args); i++ {
			if !reflect.DeepEqual(auth.args[i], goAPI.args[i]) {
				t.Errorf("domain %.20q: argument %d = %#v, go-api %#v", domain, i+1, auth.args[i], goAPI.args[i])
			}
		}
	}
}
