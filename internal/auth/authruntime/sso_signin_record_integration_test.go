//go:build integration

package authruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// samlRecordEnv is the real stack with the SAML routes mounted: the record runs through ssoRoutes, on the runtime role.
type samlRecordEnv struct {
	*routesEnv
	idp      samlConflictIdP
	provider saml.Provider
	user     uuid.UUID
	org      uuid.UUID
	nameID   string
}

func newSAMLRecordEnv(t *testing.T, ctx context.Context) *samlRecordEnv {
	t.Helper()
	extra := ssoEnvironment(map[string]string{})
	extra[authconfig.EnvAppBaseURL] = samlConflictBase
	e := newRoutesEnvWith(t, ctx, extra)
	user, org := e.seed(t, ctx)
	idp := newSAMLConflictIdP(t)
	row := uuid.New()
	config := fmt.Sprintf(`{"entity_id": %q, "sso_url": "https://idp.example.test/sso", "certificate": %q}`, samlConflictIdPEntity, idp.certB64)
	nameID := "record-" + uuid.NewString()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`UPDATE public.organizations SET tier = 'enterprise' WHERE id = $1`, []any{org}},
		{`INSERT INTO public.sso_providers (id, org_id, name, protocol, status, allow_idp_initiated, config)
VALUES ($1, $2, $3, 'saml', 'active', true, $4::json)`, []any{row, org, "idp-" + row.String(), config}},
		{`UPDATE public.users SET email = $2, auth_provider = 'saml', auth_provider_id = $3, sso_provider_id = $4 WHERE id = $1`,
			[]any{user, nameID + "@example.com", nameID, row}},
	} {
		if _, err := e.admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if err := e.identity.Project(ctx, user); err != nil {
		t.Fatalf("project the principal: %v", err)
	}
	store, err := saml.NewPGStore(e.runtime, authschemaIdent(t))
	if err != nil {
		t.Fatal(err)
	}
	provider, err := store.Provider(ctx, row)
	if err != nil {
		t.Fatal(err)
	}
	return &samlRecordEnv{routesEnv: e, idp: idp, provider: provider, user: user, org: org, nameID: nameID}
}

func authschemaIdent(t *testing.T) authschema.ValidatedIdentifier {
	t.Helper()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func (e *samlRecordEnv) acs(t *testing.T, idp samlConflictIdP) answer {
	t.Helper()
	body, err := json.Marshal(map[string]string{"SAMLResponse": idp.response(t, e.provider, e.nameID)})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, e.front.URL+"/api/v1/auth/saml/"+e.provider.ID.String()+"/acs", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := e.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return answer{status: response.StatusCode, body: raw, header: response.Header}
}

func (e *samlRecordEnv) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *samlRecordEnv) successRows(t *testing.T, ctx context.Context) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM public.audit_logs WHERE action = 'sso_login' AND status = 'success'`)
}

func (e *samlRecordEnv) sessionCount(t *testing.T, ctx context.Context) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, e.user)
}

func (e *samlRecordEnv) lastLoginSet(t *testing.T, ctx context.Context) bool {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM public.sso_providers WHERE id = $1 AND last_login_at IS NOT NULL AND updated_at = last_login_at`, e.provider.ID) == 1
}

// A SAML sign-in through the routes configure mounts, on the runtime role with the migrate grants: the answer carries the
// tokens, the provider row holds last_login_at, and the one audit row is the row go-api's own writer writes for
// finishSSOLogin, its time aside.
func TestSAMLSignInOnTheRealStackWritesGoAPIsSuccessRecord(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newSAMLRecordEnv(t, ctx)
	got := e.acs(t, e.idp)
	if got.status != http.StatusOK || !strings.Contains(string(got.body), `"access_token"`) {
		t.Fatalf("ACS = %d %s", got.status, got.body)
	}
	if !e.lastLoginSet(t, ctx) {
		t.Fatal("last_login_at and updated_at were not written")
	}
	meta := pyjson.NewObject()
	meta.Set("provider_id", e.provider.ID.String())
	meta.Set("protocol", "saml")
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		t.Fatal(err)
	}
	marker := "go-api-" + uuid.NewString()
	if _, err := (audit.PGWriter{}).Write(ctx, e.admin, audit.Entry{OrgID: e.org, UserID: &e.user, Action: audit.ActionSSOLogin,
		ResourceType: audit.ResourceSession, ResourceID: marker, RequestMetadata: []byte(metaJSON)}); err != nil {
		t.Fatal(err)
	}
	const row = `SELECT concat_ws('|', org_id::text, coalesce(user_id::text, '<null>'), action, resource_type, coalesce(description, '<null>'),
	coalesce(changes::text, '<null>'), coalesce(request_metadata::text, '<null>'), status, coalesce(error_message, '<null>'))
FROM public.audit_logs WHERE resource_id = $1`
	var mine, goAPI string
	if err := e.admin.QueryRow(ctx, row, e.user.String()).Scan(&mine); err != nil {
		t.Fatalf("the record's audit row: %v", err)
	}
	if err := e.admin.QueryRow(ctx, row, marker).Scan(&goAPI); err != nil {
		t.Fatal(err)
	}
	if mine != goAPI {
		t.Fatalf("row %q\ngo-api %q", mine, goAPI)
	}
	if n := e.successRows(t, ctx); n != 2 {
		t.Fatalf("%d success rows, want the sign-in's and the marker's", n)
	}
}

// An assertion signed by another key is refused: no session, no row, last_login_at untouched.
func TestSAMLRefusalOnTheRealStackWritesNoSuccessRecord(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newSAMLRecordEnv(t, ctx)
	if got := e.acs(t, newSAMLConflictIdP(t)); got.status == http.StatusOK {
		t.Fatalf("ACS = %d %s, want a refusal", got.status, got.body)
	}
	if e.lastLoginSet(t, ctx) || e.successRows(t, ctx) != 0 || e.sessionCount(t, ctx) != 0 {
		t.Fatal("a refused sign-in was recorded")
	}
}

// Without the grant the record fails: the 500 carries no token, and the session stays undelivered.
func TestSAMLRecordWriteFailureOnTheRealStackIsThe500(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newSAMLRecordEnv(t, ctx)
	var role string
	if err := e.runtime.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `REVOKE UPDATE ON public.sso_providers FROM "`+strings.ReplaceAll(role, `"`, `""`)+`"`); err != nil {
		t.Fatal(err)
	}
	got := e.acs(t, e.idp)
	if got.status != http.StatusInternalServerError || strings.Contains(string(got.body), "token") {
		t.Fatalf("ACS = %d %s, want a 500 without a token", got.status, got.body)
	}
	if e.successRows(t, ctx) != 0 || e.sessionCount(t, ctx) != 1 {
		t.Fatalf("rows %d, sessions %d", e.successRows(t, ctx), e.sessionCount(t, ctx))
	}
}

// public.memberships.user_id is a foreign key, so a principal whose public.users row is gone has no membership: it is
// refused at the membership check, before any session, and no record runs.
func TestSAMLPrincipalWithoutAPublicUserIsRefusedBeforeARecord(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newSAMLRecordEnv(t, ctx)
	if got := e.acs(t, e.idp); got.status != http.StatusOK {
		t.Fatalf("first sign-in = %d %s", got.status, got.body)
	}
	before := e.sessionCount(t, ctx)
	rows := e.successRows(t, ctx)
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.users WHERE id = $1`, e.user); err != nil {
		t.Fatal(err)
	}
	if got := e.acs(t, e.idp); got.status == http.StatusOK {
		t.Fatalf("ACS = %d %s, want a refusal", got.status, got.body)
	}
	if e.sessionCount(t, ctx) != before || e.successRows(t, ctx) != rows {
		t.Fatalf("sessions %d -> %d, rows %d -> %d", before, e.sessionCount(t, ctx), rows, e.successRows(t, ctx))
	}
}
