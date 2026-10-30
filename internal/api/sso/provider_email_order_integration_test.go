//go:build integration

package sso_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

const domainDeniedText = "Email domain is not allowed for this provider"

// domainDeniedLog is the server log line of a refused domain.
const domainDeniedLog = "sso sign-in refused: email domain is not allowed for this provider"

// syncBuffer is a log sink the stack's handlers write to while a test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// ssoAnswer is one callback answer plus the sso_login audit rows its provider holds after it.
type ssoAnswer struct {
	status int
	body   map[string]any
	audits []string // "<error_message>|<request_metadata>" of each failure row
	logs   string
}

func providerAudits(t *testing.T, ctx context.Context, st stack, providerID uuid.UUID) []string {
	t.Helper()
	rows, err := st.pool.Query(ctx, `SELECT coalesce(error_message, ''), coalesce(request_metadata::text, '') FROM audit_logs
WHERE resource_id = $1 AND action = 'sso_login' AND status = 'failure' ORDER BY created_at`, providerID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var message, meta string
		if err := rows.Scan(&message, &meta); err != nil {
			t.Fatal(err)
		}
		out = append(out, message+"|"+strings.ReplaceAll(meta, " ", ""))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// sameAnswer: a linked account and an unknown one get the same status and body.
func sameAnswer(t *testing.T, linked, unknown ssoAnswer) {
	t.Helper()
	if linked.status != unknown.status || !reflect.DeepEqual(linked.body, unknown.body) {
		t.Fatalf("linked %d %v, unknown %d %v: the answers differ", linked.status, linked.body, unknown.status, unknown.body)
	}
}

func wantInvalidProviderEmail(t *testing.T, a ssoAnswer, stage string) {
	t.Helper()
	detail, _ := a.body["detail"].(map[string]any)
	if a.status != http.StatusBadRequest || detail["reason"] != "invalid_provider_email" || a.body["access_token"] != nil {
		t.Fatalf("status=%d body=%v, want 400 invalid_provider_email", a.status, a.body)
	}
	if len(a.audits) != 1 || !strings.Contains(a.audits[0], `"stage":"`+stage+`"`) {
		t.Fatalf("audit rows %q, want one failure row with stage %s", a.audits, stage)
	}
	if strings.Contains(fmt.Sprint(a.body), emailLeakMarker) || strings.Contains(a.audits[0], emailLeakMarker) {
		t.Fatal("the address is in the answer or the audit row")
	}
	if strings.Contains(a.logs, emailLeakMarker) {
		t.Fatal("the address is in a log line (any level, Debug included)")
	}
}

func wantDomainDenied(t *testing.T, a ssoAnswer, protocol string) {
	t.Helper()
	if a.status != http.StatusForbidden || !reflect.DeepEqual(a.body, map[string]any{"detail": domainDeniedText}) {
		t.Fatalf("status=%d body=%v, want 403 {detail: %q}", a.status, a.body, domainDeniedText)
	}
	if len(a.audits) != 0 {
		t.Fatalf("audit rows %q, want none (the domain refusal writes no audit row)", a.audits)
	}
	var lines []string
	for _, line := range strings.Split(a.logs, "\n") {
		if strings.Contains(line, domainDeniedLog) {
			lines = append(lines, line)
		}
	}
	if len(lines) != 1 || !strings.Contains(lines[0], "protocol="+protocol) || !strings.Contains(lines[0], "domain=other.example") {
		t.Fatalf("domain refusal log lines %q, want one naming protocol=%s and domain=other.example", lines, protocol)
	}
	if strings.Contains(a.logs, "person@") {
		t.Fatal("the address is in the log")
	}
}

func oidcDomainRun(t *testing.T, email string, linked bool) ssoAnswer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	idp := startFakeIdP(t)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	config := fmt.Sprintf(`{"client_id":"test-client","issuer":%q,"token_endpoint":%q,"jwks_uri":%q,"userinfo_endpoint":%q,"scopes":["openid","email"]}`,
		idp.server.URL, idp.server.URL+"/token", idp.server.URL+"/jwks", idp.server.URL+"/userinfo")
	domains := `["allowed.example"]`
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oidc", status: "active", config: config, autoProvision: true, allowedDomains: &domains, clientSecretEncryptedFallback: "s",
	})
	if linked {
		seedBound(t, ctx, st.pool, providerID, "oidc", "linked.person@allowed.example", "subject-1")
	}
	status, body := oidcSubjectCallback(t, st, idp, providerID, email, "subject-1")
	return ssoAnswer{status: status, body: body, audits: providerAudits(t, ctx, st, providerID), logs: st.logs.String()}
}

func samlDomainRun(t *testing.T, email string, linked bool) ssoAnswer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	idp := newSAMLIdP(t)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":%q,"sp_acs_url":%q}`,
		idp.idp.MetadataURL.String(), idp.certConfigValue(), nameIDSP, "https://sp.test/api/v1/auth/saml/{provider_id}/acs")
	domains := `["allowed.example"]`
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "saml", status: "active", config: config, autoProvision: true, allowedDomains: &domains,
	})
	if linked {
		seedBound(t, ctx, st.pool, providerID, "saml", "linked.person@allowed.example", "name-1")
	}
	status, body := samlSignIn(t, st, idp, providerID, nameIDSP, email, "name-1")
	return ssoAnswer{status: status, body: body, audits: providerAudits(t, ctx, st, providerID), logs: st.logs.String()}
}

// An unusable provider address whose domain is not on the allowlist is refused by the email rule (400
// invalid_provider_email, one audit row), not by the domain check; a linked account gets the unknown one's answer.
func TestProviderEmailOrderEmailRuleBeforeDomainCheck(t *testing.T) {
	for name, email := range map[string]string{
		"replacement char": emailLeakMarker + "�@other.example",
		"c1":               emailLeakMarker + "\u0085x@other.example",
	} {
		for protocol, run := range map[string]func(*testing.T, string, bool) ssoAnswer{"oidc": oidcDomainRun, "saml": samlDomainRun} {
			t.Run(protocol+"/"+name, func(t *testing.T) {
				linked, unknown := run(t, email, true), run(t, email, false)
				sameAnswer(t, linked, unknown)
				wantInvalidProviderEmail(t, linked, "provisioning")
				wantInvalidProviderEmail(t, unknown, "provisioning")
			})
		}
	}
}

// The OIDC and SAML domain refusal names no domain: the fixed 403 text auth-service answers, no audit row; a linked
// account gets the unknown one's answer.
func TestProviderEmailOrderDomainRefusalNamesNoDomain(t *testing.T) {
	for protocol, run := range map[string]func(*testing.T, string, bool) ssoAnswer{"oidc": oidcDomainRun, "saml": samlDomainRun} {
		t.Run(protocol, func(t *testing.T) {
			linked, unknown := run(t, "person@other.example", true), run(t, "person@other.example", false)
			sameAnswer(t, linked, unknown)
			wantDomainDenied(t, linked, protocol)
			wantDomainDenied(t, unknown, protocol)
		})
	}
}

// An OAuth profile email with a lone surrogate is refused by the email rule (400 invalid_provider_email), before the
// verified-email check that the surrogate would otherwise fail; a linked account gets the unknown one's answer.
func TestProviderEmailOrderOAuthSurrogateIsTheEmailRule(t *testing.T) {
	run := func(linked bool) ssoAnswer {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()
		st, _, providerID, _ := oauthStack(t, ctx, oauthProfileWith("7", emailLeakMarker+`\ud800@example.test`, "Fay", ""))
		if linked {
			seedBound(t, ctx, st.pool, providerID, "gitlab", "linked.fay@example.test", "7")
		}
		status, body := oauthCallbackOnce(t, st, providerID)
		return ssoAnswer{status: status, body: body, audits: providerAudits(t, ctx, st, providerID), logs: st.logs.String()}
	}
	linked, unknown := run(true), run(false)
	sameAnswer(t, linked, unknown)
	wantInvalidProviderEmail(t, linked, "provisioning")
	wantInvalidProviderEmail(t, unknown, "provisioning")
}

// The stack's captured logger runs at Debug, so a Debug-only line is seen by the address assertions above: a refused
// refresh token writes its Debug line, and no captured line at any level carries the token or an address.
func TestTheSSOStackCapturesDebugLines(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	const token = "not-a-refresh-token-" + emailLeakMarker
	status, body := postJSON(t, st.server, "/api/v1/auth/refresh", map[string]any{"refresh_token": token})
	if status < 400 || status >= 500 {
		t.Fatalf("refresh with a bad token: status=%d body=%v, want a 4xx refusal", status, body)
	}
	var debug []string
	for _, line := range strings.Split(st.logs.String(), "\n") {
		if strings.Contains(line, "level=DEBUG") {
			debug = append(debug, line)
		}
	}
	found := false
	for _, line := range debug {
		found = found || strings.Contains(line, `msg="refresh token refused"`)
	}
	if !found {
		t.Fatalf("no Debug line captured for the refused refresh token; Debug lines: %q", debug)
	}
	if strings.Contains(st.logs.String(), emailLeakMarker) {
		t.Fatal("the refused token is in a log line")
	}
}
