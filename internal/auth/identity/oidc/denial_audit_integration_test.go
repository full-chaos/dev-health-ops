//go:build integration

package oidc

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type auditRow struct {
	orgID, user, action, resourceType, resourceID, description, changes, metadata, status, message, createdAt string
}

func (h *harness) auditRows(t *testing.T, where string, args ...any) []auditRow {
	t.Helper()
	rows, err := h.env.admin.Query(context.Background(), `SELECT org_id::text, coalesce(user_id::text, '<null>'), action, resource_type,
	coalesce(resource_id, '<null>'), coalesce(description, '<null>'), coalesce(changes::text, '<null>'), coalesce(request_metadata::text, '<null>'),
	status, coalesce(error_message, '<null>'), to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US')
FROM public.audit_logs WHERE `+where+` ORDER BY created_at, id`, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []auditRow
	for rows.Next() {
		var r auditRow
		if err := rows.Scan(&r.orgID, &r.user, &r.action, &r.resourceType, &r.resourceID, &r.description, &r.changes, &r.metadata,
			&r.status, &r.message, &r.createdAt); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// goAPIDenialRow writes, through go-api's own writer and as recordSSOAuditOnly builds it (internal/api/sso
// login.go), the row go-api writes for an OAuth domain refusal of domain, under a marker resource id, and reads it back.
func (h *harness) goAPIDenialRow(t *testing.T, orgID uuid.UUID, domain string) auditRow {
	t.Helper()
	marker := "go-api-" + uuid.NewString()
	sanitized := pythonparity.SanitizeErrorTextHardened(fmt.Sprintf("Email domain '%s' is not allowed for this provider", domain), 4000)
	meta := pyjson.NewObject()
	meta.Set("protocol", "oauth")
	meta.Set("stage", "domain_check")
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (audit.PGWriter{Now: func() time.Time { return h.now }}).Write(context.Background(), h.env.admin, audit.Entry{
		OrgID: orgID, Action: audit.ActionSSOLogin, ResourceType: audit.ResourceSSOProvider, ResourceID: marker,
		Status: "failure", ErrorMessage: &sanitized, RequestMetadata: []byte(metaJSON),
	}); err != nil {
		t.Fatal(err)
	}
	rows := h.auditRows(t, `resource_id = $1`, marker)
	if len(rows) != 1 {
		t.Fatalf("go-api rows %+v", rows)
	}
	return rows[0]
}

// A new and a linked OAuth key whose asserted domain is off the provider's list each get the OAuth 403 and exactly
// one public.audit_logs row, the row go-api writes for the same refusal (its own writer, same database), the
// resource id aside; nothing else is written.
func TestOAuthDomainRefusalWritesGoAPIsAuditRow(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	if _, err := h.env.admin.Exec(context.Background(), `UPDATE public.sso_providers SET allowed_domains = '["corp.example"]'::json WHERE id = $1`, provider); err != nil {
		t.Fatal(err)
	}
	linked := fakeAccountWith("linked-"+uuid.NewString()[:8]+"@other.example", true)
	h.seedOAuthMember(t, h.orgID, "oauth_gitlab", linked)
	for name, account := range map[string]fakeAccount{
		"new":    fakeAccountWith("new-"+uuid.NewString()[:8]+"@other.example", true),
		"linked": linked,
	} {
		t.Run(name, func(t *testing.T) {
			before := h.snapshot(t)
			_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), account, true)
			wantDenied(t, failure, http.StatusForbidden, ReasonDomainNotAllowed)
			if detail, ok := failure.Detail.(map[string]string); !ok || detail["reason"] != "oauth_domain_not_allowed" {
				t.Fatalf("detail %#v, want go-api's OAuth object", failure.Detail)
			}
			wantUnchanged(t, h, before)
			got := h.auditRows(t, `resource_id = $1`, provider.String())
			if len(got) != 1 {
				t.Fatalf("audit rows %+v, want exactly one", got)
			}
			want := h.goAPIDenialRow(t, h.orgID, "other.example")
			want.resourceID = provider.String()
			if got[0] != want {
				t.Fatalf("row %+v\ngo-api %+v", got[0], want)
			}
			if _, err := h.env.admin.Exec(context.Background(), `DELETE FROM public.audit_logs WHERE resource_id = $1 OR resource_id LIKE 'go-api-%'`, provider.String()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Without the INSERT privilege the refusal is go-api's 500 (h.fail), not the 403, and no row is written: the row
// is written before the refusal answers.
func TestOAuthDomainRefusalAuditWriteFailureIsThe500(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	if _, err := h.env.admin.Exec(context.Background(), `UPDATE public.sso_providers SET allowed_domains = '["corp.example"]'::json WHERE id = $1`, provider); err != nil {
		t.Fatal(err)
	}
	var role string
	if err := h.env.runtime.QueryRow(context.Background(), `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	quoted := `"` + strings.ReplaceAll(role, `"`, `""`) + `"`
	if _, err := h.env.admin.Exec(context.Background(), `REVOKE INSERT ON public.audit_logs FROM `+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := h.env.admin.Exec(context.Background(), `GRANT INSERT (`+strings.Join(authschema.PublicAuditWriteColumns(), ", ")+`) ON public.audit_logs TO `+quoted); err != nil {
			t.Errorf("restore the audit grant: %v", err)
		}
	})
	_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), fakeAccountWith("u-"+uuid.NewString()[:8]+"@other.example", true), true)
	if failure == nil || failure.Status != http.StatusInternalServerError || failure.Reason != ReasonStoreUnavailable {
		t.Fatalf("failure %+v, want 500 store_unavailable", failure)
	}
	if got := h.auditRows(t, `resource_id = $1`, provider.String()); len(got) != 0 {
		t.Fatalf("audit rows %+v, want none", got)
	}
}
