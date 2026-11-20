//go:build integration

package apiservice

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// lrAuditRow is one auth.security_audit_events row the decision point wrote.
type lrAuditRow struct{ eventType, outcome, action, resourceType string }

func lrAuditAfter(t *testing.T, ctx context.Context, w *wcWorld, after int64) []lrAuditRow {
	t.Helper()
	rows, err := w.env.Admin.Query(ctx, `SELECT event_type, outcome, coalesce(attributes->>'action_key', ''), coalesce(attributes->>'resource_type', '')
FROM auth.security_audit_events WHERE id > $1 ORDER BY id`, after)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []lrAuditRow
	for rows.Next() {
		var r lrAuditRow
		if err := rows.Scan(&r.eventType, &r.outcome, &r.action, &r.resourceType); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func lrAuditHead(t *testing.T, ctx context.Context, w *wcWorld) int64 {
	t.Helper()
	var id int64
	if err := w.env.Admin.QueryRow(ctx, `SELECT coalesce(max(id), 0) FROM auth.security_audit_events`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// The role question a caller with a live go-api impersonation row is asked on an enforced platform row, on the real
// decision point (m_on, the admin cell on): the exact questions go-api sends per request and the audit rows auth-service
// writes for them. A platform row asks its platform action once; an admin-or row asks one batch (the platform action,
// and members.manage in the token organization when the token names one). Each question is a high-risk action, so each
// writes one auth.authorize.high_risk_decided row and no delegated row (the bearer is the caller's own). A demoted caller
// (no platform role) gets the legacy deny, the answer of the cell off; a platform admin gets the session refusal: its
// allow never serves the row.
func TestAdminLiveRowRoleQuestionRealPDPCallsAndAudit(t *testing.T) {
	ctx := context.Background()
	w := newWCWorld(t, ctx, wcModes[1])
	callers := []struct {
		label                     string
		publicSuperuser, assigned bool
	}{{"demoted", false, false}, {"platform_admin", true, true}}
	type cellSpec struct {
		name, method, path, body string
		tokenOrg                 bool
		// demotedBody is the legacy deny body of the demoted caller.
		demotedBody string
	}
	cells := []cellSpec{
		{"platform_row_orgless", http.MethodGet, "/api/v1/admin/orgs", "", false, `{"detail":"Organization context required"}`},
		{"platform_row_org", http.MethodGet, "/api/v1/admin/orgs", "", true, `{"detail":"Superuser access required"}`},
		{"admin_or_write_org", http.MethodPost, "/api/v1/admin/users", `{"email":"lr-org@example.test"}`, true, `{"detail":"Admin access required"}`},
		{"admin_or_write_orgless", http.MethodPost, "/api/v1/admin/users", `{"email":"lr-orgless@example.test"}`, false, `{"detail":"Organization context required"}`},
	}
	for _, caller := range callers {
		for _, cell := range cells {
			t.Run(caller.label+"/"+cell.name, func(t *testing.T) {
				// The caller is a member (role member) of its org; its live go-api impersonation row targets a member of
				// the same org, so the impersonation scope serves that org and the questions name it.
				user := w.user(t, ctx, "lr."+caller.label+"."+cell.name, caller.publicSuperuser, caller.assigned)
				org := w.org(t, ctx, "lr."+caller.label+"."+cell.name+".org")
				w.member(t, ctx, user, org, "member")
				target := w.user(t, ctx, "lr."+caller.label+"."+cell.name+".target", false, false)
				w.member(t, ctx, target, org, "member")
				pgseed.ImpersonationSession(ctx, t, w.env.Admin, uuid.NewString(), user.String(), target.String(), org.String(), "member",
					time.Now().Add(time.Hour), nil)
				sid := w.sessionRow(t, ctx, user)
				claims := edtokens.Claims{UserID: user, SessionID: &sid}
				if cell.tokenOrg {
					claims.OrgID = org.String()
				}
				token := w.key.Tokens.MustMint(t, claims)
				c := &wcCaller{spec: wcSpec{label: "lr_" + caller.label}, user: user, sideUser: [2]uuid.UUID{user, user}, token: [2]string{token, token}}
				row := wcRow{name: cell.name, method: cell.method, path: cell.path, body: cell.body}

				off, err := w.serve(w.main.handlers[wcOff], w.main.plane, w.main.admins[wcOff], wcOff, c, row)
				if err != nil {
					t.Fatal(err)
				}
				head := lrAuditHead(t, ctx, w)
				on, err := w.serve(w.main.handlers[wcOn], w.main.plane, w.main.admins[wcOn], wcOn, c, row)
				if err != nil {
					t.Fatal(err)
				}
				audited := lrAuditAfter(t, ctx, w, head)
				t.Logf("LIVEROW|%s|%s|off %d %s|on %d %s|pdp=%v|audit=%+v", caller.label, cell.name, off.status, wcShort(off.body), on.status,
					wcShort(on.body), on.decisions, audited)

				demoted := !caller.assigned
				platformAction := "admin.orgs"
				if cell.method == http.MethodPost {
					platformAction = "admin.users"
				}
				// The demoted caller (org role member) holds neither question's action; the platform admin holds both
				// (members.manage by allowed_by_platform_admin).
				allowed := map[bool]string{true: "false", false: "true"}[demoted]
				type question struct{ prefix, action, resourceType string }
				want := []question{{platformAction + " platform allowed=" + allowed + " ", platformAction, "platform"}}
				if cell.method == http.MethodPost && cell.tokenOrg {
					want = append(want, question{"members.manage organization/" + org.String() + " allowed=" + allowed + " ", "members.manage", "organization"})
				}
				if len(on.decisions) != len(want) {
					t.Fatalf("on side asked %v, want exactly %d role question(s) %v", on.decisions, len(want), want)
				}
				for i, q := range want {
					if !strings.HasPrefix(on.decisions[i], q.prefix) {
						t.Errorf("on side question %d = %q, want %q...", i, on.decisions[i], q.prefix)
					}
				}

				var highRisk, delegated []lrAuditRow
				for _, r := range audited {
					switch r.eventType {
					case audit.EventAuthorizeHighRiskDecided:
						highRisk = append(highRisk, r)
					case audit.EventAuthorizeDelegatedDecided:
						delegated = append(delegated, r)
					}
				}
				if len(delegated) != 0 {
					t.Errorf("delegated_decided rows %+v, want none: the bearer is the caller's own", delegated)
				}
				if len(highRisk) != len(want) {
					t.Fatalf("high_risk_decided rows %+v, want exactly %d (one per role question)", highRisk, len(want))
				}
				for i, q := range want {
					action, err := policycatalog.Default().Action(q.action)
					if err != nil || action.RiskClass != policycatalog.RiskHigh {
						t.Fatalf("%s: risk class %q (%v), want high", q.action, action.RiskClass, err)
					}
					outcome := audit.OutcomeDenied
					if strings.Contains(q.prefix, "allowed=true") {
						outcome = audit.OutcomeAllowed
					}
					if got := highRisk[i]; got.action != q.action || got.resourceType != q.resourceType || got.outcome != string(outcome) {
						t.Errorf("high_risk_decided row %d = %+v, want action %s on %s, outcome %s", i, got, q.action, q.resourceType, outcome)
					}
				}

				if demoted {
					if on.status != http.StatusForbidden || on.body != cell.demotedBody {
						t.Errorf("demoted on side %d %s, want 403 %s (the legacy deny)", on.status, on.body, cell.demotedBody)
					}
					if off.status != on.status || off.body != on.body {
						t.Errorf("demoted: on %d %s, off %d %s; want the cell-off answer", on.status, on.body, off.status, off.body)
					}
				} else if on.status != http.StatusForbidden || on.body != impersonationBody {
					t.Errorf("platform admin on side %d %s, want 403 %s", on.status, on.body, impersonationBody)
				}
				if len(on.usersAdmin) != 0 {
					t.Errorf("on side reached the user-admin client: %v", on.usersAdmin)
				}
			})
		}
	}
}
