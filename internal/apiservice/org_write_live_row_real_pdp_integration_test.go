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
	"github.com/full-chaos/dev-health-ops/internal/testsupport/authplane"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// orgWriteRealEnforce turns on the organization cells of the rows below.
const orgWriteRealEnforce = "org,teams,settings"

func orgWriteRealOrg(t *testing.T, ctx context.Context, w *wcWorld, org uuid.UUID) string {
	t.Helper()
	var name, updated string
	if err := w.env.Admin.QueryRow(ctx, `SELECT name, updated_at::text FROM organizations WHERE id = $1`, org).Scan(&name, &updated); err != nil {
		t.Fatal(err)
	}
	return name + "|" + updated
}

// A caller with a live go-api impersonation row and no admin.impersonate (no session context) on an enforced organization
// write row, on the real decision point and real Postgres: go-api asks the question it asks with no row, once, and
// auth-service writes one auth.authorize.high_risk_decided row per question and no delegated row (the bearer is the
// caller's own). A member is denied: it gets the cell-off answer, which on PATCH /orgs/me is the handler's own refusal
// read from its membership row, and nothing is written. An owner is allowed: it gets the session refusal (D6833), and its
// organization is not written. A token with no organization is asked admin.orgs on an Admin row and nothing on an
// AdminOrg row.
func TestOrganizationWriteLiveRowRoleQuestionRealPDPCallsAndAudit(t *testing.T) {
	ctx := context.Background()
	w := newWCWorld(t, ctx, wcModes[1])
	off, offAdmin := w.goAPI(t, w.main.plane, "", AuthProxyConfig{}, authplane.RigClientTimeout)
	on, onAdmin := w.goAPI(t, w.main.plane, orgWriteRealEnforce, AuthProxyConfig{}, authplane.RigClientTimeout)
	type question struct{ action, resource string }
	type cellSpec struct {
		name, method, path, body string
		tokenOrg                 bool
		// memberBody is the cell-off answer of a member; asked the questions go-api sends.
		memberBody string
		asked      []question
		// ownerSession: the owner is allowed, so it gets the session refusal; else it is refused as the member.
		ownerSession bool
	}
	ocr := `{"detail":"Organization context required"}`
	cells := []cellSpec{
		{"orgs_me_patch", http.MethodPatch, "/api/v1/orgs/me", `{"name":"Renamed by the cell"}`, true,
			`{"detail":"You must be an org admin or owner to update organization settings"}`, []question{{"org.write", "organization"}}, true},
		{"teams_create", http.MethodPost, "/api/v1/admin/teams", `{"name":"Cell team"}`, true,
			`{"detail":"Admin access required"}`, []question{{"teams.write", "organization"}}, true},
		{"teams_create_orgless", http.MethodPost, "/api/v1/admin/teams", `{"name":"Cell team"}`, false, ocr, nil, false},
		{"settings_create_orgless", http.MethodPost, "/api/v1/admin/settings", `{}`, false, ocr, []question{{"admin.orgs", "platform"}}, false},
	}
	for _, role := range []string{"member", "owner"} {
		for _, cell := range cells {
			t.Run(role+"/"+cell.name, func(t *testing.T) {
				user := w.user(t, ctx, "ow."+role+"."+cell.name, false, false)
				org := w.org(t, ctx, "ow."+role+"."+cell.name+".org")
				w.member(t, ctx, user, org, role)
				target := w.user(t, ctx, "ow."+role+"."+cell.name+".target", false, false)
				w.member(t, ctx, target, org, "member")
				pgseed.ImpersonationSession(ctx, t, w.env.Admin, uuid.NewString(), user.String(), target.String(), org.String(), "member",
					time.Now().Add(time.Hour), nil)
				sid := w.sessionRow(t, ctx, user)
				claims := edtokens.Claims{UserID: user, SessionID: &sid}
				if cell.tokenOrg {
					claims.OrgID = org.String()
				}
				token := w.key.Tokens.MustMint(t, claims)
				c := &wcCaller{spec: wcSpec{label: "ow_" + role}, user: user, sideUser: [2]uuid.UUID{user, user}, token: [2]string{token, token}}
				row := wcRow{name: cell.name, method: cell.method, path: cell.path, body: cell.body}

				before := orgWriteRealOrg(t, ctx, w, org)
				head := lrAuditHead(t, ctx, w)
				got, err := w.serve(on, w.main.plane, onAdmin, wcOn, c, row)
				if err != nil {
					t.Fatal(err)
				}
				audited := lrAuditAfter(t, ctx, w, head)
				afterOn := orgWriteRealOrg(t, ctx, w, org)
				legacy, err := w.serve(off, w.main.plane, offAdmin, wcOff, c, row)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("ORGLIVEROW|%s|%s|off %d %s|on %d %s|pdp=%v|audit=%+v", role, cell.name, legacy.status, wcShort(legacy.body), got.status,
					wcShort(got.body), got.decisions, audited)

				if afterOn != before {
					t.Errorf("the on side wrote the organization: %s, then %s", before, afterOn)
				}
				allowed := "false"
				if role == "owner" && cell.ownerSession {
					allowed = "true"
				}
				if len(got.decisions) != len(cell.asked) {
					t.Fatalf("on side asked %v, want exactly %d question(s) %v", got.decisions, len(cell.asked), cell.asked)
				}
				for i, q := range cell.asked {
					prefix := q.action + " " + q.resource
					if q.resource == "organization" {
						prefix += "/" + org.String()
					}
					prefix += " allowed=" + allowed + " "
					if !strings.HasPrefix(got.decisions[i], prefix) {
						t.Errorf("on side question %d = %q, want %q...", i, got.decisions[i], prefix)
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
				if len(highRisk) != len(cell.asked) {
					t.Fatalf("high_risk_decided rows %+v, want exactly %d (one per question)", highRisk, len(cell.asked))
				}
				for i, q := range cell.asked {
					action, err := policycatalog.Default().Action(q.action)
					if err != nil || action.RiskClass != policycatalog.RiskHigh {
						t.Fatalf("%s: risk class %q (%v), want high", q.action, action.RiskClass, err)
					}
					outcome := audit.OutcomeDenied
					if allowed == "true" {
						outcome = audit.OutcomeAllowed
					}
					if r := highRisk[i]; r.action != q.action || r.resourceType != q.resource || r.outcome != string(outcome) {
						t.Errorf("high_risk_decided row %d = %+v, want action %s on %s, outcome %s", i, r, q.action, q.resource, outcome)
					}
				}

				if allowed == "true" {
					if got.status != http.StatusForbidden || got.body != impersonationBody {
						t.Errorf("owner on side %d %s, want 403 %s (D6833)", got.status, got.body, impersonationBody)
					}
					if legacy.status == http.StatusForbidden {
						t.Errorf("owner off side %d %s, want the row admitted (the cell where D6833 keeps the session refusal)", legacy.status, legacy.body)
					}
					return
				}
				if got.status != http.StatusForbidden || got.body != cell.memberBody {
					t.Errorf("on side %d %s, want 403 %s (the legacy refusal)", got.status, got.body, cell.memberBody)
				}
				if legacy.status != got.status || legacy.body != got.body {
					t.Errorf("on %d %s, off %d %s; want the cell-off answer", got.status, got.body, legacy.status, legacy.body)
				}
				if after := orgWriteRealOrg(t, ctx, w, org); after != before {
					t.Errorf("the off side wrote the organization: %s, then %s", before, after)
				}
			})
		}
	}
}
