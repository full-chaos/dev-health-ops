package apiservice

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// noRowPlatformAdmin is the decision state of a superuser with no membership row in an active organization.
type noRowPlatformAdmin struct{}

func (noRowPlatformAdmin) PrincipalStatus() authorization.Status { return authorization.StatusActive }
func (noRowPlatformAdmin) PlatformRole() string                  { return rule.PlatformAdmin }
func (noRowPlatformAdmin) OrganizationStatus(string) authorization.Status {
	return authorization.StatusActive
}
func (noRowPlatformAdmin) Membership(string) (authorization.Membership, bool) {
	return authorization.Membership{}, false
}
func (noRowPlatformAdmin) Grants(string) ([]authorization.Grant, error) { return nil, nil }
func (noRowPlatformAdmin) Workload() (authorization.WorkloadLimits, bool) {
	return authorization.WorkloadLimits{}, false
}
func (noRowPlatformAdmin) ServiceAccount(string) (authorization.ServiceAccount, bool) {
	return authorization.ServiceAccount{}, false
}
func (noRowPlatformAdmin) NowUnixNano() int64 { return time.Now().UnixNano() }
func (noRowPlatformAdmin) Delegation() (authorization.Delegation, bool) {
	return authorization.Delegation{}, false
}

// evaluatorCellPDP answers each decision request on the wire with the production evaluator over noRowPlatformAdmin.
type evaluatorCellPDP struct {
	srv  *httptest.Server
	mu   sync.Mutex
	seen []string
}

func newEvaluatorCellPDP(t *testing.T) *evaluatorCellPDP {
	t.Helper()
	p := &evaluatorCellPDP{}
	evaluator := authorization.NewEvaluator()
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Action   string `json:"action"`
			Resource struct {
				Type string  `json:"type"`
				Org  *string `json:"organization_id"`
			} `json:"resource"`
		}
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &body); err != nil {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		req := authorization.Request{Action: body.Action, ResourceKind: body.Resource.Type}
		var org any
		if body.Resource.Org != nil {
			req.OrganizationID, org = *body.Resource.Org, *body.Resource.Org
		}
		d := evaluator.Evaluate(req, noRowPlatformAdmin{})
		reason := string(d.Reason)
		answer := map[string]any{
			"schema_version": "authorization_decision.v1", "decision_id": "dec_" + strings.Repeat("A", 26), "allowed": d.Allowed,
			"reason_code": reason, "principal_id": "prn_TESTPRINCIPAL", "organization_id": org, "policy_revision": 1,
			"expires_at": time.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
		}
		if d.Allowed && d.Obligations != nil && d.Obligations.TenantScope != "" {
			answer["obligations"] = map[string]any{"tenant_scope": d.Obligations.TenantScope}
		}
		p.mu.Lock()
		p.seen = append(p.seen, body.Action+" "+req.OrganizationID+" allowed="+map[bool]string{true: "true", false: "false"}[d.Allowed])
		p.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(answer)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *evaluatorCellPDP) client(t *testing.T) *pep.Client {
	t.Helper()
	c, err := pep.New(pep.Config{BaseURL: p.srv.URL, AllowPlainHTTP: true, HTTPClient: p.srv.Client(), ClientService: goAPIAccessService, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func (p *evaluatorCellPDP) calls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.seen)
}

// C2. The 5 billing.read and 2 license.read rows with the billing and license namespaces on and the production
// evaluator deciding for a superuser with no membership row: one PDP call per request, on the row's action and the
// token's org, answered with an allow, so the request reaches the handler as it does with no Enforcer (legacy).
func TestPlatformAdminNoMembershipBillingAndLicenseReadsEqualLegacy(t *testing.T) {
	var keys []string
	for _, key := range classAKeys() {
		if a := billingClassA[key]; a == "billing.read" || a == "license.read" {
			keys = append(keys, key)
		}
	}
	if len(keys) != 7 {
		t.Fatalf("billing.read and license.read rows %v, want 7", keys)
	}
	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			legacy := newCellRig(t, "", nil, false)
			legacyStatus, legacyBody, _ := legacy.serve(key, legacy.userBearer())
			if !handlerReached(legacyStatus, legacyBody) {
				t.Fatalf("legacy: %d %s, want the handler reached", legacyStatus, legacyBody)
			}
			pdp := newEvaluatorCellPDP(t)
			enforced := newCellRig(t, "billing,license", pdp.client(t), false)
			status, body, _ := enforced.serve(key, enforced.userBearer())
			want := billingClassA[key] + " " + probeOrg.String() + " allowed=true"
			if calls := pdp.calls(); len(calls) != 1 || calls[0] != want {
				t.Errorf("PDP calls %v, want [%s]", calls, want)
			}
			if status != legacyStatus || body != legacyBody {
				t.Errorf("enforced %d %s, legacy %d %s; product rule 1 wants the legacy answer", status, body, legacyStatus, legacyBody)
			}
		})
	}
}
