package policy

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
)

const testDecisionID = "dec_AAAAAAAAAAAAAAAAAAAAAAAAAA"

func resultLines(t *testing.T, logs *bytes.Buffer) []map[string]string {
	t.Helper()
	var out []map[string]string
	for _, raw := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if raw == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			t.Fatalf("log line %q: %v", raw, err)
		}
		if rec["msg"] != "api shadow pdp: result" {
			continue
		}
		line := map[string]string{}
		for k, v := range rec {
			if k == "time" || k == "msg" {
				continue
			}
			s, ok := v.(string)
			if !ok {
				t.Fatalf("field %s of %q is not a string", k, raw)
			}
			line[k] = s
		}
		out = append(out, line)
	}
	return out
}

func jsonShadow(t *testing.T, decider pep.Decider, mutate func(*ShadowConfig)) (*Shadow, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	return mustShadow(t, decider, func(c *ShadowConfig) {
		c.Logger = slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
		if mutate != nil {
			mutate(c)
		}
	}), logs
}

func mustShadow(t *testing.T, decider pep.Decider, mutate func(*ShadowConfig)) *Shadow {
	t.Helper()
	sh, _ := newShadowFor(t, decider, mutate)
	return sh
}

func observation(pattern, caller string, legacyAllowed bool) ShadowObservation {
	return ShadowObservation{Pattern: pattern, Bearer: shadowBearer, OrgID: ownOrg, Caller: caller, UserAccessToken: true, LegacyAllowed: legacyAllowed}
}

// Every result path writes exactly one result line with its route, namespace, caller and result; decision_id is
// present only when the decision point answered.
func TestShadowResultLineAtEveryResultSite(t *testing.T) {
	panics := funcDecider{func(context.Context, pep.Request) pep.Decision { panic("decider") }}
	local := funcDecider{func(context.Context, pep.Request) pep.Decision { return pep.Decision{} }}
	cells := []struct {
		name    string
		decider func(t *testing.T) pep.Decider
		mutate  func(*ShadowConfig)
		obs     ShadowObservation
		want    map[string]string
	}{
		{"impersonation refused", nil, nil,
			func() ShadowObservation {
				o := observation("GET /api/v1/admin/teams", ShadowCallerAdmin, true)
				o.Impersonating = true
				return o
			}(),
			map[string]string{"route": "GET /api/v1/admin/teams", "namespace": "teams", "caller": "admin", "result": "impersonation_refused"}},
		{"org-less token, delegated session: keeps the legacy answer, skipped", nil, nil,
			func() ShadowObservation {
				o := observation("GET /api/v1/admin/teams", ShadowCallerSuperuser, true)
				o.OrgID, o.orgless = "", orglessInputs{delegated: true, action: ActionAdminOrgs}
				return o
			}(),
			map[string]string{"route": "GET /api/v1/admin/teams", "namespace": "teams", "caller": "superuser", "result": "skipped"}},
		{"high-risk row with the second switch off", nil, func(c *ShadowConfig) { c.HighRisk = false },
			observation("POST /api/v1/admin/teams", ShadowCallerSuperuser, true),
			map[string]string{"route": "POST /api/v1/admin/teams", "namespace": "teams", "caller": "superuser", "result": "skipped"}},
		{"no user access token", nil, nil,
			func() ShadowObservation {
				o := observation("GET /api/v1/billing/invoices", ShadowCallerMember, true)
				o.UserAccessToken = false
				return o
			}(),
			map[string]string{"route": "GET /api/v1/billing/invoices", "namespace": "billing", "caller": "member", "result": "skipped"}},
		{"org-less token on an AdminOrg row: decided with no question", nil, nil,
			func() ShadowObservation {
				o := observation("GET /api/v1/admin/teams", ShadowCallerSuperuser, false)
				o.OrgID, o.orgless = "", orglessInputs{orgRequired: true, action: ActionAdminOrgs}
				return o
			}(),
			map[string]string{"route": "GET /api/v1/admin/teams", "namespace": "teams", "caller": "superuser", "result": "decided_no_question"}},
		{"org-less token asks the platform action", nil, nil,
			func() ShadowObservation {
				o := observation("GET /api/v1/admin/teams", ShadowCallerSuperuser, true)
				o.OrgID, o.orgless = "", orglessInputs{action: ActionAdminOrgs}
				return o
			}(),
			map[string]string{"route": "GET /api/v1/admin/teams", "namespace": "teams", "caller": "superuser", "result": "match", "decision_id": testDecisionID}},
		{"decider panics", func(*testing.T) pep.Decider { return panics }, nil,
			observation("GET /api/v1/admin/orgs", ShadowCallerSuperuser, true),
			map[string]string{"route": "GET /api/v1/admin/orgs", "namespace": "admin", "caller": "superuser", "result": "error"}},
		{"no verdict", func(*testing.T) pep.Decider { return local }, nil,
			observation("GET /api/v1/billing/invoices", ShadowCallerAdmin, true),
			map[string]string{"route": "GET /api/v1/billing/invoices", "namespace": "billing", "caller": "admin", "result": "error"}},
		{"match", func(t *testing.T) pep.Decider { return newTestPDP(t, allowAll).client(t) }, nil,
			observation("GET /api/v1/admin/teams", ShadowCallerAdmin, true),
			map[string]string{"route": "GET /api/v1/admin/teams", "namespace": "teams", "caller": "admin", "result": "match", "decision_id": testDecisionID}},
		{"mismatch", func(t *testing.T) pep.Decider { return newTestPDP(t, denyWith("grant_absent")).client(t) }, nil,
			observation("GET /api/v1/billing/invoices", ShadowCallerMember, true),
			map[string]string{"route": "GET /api/v1/billing/invoices", "namespace": "billing", "caller": "member", "result": "mismatch", "decision_id": testDecisionID}},
		{"unknown caller kind is member", func(t *testing.T) pep.Decider { return newTestPDP(t, allowAll).client(t) }, nil,
			observation("GET /api/v1/admin/teams", "owner", true),
			map[string]string{"route": "GET /api/v1/admin/teams", "namespace": "teams", "caller": "member", "result": "match", "decision_id": testDecisionID}},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			recordShadow(t)
			var decider pep.Decider = newTestPDP(t, allowAll).client(t)
			if cell.decider != nil {
				decider = cell.decider(t)
			}
			sh, logs := jsonShadow(t, decider, cell.mutate)
			sh.Observe(context.Background(), cell.obs)
			sh.wg.Wait()
			lines := resultLines(t, logs)
			if len(lines) != 1 {
				t.Fatalf("%d result lines, want 1: %q", len(lines), logs.String())
			}
			want := maps.Clone(cell.want)
			want["level"] = "INFO"
			if !maps.Equal(lines[0], want) {
				t.Fatalf("result line %v, want %v", lines[0], want)
			}
		})
	}
}

// The dropped path: the one slot is busy, so the second request writes its own dropped line at once.
func TestShadowResultLineWhenTheCallIsDropped(t *testing.T) {
	recordShadow(t)
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	sh, logs := jsonShadow(t, funcDecider{func(context.Context, pep.Request) pep.Decision {
		started <- struct{}{}
		<-release
		return pep.Decision{}
	}}, func(c *ShadowConfig) { c.MaxInFlight = 1 })
	sh.Observe(context.Background(), observation("GET /api/v1/admin/teams", ShadowCallerAdmin, true))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the first shadow call never started")
	}
	sh.Observe(context.Background(), observation("POST /api/v1/admin/teams", ShadowCallerSuperuser, true))
	lines := resultLines(t, logs)
	want := map[string]string{"level": "INFO", "route": "POST /api/v1/admin/teams", "namespace": "teams", "caller": "superuser", "result": "dropped"}
	if len(lines) != 1 || !maps.Equal(lines[0], want) {
		t.Fatalf("result lines %v, want only %v", lines, want)
	}
	close(release)
	sh.wg.Wait()
}

// A route with no action row and a sampled-out request are not shadowed requests: no line.
func TestShadowResultLineOnlyForShadowedRequests(t *testing.T) {
	recordShadow(t)
	sh, logs := jsonShadow(t, newTestPDP(t, allowAll).client(t), nil)
	sh.Observe(context.Background(), observation("GET /api/v1/admin/other", ShadowCallerAdmin, true))
	sh.wg.Wait()
	if lines := resultLines(t, logs); len(lines) != 0 {
		t.Fatalf("result lines %v for a route with no row, want none", lines)
	}
}
