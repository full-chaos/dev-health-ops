package authapi

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// decisionLogFixture is an authorize fixture whose logger is the production JSON
// handler (with its key scrubber), so a cell reads the line as it is shipped.
func decisionLogFixture(t *testing.T, opts ...func(*AuthorizeDeps)) (*authorizeHandlerFixture, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	opts = append(opts, func(d *AuthorizeDeps) { d.Logger = logging.NewJSON(logs, slog.LevelInfo) })
	return newAuthorizeHandlerFixture(t, opts...), logs
}

// decisionLogLines are the "authorize decision" lines of logs, raw and decoded.
func decisionLogLines(t *testing.T, logs *bytes.Buffer) ([]string, []map[string]any) {
	t.Helper()
	var raw []string
	var entries []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		var entry map[string]any
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("log line %s: %v", line, err)
		}
		if entry["msg"] == "authorize decision" {
			raw, entries = append(raw, string(line)), append(entries, entry)
		}
	}
	if len(entries) == 0 {
		t.Fatalf("no decision log line in %s", logs.String())
	}
	return raw, entries
}

// askAndLog asks one question, wants a 200 decision with the given allowed value and returns it with its one
// decision log line.
func askAndLog(t *testing.T, h *authorizeHandlerFixture, logs *bytes.Buffer, q map[string]any, allowed bool) (wireDecision, string, map[string]any) {
	t.Helper()
	logs.Reset()
	rec := h.one(q)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	d := decodeWire[wireDecision](t, rec)
	if d.Allowed != allowed {
		t.Fatalf("decision %+v, want allowed=%v", d, allowed)
	}
	raw, entries := decisionLogLines(t, logs)
	if len(entries) != 1 {
		t.Fatalf("%d decision log lines, want 1: %v", len(entries), raw)
	}
	if entries[0]["decision_id"] != d.DecisionID {
		t.Fatalf("log line %v is not decision %s", entries[0], d.DecisionID)
	}
	return d, raw[0], entries[0]
}

// obligationKeys are the obligation_* members of a decision log line, with their values.
func obligationKeys(entry map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range entry {
		if strings.HasPrefix(k, "obligation_") {
			out[k] = v
		}
	}
	return out
}

// wantObligationLog holds the line's obligation_* members to exactly want, and the presence flags to the wire.
func wantObligationLog(t *testing.T, d wireDecision, entry map[string]any, want map[string]any) {
	t.Helper()
	got := obligationKeys(entry)
	if !maps.Equal(got, want) {
		t.Errorf("obligation log members %v, want %v (line %v)", got, want, entry)
	}
	for member, key := range map[string]string{"redaction": "obligation_redaction", "minimum_assurance": "obligation_minimum_assurance"} {
		_, served := d.Obligations[member]
		if _, logged := got[key]; served != logged {
			t.Errorf("wire %s present=%v, log %s present=%v", member, served, key, logged)
		}
	}
}

// scopeOnly is the expected line of an allow whose wire carries no redaction and no minimum assurance.
func scopeOnly(t *testing.T, d wireDecision, scope string, size ...int) map[string]any {
	t.Helper()
	if _, ok := d.Obligations["redaction"]; ok {
		t.Fatalf("obligations %v carry redaction; the cell wants none", d.Obligations)
	}
	if _, ok := d.Obligations["minimum_assurance"]; ok {
		t.Fatalf("obligations %v carry a minimum assurance; the cell wants none", d.Obligations)
	}
	want := map[string]any{"obligation_repository_scope": scope}
	for _, n := range size {
		want["obligation_repository_allowlist_size"] = float64(n)
	}
	return want
}

// Product rule 2: an org-wide read carries the tenant scope alone, and its Info line says so.
func TestAuthorizeDecisionLogStatesAnOrganizationWideRead(t *testing.T) {
	h, logs := decisionLogFixture(t)
	h.withGrants("member", repoGrant("acme/api"))
	d, _, entry := askAndLog(t, h, logs, h.question("metrics.read", "organization", "acme", h.org), true)
	if _, ok := d.Obligations["repository_allowlist"]; ok {
		t.Fatalf("obligations %v, want no repository member", d.Obligations)
	}
	wantObligationLog(t, d, entry, scopeOnly(t, d, "none"))
}

// A repository allow is narrowed to an allowlist: the Info line states the narrowing and its size.
func TestAuthorizeDecisionLogStatesARepositoryAllowlist(t *testing.T) {
	h, logs := decisionLogFixture(t)
	h.withGrants("member", repoGrant("acme/web"), repoGrant("acme/api"))
	d, _, entry := askAndLog(t, h, logs, h.question("acr.context.read", "repository", "acme/api", h.org), true)
	wantObligationLog(t, d, entry, scopeOnly(t, d, "allowlist", 1))
}

// Each obligation family reaches the line as the wire serves it; an empty allowlist (NONE) is an allowlist of
// size 0, never "none" (no repository member).
func TestAuthorizeDecisionLogStatesEveryObligationFamily(t *testing.T) {
	planted := map[string]authorization.Decision{}
	h, logs := decisionLogFixture(t, withPlanted(planted))
	planted["analytics.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryRestricted: true,
		RepositoryAllowlist: []string{"acme/api", "acme/web"}, Redaction: []string{"free_text"}, MinimumAssurance: "aal2"})
	planted["metrics.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryScope: authorization.RepositoryScopeOrganization})
	planted["git.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryRestricted: true, RepositoryAllowlist: []string{}})
	planted["work_items.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org, MinimumAssurance: "aal2"})
	for action, want := range map[string]map[string]any{
		"analytics.read": {"obligation_repository_scope": "allowlist", "obligation_repository_allowlist_size": float64(2),
			"obligation_redaction": true, "obligation_minimum_assurance": true},
		"metrics.read":    {"obligation_repository_scope": "organization"},
		"git.read":        {"obligation_repository_scope": "allowlist", "obligation_repository_allowlist_size": float64(0)},
		"work_items.read": {"obligation_repository_scope": "none", "obligation_minimum_assurance": true},
	} {
		t.Run(action, func(t *testing.T) {
			d, _, entry := askAndLog(t, h, logs, h.question(action, "organization", "acme", h.org), true)
			wantObligationLog(t, d, entry, want)
		})
	}
}

// A deny serves no obligations, so its line carries no obligation_* member.
func TestAuthorizeDecisionLogOfADenyHasNoObligations(t *testing.T) {
	h, logs := decisionLogFixture(t)
	h.withGrants("member", repoGrant("acme/web"))
	for _, q := range []map[string]any{
		h.question("acr.context.read", "repository", "acme/api", h.org),
		h.question("settings.write", "organization", "acme", h.org),
	} {
		d, _, entry := askAndLog(t, h, logs, q, false)
		if d.Obligations != nil {
			t.Fatalf("deny %+v carries obligations", d)
		}
		if got := obligationKeys(entry); len(got) != 0 {
			t.Errorf("deny line %v carries %v", entry, got)
		}
	}
}

// A platform admin allow logs the scope it is served, with no default: an organization allow its tenant scope
// alone, a platform question (no organization) nothing.
func TestAuthorizeDecisionLogOfAPlatformAdminAllowIsWhatIsServed(t *testing.T) {
	h, logs := decisionLogFixture(t)
	snap := h.state.snapshots[snapKey{h.principal, h.org}]
	snap.platformRole = "platform_admin"
	snap.membership = nil
	d, _, entry := askAndLog(t, h, logs, h.question("settings.write", "organization", "acme", h.org), true)
	if d.Obligations == nil {
		t.Fatalf("decision %+v carries no obligations", d)
	}
	wantObligationLog(t, d, entry, scopeOnly(t, d, "none"))

	h.state.snapshots[snapKey{h.principal, ""}].platformRole = "platform_admin"
	d, _, entry = askAndLog(t, h, logs, h.question("admin.users", "platform", "platform", ""), true)
	if d.Obligations != nil {
		t.Fatalf("platform allow %+v carries obligations", d)
	}
	if got := obligationKeys(entry); len(got) != 0 {
		t.Errorf("platform allow line %v carries %v", entry, got)
	}
}

// Product rule 1: a platform admin with no membership row is allowed acr.credential.issue (a filtered action)
// org-wide by the real evaluator, and the Info line states the served "organization" scope.
func TestAuthorizeDecisionLogOfAPlatformAdminCredentialIssueIsOrganizationWide(t *testing.T) {
	h, logs := decisionLogFixture(t)
	snap := h.state.snapshots[snapKey{h.principal, h.org}]
	snap.platformRole = "platform_admin"
	snap.membership = nil
	d, _, entry := askAndLog(t, h, logs, h.question("acr.credential.issue", "organization", "acme", h.org), true)
	if got := d.Obligations["repository_scope"]; got != authorization.RepositoryScopeOrganization {
		t.Fatalf("obligations %v, want repository_scope %q", d.Obligations, authorization.RepositoryScopeOrganization)
	}
	wantObligationLog(t, d, entry, scopeOnly(t, d, "organization"))
}

// The summary is bounded: the decision line names no repository key, no tenant, no class and no level.
func TestAuthorizeDecisionLogNamesNoRepositoryOrTenant(t *testing.T) {
	planted := map[string]authorization.Decision{}
	h, logs := decisionLogFixture(t, withPlanted(planted))
	planted["analytics.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryRestricted: true,
		RepositoryAllowlist: []string{"acme/api", "acme/web"}, Redaction: []string{"free_text"}, MinimumAssurance: "aal2"})
	h.withGrants("member", repoGrant("acme/web"), repoGrant("acme/api"))
	for _, q := range []map[string]any{
		h.question("analytics.read", "organization", "acme", h.org),
		h.question("acr.context.read", "repository", "acme/api", h.org),
		h.question("metrics.read", "organization", "acme", h.org),
	} {
		_, raw, _ := askAndLog(t, h, logs, q, true)
		for _, value := range []string{"acme/api", "acme/web", h.org, h.principal.String(), "free_text", "aal2"} {
			if strings.Contains(raw, value) {
				t.Errorf("decision log line %s holds %q", raw, value)
			}
		}
	}
}
