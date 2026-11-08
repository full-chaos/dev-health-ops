package authapi

import (
	"bytes"
	"encoding/json"
	"log/slog"
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

// askAndLog asks one question, wants a 200 allow and returns its one decision log line.
func askAndLog(t *testing.T, h *authorizeHandlerFixture, logs *bytes.Buffer, q map[string]any) (string, map[string]any) {
	t.Helper()
	logs.Reset()
	rec := h.one(q)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	if d := decodeWire[wireDecision](t, rec); !d.Allowed {
		t.Fatalf("decision %+v, want an allow", d)
	}
	raw, entries := decisionLogLines(t, logs)
	if len(entries) != 1 {
		t.Fatalf("%d decision log lines, want 1: %v", len(entries), raw)
	}
	return raw[0], entries[0]
}

func wantRepositoryScopeLog(t *testing.T, entry map[string]any, scope string, size int) {
	t.Helper()
	if got, ok := entry["obligation_repository_scope"].(string); !ok || got != scope {
		t.Errorf("obligation_repository_scope = %v, want %q in %v", entry["obligation_repository_scope"], scope, entry)
	}
	if got, ok := entry["obligation_repository_allowlist_size"].(float64); !ok || got != float64(size) {
		t.Errorf("obligation_repository_allowlist_size = %v, want %d in %v", entry["obligation_repository_allowlist_size"], size, entry)
	}
}

// Product rule 2: an org-wide read carries the tenant scope alone, and its Info line says so.
func TestAuthorizeDecisionLogStatesAnOrganizationWideRead(t *testing.T) {
	h, logs := decisionLogFixture(t)
	h.withGrants("member", repoGrant("acme/api"))
	_, entry := askAndLog(t, h, logs, h.question("metrics.read", "organization", "acme", h.org))
	wantRepositoryScopeLog(t, entry, "none", 0)
}

// A repository allow is narrowed to an allowlist: the Info line states the narrowing and its size.
func TestAuthorizeDecisionLogStatesARepositoryAllowlist(t *testing.T) {
	h, logs := decisionLogFixture(t)
	h.withGrants("member", repoGrant("acme/web"), repoGrant("acme/api"))
	_, entry := askAndLog(t, h, logs, h.question("acr.context.read", "repository", "acme/api", h.org))
	wantRepositoryScopeLog(t, entry, "allowlist", 1)
}

// The summary is bounded: the decision line names no repository key and no tenant.
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
		raw, _ := askAndLog(t, h, logs, q)
		for _, value := range []string{"acme/api", "acme/web", h.org, h.principal.String(), "free_text", "aal2"} {
			if strings.Contains(raw, value) {
				t.Errorf("decision log line %s holds %q", raw, value)
			}
		}
	}
}
