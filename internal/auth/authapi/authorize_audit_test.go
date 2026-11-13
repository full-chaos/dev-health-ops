package authapi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

// fakeAudit records each call of the audit sink: one slice per call.
type fakeAudit struct {
	calls [][]audit.AuditEvent
	err   error
}

func (f *fakeAudit) write(_ context.Context, events []audit.AuditEvent) error {
	f.calls = append(f.calls, slices.Clone(events))
	return f.err
}

func (f *fakeAudit) rows() []audit.AuditEvent {
	var out []audit.AuditEvent
	for _, c := range f.calls {
		out = append(out, c...)
	}
	return out
}

func attrString(t *testing.T, e audit.AuditEvent, key string) string {
	t.Helper()
	v, ok := e.Attributes[key].(string)
	if !ok {
		t.Fatalf("attribute %q missing or not a string in %v", key, e.Attributes)
	}
	return v
}

// C1
func TestAuthorizeHighRiskAllowWritesOneAuditRow(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	rec := h.one(h.question("settings.write", "organization", "acme", h.org))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	d := decodeWire[wireDecision](t, rec)
	rows := h.audit.rows()
	if len(rows) != 1 || len(h.audit.calls) != 1 {
		t.Fatalf("%d rows in %d calls, want 1 row in 1 call", len(rows), len(h.audit.calls))
	}
	e := rows[0]
	if e.EventType != audit.EventAuthorizeHighRiskDecided || e.Outcome != audit.OutcomeAllowed {
		t.Fatalf("event %s outcome %s", e.EventType, e.Outcome)
	}
	if attrString(t, e, "decision_id") != d.DecisionID || attrString(t, e, "decision_reason_code") != "allowed_by_role" ||
		attrString(t, e, "action_key") != "settings.write" || attrString(t, e, "resource_type") != "organization" {
		t.Fatalf("attributes %v, response %+v", e.Attributes, d)
	}
	if e.ActorPrincipalID == nil || *e.ActorPrincipalID != h.principal.String() || e.SubjectPrincipalID != nil {
		t.Fatalf("actor %v subject %v, want actor = the user principal and no subject", e.ActorPrincipalID, e.SubjectPrincipalID)
	}
	if e.OrganizationID == nil || *e.OrganizationID != h.org {
		t.Fatalf("organization %v, want %s", e.OrganizationID, h.org)
	}
	if e.PolicyRevision == nil || *e.PolicyRevision != int64(d.PolicyRevision) {
		t.Fatalf("policy revision %v, want %d", e.PolicyRevision, d.PolicyRevision)
	}
	caller, _ := parseWirePrincipal(h.subject)
	if attrString(t, e, "caller_principal_id") != caller.String() || attrString(t, e, "workload_jti") == "" {
		t.Fatalf("caller attributes %v", e.Attributes)
	}
}

// C1
func TestAuthorizeHighRiskDenyWritesOneAuditRow(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	d := decodeWire[wireDecision](t, h.one(h.question("org.delete", "organization", "acme", h.org)))
	rows := h.audit.rows()
	if d.Allowed || len(rows) != 1 {
		t.Fatalf("decision %+v with %d rows, want a deny with 1 row", d, len(rows))
	}
	if rows[0].Outcome != audit.OutcomeDenied || attrString(t, rows[0], "decision_reason_code") != "role_lacks_action" ||
		attrString(t, rows[0], "decision_id") != d.DecisionID {
		t.Fatalf("row %+v attributes %v", rows[0], rows[0].Attributes)
	}
}

// C1: every reason that can reach a high-risk deny is recorded as the wire code.
func TestAuthorizeHighRiskDenyRecordsTheWireReason(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	cases := map[string]map[string]any{
		"role_lacks_action":      h.question("org.delete", "organization", "acme", h.org),
		"resource_mismatch":      h.question("settings.write", "platform", "platform", ""),
		"org_mismatch":           h.question("settings.write", "organization", "acme", uuid.NewString()),
		"platform_role_absent":   h.question("admin.users", "platform", "platform", ""),
		"no_membership_or_alike": h.question("settings.write", "organization", "acme", "not-an-org"),
	}
	for name, q := range cases {
		before := len(h.audit.rows())
		d := decodeWire[wireDecision](t, h.one(q))
		rows := h.audit.rows()
		if d.Allowed || len(rows) != before+1 {
			t.Fatalf("%s: decision %+v, rows %d -> %d", name, d, before, len(rows))
		}
		if got := attrString(t, rows[len(rows)-1], "decision_reason_code"); got != d.ReasonCode {
			t.Errorf("%s: row reason %s, response reason %s", name, got, d.ReasonCode)
		}
		auditwire.MustValidate(t, rows[len(rows)-1])
	}
}

// C2
func TestAuthorizeLowRiskWritesNoRow(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	if rec := h.one(h.question("analytics.read", "organization", "acme", h.org)); rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if n := len(h.audit.calls); n != 0 {
		t.Fatalf("%d audit calls for a low-risk decision, want 0", n)
	}
}

// C2
func TestAuthorizeUnknownActionWritesNoRow(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	d := decodeWire[wireDecision](t, h.one(h.question("nothing.here", "organization", "acme", h.org)))
	if d.Allowed || d.ReasonCode != "unknown_action" || len(h.audit.calls) != 0 {
		t.Fatalf("decision %+v with %d audit calls, want an unknown_action deny and none", d, len(h.audit.calls))
	}
}

// C3: the row shape is closed, and a repository key rides the resource_id rule only.
func TestAuthorizeAuditRowShapeIsClosed(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.one(h.question("settings.write", "organization", "acme", h.org))
	h.one(h.question("settings.write", "repository", "acme/widget", h.org))
	rows := h.audit.rows()
	if len(rows) != 2 {
		t.Fatalf("%d rows, want 2", len(rows))
	}
	closed := []string{"action_key", "caller_principal_id", "decision_id", "decision_reason_code", "policy_revision", "resource_type", "workload_jti"}
	for i, e := range rows {
		keys := make([]string, 0, len(e.Attributes))
		for k := range e.Attributes {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		want := slices.Clone(closed)
		if i == 1 {
			want = append(want, "resource_id")
			slices.Sort(want)
		}
		if !slices.Equal(keys, want) {
			t.Errorf("row %d keys %v, want %v", i, keys, want)
		}
		if err := audit.ValidateAttributes(e.Attributes); err != nil {
			t.Errorf("row %d: %v", i, err)
		}
		auditwire.MustValidate(t, e)
	}
	if attrString(t, rows[1], "resource_id") != "acme/widget" {
		t.Errorf("repository key %v", rows[1].Attributes)
	}
}

// C3
func TestAuthorizeAuditRowCarriesNoSecret(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	bearer := h.workloadToken(nil)
	body := mustJSON(t, h.question("settings.write", "organization", "secret-marker-key", h.org))
	if rec := h.post(PatternAuthorize, bearer, body); rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	rows := h.audit.rows()
	if len(rows) != 1 {
		t.Fatalf("%d rows", len(rows))
	}
	flat := rows[0].EventType + " " + string(rows[0].Outcome)
	for k, v := range rows[0].Attributes {
		flat += " " + k + "=" + toText(v)
	}
	for _, leak := range []string{bearer, h.reference, "secret-marker-key", "req_EXAMPLE", "dev-health-ops-api", session.WireSessionID(h.sid)} {
		if strings.Contains(flat, leak) {
			t.Errorf("the row carries %.12q", leak)
		}
	}
}

func toText(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return "n"
	}
	return "?"
}

// An organization the call names is a column only when it is a known organization.
func TestAuthorizeAuditOrganizationColumnIsKnownOrganizationsOnly(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.one(h.question("settings.write", "organization", "acme", uuid.NewString()))
	h.one(h.question("settings.write", "organization", "acme", "not-an-org"))
	rows := h.audit.rows()
	if len(rows) != 2 {
		t.Fatalf("%d rows, want 2", len(rows))
	}
	for i, e := range rows {
		if e.OrganizationID != nil {
			t.Errorf("row %d carries organization %q for an unknown organization", i, *e.OrganizationID)
		}
	}
}

// C4
func TestAuthorizeAuditFailureIs503AndNoBody(t *testing.T) {
	for name, run := range map[string]func(h *authorizeHandlerFixture) *httptest.ResponseRecorder{
		"single allow": func(h *authorizeHandlerFixture) *httptest.ResponseRecorder {
			return h.one(h.question("settings.write", "organization", "acme", h.org))
		},
		"single deny": func(h *authorizeHandlerFixture) *httptest.ResponseRecorder {
			return h.one(h.question("org.delete", "organization", "acme", h.org))
		},
		"batch with a low-risk item": func(h *authorizeHandlerFixture) *httptest.ResponseRecorder {
			return h.batch(h.question("analytics.read", "organization", "acme", h.org), h.question("settings.write", "organization", "acme", h.org))
		},
	} {
		h := newAuthorizeHandlerFixture(t)
		h.audit.err = errors.New("audit store down")
		assertError(t, run(h), http.StatusServiceUnavailable, reasonStoreUnavailable)
		if len(h.audit.calls) != 1 {
			t.Errorf("%s: %d audit calls, want 1", name, len(h.audit.calls))
		}
	}
}

func TestAuthorizeRoutesRefuseNoAuditSink(t *testing.T) {
	f := newAuthorizeFixture(t)
	deps := AuthorizeDeps{Callers: f.callers(), References: f.references(), State: &fakeDecisionState{}, Evaluator: authorization.NewEvaluator(),
		ProfileRows: authorizeProfileDoc(t, PatternAuthorize)}
	if _, err := AuthorizeRoutes(deps); err == nil {
		t.Fatal("routes built with no audit sink")
	}
	deps.Audit = (&fakeAudit{}).write
	if _, err := AuthorizeRoutes(deps); err != nil {
		t.Fatalf("routes with a sink: %v", err)
	}
}

// C5: one audit call carries every high-risk item of a batch, in item order.
func TestAuthorizeBatchWritesOneAuditCall(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	rec := h.batch(
		h.question("settings.write", "organization", "acme", h.org),
		h.question("analytics.read", "organization", "acme", h.org),
		h.question("org.delete", "organization", "acme", h.org),
	)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %s", rec.Code, rec.Body)
	}
	batch := decodeWire[struct {
		Decisions []struct {
			Decision wireDecision `json:"decision"`
		} `json:"decisions"`
	}](t, rec)
	if len(h.audit.calls) != 1 || len(h.audit.calls[0]) != 2 {
		t.Fatalf("calls %d, rows in the first %v, want one call of 2 rows", len(h.audit.calls), len(h.audit.rows()))
	}
	rows := h.audit.calls[0]
	if attrString(t, rows[0], "decision_id") != batch.Decisions[0].Decision.DecisionID ||
		attrString(t, rows[1], "decision_id") != batch.Decisions[2].Decision.DecisionID {
		t.Fatalf("rows are not the decisions 0 and 2: %v %v", rows[0].Attributes, rows[1].Attributes)
	}
}

// C4/C5: a call of low-risk items only never touches the audit sink, even when it is down.
func TestAuthorizeLowRiskOnlyBatchIgnoresADownAuditSink(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.audit.err = errors.New("audit store down")
	if rec := h.batch(h.question("analytics.read", "organization", "acme", h.org)); rec.Code != http.StatusOK {
		t.Fatalf("status %d %s", rec.Code, rec.Body)
	}
	if len(h.audit.calls) != 0 {
		t.Fatalf("%d audit calls", len(h.audit.calls))
	}
}

// A handler reached with no sink fails closed on a high-risk item.
func TestAuthorizeHighRiskWithNoSinkIs503(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rec, err := newRecorder(logger, noop.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	handlers := authorizeHandlers{AuthorizeDeps: AuthorizeDeps{Callers: h.callers(), References: h.references(), State: h.state,
		Evaluator: authorization.NewEvaluator(), Logger: logger}, rec: rec, decisionID: newDecisionID}
	req := httptest.NewRequest(http.MethodPost, PatternAuthorize, bytes.NewReader(mustJSON(t, h.question("settings.write", "organization", "acme", h.org))))
	req.Header.Set("Authorization", "Bearer "+h.workloadToken(nil))
	w := httptest.NewRecorder()
	admit := handlers.admit(opAuthorize)
	requestid.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if admitted, ok := admit(w, r); ok {
			handlers.authorize(w, admitted)
		}
	})).ServeHTTP(w, req)
	assertError(t, w, http.StatusServiceUnavailable, reasonStoreUnavailable)
}

// C6: the audit row's closed value sets are the wire sets.
func TestAuthorizeAuditVocabularyIsTheWireVocabulary(t *testing.T) {
	vocab := audit.AttributeVocabulary()
	reasons := slices.Clone(vocab["decision_reason_code"])
	slices.Sort(reasons)
	want := AuthorizeWireReasonCodes()
	if !slices.Equal(reasons, want) {
		t.Errorf("decision_reason_code %v, want the wire codes %v", reasons, want)
	}
	types := slices.Clone(vocab["resource_type"])
	slices.Sort(types)
	want = slices.Clone(authorizeResourceTypes)
	slices.Sort(want)
	if !slices.Equal(types, want) {
		t.Errorf("resource_type %v, want %v", types, want)
	}
}

// Every high-risk catalog action is audited and no other is.
func TestHighRiskActionIsTheCatalogClass(t *testing.T) {
	high := 0
	for _, a := range policycatalog.Default().Actions() {
		if got, want := highRiskAction(a.ID), a.RiskClass == policycatalog.RiskHigh; got != want {
			t.Errorf("%s: highRiskAction %v, risk class %s", a.ID, got, a.RiskClass)
		}
		if a.RiskClass == policycatalog.RiskHigh {
			high++
		}
	}
	if high == 0 || highRiskAction("nothing.here") {
		t.Fatalf("%d high-risk actions; unknown action audited: %v", high, highRiskAction("nothing.here"))
	}
}

// With the decision cache wired, no high-risk decision is stored or served from it: the wire
// expiry is the decision time, and a second identical call decides again and writes a second
// audit row. A pin cell: the rule holds on main before the audit existed.
func TestAuthorizeHighRiskDecisionsAreNeverCachedSoEachIsAudited(t *testing.T) {
	t.Parallel()
	high := 0
	for _, a := range delegatedCatalogActions {
		act, err := policycatalog.Default().Action(a.action)
		if err != nil || act.RiskClass != policycatalog.RiskHigh {
			continue
		}
		high++
		c := newCacheFixture(t)
		decisionTime := time.Unix(0, authorizeNow).UTC().Format(expiresAtLayout)
		for call := 1; call <= 2; call++ {
			rec := c.one(c.delegatedQuestion(a))
			if rec.Code != http.StatusOK {
				t.Fatalf("%s call %d: status %d %s", a.action, call, rec.Code, rec.Body)
			}
			d := decodeWire[wireDecision](t, rec)
			if d.ExpiresAt != decisionTime {
				t.Errorf("%s call %d: expires_at %s, want the decision time %s", a.action, call, d.ExpiresAt, decisionTime)
			}
			if n := len(c.audit.rows()); n != call {
				t.Errorf("%s after call %d: %d audit rows, want %d", a.action, call, n, call)
			}
		}
		if hits := c.counter(MetricDecisionCacheLookups, "hit"); hits != 0 {
			t.Errorf("%s: %d cache hits for a high-risk action", a.action, hits)
		}
		if stored := c.counter(MetricDecisionCachePuts, "stored"); stored != 0 {
			t.Errorf("%s: %d decisions stored for a high-risk action", a.action, stored)
		}
	}
	if high != 27 {
		t.Fatalf("walked %d high-risk actions, want 27", high)
	}
}
