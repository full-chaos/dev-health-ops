package authapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/outboxprojection"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

const acrEventOrg = "11111111-2222-4333-8444-555555555555"

func acrEventRow(classes ...string) string {
	listed, _ := json.Marshal(classes)
	return fmt.Sprintf(`{"service":%q,"method":"POST","route":%q,"accepted_credential_classes":%s}`, AccessService, PatternAcrCredentialEvents, listed)
}

func acrEventRows(classes ...string) []byte {
	return []byte(`{"rows":[` + acrEventRow(classes...) + `]}`)
}

// acrEventBody is a valid issued event, one revision of one credential.
func acrEventBody(id string, revision int, status string) string {
	return fmt.Sprintf(`{"schema_version":"acr-credential-lifecycle-event.v1","aggregate_type":"acr_credential","aggregate_id":%q,`+
		`"event_type":"acr_credential.issued","idempotency_key":"acr_credential:%s:%d","created_at":"2026-10-22T10:15:00Z",`+
		`"payload":{"revision":%d,"organization_id":"org_%s","actor_type":"system","change":{"status":%q,"acr_class":"oauth_user","scopes":["context:read"],"owner_subject":"subject_EXAMPLE0000000001"}}}`,
		id, id, revision, revision, acrEventOrg, status)
}

type fakeAcrWorkloads struct {
	reads int
	info  *workload.Info
	err   error
}

func (f *fakeAcrWorkloads) LookupPrincipal(context.Context, uuid.UUID) (*workload.Info, error) {
	f.reads++
	return f.info, f.err
}

type fakeCredentialProjector struct {
	inputs  []outboxprojection.Input
	outcome outboxprojection.Outcome
	err     error
}

func (f *fakeCredentialProjector) Project(_ context.Context, in outboxprojection.Input) (outboxprojection.Result, error) {
	f.inputs = append(f.inputs, in)
	return outboxprojection.Result{Outcome: f.outcome, Class: outboxprojection.ClassOf(f.outcome)}, f.err
}

// acrCountingBody counts reads of the request body.
type acrCountingBody struct {
	inner io.Reader
	reads int
}

func (b *acrCountingBody) Read(p []byte) (int, error) {
	b.reads++
	return b.inner.Read(p)
}

func (b *acrCountingBody) Close() error { return nil }

func acrEventDepsOf(w *workloadIssueFixture, workloads AcrWorkloads, projector CredentialProjector) AcrEventDeps {
	return AcrEventDeps{Workload: w.workload, Workloads: workloads, Projector: projector, Meter: w.meter,
		Logger: slog.New(slog.NewJSONHandler(w.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))}
}

type acrEventsFixture struct {
	*workloadIssueFixture
	workloads *fakeAcrWorkloads
	projector *fakeCredentialProjector
	server    http.Handler
}

func newAcrEventsFixture(t *testing.T, rows []byte) *acrEventsFixture {
	t.Helper()
	w := newWorkloadIssueFixture(t)
	workloads := &fakeAcrWorkloads{info: &workload.Info{Audiences: []string{"auth-service"}, AllowedActions: []string{"credentials.project"}}}
	projector := &fakeCredentialProjector{outcome: "applied"}
	routes, err := acrEventRoutes(acrEventDepsOf(w, workloads, projector), rows)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	return &acrEventsFixture{workloadIssueFixture: w, workloads: workloads, projector: projector, server: mux}
}

func (a *acrEventsFixture) post(header map[string]string, body string) (*httptest.ResponseRecorder, *acrCountingBody) {
	counted := &acrCountingBody{inner: strings.NewReader(body)}
	request := httptest.NewRequest(http.MethodPost, PatternAcrCredentialEvents, counted)
	for k, v := range header {
		request.Header.Set(k, v)
	}
	response := httptest.NewRecorder()
	a.server.ServeHTTP(response, request)
	return response, counted
}

func (a *acrEventsFixture) token() map[string]string {
	h, _ := a.mint("workload+jwt", nil)
	return h
}

func (a *acrEventsFixture) nothingRead(t *testing.T, name string, body *acrCountingBody) {
	t.Helper()
	if body.reads != 0 || a.workloads.reads != 0 || len(a.projector.inputs) != 0 {
		t.Fatalf("%s: body reads %d, workload reads %d, projections %d", name, body.reads, a.workloads.reads, len(a.projector.inputs))
	}
}

func TestAcrEventsRouteIsDarkWithoutItsRow(t *testing.T) {
	for name, rows := range map[string][]byte{
		"no rows":        []byte(`{"rows":[]}`),
		"other class":    acrEventRows("user_access_token"),
		"a second class": acrEventRows(workloadTokenClass, "internal_svc_authsess_token"),
		"not json":       []byte(`{`),
	} {
		a := newAcrEventsFixture(t, rows)
		if r, _ := a.post(a.token(), acrEventBody("cred_exampleaaaaaaaaaa", 1, "active")); r.Code != http.StatusNotFound {
			t.Errorf("%s: %d, want 404", name, r.Code)
		}
		if !strings.Contains(a.logs.String(), "auth acr events route not mounted") {
			t.Errorf("%s: no line names the unmounted route", name)
		}
	}
}

func TestAcrEventsMountsWithItsRow(t *testing.T) {
	a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
	if r, _ := a.post(a.token(), acrEventBody("cred_exampleaaaaaaaaaa", 1, "active")); r.Code != http.StatusNoContent {
		t.Fatalf("%d: %s", r.Code, r.Body)
	}
}

// The embedded document holds no row yet, so a configured route stays dark.
func TestAcrEventsRouteHasNoRowInTheEmbeddedDocument(t *testing.T) {
	a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
	routes, err := AcrEventRoutes(acrEventDepsOf(a.workloadIssueFixture, a.workloads, a.projector))
	if err != nil || len(routes) != 0 {
		t.Fatalf("routes = %d, %v, want 0", len(routes), err)
	}
}

// Every other class, and a wrong audience or subject, is refused from the
// bearer's text or the unverified payload: no key, no row, no body, no store.
func TestAcrEventsRefuseEveryOtherCallerBeforeAnyRead(t *testing.T) {
	refusals := issuePreKeyRefusals()
	refusals["compat service credential"] = func(*workloadIssueFixture) map[string]string {
		return bearerOf("svc_authsess_" + strings.Repeat("A", 43))
	}
	refusals["no bearer"] = func(*workloadIssueFixture) map[string]string { return nil }
	for name, build := range refusals {
		a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
		response, body := a.post(build(a.workloadIssueFixture), acrEventBody("cred_exampleaaaaaaaaaa", 1, "active"))
		expectError(t, response, http.StatusUnauthorized, "invalid_credentials")
		a.nothingRead(t, name, body)
		if a.keys.reads != 0 {
			t.Fatalf("%s: read the key set %d times", name, a.keys.reads)
		}
	}
}

func TestAcrEventsRefuseATokenTheVerifierRefuses(t *testing.T) {
	for name, build := range issueVerifierRefusals() {
		a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
		response, body := a.post(build(a.workloadIssueFixture), acrEventBody("cred_exampleaaaaaaaaaa", 1, "active"))
		expectError(t, response, http.StatusUnauthorized, "invalid_credentials")
		a.nothingRead(t, name, body)
	}
}

func TestAcrEventsAnswer503WhenTheKeySetIsUnavailable(t *testing.T) {
	a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
	a.keys.err = errors.New("key set down")
	response, body := a.post(a.token(), acrEventBody("cred_exampleaaaaaaaaaa", 1, "active"))
	expectError(t, response, http.StatusServiceUnavailable, reasonDependency)
	a.nothingRead(t, "keys", body)
}

// The live row decides: its enabled state, audience and action, before the body.
func TestAcrEventsRefuseARowThatDoesNotAdmit(t *testing.T) {
	for name, tc := range map[string]struct {
		info *workload.Info
		err  error
	}{
		"no row":          {err: workload.ErrNotFound},
		"disabled":        {info: &workload.Info{Disabled: true, Audiences: []string{"auth-service"}, AllowedActions: []string{"credentials.project"}}},
		"audience absent": {info: &workload.Info{Audiences: []string{"dev-health-api"}, AllowedActions: []string{"credentials.project"}}},
		"action absent":   {info: &workload.Info{Audiences: []string{"auth-service"}, AllowedActions: []string{"authz.decide"}}},
		"no action":       {info: &workload.Info{Audiences: []string{"auth-service"}}},
	} {
		a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
		a.workloads.info, a.workloads.err = tc.info, tc.err
		response, body := a.post(a.token(), acrEventBody("cred_exampleaaaaaaaaaa", 1, "active"))
		expectError(t, response, http.StatusForbidden, reasonNotEntitled)
		if body.reads != 0 || len(a.projector.inputs) != 0 {
			t.Fatalf("%s: body reads %d, projections %d", name, body.reads, len(a.projector.inputs))
		}
	}
}

func TestAcrEventsAnswer503WhenTheWorkloadReadFails(t *testing.T) {
	a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
	a.workloads.err = workload.ErrStore
	response, body := a.post(a.token(), acrEventBody("cred_exampleaaaaaaaaaa", 1, "active"))
	expectError(t, response, http.StatusServiceUnavailable, reasonStoreUnavailable)
	if body.reads != 0 || len(a.projector.inputs) != 0 {
		t.Fatalf("body reads %d, projections %d", body.reads, len(a.projector.inputs))
	}
}

func TestAcrEventsRefuseABadBodyBeforeTheStore(t *testing.T) {
	valid := acrEventBody("cred_exampleaaaaaaaaaa", 1, "active")
	for name, body := range map[string]string{
		"not json":          `{`,
		"duplicate member":  strings.Replace(valid, `{"schema_version"`, `{"schema_version":"x","schema_version"`, 1),
		"unknown member":    strings.Replace(valid, `{"schema_version"`, `{"token":"x","schema_version"`, 1),
		"decimal revision":  strings.Replace(valid, `"revision":1`, `"revision":1.0`, 1),
		"key mismatch":      strings.Replace(valid, `credential:cred_exampleaaaaaaaaaa:1`, `credential:cred_exampleaaaaaaaaaa:2`, 1),
		"org not a uuid":    strings.Replace(valid, "org_"+acrEventOrg, "org_acme", 1),
		"org not canonical": strings.Replace(valid, "org_"+acrEventOrg, "org_"+strings.ReplaceAll(acrEventOrg, "-", ""), 1),
		"org in capitals":   strings.Replace(valid, "org_"+acrEventOrg, "org_AAAAAAAA-2222-4333-8444-555555555555", 1),
		"oversize":          strings.Replace(valid, `{"schema_version"`, `{"x":"`+strings.Repeat("a", maxAcrEventBody)+`","schema_version"`, 1),
	} {
		a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
		response, _ := a.post(a.token(), body)
		if response.Code != http.StatusBadRequest || jsonBody(t, response)["reason_code"] != reasonMalformed {
			t.Fatalf("%s: %d: %s", name, response.Code, response.Body)
		}
		if len(a.projector.inputs) != 0 {
			t.Fatalf("%s: the store was reached", name)
		}
	}
}

// Every rejected example of the contract is refused, so no secret member gets in.
func TestAcrEventsRefuseEveryRejectedExample(t *testing.T) {
	names, err := filepath.Glob(filepath.Join(repoRoot(t), "contracts/auth/v1/examples/acr-credential-lifecycle-event/*.json"))
	if err != nil || len(names) < 10 {
		t.Fatalf("examples: %d, %v", len(names), err)
	}
	for _, name := range names {
		base := filepath.Base(name)
		if strings.HasPrefix(base, "valid-") || base == "manifest.json" {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		// The corpus organization id is a placeholder the handler refuses on its own;
		// a real one leaves the schema and the parse as the only refusers.
		body := strings.ReplaceAll(string(raw), `"org_EXAMPLE0000000000000001"`, `"org_`+acrEventOrg+`"`)
		a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
		response, _ := a.post(a.token(), body)
		if response.Code != http.StatusBadRequest || jsonBody(t, response)["reason_code"] != reasonMalformed {
			t.Fatalf("%s: %d: %s", base, response.Code, response.Body)
		}
		if len(a.projector.inputs) != 0 {
			t.Fatalf("%s: the store was reached", base)
		}
	}
}

func TestAcrEventsHandTheEngineTheEventAndTheDigestOfThePostedBytes(t *testing.T) {
	a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
	body := acrEventBody("cred_exampleaaaaaaaaaa", 1, "active")
	if r, _ := a.post(a.token(), body); r.Code != http.StatusNoContent || r.Body.Len() != 0 {
		t.Fatalf("%d: %s", r.Code, r.Body)
	}
	in := a.projector.inputs[0]
	if in.Domain != outboxprojection.DomainAcr || in.AcrClass != "oauth_user" || in.OwnerSubject != "subject_EXAMPLE0000000001" || in.RevokeReason != "" {
		t.Fatalf("input = %+v", in)
	}
	if in.BodyDigest != sha256.Sum256([]byte(body)) {
		t.Fatal("the digest is not of the posted bytes")
	}
	if in.Actor != uuidOfWire(t, a.subject) {
		t.Fatalf("actor = %s, want the verified workload", in.Actor)
	}
	if in.Event.AggregateID != "cred_exampleaaaaaaaaaa" || in.Event.Payload.SourceID != "acr" || in.Event.Payload.Change.Fingerprint != "" {
		t.Fatalf("event = %+v", in.Event)
	}
}

// Every outcome the engine can return has one answer, and no outcome falls
// through to 204.
func TestAcrEventsAnswerEachOutcome(t *testing.T) {
	want := map[outboxprojection.Class]struct {
		status int
		reason string
	}{
		outboxprojection.ClassDone:     {http.StatusNoContent, ""},
		outboxprojection.ClassRetry:    {http.StatusConflict, "projection_retry"},
		outboxprojection.ClassConflict: {http.StatusConflict, "projection_conflict"},
		outboxprojection.ClassRefused:  {http.StatusConflict, "projection_refused"},
		outboxprojection.ClassUnfenced: {http.StatusServiceUnavailable, reasonStoreUnavailable},
	}
	for _, outcome := range outboxprojection.Outcomes {
		a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
		a.projector.outcome = outcome
		answer := want[outboxprojection.ClassOf(outcome)]
		response, _ := a.post(a.token(), acrEventBody("cred_exampleaaaaaaaaaa", 1, "active"))
		if answer.reason == "" {
			if response.Code != answer.status {
				t.Errorf("%s: %d, want %d", outcome, response.Code, answer.status)
			}
			continue
		}
		expectError(t, response, answer.status, answer.reason)
	}
}

func TestAcrEventsAnswerAStateChangeAsARetry(t *testing.T) {
	a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
	a.projector.outcome = outboxprojection.OutcomeStateChanged
	response, _ := a.post(a.token(), acrEventBody("cred_exampleaaaaaaaaaa", 1, "active"))
	expectError(t, response, http.StatusConflict, reasonProjectionRetry)
}

func TestAcrEventsAnswer503OnAnEngineFault(t *testing.T) {
	a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
	a.projector.err = errors.New("store down")
	response, _ := a.post(a.token(), acrEventBody("cred_exampleaaaaaaaaaa", 1, "active"))
	expectError(t, response, http.StatusServiceUnavailable, reasonStoreUnavailable)
}

func TestAcrEventsCountAClosedReason(t *testing.T) {
	a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
	a.projector.outcome = "gap"
	a.post(a.token(), acrEventBody("cred_exampleaaaaaaaaaa", 1, "active"))
	total, _ := a.counter(outcomeCounter, map[string]string{"operation": opAcrCredentialEvent, "result": "denied", "reason_code": reasonProjectionRetry})
	if total != 1 {
		t.Fatalf("outcomes = %d, want 1", total)
	}
}

// An error line holds no event body and no bearer.
func TestAcrEventsLogNoEventText(t *testing.T) {
	a := newAcrEventsFixture(t, acrEventRows(workloadTokenClass))
	a.projector.err = errors.New("store down")
	a.post(a.token(), acrEventBody("cred_exampleaaaaaaaaaa", 1, "active"))
	for _, text := range []string{"cred_exampleaaaaaaaaaa", "subject_EXAMPLE", acrEventOrg, "Bearer"} {
		if strings.Contains(a.logs.String(), text) {
			t.Fatalf("the log holds %q", text)
		}
	}
}

// The route writes only the projection: no acr client, no http client, in the
// handler or the engine.
func TestAcrEventCodeImportsNoClient(t *testing.T) {
	root := repoRoot(t)
	banned := regexp.MustCompile(`http\.(Client|NewRequest|Get|Post|DefaultClient)\b|net/http/httputil|acrclient`)
	files := []string{"internal/auth/authapi/acr_events.go"}
	engine, err := filepath.Glob(filepath.Join(root, "internal/auth/outboxprojection/*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range engine {
		if !strings.HasSuffix(f, "_test.go") {
			files = append(files, strings.TrimPrefix(f, root+string(filepath.Separator)))
		}
	}
	if len(files) < 4 {
		t.Fatalf("files = %v", files)
	}
	for _, f := range files {
		raw, err := os.ReadFile(filepath.Join(root, f))
		if err != nil {
			t.Fatal(err)
		}
		if match := banned.Find(raw); match != nil {
			t.Errorf("%s holds %s", f, match)
		}
		if strings.HasSuffix(f, "store.go") && bytes.Contains(raw, []byte(`"net/http"`)) {
			t.Errorf("%s imports net/http", f)
		}
	}
}
