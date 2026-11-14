package server

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/platform/buildstamp"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// The endpoint-profile rows of the query-api are prose and enums. This file
// drives each protected row through the real handlers (the listener route
// sets, the identity middleware, the document dispatcher; the REST rows
// through the real REST authenticator over the production route patterns,
// because the REST builders need ClickHouse) and compares what came back with
// the row: the status codes the disclosure text states, the organization the
// handler saw, and the impersonation effect. A row that no driver observes
// fails by its id.

var (
	liveTargetOrg = uuid.MustParse("55555555-5555-5555-5555-555555555555")
	liveOtherOrg  = uuid.MustParse("66666666-6666-6666-6666-666666666666")
	liveActor     = uuid.MustParse("77777777-7777-4777-8777-777777777777")
	liveStatusRe  = regexp.MustCompile(`\b([45][0-9][0-9])\b`)
	livePathVarRe = regexp.MustCompile(`\{[^}]+\}`)
)

type liveRow struct {
	id, method, route string
	listeners         []string
	raw               map[string]any
}

func (r liveRow) str(field string) string {
	value, _ := r.raw[field].(string)
	return value
}

func (r liveRow) clone() liveRow {
	raw := map[string]any{}
	for key, value := range r.raw {
		raw[key] = value
	}
	r.raw = raw
	return r
}

func (r liveRow) hasGap(field string) bool {
	gaps, _ := r.raw["gaps"].([]any)
	for _, gap := range gaps {
		if text, _ := gap.(string); strings.HasPrefix(text, field+": declared gap") {
			return true
		}
	}
	return false
}

func loadLiveRows(t *testing.T) []liveRow {
	t.Helper()
	var rows []liveRow
	for _, raw := range loadQueryAPIRows(t) {
		id, _ := raw["id"].(string)
		match := rowListeners.FindStringSubmatch(id)
		if match == nil {
			t.Fatalf("row id %q has no @listener suffix", id)
		}
		row := liveRow{id: id, listeners: strings.Split(match[1], ","), raw: raw}
		row.method, _ = raw["method"].(string)
		row.route, _ = raw["route"].(string)
		rows = append(rows, row)
	}
	return rows
}

type liveAnswer struct {
	status int
	body   string
	header http.Header
}

type liveReq struct {
	listener, method, path, bearer, body string
	headers                              map[string]string
}

func (s *behaviourStack) live(q liveReq) liveAnswer {
	var request *http.Request
	if q.body != "" {
		request = httptest.NewRequest(q.method, q.path, strings.NewReader(q.body))
		request.Header.Set("Content-Type", "application/json")
	} else {
		request = httptest.NewRequest(q.method, q.path, nil)
	}
	if q.bearer != "" {
		request.Header.Set("Authorization", "Bearer "+q.bearer)
	}
	for name, value := range q.headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	s.handlers[q.listener].ServeHTTP(recorder, request)
	return liveAnswer{recorder.Code, recorder.Body.String(), recorder.Header()}
}

func documentBody(document string) string {
	encoded, _ := json.Marshal(map[string]string{"query": document})
	return string(encoded)
}

// liveStack is the real listener stack over a fake store the cell shapes.
func liveStack(t *testing.T, shape func(*accessStore)) *behaviourStack {
	t.Helper()
	minter := newAccessMinter(t)
	store := newAccessStore()
	if shape != nil {
		shape(store)
	}
	stack := newAccessStack(t, store, testAccess(minter.keys, nil, accessTestNow))
	stack.user = minter.mint(ecUser, nil, nil)
	return stack
}

// liveStackKeyDown is the stack whose token key source (the JWKS) cannot be read.
func liveStackKeyDown(t *testing.T) *behaviourStack {
	t.Helper()
	minter := newAccessMinter(t)
	stack := newAccessStack(t, newAccessStore(), testAccess(downKeys{}, nil, accessTestNow))
	stack.user = minter.mint(ecUser, nil, nil)
	return stack
}

func asSuperuser(s *accessStore) {
	s.states[ecUser] = policy.UserState{IsActive: true, IsSuperuser: true, TokenVersion: 5}
}

func withSession(s *accessStore) {
	s.sessions = map[uuid.UUID]*policy.Impersonation{ecUser: {AdminUserID: ecUser, TargetUserID: ecTarget, TargetOrgID: liveTargetOrg, TargetRole: "viewer"}}
}

func edgeBearer(t *testing.T, orgID string) string {
	t.Helper()
	return ecMintHS256Token(t, ecEdgeClaims{sub: ecUser.String(), orgID: orgID, tokenVersion: 5})
}

type liveObs struct {
	row    liveRow
	codes  map[int]bool
	defect []string
}

func newObs(row liveRow) *liveObs { return &liveObs{row: row, codes: map[int]bool{}} }

func (o *liveObs) fail(format string, args ...any) {
	o.defect = append(o.defect, o.row.id+": "+fmt.Sprintf(format, args...))
}

func (o *liveObs) see(status int) { o.codes[status] = true }

func (o *liveObs) wantStatus(cell string, got liveAnswer, want int) {
	o.see(got.status)
	if got.status != want {
		o.fail("%s: status %d, want %d (body %q)", cell, got.status, want, trim(got.body))
	}
}

func (o *liveObs) wantOrg(cell string, stack *behaviourStack, want authctx.Claims) {
	if stack.seen == nil || len(*stack.seen) == 0 {
		o.fail("%s: the handler never ran", cell)
		return
	}
	got := (*stack.seen)[len(*stack.seen)-1]
	if got.OrgID != want.OrgID || got.ImpersonationActive != want.ImpersonationActive || (want.Role != "" && got.Role != want.Role) {
		o.fail("%s: handler saw org=%q role=%q impersonation=%v, want org=%q role=%q impersonation=%v",
			cell, got.OrgID, got.Role, got.ImpersonationActive, want.OrgID, want.Role, want.ImpersonationActive)
	}
}

func (o *liveObs) wantNoRun(cell string, stack *behaviourStack) {
	if stack.seen != nil && len(*stack.seen) != 0 {
		o.fail("%s: the handler ran for a refused request", cell)
	}
}

func trim(body string) string {
	body = strings.TrimSpace(body)
	if len(body) > 80 {
		return body[:80]
	}
	return body
}

// statusText compares the codes the live cells produced with the codes the
// row's disclosure text states, both directions.
func (o *liveObs) statusText() {
	text := o.row.str("disclosure_behavior")
	stated := map[int]bool{}
	for _, match := range liveStatusRe.FindAllString(text, -1) {
		code, _ := strconv.Atoi(match)
		stated[code] = true
	}
	for code := range o.codes {
		if code >= 400 && !stated[code] {
			o.fail("the live stack answers %d, the disclosure text does not state it", code)
		}
	}
	for code := range stated {
		if !o.codes[code] {
			o.fail("the disclosure text states %d, no live cell produced it", code)
		}
	}
}

func (o *liveObs) textHas(field string, phrases ...string) {
	text := o.row.str(field)
	for _, phrase := range phrases {
		if !strings.Contains(text, phrase) {
			o.fail("%s lacks %q, which the live stack shows", field, phrase)
		}
	}
}

// unknownIsDeclared: a field the row states as unknown must carry its named
// gap; it is asserted, never skipped.
func (o *liveObs) unknownIsDeclared(fields ...string) {
	for _, field := range fields {
		if o.row.str(field) == "unknown" && !o.row.hasGap(field) {
			o.fail("%s is unknown with no declared gap", field)
		}
	}
}

// impersonationFact: the live fact the row's impersonation_policy must match.
func (o *liveObs) impersonationPolicy(rebinds bool) {
	want := "subject_to_impersonation_override"
	if !rebinds {
		want = "exempt_no_org_context"
	}
	if got := o.row.str("impersonation_policy"); got != want {
		o.fail("impersonation_policy is %q, the live stack's behaviour is %q", got, want)
	}
}

func edgeSession(t *testing.T, o *liveObs, q liveReq, rebindHeader bool, sessionFailureStatus int) {
	t.Helper()
	run := func(cell string, shape func(*accessStore)) (*behaviourStack, liveAnswer) {
		stack := liveStack(t, shape)
		q2 := q
		q2.bearer = stack.user
		answer := stack.live(q2)
		_ = cell
		return stack, answer
	}
	stack, a := run("live superuser with an active session", func(s *accessStore) { asSuperuser(s); withSession(s) })
	o.wantStatus("live superuser with an active session", a, http.StatusOK)
	o.wantOrg("live superuser with an active session", stack, authctx.Claims{OrgID: liveTargetOrg.String(), Role: "viewer", ImpersonationActive: true})
	if rebindHeader && a.header.Get("X-Impersonating") != "true" {
		o.fail("live superuser with an active session: no X-Impersonating response header")
	}
	stack, a = run("non-superuser with a session row", func(s *accessStore) { withSession(s) })
	o.wantStatus("non-superuser with a session row", a, http.StatusOK)
	o.wantOrg("non-superuser with a session row", stack, authctx.Claims{OrgID: ecOrg.String(), ImpersonationActive: false})
	stack, a = run("superuser with no session", asSuperuser)
	o.wantStatus("superuser with no session", a, http.StatusOK)
	o.wantOrg("superuser with no session", stack, authctx.Claims{OrgID: ecOrg.String(), ImpersonationActive: false})
	stack, a = run("session read fails (fail closed)", func(s *accessStore) {
		asSuperuser(s)
		withSession(s)
		s.errSession = fmt.Errorf("session store timeout")
	})
	o.wantStatus("session read fails (fail closed)", a, sessionFailureStatus)
	if sessionFailureStatus == http.StatusServiceUnavailable && strings.TrimSpace(a.body) != restUnavailable {
		o.fail("session read fails (fail closed): body %q, want %q", trim(a.body), restUnavailable)
	}
	o.wantNoRun("session read fails (fail closed)", stack)
	const legacy = "legacy impersonation caller, AUTH_PROXY_IMPERSONATION on"
	stack = liveStackRefusing(t, func(s *accessStore) { asSuperuser(s); withSession(s) })
	q2 := q
	q2.bearer = stack.user
	a = stack.live(q2)
	o.wantStatus(legacy, a, http.StatusForbidden)
	o.wantNoRun(legacy, stack)
	if !strings.Contains(a.body, "Not allowed in an impersonation session") {
		o.fail("%s: body %q lacks %q", legacy, trim(a.body), "Not allowed in an impersonation session")
	}
}

// liveStackRefusing is liveStack with AUTH_PROXY_IMPERSONATION on.
func liveStackRefusing(t *testing.T, shape func(*accessStore)) *behaviourStack {
	t.Helper()
	minter := newAccessMinter(t)
	store := newAccessStore()
	shape(store)
	stack := newAccessStackRefusing(t, newQueryAPIUserStoreFrom(store), testAccess(minter.keys, nil, accessTestNow), true)
	stack.user = minter.mint(ecUser, nil, nil)
	return stack
}

type liveREST struct {
	handler  http.Handler
	envelope string
	user     string
	orgless  string
	store    *accessStore
	priv     ed25519.PrivateKey
	minter   *accessMinter
}

func newLiveREST(t *testing.T, pattern string, shape func(*accessStore)) *liveREST {
	t.Helper()
	minter := newAccessMinter(t)
	store := newAccessStore()
	if shape != nil {
		shape(store)
	}
	setup := testAccess(minter.keys, nil, accessTestNow)
	verifier, priv := iaVerifier(t)
	edge, err := principal.NewEdgeVerifier(newQueryAPIUserStoreFrom(store))
	if err != nil {
		t.Fatal(err)
	}
	if err := routeEdgeVerifier(edge, setup.opts); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authenticateRESTRequest(w, r, verifier, edge, "live")
		if !ok {
			return
		}
		_, _ = fmt.Fprintf(w, `{"org":%q}`, claims.OrgID)
	})
	return &liveREST{
		handler:  mux,
		envelope: iaEnvelope(t, priv, principal.Claims{OrgID: ecOrg.String(), Role: "admin"}),
		user:     minter.mint(ecUser, nil, nil),
		orgless:  minter.mint(ecUser, map[string]any{"org_id": nil}, nil),
		store:    store,
		priv:     priv,
		minter:   minter,
	}
}

// stateCell is one current-state cell: the same credential answers ok, the store changes, and the next request
// answers after.
type stateCell struct {
	name   string
	change func(*accessStore)
	after  int
}

// userStateCells change what a user access token's request reads between two requests: nothing read for the first
// request may answer the second.
var userStateCells = []stateCell{
	{"users row deactivated after the first request", func(s *accessStore) {
		s.states[ecUser] = policy.UserState{IsActive: false, TokenVersion: 5}
	}, http.StatusUnauthorized},
	{"session revoked after the first request", func(s *accessStore) { delete(s.liveSessions, accessTestSession) }, http.StatusUnauthorized},
	{"token version moved after the first request", func(s *accessStore) {
		s.states[ecUser] = policy.UserState{IsActive: true, TokenVersion: 6}
	}, http.StatusUnauthorized},
	{"membership removed after the first request", func(s *accessStore) { delete(s.members, [2]uuid.UUID{ecUser, ecOrg}) }, http.StatusUnauthorized},
}

// noStateRead is the change after which a credential that reads no state still answers: every store read fails.
func noStateRead(s *accessStore) {
	s.errUserState = fmt.Errorf("down: %w", policy.ErrUnavailable)
	s.errSession = fmt.Errorf("down: %w", policy.ErrUnavailable)
	s.errSessionState = fmt.Errorf("down: %w", policy.ErrUnavailable)
	s.errIsMember = fmt.Errorf("down: %w", policy.ErrUnavailable)
}

// stackStateCells runs userStateCells on a listener stack: q with the stack's user token.
func stackStateCells(t *testing.T, o *liveObs, label string, q liveReq) {
	t.Helper()
	stackStateCellsAs(t, o, label, q, nil)
}

// stackStateCellsAs is stackStateCells over a store base shapes first.
func stackStateCellsAs(t *testing.T, o *liveObs, label string, q liveReq, base func(*accessStore)) {
	t.Helper()
	for _, cell := range userStateCells {
		var store *accessStore
		stack := liveStack(t, func(s *accessStore) {
			if base != nil {
				base(s)
			}
			store = s
		})
		q2 := q
		q2.bearer = stack.user
		o.wantStatus(label+"user token, first request ("+cell.name+")", stack.live(q2), http.StatusOK)
		cell.change(store)
		o.wantStatus(label+"user token, "+cell.name, stack.live(q2), cell.after)
	}
}

func (f *liveREST) do(method, path, bearer string, headers map[string]string) liveAnswer {
	request := httptest.NewRequest(method, path, nil)
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, request)
	return liveAnswer{recorder.Code, recorder.Body.String(), recorder.Header()}
}

func restOrg(a liveAnswer) string {
	var out struct{ Org string }
	_ = json.Unmarshal([]byte(a.body), &out)
	return out.Org
}

func driveREST(t *testing.T, row liveRow) *liveObs {
	t.Helper()
	o := newObs(row)
	path := livePathVarRe.ReplaceAllString(row.route, "x")
	fixture := func(shape func(*accessStore)) *liveREST { return newLiveREST(t, row.route, shape) }
	f := fixture(nil)
	o.wantStatus("no credential", f.do(row.method, path, "", nil), http.StatusUnauthorized)
	o.wantStatus("unparseable bearer", f.do(row.method, path, "not.a.token", nil), http.StatusUnauthorized)

	accepted := map[string]bool{}
	if list, ok := row.raw["accepted_credential_classes"].([]any); ok {
		for _, class := range list {
			accepted[class.(string)] = true
		}
	}
	other := map[string]string{"X-Org-Id": liveOtherOrg.String()}
	for class, carrier := range map[string]string{classEnvelope: f.envelope, classEdgeToken: edgeBearer(t, ecOrg.String()), classUserToken: f.user} {
		got := f.do(row.method, path+"?org_id="+liveOtherOrg.String(), carrier, other)
		if !accepted[class] {
			o.wantStatus("class "+class+" (not accepted by the row)", got, http.StatusUnauthorized)
			continue
		}
		o.wantStatus("class "+class, got, http.StatusOK)
		if org := restOrg(got); org != ecOrg.String() {
			o.fail("class %s: the handler saw org %q, want the credential's %q (a header or parameter named %q)", class, org, ecOrg, liveOtherOrg)
		}
	}
	o.textHas("resource_resolver", "organization_from_credential")
	disclosureKnown := row.str("disclosure_behavior") != "unknown"

	got := f.do(row.method, path, f.orgless, nil)
	o.wantStatus("user token with no organization", got, http.StatusForbidden)
	if !strings.Contains(got.body, "Organization context required") {
		o.fail("user token with no organization: body %q lacks the 403 text", trim(got.body))
	}
	if disclosureKnown {
		o.textHas("disclosure_behavior", "403 Organization context required", "superuser, who passes")
	}
	super := fixture(asSuperuser)
	o.wantStatus("superuser with no organization", super.do(row.method, path, super.orgless, nil), http.StatusOK)
	down := fixture(func(s *accessStore) {
		s.errUserState = fmt.Errorf("down: %w", policy.ErrUnavailable)
	})
	o.wantStatus("users store unavailable", down.do(row.method, path, down.user, nil), http.StatusServiceUnavailable)
	broken := fixture(func(s *accessStore) { s.errUserState = fmt.Errorf("syntax error") })
	o.wantStatus("users store error", broken.do(row.method, path, broken.user, nil), http.StatusInternalServerError)
	if strings.Contains(row.route, "{") {
		restIDCells(t, o, row)
		o.textHas("disclosure_behavior", "404 for a path id with no row in the caller's organization", "the same answer as an id that exists nowhere")
	}
	if disclosureKnown {
		o.statusText()
	} else {
		o.unknownIsDeclared("disclosure_behavior")
	}

	restStateCells(t, o, row, path, fixture, accepted)
	restImpersonationCells(t, o, row, path, fixture, accepted)
	return o
}

// liveKnownPerson is the person id orgBoundPersonClient knows, in liveOtherOrg only.
const liveKnownPerson = "0123456789abcdef0123456789abcdef"

// orgBoundPersonClient answers the person lookup the way its statement reads: an identity only for the person id it
// knows and only under the organization it binds; every other read has no rows.
type orgBoundPersonClient struct{}

func (orgBoundPersonClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	org, _ := bindingValue(bindings, "org_id")
	person, _ := bindingValue(bindings, "person_id")
	if strings.Contains(query, "lower(hex(MD5(identity)))") && org == liveOtherOrg.String() && person == liveKnownPerson {
		return &personFoundScanner{identity: "alice@example.com"}, nil
	}
	return emptyScanner{}, nil
}

var livePersonHandlers = map[string]func(*people.Reader) http.HandlerFunc{
	"/api/v1/people/{person_id}/summary":          newPeopleSummaryHandler,
	"/api/v1/people/{person_id}/metric":           newPeopleMetricHandler,
	"/api/v1/people/{person_id}/drilldown/issues": newPeopleDrilldownIssuesHandler,
	"/api/v1/people/{person_id}/drilldown/prs":    newPeopleDrilldownPRsHandler,
}

// restIDCells drives the real handler of a row whose path names an id: the id is looked up under the credential's
// organization only, so an id another organization holds answers the same 404 as an id nothing holds.
func restIDCells(t *testing.T, o *liveObs, row liveRow) {
	t.Helper()
	ask := func(handler http.HandlerFunc, name, value, org, query string) liveAnswer {
		target := livePathVarRe.ReplaceAllString(row.route, value)
		if query != "" {
			target += "?" + query
		}
		request := httptest.NewRequest(row.method, target, nil)
		request.SetPathValue(name, value)
		request = request.WithContext(authctx.WithClaims(request.Context(), authctx.Claims{OrgID: org, Scope: pepgate.Legacy(org)}))
		recorder := httptest.NewRecorder()
		serveRoute(t, handler, recorder, request)
		return liveAnswer{recorder.Code, recorder.Body.String(), recorder.Header()}
	}
	if build, ok := livePersonHandlers[row.route]; ok {
		t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
		reader, err := people.NewReader(orgBoundPersonClient{})
		if err != nil {
			t.Fatal(err)
		}
		handler := build(reader)
		query := ""
		if strings.HasSuffix(row.route, "/metric") {
			query = "metric=cycle_time"
		}
		own := ask(handler, "person_id", liveKnownPerson, liveOtherOrg.String(), query)
		if own.status == http.StatusNotFound {
			o.fail("the person in its own organization: status 404, want it found (body %q)", trim(own.body))
		}
		foreign := ask(handler, "person_id", liveKnownPerson, ecOrg.String(), query)
		o.wantStatus("a person id another organization holds", foreign, http.StatusNotFound)
		unknown := ask(handler, "person_id", strings.Repeat("f", 32), ecOrg.String(), query)
		o.wantStatus("a person id nothing holds", unknown, http.StatusNotFound)
		if foreign.body != unknown.body {
			o.fail("a person of another organization answers %q, an unknown person %q: the answer tells them apart", trim(foreign.body), trim(unknown.body))
		}
		return
	}
	if row.route != "/api/v1/work-units/{work_unit_id}/explain" {
		o.fail("no id cell drives the handler of %s", row.route)
		return
	}
	t.Setenv("LLM_PROVIDER", "mock")
	client := &capturingWorkUnitsClient{}
	reader, err := investmentexplain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	handler := newWorkUnitExplainHandler(reader, nil, nil, nil, testPlatformGate{})
	got := ask(handler, "work_unit_id", "wu-live-1", ecOrg.String(), "llm_provider=mock")
	o.wantStatus("a work unit id with no row in the caller's organization", got, http.StatusNotFound)
	if !strings.Contains(got.body, "Work unit wu-live-1 not found") {
		o.fail("work unit not found: body %q", trim(got.body))
	}
	bound := false
	for i, statement := range client.queries {
		if !strings.Contains(statement, "work_unit_investments") {
			continue
		}
		org, _ := bindingValue(client.bindings[i], "org_id")
		unit, _ := bindingValue(client.bindings[i], "work_unit_id")
		bound = bound || (org == ecOrg.String() && unit == "wu-live-1")
	}
	if !bound {
		o.fail("no work unit read binds the caller's organization and the path id: an id of another organization is not refused by the read")
	}
}

// restStateCells: a user access token's users row, session, token version and membership are read on every
// request, so a change between two requests answers the second.
func restStateCells(t *testing.T, o *liveObs, row liveRow, path string, fixture func(func(*accessStore)) *liveREST, accepted map[string]bool) {
	t.Helper()
	if accepted[classUserToken] {
		for _, cell := range userStateCells {
			f := fixture(nil)
			o.wantStatus("user token, first request ("+cell.name+")", f.do(row.method, path, f.user, nil), http.StatusOK)
			cell.change(f.store)
			o.wantStatus("user token, "+cell.name, f.do(row.method, path, f.user, nil), cell.after)
		}
	}
	o.textHas("current_state_cache_behavior", "none: per-request read", "nothing is cached between requests")
}

// restImpersonationCells: a REST row serves the organization its credential names, and an impersonation credential
// names the target's: a delegated user access token is served as the target organization, never as the actor's.
func restImpersonationCells(t *testing.T, o *liveObs, row liveRow, path string, fixture func(func(*accessStore)) *liveREST, accepted map[string]bool) {
	t.Helper()
	if accepted[classUserToken] {
		f := fixture(func(s *accessStore) { s.members[[2]uuid.UUID{ecUser, liveTargetOrg}] = true })
		delegated := f.minter.mint(ecUser, map[string]any{"org_id": liveTargetOrg.String(), "act": map[string]any{"sub": wirePrincipal(liveActor)}}, nil)
		got := f.do(row.method, path, delegated, nil)
		o.wantStatus("delegated user token for the target organization", got, http.StatusOK)
		if org := restOrg(got); org != liveTargetOrg.String() {
			o.fail("delegated user token: the handler saw org %q, want the target's %q", org, liveTargetOrg)
		}
	}
	o.impersonationPolicy(true)
}

func driveInternal(t *testing.T, row liveRow) *liveObs {
	t.Helper()
	o := newObs(row)
	document := documentBody(iaDocument)
	for _, listener := range row.listeners {
		q := liveReq{listener: listener, method: row.method, path: row.route, body: document}
		with := func(mutate func(*liveReq)) liveReq { c := q; mutate(&c); return c }
		stack := liveStack(t, nil)
		o.wantStatus(listener+": no credential", stack.live(q), http.StatusUnauthorized)
		o.wantStatus(listener+": unparseable bearer", stack.live(with(func(c *liveReq) { c.bearer = "not.a.token" })), http.StatusUnauthorized)
		o.wantStatus(listener+": edge token (retired HS256 class)", stack.live(with(func(c *liveReq) { c.bearer = stack.edge })), http.StatusUnauthorized)
		o.wantStatus(listener+": envelope (retired class)", stack.live(with(func(c *liveReq) { c.bearer = stack.envelope })), http.StatusUnauthorized)
		o.wantNoRun(listener+": envelope (retired class)", stack)
		stack = liveStack(t, nil)
		a := stack.live(with(func(c *liveReq) {
			c.bearer = stack.user
			c.headers = map[string]string{"X-Org-Id": liveOtherOrg.String()}
		}))
		o.wantStatus(listener+": user token", a, http.StatusOK)
		o.wantOrg(listener+": user token", stack, authctx.Claims{OrgID: ecOrg.String()})
		stack = liveStack(t, nil)
		if row.route != "/query/proof-write" {
			o.wantStatus(listener+": unregistered document", stack.live(with(func(c *liveReq) {
				c.bearer = stack.user
				c.body = documentBody("query Unregistered { nothing }")
			})), http.StatusNotFound)
		}

		// Impersonation: the edge carrier reads the session; the internal
		// headers state it themselves and never read one.
		sessionDown := func(s *accessStore) { asSuperuser(s); s.errSession = fmt.Errorf("session store timeout") }
		if listener == "internal" {
			stack = liveStack(t, sessionDown)
			a = stack.live(with(func(c *liveReq) {
				c.headers = map[string]string{
					internalidentity.HeaderOrgID: ecOrg.String(), internalidentity.HeaderRole: "admin",
					internalidentity.HeaderSuperuser: "false", internalidentity.HeaderImpersonationActive: "true",
				}
			}))
			o.wantStatus("internal headers claiming impersonation", a, http.StatusOK)
			o.wantOrg("internal headers claiming impersonation", stack, authctx.Claims{OrgID: ecOrg.String(), ImpersonationActive: true})
		}
		edgeSession(t, o, q, false, http.StatusServiceUnavailable)
		if row.route == "/query/proof-write" {
			proofWriteCells(t, o)
		}
		stackStateCells(t, o, listener+": ", q)
		if listener == "internal" {
			stack = liveStack(t, noStateRead)
			o.wantStatus("internal headers, every store read failing", stack.live(with(func(c *liveReq) {
				c.headers = map[string]string{
					internalidentity.HeaderOrgID: ecOrg.String(), internalidentity.HeaderRole: "admin",
					internalidentity.HeaderSuperuser: "false", internalidentity.HeaderImpersonationActive: "false",
				}
			})), http.StatusOK)
		}
	}
	o.textHas("resource_resolver", "organization_from_credential")
	o.impersonationPolicy(true)
	o.textHas("current_state_cache_behavior", "none: per-request read", "nothing is cached between requests",
		"the internal identity headers read no state")
	if row.route == "/query/proof-write" {
		o.textHas("current_state_cache_behavior", "the go_api_proof_orgs allowlist is read on every request")
	}
	o.statusText()
	return o
}

// proofWriteCells drives the real dispatcher the route mounts: the stack's
// probe stands in for the proof-write handler there, and the allowlist is a
// database read the stack does not carry.
func proofWriteCells(t *testing.T, o *liveObs) {
	t.Helper()
	const mutation = "mutation LiveProofWrite { probe }"
	for _, cell := range []struct {
		name    string
		allowed bool
		readErr error
		want    int
		wantRan int
	}{
		{"allowed", true, nil, http.StatusOK, 1},
		{"not on the list", false, nil, http.StatusForbidden, 0},
		{"allowlist read fails", true, errors.New("allowlist read failed"), http.StatusServiceUnavailable, 0},
	} {
		verifier, _ := iaVerifier(t)
		ran := 0
		mux := routeswitch.NewMux(routeswitch.StaticSwitch{"m": true})
		mux.Register("m", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ran++ }))
		handler := newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(mutation): "m"}, verifier, nil, nil, digest.KindMutation,
			func(_ context.Context, org string) (bool, error) {
				return cell.allowed && org == ecOrg.String(), cell.readErr
			})
		request := httptest.NewRequest(http.MethodPost, "/query/proof-write", strings.NewReader(documentBody(mutation)))
		request.Header.Set("Content-Type", "application/json")
		iaHeaders(ecOrg.String(), "admin", false, false)(request)
		recorder := httptest.NewRecorder()
		handler(recorder, request)
		o.see(recorder.Code)
		if recorder.Code != cell.want || ran != cell.wantRan {
			o.fail("proof-write, %s: status %d, ran %d, want %d, %d", cell.name, recorder.Code, ran, cell.want, cell.wantRan)
		}
		if cell.want == http.StatusServiceUnavailable {
			if problem := unavailableProblem(faultAnswer{recorder.Code, recorder.Body.String(), recorder.Header()}); problem != "" {
				o.fail("proof-write, %s: %s", cell.name, problem)
			}
		}
	}
	o.textHas("resource_resolver", "go_api_proof_orgs")
}

func driveBuildInfo(t *testing.T, row liveRow) *liveObs {
	t.Helper()
	o := newObs(row)
	for _, listener := range row.listeners {
		q := liveReq{listener: listener, method: row.method, path: row.route}
		stack := liveStack(t, nil)
		o.wantStatus(listener+": no credential", stack.live(q), http.StatusUnauthorized)
		q.bearer = "not.a.token"
		o.wantStatus(listener+": unparseable bearer", stack.live(q), http.StatusUnauthorized)
		q.bearer = stack.edge
		o.wantStatus(listener+": edge token (not an accepted class)", stack.live(q), http.StatusUnauthorized)
		q.bearer = stack.envelope
		o.wantStatus(listener+": envelope (retired class)", stack.live(q), http.StatusUnauthorized)
		q.bearer = ""
		identity := liveBuildIdentity(t)
		if listener == "internal" {
			for name, headers := range map[string][4]string{
				"another organization":  {liveOtherOrg.String(), "viewer", "false", "false"},
				"no organization":       {"", "viewer", "false", "false"},
				"impersonation active":  {ecOrg.String(), "admin", "false", "true"},
				"another org and super": {liveOtherOrg.String(), "viewer", "true", "false"},
			} {
				for _, store := range []func(*accessStore){nil, noStateRead} {
					h := q
					h.headers = map[string]string{internalidentity.HeaderOrgID: headers[0], internalidentity.HeaderRole: headers[1],
						internalidentity.HeaderSuperuser: headers[2], internalidentity.HeaderImpersonationActive: headers[3]}
					got := liveStack(t, store).live(h)
					o.see(got.status)
					if got.status != http.StatusOK || got.body != identity {
						o.fail("internal headers, %s (every store read failing: %v): status %d body %q, want the build identity %q: the credential's organization and no state are read",
							name, store != nil, got.status, trim(got.body), trim(identity))
					}
				}
			}
		}
		buildInfoUserTokenCells(t, o, listener, q, identity)
		stackStateCellsAs(t, o, listener+": platform admin, ", q, asSuperuser)
		down := liveStack(t, func(s *accessStore) { asSuperuser(s); s.errUserState = fmt.Errorf("down: %w", policy.ErrUnavailable) })
		q.bearer = down.user
		got := down.live(q)
		o.wantStatus(listener+": platform admin, users store unavailable", got, http.StatusServiceUnavailable)
		if strings.TrimSpace(got.body) != restUnavailable {
			o.fail("%s: platform admin, users store unavailable: body %q, want %q", listener, trim(got.body), restUnavailable)
		}
		q.bearer = ""
	}
	o.textHas("resource_resolver", "none:")
	o.impersonationPolicy(false)
	o.textHas("current_state_cache_behavior", "none: per-request read", "nothing is cached between requests",
		"the internal identity headers read no state")
	o.textHas("disclosure_behavior", "not a platform admin (admin.orgs)", "active impersonation session", "delegated")
	o.statusText()
	return o
}

// liveBuildIdentity is the body /buildinfo serves an accepted caller: the process's build identity.
func liveBuildIdentity(t *testing.T) string {
	t.Helper()
	body, err := buildstamp.Body(version.Current("query-api"))
	if err != nil {
		t.Fatal(err)
	}
	return string(body) + "\n"
}

// buildInfoUserTokenCells: /buildinfo serves a user access token only when its live users row is a platform admin
// (admin.orgs) acting as itself; an organization role, an active impersonation session and a delegated token each get
// the bare 401 every unaccepted class gets, before any build identity is written.
func buildInfoUserTokenCells(t *testing.T, o *liveObs, listener string, q liveReq, identity string) {
	t.Helper()
	answer := func(shape func(*accessStore), extra map[string]any) liveAnswer {
		minter := newAccessMinter(t)
		store := newAccessStore()
		if shape != nil {
			shape(store)
		}
		stack := newAccessStack(t, store, testAccess(minter.keys, nil, accessTestNow))
		q2 := q
		q2.bearer = minter.mint(ecUser, extra, nil)
		return stack.live(q2)
	}
	for name, cell := range map[string]struct {
		shape func(*accessStore)
		extra map[string]any
	}{
		"platform-admin user token":             {asSuperuser, nil},
		"platform-admin user token with no org": {asSuperuser, map[string]any{"org_id": nil}},
	} {
		got := answer(cell.shape, cell.extra)
		o.see(got.status)
		if got.status != http.StatusOK || got.body != identity {
			o.fail("%s: %s: status %d body %q, want 200 and the build identity %q", listener, name, got.status, trim(got.body), trim(identity))
		}
	}
	refused := map[string]struct {
		shape func(*accessStore)
		extra map[string]any
	}{
		"platform-admin user token with an active impersonation session": {func(s *accessStore) { asSuperuser(s); withSession(s) }, nil},
		"delegated platform-admin user token":                            {asSuperuser, map[string]any{"act": map[string]any{"sub": wirePrincipal(liveActor)}}},
	}
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		role := role
		refused[role+" user token"] = struct {
			shape func(*accessStore)
			extra map[string]any
		}{func(s *accessStore) { s.roles = map[[2]uuid.UUID]string{{ecUser, ecOrg}: role} }, nil}
	}
	for name, cell := range refused {
		got := answer(cell.shape, cell.extra)
		o.wantStatus(listener+": "+name, got, http.StatusUnauthorized)
		if strings.Contains(got.body, identity) || strings.TrimSpace(got.body) != "unauthorized" {
			o.fail("%s: %s: body %q, want the bare refusal and no build identity", listener, name, trim(got.body))
		}
	}
}

func driveGraphQL(t *testing.T, row liveRow) *liveObs {
	t.Helper()
	o := newObs(row)
	for _, listener := range row.listeners {
		request := func(document string, headers map[string]string, bearer string) liveReq {
			q := liveReq{listener: listener, method: row.method, path: "/graphql", bearer: bearer, headers: headers}
			if row.method == http.MethodGet {
				q.path = "/graphql?query=" + url.QueryEscape(document)
			} else {
				q.body = documentBody(document)
			}
			return q
		}
		stack := liveStack(t, nil)
		a := stack.live(request(iaDocument, nil, ""))
		o.wantStatus(listener+": no credential", a, http.StatusUnauthorized)
		if !strings.Contains(a.body, "Authentication required") {
			o.fail("no credential: body %q lacks %q", trim(a.body), "Authentication required")
		}
		o.wantStatus(listener+": unparseable bearer", stack.live(request(iaDocument, nil, "not.a.token")), http.StatusUnauthorized)
		o.wantStatus(listener+": envelope (not an accepted class)", stack.live(request(iaDocument, nil, stack.envelope)), http.StatusUnauthorized)
		browser := request(iaDocument, map[string]string{"Accept": "text/html"}, "")
		browser.method = http.MethodGet
		browser.path = "/graphql?query=" + url.QueryEscape(iaDocument)
		browser.body = ""
		o.wantStatus(listener+": browser GET", stack.live(browser), http.StatusNotFound)

		type orgCell struct {
			name   string
			shape  func(*accessStore)
			header string
			status int
			org    string
		}
		for _, cell := range []orgCell{
			{"no X-Org-Id", nil, "", http.StatusOK, ecOrg.String()},
			{"own X-Org-Id", nil, ecOrg.String(), http.StatusOK, ecOrg.String()},
			{"X-Org-Id of an organization the caller is not in", nil, liveOtherOrg.String(), http.StatusForbidden, ""},
			{"X-Org-Id of an organization the caller is a member of", func(s *accessStore) { s.members[[2]uuid.UUID{ecUser, liveOtherOrg}] = true }, liveOtherOrg.String(), http.StatusOK, liveOtherOrg.String()},
			{"superuser X-Org-Id of another organization", asSuperuser, liveOtherOrg.String(), http.StatusOK, liveOtherOrg.String()},
		} {
			for name, bearer := range map[string]func(*behaviourStack) string{"user token": func(s *behaviourStack) string { return s.user }} {
				stack = liveStack(t, cell.shape)
				headers := map[string]string{}
				if cell.header != "" {
					headers["X-Org-Id"] = cell.header
				}
				label := listener + ": " + name + ", " + cell.name
				o.wantStatus(label, stack.live(request(iaDocument, headers, bearer(stack))), cell.status)
				if cell.status == http.StatusOK {
					o.wantOrg(label, stack, authctx.Claims{OrgID: cell.org})
				} else {
					o.wantNoRun(label, stack)
				}
			}
		}
		stack = liveStack(t, nil)
		o.wantStatus(listener+": edge token (retired HS256 class)", stack.live(request(iaDocument, nil, stack.edge)), http.StatusUnauthorized)
		o.wantNoRun(listener+": edge token (retired HS256 class)", stack)
		stack = liveStack(t, nil)
		a = stack.live(request("query Unregistered { nothing }", nil, stack.user))
		o.wantStatus(listener+": unregistered document", a, http.StatusNotFound)
		if !strings.Contains(a.body, "UNREGISTERED_DOCUMENT") {
			o.fail("unregistered document: body %q lacks UNREGISTERED_DOCUMENT", trim(a.body))
		}
		edgeSession(t, o, request(iaDocument, nil, ""), true, http.StatusServiceUnavailable)
		broken := liveStack(t, func(s *accessStore) { s.errUserState = fmt.Errorf("syntax error") })
		o.wantStatus(listener+": user token, users store error", broken.live(request(iaDocument, nil, broken.user)), http.StatusInternalServerError)
		o.wantNoRun(listener+": user token, users store error", broken)
		down := liveStackKeyDown(t)
		cell := listener + ": user token, token key source unreadable"
		o.wantStatus(cell, down.live(request(iaDocument, nil, down.user)), http.StatusServiceUnavailable)
		o.wantNoRun(cell, down)
		stackStateCells(t, o, listener+": ", request(iaDocument, nil, ""))
	}
	o.textHas("resource_resolver", "organization_from_header_or_credential", "X-Org-Id")
	o.impersonationPolicy(true)
	o.textHas("current_state_cache_behavior", "none: per-request read", "nothing is cached between requests")
	o.statusText()
	return o
}

func driveMCP(t *testing.T, row liveRow) *liveObs {
	t.Helper()
	o := newObs(row)
	body := mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg))
	fresh := func(sw routeswitch.Switch) mcpTestListeners {
		return newMCPTestListeners(t, &countingMCPClient{}, sw, mcpDefaultLimits())
	}
	post := func(l mcpTestListeners, header http.Header, document string) int {
		code := mcpDo(l.mcp, http.MethodPost, header, document).Code
		o.see(code)
		return code
	}
	cell := func(name string, got, want int) {
		if got != want {
			o.fail("%s: status %d, want %d", name, got, want)
		}
	}
	l := fresh(allMCPRootsEnabled())
	cell("no identity", post(l, http.Header{"Content-Type": {"application/json"}}, body), http.StatusUnauthorized)
	cell("a bearer token", post(l, http.Header{"Content-Type": {"application/json"}, "Authorization": {"Bearer not.a.token"}}, body), http.StatusUnauthorized)
	cell("a valid identity", post(l, validMCPHeaders(), body), http.StatusOK)
	cell("an orgId that is not the identity's organization", post(l, validMCPHeaders(), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(liveOtherOrg.String()))), http.StatusForbidden)
	for name, header := range map[string]http.Header{
		"superuser":     mcpHeaders(mcpTestOrg, "member", "true", "false"),
		"impersonation": mcpHeaders(mcpTestOrg, "member", "false", "true"),
		"operator role": mcpHeaders(mcpTestOrg, "operator", "false", "false"),
	} {
		cell(name, post(l, header, body), http.StatusForbidden)
	}
	cell("a root field outside the allowlist", post(l, validMCPHeaders(), mcpBody(t, `query D { dataHealth(team: "t1") { __typename } }`, nil)), http.StatusForbidden)
	sw := allMCPRootsEnabled()
	sw[mcpRoutingOperationPrefix+"hotspots"] = false
	cell("a root field the class row has off", post(fresh(sw), validMCPHeaders(), body), http.StatusNotFound)
	o.textHas("resource_resolver", "internal identity headers", "a bearer token is refused")
	cell("a valid identity again after a refused one", post(l, validMCPHeaders(), body), http.StatusOK)
	o.textHas("current_state_cache_behavior", "none: no state read", "nothing is read or cached")
	impersonating := mcpDo(l.mcp, http.MethodPost, mcpHeaders(mcpTestOrg, "member", "false", "true"), body)
	o.see(impersonating.Code)
	if reason, _ := mcpReason(t, impersonating); impersonating.Code != http.StatusForbidden || reason != mcpReasonElevatedClaim {
		o.fail("an identity claiming impersonation: status %d reason %q, want 403 %s", impersonating.Code, reason, mcpReasonElevatedClaim)
	}
	if got := row.str("impersonation_policy"); got != "not_applicable_non_main_app" {
		o.fail("impersonation_policy is %q: the MCP caller class refuses every impersonation claim (403 %s), so no override reaches it", got, mcpReasonElevatedClaim)
	}
	o.statusText()
	return o
}

var liveDrivers = map[string]func(*testing.T, liveRow) *liveObs{
	"rest":      driveREST,
	"internal":  driveInternal,
	"buildinfo": driveBuildInfo,
	"graphql":   driveGraphQL,
	"mcp":       driveMCP,
}

func classifyLive(row liveRow) string {
	if row.str("classification") == "public" {
		return "public"
	}
	if strings.HasPrefix(row.route, "/api/v1/") {
		return "rest"
	}
	switch {
	case row.route == "/query" && len(row.listeners) == 1 && row.listeners[0] == "mcp":
		return "mcp"
	case row.route == "/buildinfo":
		return "buildinfo"
	case row.route == "/graphql":
		return "graphql"
	case row.route == "/query/proof-write":
		return "internal"
	case row.route == "/query" || row.route == "/query/proof" || row.route == "/query/run-operation":
		return "internal"
	}
	return ""
}

// driveRow is the defects of one row: a row with no driver is one.
func driveRow(t *testing.T, row liveRow) []string {
	t.Helper()
	kind := classifyLive(row)
	if kind == "public" {
		return nil
	}
	driver, ok := liveDrivers[kind]
	if !ok {
		return []string{row.id + ": no live driver observes this row (kind " + strconv.Quote(kind) + ")"}
	}
	return driver(t, row).defect
}

func TestQueryAPIRowsMatchLiveBehaviour(t *testing.T) {
	quietStdLog(t)
	rows := loadLiveRows(t)
	kinds := map[string]int{}
	for _, row := range rows {
		kinds[classifyLive(row)]++
		t.Run(row.id, func(t *testing.T) {
			for _, defect := range driveRow(t, row) {
				t.Error(defect)
			}
		})
	}
	var unused []string
	for kind := range liveDrivers {
		if kinds[kind] == 0 {
			unused = append(unused, kind)
		}
	}
	sort.Strings(unused)
	if len(unused) != 0 {
		t.Errorf("drivers with no row: %v", unused)
	}
	if kinds["public"] == 0 || len(rows) < 40 {
		t.Fatalf("only %d rows (%d public): the contract was not read", len(rows), kinds["public"])
	}
}

func liveRowByID(t *testing.T, id string) liveRow {
	t.Helper()
	for _, row := range loadLiveRows(t) {
		if row.id == id {
			return row.clone()
		}
	}
	t.Fatalf("no row %q", id)
	return liveRow{}
}

func mustDefect(t *testing.T, name string, defects []string, id, contains string) {
	t.Helper()
	for _, defect := range defects {
		if strings.HasPrefix(defect, id) && strings.Contains(defect, contains) {
			return
		}
	}
	t.Errorf("%s: no defect naming %q containing %q; got %v", name, id, contains, defects)
}

// TestLiveRowGuardFiresOnPlantedRows: each planted row differs from the live
// behaviour in one field, and the guard names that row.
func TestLiveRowGuardFiresOnPlantedRows(t *testing.T) {
	quietStdLog(t)
	const rest = "GET /api/v1/home [dev-health-query-api @query,internal]"
	const query = "POST /query [dev-health-query-api @query,internal]"
	const buildinfo = "GET /buildinfo [dev-health-query-api @query,internal]"

	t.Run("R1 a disclosure that swaps 403 for 401", func(t *testing.T) {
		row := liveRowByID(t, rest)
		row.raw["disclosure_behavior"] = strings.Replace(row.str("disclosure_behavior"), "403 Organization context required", "401 Organization context required", 1)
		mustDefect(t, "swap", driveRow(t, row), row.id, "answers 403")
	})
	t.Run("R1b a disclosure that drops the 503", func(t *testing.T) {
		row := liveRowByID(t, rest)
		row.raw["disclosure_behavior"] = strings.Replace(row.str("disclosure_behavior"), "503 when", "when", 1)
		mustDefect(t, "drop", driveRow(t, row), row.id, "answers 503")
	})
	t.Run("R1c a disclosure that states a status nothing answers", func(t *testing.T) {
		row := liveRowByID(t, query)
		row.raw["disclosure_behavior"] = row.str("disclosure_behavior") + "; 409 for a conflict"
		mustDefect(t, "extra", driveRow(t, row), row.id, "states 409")
	})
	t.Run("R5 a /graphql row that drops the 500 clause", func(t *testing.T) {
		row := liveRowByID(t, "POST /graphql [dev-health-query-api @query,internal]")
		text := row.str("disclosure_behavior")
		cut := strings.Index(text, "; 500 when")
		if cut < 0 {
			t.Fatal("the row states no 500 clause")
		}
		row.raw["disclosure_behavior"] = text[:cut]
		mustDefect(t, "drop 500", driveRow(t, row), row.id, "answers 500")
	})
	t.Run("R4 an exempt policy on a route whose edge carrier rebinds", func(t *testing.T) {
		row := liveRowByID(t, query)
		row.raw["impersonation_policy"] = "exempt_no_org_context"
		mustDefect(t, "exempt", driveRow(t, row), row.id, "impersonation_policy")
	})
	t.Run("R4d an MCP row whose impersonation policy is not the refused-claim value", func(t *testing.T) {
		row := liveRowByID(t, "POST /query [dev-health-query-api @mcp]")
		row.raw["impersonation_policy"] = "subject_to_impersonation_override"
		mustDefect(t, "mcp", driveRow(t, row), row.id, "refuses every impersonation claim")
	})
	t.Run("R4b an override policy on a route that never reads the organization", func(t *testing.T) {
		row := liveRowByID(t, buildinfo)
		row.raw["impersonation_policy"] = "subject_to_impersonation_override"
		mustDefect(t, "override", driveRow(t, row), row.id, "impersonation_policy")
	})
	t.Run("R4c an unknown field that lost its gap", func(t *testing.T) {
		row := liveRowByID(t, rest)
		row.raw["disclosure_behavior"] = "unknown"
		row.raw["gaps"] = []any{}
		mustDefect(t, "gap", driveRow(t, row), row.id, "no declared gap")
	})
	t.Run("a resolver that names a request parameter", func(t *testing.T) {
		row := liveRowByID(t, query)
		row.raw["resource_resolver"] = "organization_from_request: the org_id parameter"
		mustDefect(t, "resolver", driveRow(t, row), row.id, "resource_resolver")
	})
}

// TestLiveRowCompletenessRefusesARowNoDriverObserves: a new row shape with no
// driver, and a class of credential the row lists that the stack refuses,
// fail by id; there is no silent skip.
func TestLiveRowCompletenessRefusesARowNoDriverObserves(t *testing.T) {
	quietStdLog(t)
	t.Run("a protected route no driver knows", func(t *testing.T) {
		row := liveRowByID(t, "POST /query [dev-health-query-api @query,internal]")
		row.id, row.route = "POST /query/new [dev-health-query-api @query,internal]", "/query/new"
		mustDefect(t, "new route", driveRow(t, row), row.id, "no live driver")
	})
	t.Run("a class the stack does not accept", func(t *testing.T) {
		row := liveRowByID(t, "GET /api/v1/home [dev-health-query-api @query,internal]")
		row.raw["accepted_credential_classes"] = []any{classEnvelope, classEdgeToken}
		mustDefect(t, "class", driveRow(t, row), row.id, classUserToken)
	})
	t.Run("every kind a row can take has a driver", func(t *testing.T) {
		for _, row := range loadLiveRows(t) {
			kind := classifyLive(row)
			if _, ok := liveDrivers[kind]; !ok && kind != "public" {
				t.Errorf("%s: kind %q has no driver", row.id, kind)
			}
		}
	})
}

// The impersonation cells of the edge carrier, one subtest per cell, on the
// two routes that read a session. Each cell is named by its mutant: a change
// that serves a refused or rebound request as the plain principal fails the
// cell of that name.
func TestLiveImpersonationCellsOnTheEdgeCarrier(t *testing.T) {
	quietStdLog(t)
	document := documentBody(iaDocument)
	routes := map[string]struct {
		q       liveReq
		header  bool
		failure int
	}{
		"query":   {liveReq{listener: "internal", method: http.MethodPost, path: "/query", body: document}, false, http.StatusServiceUnavailable},
		"graphql": {liveReq{listener: "query", method: http.MethodPost, path: "/graphql", body: document}, true, http.StatusServiceUnavailable},
	}
	cells := []string{
		"live superuser with an active session",
		"non-superuser with a session row",
		"superuser with no session",
		"session read fails (fail closed)",
	}
	for name, route := range routes {
		o := newObs(liveRow{id: name})
		edgeSession(t, o, route.q, route.header, route.failure)
		for _, cell := range cells {
			t.Run(name+"/"+cell, func(t *testing.T) {
				for _, defect := range o.defect {
					if strings.Contains(defect, cell) {
						t.Error(defect)
					}
				}
			})
		}
	}
}
