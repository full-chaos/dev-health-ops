package server

import (
	"context"
	"encoding/json"
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

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
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
	stack := newAccessStack(t, store, testAccess(minter.keys, nil, accessTestNow), true)
	stack.user = minter.mint(ecUser, nil, nil)
	return stack
}

// liveStackKeyDown is the stack whose token key source (the JWKS) cannot be read.
func liveStackKeyDown(t *testing.T) *behaviourStack {
	t.Helper()
	minter := newAccessMinter(t)
	stack := newAccessStack(t, newAccessStore(), testAccess(downKeys{}, nil, accessTestNow), true)
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
	return ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: orgID, role: "admin", tokenVersion: 5})
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
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", isSuperuser: true, tokenVersion: 5})
	run := func(cell string, shape func(*accessStore)) (*behaviourStack, liveAnswer) {
		stack := liveStack(t, shape)
		q2 := q
		q2.bearer = token
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
	o.wantNoRun("session read fails (fail closed)", stack)
}

type liveREST struct {
	handler  http.Handler
	envelope string
	user     string
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
	edge, err := principal.NewEdgeVerifier(ecSecret, ecIssuer, ecAudience, newQueryAPIUserStoreFrom(store))
	if err != nil {
		t.Fatal(err)
	}
	if err := routeEdgeVerifier(edge, setup.opts, true); err != nil {
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

	orgless := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), role: "admin", tokenVersion: 5})
	got := fixture(nil).do(row.method, path, orgless, nil)
	o.wantStatus("edge token with no organization", got, http.StatusForbidden)
	if !strings.Contains(got.body, "Organization context required") {
		o.fail("edge token with no organization: body %q lacks the 403 text", trim(got.body))
	}
	if disclosureKnown {
		o.textHas("disclosure_behavior", "403 Organization context required", "superuser, who passes")
	}
	o.wantStatus("superuser with no organization", fixture(asSuperuser).do(row.method, path, orgless, nil), http.StatusOK)
	o.wantStatus("users store unavailable", fixture(func(s *accessStore) {
		s.errUserState = fmt.Errorf("down: %w", policy.ErrUnavailable)
	}).do(row.method, path, edgeBearer(t, ecOrg.String()), nil), http.StatusServiceUnavailable)
	broken := fixture(func(s *accessStore) { s.errUserState = fmt.Errorf("syntax error") })
	o.wantStatus("users store error", broken.do(row.method, path, broken.user, nil), http.StatusInternalServerError)
	if disclosureKnown {
		o.statusText()
	} else {
		o.unknownIsDeclared("disclosure_behavior")
	}

	for _, field := range []string{"action", "current_state_cache_behavior", "impersonation_policy"} {
		if row.str(field) != "unknown" {
			o.fail("%s is %q: no live cell observes a REST row's %s", field, row.str(field), field)
		}
	}
	o.unknownIsDeclared("action", "current_state_cache_behavior", "impersonation_policy")
	return o
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
		for name, bearer := range map[string]string{"envelope": stack.envelope, "edge token": stack.edge, "user token": stack.user} {
			cell := listener + ": " + name
			stack = liveStack(t, nil)
			bearer = map[string]string{"envelope": stack.envelope, "edge token": stack.edge, "user token": stack.user}[name]
			a := stack.live(with(func(c *liveReq) {
				c.bearer = bearer
				c.headers = map[string]string{"X-Org-Id": liveOtherOrg.String()}
			}))
			o.wantStatus(cell, a, http.StatusOK)
			o.wantOrg(cell, stack, authctx.Claims{OrgID: ecOrg.String()})
		}
		stack = liveStack(t, nil)
		if row.route != "/query/proof-write" {
			o.wantStatus(listener+": unregistered document", stack.live(with(func(c *liveReq) {
				c.bearer = stack.edge
				c.body = documentBody("query Unregistered { nothing }")
			})), http.StatusNotFound)
		}

		// Impersonation: the edge carrier reads the session; the envelope and
		// the internal headers state it themselves and never read one.
		sessionDown := func(s *accessStore) { asSuperuser(s); s.errSession = fmt.Errorf("session store timeout") }
		stack = liveStack(t, sessionDown)
		a := stack.live(with(func(c *liveReq) {
			c.bearer = iaEnvelope(t, stack.priv, principal.Claims{OrgID: ecOrg.String(), Role: "admin", ImpersonationActive: true})
		}))
		o.wantStatus(listener+": envelope claiming impersonation", a, http.StatusOK)
		o.wantOrg(listener+": envelope claiming impersonation", stack, authctx.Claims{OrgID: ecOrg.String(), ImpersonationActive: true})
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
		edgeSession(t, o, q, false, http.StatusUnauthorized)
		if row.route == "/query/proof-write" {
			proofWriteCells(t, o)
		}
	}
	o.textHas("resource_resolver", "organization_from_credential")
	o.impersonationPolicy(true)
	o.unknownIsDeclared("action", "current_state_cache_behavior")
	o.statusText()
	return o
}

// proofWriteCells drives the real dispatcher the route mounts: the stack's
// probe stands in for the proof-write handler there, and the allowlist is a
// database read the stack does not carry.
func proofWriteCells(t *testing.T, o *liveObs) {
	t.Helper()
	const mutation = "mutation LiveProofWrite { probe }"
	for _, allowed := range []bool{true, false} {
		verifier, priv := iaVerifier(t)
		ran := 0
		mux := routeswitch.NewMux(routeswitch.StaticSwitch{"m": true})
		mux.Register("m", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ran++ }))
		handler := newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(mutation): "m"}, verifier, nil, nil, digest.KindMutation,
			func(_ context.Context, org string) bool { return allowed && org == ecOrg.String() })
		request := httptest.NewRequest(http.MethodPost, "/query/proof-write", strings.NewReader(documentBody(mutation)))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+iaEnvelope(t, priv, principal.Claims{OrgID: ecOrg.String(), Role: "admin"}))
		recorder := httptest.NewRecorder()
		handler(recorder, request)
		o.see(recorder.Code)
		want, wantRan := http.StatusForbidden, 0
		if allowed {
			want, wantRan = http.StatusOK, 1
		}
		if recorder.Code != want || ran != wantRan {
			o.fail("proof-write with the organization allowed=%v: status %d, ran %d, want %d, %d", allowed, recorder.Code, ran, want, wantRan)
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
		first := stack.live(q)
		o.wantStatus(listener+": envelope", first, http.StatusOK)
		for name, claims := range map[string]principal.Claims{
			"another organization":  {OrgID: liveOtherOrg.String(), Role: "viewer"},
			"no organization":       {Role: "viewer"},
			"impersonation active":  {OrgID: ecOrg.String(), Role: "admin", ImpersonationActive: true},
			"another org and super": {OrgID: liveOtherOrg.String(), Role: "viewer", IsSuperuser: true},
		} {
			q.bearer = iaEnvelope(t, stack.priv, claims)
			got := stack.live(q)
			if got.status != http.StatusOK || got.body != first.body {
				o.fail("%s: status %d body %q, want the same build identity %q: the credential's organization is not read", name, got.status, trim(got.body), trim(first.body))
			}
		}
	}
	o.textHas("resource_resolver", "none:")
	o.impersonationPolicy(false)
	o.unknownIsDeclared("action", "current_state_cache_behavior")
	o.statusText()
	return o
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
			for name, bearer := range map[string]func(*behaviourStack) string{"edge token": func(s *behaviourStack) string { return s.edge }, "user token": func(s *behaviourStack) string { return s.user }} {
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
		a = stack.live(request("query Unregistered { nothing }", nil, stack.edge))
		o.wantStatus(listener+": unregistered document", a, http.StatusNotFound)
		if !strings.Contains(a.body, "UNREGISTERED_DOCUMENT") {
			o.fail("unregistered document: body %q lacks UNREGISTERED_DOCUMENT", trim(a.body))
		}
		edgeSession(t, o, request(iaDocument, nil, ""), true, http.StatusInternalServerError)
		down := liveStackKeyDown(t)
		cell := listener + ": user token, token key source unreadable"
		o.wantStatus(cell, down.live(request(iaDocument, nil, down.user)), http.StatusServiceUnavailable)
		o.wantNoRun(cell, down)
	}
	o.textHas("resource_resolver", "organization_from_header_or_credential", "X-Org-Id")
	o.impersonationPolicy(true)
	o.unknownIsDeclared("action", "current_state_cache_behavior")
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
	o.unknownIsDeclared("action", "current_state_cache_behavior", "impersonation_policy")
	if got := row.str("impersonation_policy"); got != "unknown" {
		o.fail("impersonation_policy is %q: no live cell observes an MCP row's impersonation effect (a claim of it is refused 403)", got)
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
	t.Run("R4b an override policy on a route that never reads the organization", func(t *testing.T) {
		row := liveRowByID(t, buildinfo)
		row.raw["impersonation_policy"] = "subject_to_impersonation_override"
		mustDefect(t, "override", driveRow(t, row), row.id, "impersonation_policy")
	})
	t.Run("R4c an unknown action that lost its gap", func(t *testing.T) {
		row := liveRowByID(t, query)
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
		"query":   {liveReq{listener: "internal", method: http.MethodPost, path: "/query", body: document}, false, http.StatusUnauthorized},
		"graphql": {liveReq{listener: "query", method: http.MethodPost, path: "/graphql", body: document}, true, http.StatusInternalServerError},
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
