package authapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const delegatedClasses = `,"accepted_credential_classes":["internal_svc_authsess_token","workload_token"]}`

func delegatedRow(route string) string {
	return `{"service":"` + AccessService + `","method":"POST","route":"` + route + `"` + delegatedClasses
}

// delegatedRowsDocument holds both delegated route rows, as the rows change will.
var delegatedRowsDocument = []byte(`{"rows":[` + delegatedRow(PatternDelegatedSession) + `,` + delegatedRow(PatternDelegatedSessionEnd) + `]}`)

func delegatedRecord(f *fixture, until time.Time) session.Record {
	pin := int64(4)
	now := time.Now().UTC()
	return session.Record{
		ID: uuid.New(), PrincipalID: f.principal, OrganizationID: &f.org, Revision: 1, AuthTime: now, AMR: []string{"pwd"},
		CreatedAt: now, ExpiresAt: until, LegacyTokenVersion: &pin,
		Delegation: &session.Delegation{Actor: uuid.New(), Reason: "support ticket", PermittedActions: []string{"data.read"}},
	}
}

func delegatedHandlers(t *testing.T, f *compatFixture) compatHandlers {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	rec, err := newRecorder(logger, otel.Meter("delegated-test"))
	if err != nil {
		t.Fatal(err)
	}
	deps := CompatDeps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer, IssueWorkload: f.workload, Logger: logger, Projector: f.projector}
	if f.creds != nil {
		deps.ServiceCredentials = f.creds
	}
	return compatHandlers{CompatDeps: deps, rec: rec}
}

func delegatedServer(t *testing.T, f *compatFixture, rows []byte) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	for _, route := range MarkResponseModels(delegatedHandlers(t, f).withDelegatedRouteRows(nil, rows)) {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(flaggedHandler(route)))
	}
	return mux
}

func delegatedBody(f *compatFixture, base uuid.UUID) string {
	return `{"base_session_id":"` + base.String() + `","target_principal_id":"` + f.principal.String() +
		`","organization_id":"` + f.org.String() + `","reason":"support ticket"}`
}

// startFixture is a fixture whose store answers StartDelegation with a row.
func startFixture(t *testing.T) (*compatFixture, session.Record) {
	t.Helper()
	f := newCompatFixture(t)
	record := delegatedRecord(f.fixture, time.Now().Add(10*time.Minute).UTC())
	f.sessions.delegated, f.sessions.records[record.ID] = record, record
	return f, record
}

func postDelegated(server http.Handler, path, body string, header map[string]string) *httptest.ResponseRecorder {
	return (&fixture{server: server}).do(http.MethodPost, path, body, header)
}

// expectMalformed reads a 400: error.v1's closed status list has no 400, and the
// internal routes answer one in that envelope.
func expectMalformed(t *testing.T, r *httptest.ResponseRecorder) {
	t.Helper()
	if r.Code != http.StatusBadRequest || jsonBody(t, r)["reason_code"] != "malformed" {
		t.Fatalf("%d %s, want 400 malformed", r.Code, r.Body)
	}
}

func TestDelegatedRoutesAreDarkWithoutTheirRows(t *testing.T) {
	f, record := startFixture(t)
	body := delegatedBody(f, f.record.ID)
	if r := f.do(http.MethodPost, PatternDelegatedSession, body, issuerBearer); r.Code != http.StatusNotFound {
		t.Fatalf("embedded document: %d, want 404", r.Code)
	}
	if !strings.Contains(f.logs.String(), "auth delegated routes not mounted") || !strings.Contains(f.logs.String(), PatternDelegatedSession) {
		t.Fatalf("no line names the unmounted route: %s", f.logs)
	}
	for name, rows := range map[string][]byte{
		"no rows":     []byte(`{"rows":[]}`),
		"start only":  []byte(`{"rows":[` + delegatedRow(PatternDelegatedSession) + `]}`),
		"end only":    []byte(`{"rows":[` + delegatedRow(PatternDelegatedSessionEnd) + `]}`),
		"wrong class": []byte(strings.ReplaceAll(string(delegatedRowsDocument), "internal_svc_authsess_token", "user_access_token")),
	} {
		server := delegatedServer(t, f, rows)
		if r := postDelegated(server, PatternDelegatedSession, body, issuerBearer); r.Code != http.StatusNotFound {
			t.Errorf("%s: start %d, want 404", name, r.Code)
		}
		if r := postDelegated(server, PatternDelegatedSessionEnd, `{"session_id":"`+record.ID.String()+`"}`, issuerBearer); r.Code != http.StatusNotFound {
			t.Errorf("%s: end %d, want 404", name, r.Code)
		}
	}
}

func TestDelegatedRoutesMountWithBothRows(t *testing.T) {
	f, record := startFixture(t)
	server := delegatedServer(t, f, delegatedRowsDocument)
	if r := postDelegated(server, PatternDelegatedSession, delegatedBody(f, f.record.ID), issuerBearer); r.Code != http.StatusCreated {
		t.Fatalf("start %d: %s", r.Code, r.Body)
	}
	if r := postDelegated(server, PatternDelegatedSessionEnd, `{"session_id":"`+record.ID.String()+`"}`, issuerBearer); r.Code != http.StatusNoContent {
		t.Fatalf("end %d: %s", r.Code, r.Body)
	}
}

func memberNames(t *testing.T, raw []byte) []string {
	t.Helper()
	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(members))
	for name := range members {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func TestDelegatedStartAnswersExactlyItsMembers(t *testing.T) {
	f, record := startFixture(t)
	r := postDelegated(delegatedServer(t, f, delegatedRowsDocument), PatternDelegatedSession, delegatedBody(f, f.record.ID), issuerBearer)
	if r.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", r.Code, r.Body)
	}
	want := []string{"access_token", "expires_in", "session", "token_type"}
	if got := memberNames(t, r.Body.Bytes()); !slices.Equal(got, want) {
		t.Fatalf("members %v, want %v", got, want)
	}
	var answer struct {
		Session     json.RawMessage `json:"session"`
		AccessToken string          `json:"access_token"`
		TokenType   string          `json:"token_type"`
		ExpiresIn   int64           `json:"expires_in"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	parsed, err := contracts.ParseSession(repoRoot(t), answer.Session)
	if err != nil {
		t.Fatalf("ParseSession: %v", err)
	}
	if parsed.Delegation == nil || parsed.Delegation.ActorPrincipalID != session.WirePrincipalID(record.Delegation.Actor) {
		t.Fatalf("delegation = %+v", parsed.Delegation)
	}
	wantSession := []string{"amr", "auth_time", "created_at", "delegation", "expires_at", "principal_id", "revision", "revoked_at", "schema_version", "session_id"}
	if got := memberNames(t, answer.Session); !slices.Equal(got, wantSession) {
		t.Fatalf("session members %v, want %v", got, wantSession)
	}
	claims := payloadOf(t, answer.AccessToken)
	if answer.TokenType != "bearer" || answer.ExpiresIn != int64(claims["exp"].(float64)-claims["iat"].(float64)) {
		t.Fatalf("token_type %q expires_in %d, claims %v", answer.TokenType, answer.ExpiresIn, claims)
	}
	if claims["sub"] != f.principal.String() || claims["impersonating_user_id"] != record.Delegation.Actor.String() {
		t.Fatalf("claims %v", claims)
	}
	if got := f.sessions.started; len(got) != 1 || got[0].BaseSessionID != f.record.ID || got[0].TargetPrincipalID != f.principal ||
		got[0].OrganizationID != f.org || got[0].Reason != "support ticket" {
		t.Fatalf("StartDelegation inputs %+v", got)
	}
}

func TestDelegatedStartUnderEdDSAAddsTheSessionToken(t *testing.T) {
	signer, _, _ := fileSigner(t)
	f, record := startFixture(t)
	f.issuer = newEdDSAIssuer(t, signer, testAudience)
	r := postDelegated(delegatedServer(t, f, delegatedRowsDocument), PatternDelegatedSession, delegatedBody(f, f.record.ID), issuerBearer)
	if r.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", r.Code, r.Body)
	}
	want := []string{"access_token", "expires_in", "session", "session_access_token", "token_type"}
	if got := memberNames(t, r.Body.Bytes()); !slices.Equal(got, want) {
		t.Fatalf("members %v, want %v", got, want)
	}
	answer := jsonBody(t, r)
	act, _ := payloadOf(t, answer["session_access_token"].(string))["act"].(map[string]any)
	if act["sub"] != session.WirePrincipalID(record.Delegation.Actor) {
		t.Fatalf("session token act = %v", act)
	}
}

func TestDelegatedStartRefusalMap(t *testing.T) {
	forbidden := []error{
		session.ErrNotOwned, session.ErrActorNotAuthorized, session.ErrActorUnavailable, session.ErrNestedDelegation, session.ErrTargetIsActor,
		session.ErrTargetUnavailable, session.ErrTargetPlatformRole, session.ErrTargetNotMember, session.ErrTargetRoleUnknown,
		session.ErrOrganizationUnavailable,
	}
	cases := map[error]struct {
		status int
		reason string
	}{
		session.ErrBaseNotLive: {http.StatusUnauthorized, "session_revoked"},
		session.ErrBusy:        {http.StatusServiceUnavailable, "store_unavailable"},
		session.ErrStore:       {http.StatusServiceUnavailable, "store_unavailable"},
	}
	for _, err := range forbidden {
		cases[err] = struct {
			status int
			reason string
		}{http.StatusForbidden, "not_entitled"}
	}
	for err, want := range cases {
		t.Run(err.Error(), func(t *testing.T) {
			f, _ := startFixture(t)
			f.sessions.startErr = err
			r := postDelegated(delegatedServer(t, f, delegatedRowsDocument), PatternDelegatedSession, delegatedBody(f, f.record.ID), issuerBearer)
			expectError(t, r, want.status, want.reason)
			if strings.Contains(r.Body.String(), err.Error()) {
				t.Fatalf("the cause reached the body: %s", r.Body)
			}
		})
	}
}

func TestDelegatedStartAnswersABadReasonMalformed(t *testing.T) {
	f, _ := startFixture(t)
	f.sessions.startErr = session.ErrInvalidReason
	expectMalformed(t, postDelegated(delegatedServer(t, f, delegatedRowsDocument), PatternDelegatedSession, delegatedBody(f, f.record.ID), issuerBearer))
}

func TestDelegatedStartMapsAMissingProjection(t *testing.T) {
	f, _ := startFixture(t)
	f.projector.gone[f.principal] = true
	r := postDelegated(delegatedServer(t, f, delegatedRowsDocument), PatternDelegatedSession, delegatedBody(f, f.record.ID), issuerBearer)
	expectError(t, r, http.StatusForbidden, "not_entitled")
	if len(f.sessions.started) != 0 {
		t.Fatal("the route reached StartDelegation")
	}
}

func TestDelegatedStartBodyIsStrict(t *testing.T) {
	f, _ := startFixture(t)
	server := delegatedServer(t, f, delegatedRowsDocument)
	good := delegatedBody(f, f.record.ID)
	for name, body := range map[string]string{
		"names an actor":    strings.TrimSuffix(good, "}") + `,"actor_principal_id":"` + uuid.NewString() + `"}`,
		"unknown member":    strings.TrimSuffix(good, "}") + `,"extra":1}`,
		"no reason":         strings.Replace(good, `,"reason":"support ticket"`, "", 1),
		"reason not a text": strings.Replace(good, `"support ticket"`, `7`, 1),
		"bad base":          strings.Replace(good, f.record.ID.String(), "not-a-uuid", 1),
		"not an object":     `[]`,
		"two values":        good + good,
	} {
		expectMalformed(t, postDelegated(server, PatternDelegatedSession, body, issuerBearer))
		if len(f.sessions.started) != 0 {
			t.Fatalf("%s reached StartDelegation", name)
		}
	}
}

func TestDelegatedRoutesRefuseAnyOtherCaller(t *testing.T) {
	f, record := startFixture(t)
	server := delegatedServer(t, f, delegatedRowsDocument)
	user := f.bearer(f.grant())
	for name, header := range map[string]map[string]string{"no bearer": nil, "user token": user} {
		expectError(t, postDelegated(server, PatternDelegatedSession, delegatedBody(f, f.record.ID), header), http.StatusUnauthorized, "invalid_credentials")
		expectError(t, postDelegated(server, PatternDelegatedSessionEnd, `{"session_id":"`+record.ID.String()+`"}`, header), http.StatusUnauthorized, "invalid_credentials")
		if len(f.sessions.started)+len(f.sessions.ended) != 0 {
			t.Fatalf("%s reached the store", name)
		}
	}
}

func TestDelegatedMintFailureEndsTheRow(t *testing.T) {
	for name, plant := range map[string]func(f *compatFixture, record *session.Record){
		"token past the row's end": func(_ *compatFixture, record *session.Record) { record.ExpiresAt = time.Now().Add(-time.Second) },
		"user read fails":          func(f *compatFixture, _ *session.Record) { f.users.err = errors.New("read failed") },
		"role read fails":          func(f *compatFixture, _ *session.Record) { f.users.roleErr = errors.New("read failed") },
		"user gone":                func(f *compatFixture, _ *session.Record) { delete(f.users.users, f.principal) },
		"membership gone":          func(f *compatFixture, _ *session.Record) { delete(f.users.roles, [2]uuid.UUID{f.principal, f.org}) },
	} {
		t.Run(name, func(t *testing.T) {
			f, record := startFixture(t)
			plant(f, &record)
			f.sessions.delegated, f.sessions.records[record.ID] = record, record
			r := postDelegated(delegatedServer(t, f, delegatedRowsDocument), PatternDelegatedSession, delegatedBody(f, f.record.ID), issuerBearer)
			expectError(t, r, http.StatusServiceUnavailable, "dependency_unavailable")
			if strings.Contains(r.Body.String(), "access_token") {
				t.Fatalf("a token left: %s", r.Body)
			}
			if got := f.sessions.ended; len(got) != 1 || got[0] != record.ID {
				t.Fatalf("ended %v, want %s", got, record.ID)
			}
		})
	}
}

func TestDelegatedEndEndsOnlyADelegatedSession(t *testing.T) {
	f, record := startFixture(t)
	server := delegatedServer(t, f, delegatedRowsDocument)
	endBody := func(id uuid.UUID) string { return `{"session_id":"` + id.String() + `"}` }
	expectError(t, postDelegated(server, PatternDelegatedSessionEnd, endBody(f.record.ID), issuerBearer), http.StatusNotFound, "session_not_found")
	expectError(t, postDelegated(server, PatternDelegatedSessionEnd, endBody(uuid.New()), issuerBearer), http.StatusNotFound, "session_not_found")
	expectMalformed(t, postDelegated(server, PatternDelegatedSessionEnd, `{"session_id":"x"}`, issuerBearer))
	expectMalformed(t, postDelegated(server, PatternDelegatedSessionEnd, `{"session_id":"`+record.ID.String()+`","x":1}`, issuerBearer))
	if len(f.sessions.ended) != 0 {
		t.Fatalf("ended %v", f.sessions.ended)
	}
	if r := postDelegated(server, PatternDelegatedSessionEnd, endBody(record.ID), issuerBearer); r.Code != http.StatusNoContent || r.Body.Len() != 0 {
		t.Fatalf("end %d: %s", r.Code, r.Body)
	}
	if got := f.sessions.ended; len(got) != 1 || got[0] != record.ID {
		t.Fatalf("ended %v, want %s", got, record.ID)
	}
	f.sessions.endErr = session.ErrStore
	expectError(t, postDelegated(server, PatternDelegatedSessionEnd, endBody(record.ID), issuerBearer), http.StatusServiceUnavailable, "store_unavailable")
}

func TestIssueAnswersADelegatedPredecessorNotEntitled(t *testing.T) {
	f := newCompatFixture(t)
	f.sessions.createErr = session.ErrPredecessorDelegated
	r := f.do(http.MethodPost, PatternIssueSession, issueBodyWithPredecessor(f.principal, f.org.String(), `"`+uuid.NewString()+`"`), issuerBearer)
	expectError(t, r, http.StatusForbidden, "not_entitled")
}

func TestSessionStateNamesTheActorOnADelegatedRowOnly(t *testing.T) {
	f, record := startFixture(t)
	server := stateServer(t, f, stateRowsDocument)
	code, answer := doState(t, server, stateBody(record.ID.String()), issuerBearer)
	if code != http.StatusOK || answer["actor_principal_id"] != record.Delegation.Actor.String() {
		t.Fatalf("%d %v", code, answer)
	}
	code, answer = doState(t, server, stateBody(f.record.ID.String()), issuerBearer)
	if _, present := answer["actor_principal_id"]; code != http.StatusOK || present {
		t.Fatalf("%d %v", code, answer)
	}
}

// markDelegated makes the fixture's session row delegated. The token is the
// plain one: the row decides, not a claim.
func markDelegated(f *fixture) {
	record := f.record
	record.Delegation = &session.Delegation{Actor: uuid.New(), Reason: "support ticket", PermittedActions: []string{"data.read"}}
	f.sessions.records[record.ID] = record
}

func denyClasses(t *testing.T) map[string]*fixture {
	t.Helper()
	return map[string]*fixture{"hs256": newFixture(t), "eddsa": newAccessFixture(t).fixture}
}

func TestADelegatedBearerReachesCurrentAndRevokeOnly(t *testing.T) {
	for class, f := range denyClasses(t) {
		t.Run(class, func(t *testing.T) {
			markDelegated(f)
			header := f.bearer(f.grant())
			current := f.do(http.MethodGet, PatternCurrent, "", header)
			if current.Code != http.StatusOK {
				t.Fatalf("current %d: %s", current.Code, current.Body)
			}
			if _, ok := jsonBody(t, current)["delegation"]; !ok {
				t.Fatalf("current has no delegation: %s", current.Body)
			}
			expectError(t, f.do(http.MethodPost, PatternRevokeAll, "", header), http.StatusForbidden, "not_entitled")
			if len(f.sessions.revokedAll) != 0 {
				t.Fatalf("revoke-all ran: %v", f.sessions.revokedAll)
			}
			if r := f.do(http.MethodPost, PatternRevoke, "", header); r.Code != http.StatusNoContent {
				t.Fatalf("revoke %d: %s", r.Code, r.Body)
			}
		})
	}
}

func TestADelegatedBearerCannotChangeThePassword(t *testing.T) {
	for class, f := range denyClasses(t) {
		t.Run(class, func(t *testing.T) {
			markDelegated(f)
			c := newCredFixtureOn(t, httpapi.NewMemoryCounters(nil), f)
			body := `{"current_password":"current-password-1","new_password":"Brand-new-pass-77!"}`
			r := c.call(http.MethodPost, PatternChangePassword, body, f.bearer(f.grant()))
			if want := `{"detail":{"message":"` + textChangeNotAllowed + `"}}`; r.Code != http.StatusForbidden || r.Body.String() != want {
				t.Fatalf("change %d: %s, want 403 %s", r.Code, r.Body, want)
			}
			if c.fakes.pwdSet != 0 || c.fakes.created != 0 {
				t.Fatalf("writes %d, sessions %d", c.fakes.pwdSet, c.fakes.created)
			}
		})
	}
}

func TestADelegatedBearerCannotLinkOrUnlinkAnIdentity(t *testing.T) {
	for class, f := range denyClasses(t) {
		t.Run(class, func(t *testing.T) {
			markDelegated(f)
			domain := &fakeLinkDomain{proofs: map[string]uuid.UUID{}, owners: map[uuid.UUID]uuid.UUID{}, methods: map[uuid.UUID]int{}}
			routes, err := SocialLinkRoutes(SocialLinkDeps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer, Social: domain,
				Limits: mustLimits(t, httpapi.NewMemoryCounters(nil))})
			if err != nil {
				t.Fatal(err)
			}
			mux := http.NewServeMux()
			for _, route := range routes {
				mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
			}
			f.server = mux
			header := f.bearer(f.grant())
			expectError(t, f.do(http.MethodPost, PatternSocialLink, linkBody, header), http.StatusForbidden, "not_entitled")
			expectError(t, f.do(http.MethodDelete, "/v1/identities/social/"+uuid.NewString(), "", header), http.StatusForbidden, "not_entitled")
			if len(domain.whos) != 0 {
				t.Fatal("the domain was reached")
			}
		})
	}
}

func TestAuthenticateTakesImpersonatedFromTheRow(t *testing.T) {
	a := newAccessFixture(t)
	rec, err := newRecorder(slog.New(slog.DiscardHandler), otel.Meter("delegated-test"))
	if err != nil {
		t.Fatal(err)
	}
	h := handlers{Deps: Deps{Sessions: a.sessions, Users: a.users, Issuer: a.issuer}, rec: rec}
	ask := func() Bearer {
		request := httptest.NewRequest(http.MethodGet, PatternCurrent, nil)
		request.Pattern = "GET " + PatternCurrent
		for k, v := range a.bearer(a.grant()) {
			request.Header.Set(k, v)
		}
		bearer, _, ok := h.authenticate(httptest.NewRecorder(), request, opSessionCurrent)
		if !ok {
			t.Fatal("refused")
		}
		return bearer
	}
	if ask().Impersonated {
		t.Fatal("a plain row reads as impersonated")
	}
	markDelegated(a.fixture)
	if !ask().Impersonated {
		t.Fatal("an EdDSA bearer of a delegated row is not impersonated")
	}
}
