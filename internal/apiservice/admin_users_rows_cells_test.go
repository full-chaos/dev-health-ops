package apiservice

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// adminUsersKeys are the /admin/users rows, mounted WrapAdminOr(admin.users) in admin/users.go.
var adminUsersKeys = []string{
	"DELETE /api/v1/admin/users/{user_id}",
	"GET /api/v1/admin/users",
	"GET /api/v1/admin/users/{user_id}",
	"PATCH /api/v1/admin/users/{user_id}",
	"POST /api/v1/admin/users",
	"POST /api/v1/admin/users/{user_id}/password",
}

// adminUsersBodies are bodies each write route's own decoder accepts, so the guard runs after the body-first read.
var adminUsersBodies = map[string]string{
	"POST /api/v1/admin/users":                    `{"email":"cell-new@example.test","password":"cell-password-1"}`,
	"PATCH /api/v1/admin/users/{user_id}":         `{"full_name":"Cell Name"}`,
	"POST /api/v1/admin/users/{user_id}/password": `{"admin_password":"admin-password","password":"new-password-1"}`,
}

var (
	usersCellOwner     = uuid.MustParse("00000000-0000-4000-8000-0000000001a1")
	usersCellViewer    = uuid.MustParse("00000000-0000-4000-8000-0000000001a2")
	usersCellOwnerSID  = uuid.MustParse("00000000-0000-4000-8000-0000000001c1")
	usersCellViewerSID = uuid.MustParse("00000000-0000-4000-8000-0000000001c2")
)

// withUsersCellCallers adds the platform admins of withPlatformAdmins, an org owner and an org viewer, and a live
// session of probeAdminUser, the superuser whose impersonation row the probe store holds.
func withUsersCellCallers(store *probeStore) {
	withPlatformAdmins(store)
	store.users[usersCellOwner] = probeUserRow{state: policy.UserState{IsActive: true}, email: "owner@example.test"}
	store.users[usersCellViewer] = probeUserRow{state: policy.UserState{IsActive: true}, email: "viewer@example.test"}
	store.members[[2]uuid.UUID{usersCellOwner, probeOrg}] = "owner"
	store.members[[2]uuid.UUID{usersCellViewer, probeOrg}] = "viewer"
	store.sessions[usersCellOwnerSID] = policy.SessionRow{PrincipalID: usersCellOwner, LegacyTokenVersion: i64ptr(0)}
	store.sessions[usersCellViewerSID] = policy.SessionRow{PrincipalID: usersCellViewer, LegacyTokenVersion: i64ptr(0)}
	store.sessions[cellAdminSID] = policy.SessionRow{PrincipalID: probeAdminUser, LegacyTokenVersion: i64ptr(0)}
}

// usersCaller is one caller class: its bearer ("" = anonymous), the decision point's state for it (judged false =
// a credential the decision point is never asked about), and whether a live impersonation row is in force.
type usersCaller struct {
	name          string
	token         string
	snap          cellSnapshot
	judged        bool
	impersonating bool
}

func usersCallers(t *testing.T, minter *eddsaMinter) []usersCaller {
	t.Helper()
	mint := func(user, sid uuid.UUID, extra map[string]any) string {
		claims := map[string]any{"sid": sessionid.Wire(sid)}
		for k, v := range extra {
			claims[k] = v
		}
		return minter.mint(user, claims)
	}
	noOrg := map[string]any{"org_id": nil}
	platform := cellSnapshot{platformRole: "platform_admin"}
	return []usersCaller{
		{name: "platform admin, no membership", token: mint(cellPlatformNoRow, cellNoRowSID, nil), snap: platform, judged: true},
		{name: "platform admin, no org claim", token: mint(cellPlatformNoRow, cellNoRowSID, noOrg), snap: platform, judged: true},
		{name: "platform admin + org member", token: mint(cellPlatformMember, cellPlatformSID, nil), snap: cellSnapshot{platformRole: "platform_admin", orgRole: "member"}, judged: true},
		{name: "org owner", token: mint(usersCellOwner, usersCellOwnerSID, nil), snap: cellSnapshot{orgRole: "owner"}, judged: true},
		{name: "org admin", token: mint(probeUser, probeLiveSID, nil), snap: cellSnapshot{orgRole: "admin"}, judged: true},
		{name: "org member", token: mint(probeMember, probeOtherSID, nil), snap: cellSnapshot{orgRole: "member"}, judged: true},
		{name: "org viewer", token: mint(usersCellViewer, usersCellViewerSID, nil), snap: cellSnapshot{orgRole: "viewer"}, judged: true},
		{name: "no platform role, no org claim", token: mint(probeMember, probeOtherSID, noOrg), snap: cellSnapshot{orgRole: "member"}, judged: true},
		{name: "impersonating platform admin", token: mint(probeAdminUser, cellAdminSID, noOrg), snap: platform, judged: true, impersonating: true},
		{name: "retired HS256 token", token: probeHS256(t, probeKey, map[string]any{"sub": cellPlatformNoRow.String(), "org_id": probeOrg.String(), "role": "member", "tv": 0})},
		{name: "service key bearer", token: "svc_acr_" + strings.Repeat("k", 40)},
		{name: "anonymous"},
	}
}

// registerUsers lets the evaluate mode decide for the judged callers' tokens, beside any already registered.
func (p *cellPDP) registerUsers(callers []usersCaller) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.callers == nil {
		p.callers = map[string]cellSnapshot{}
	}
	for _, c := range callers {
		if c.judged {
			p.callers[c.token] = c.snap
		}
	}
}

// usersEcho answers 200 with what an /admin/users handler acts on: the principal echo plus the request's admin.users
// answer (policy.HoldsPlatform, the question ensureUserInScope, orgIDForNonSuperuser and the list and create handlers
// ask).
func usersEcho(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{}
	if user := policy.UserFrom(r.Context()); user != nil {
		held, err := policy.HoldsPlatform(r.Context(), user, policy.ActionAdminUsers)
		if err != nil {
			policy.WritePlatformDecisionRefusal(w, err)
			return
		}
		out["holds_admin_users"] = held
		out["user_id"] = user.UserID
		out["org"] = policy.EffectiveOrgID(r.Context(), user)
		out["org_id"] = user.OrgID
		out["role"] = user.Role
		out["is_admin"] = user.IsAdmin()
		out["is_superuser"] = user.IsSuperuser
	}
	if session := policy.ImpersonationFrom(r.Context()); session != nil {
		out["impersonating"] = session.TargetUserID.String()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// usersEchoRoutes mounts the /admin/users rows over usersEcho with the production wrap: WrapAdminOr(admin.users),
// body first on the three writes with a body.
func usersEchoRoutes(_ *testing.T, deps Deps) []httpapi.Route {
	echo := http.HandlerFunc(usersEcho)
	var routes []httpapi.Route
	for _, key := range adminUsersKeys {
		method, pattern, _ := strings.Cut(key, " ")
		handler := deps.Guard.WrapAdminOr(policy.ActionAdminUsers, echo)
		if _, body := adminUsersBodies[key]; body {
			handler = deps.Guard.BodyFirstAdminOr(policy.ActionAdminUsers, echo)
		}
		routes = append(routes, httpapi.Route{Method: method, Pattern: pattern, Handler: handler})
	}
	return routes
}

// usersServers are the four servers of a cell: the real routes and the echo routes, each with the admin cell off (no
// Enforcer) and on (the production profile's rows, the decision point in evaluate mode).
type usersServers struct {
	realOff, realOn   http.Handler
	echoOff, echoOn   http.Handler
	realOffC, realOnC []usersCaller
	echoOffC, echoOnC []usersCaller
}

func newUsersServers(t *testing.T, pdp *cellPDP) *usersServers {
	t.Helper()
	on := func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, pdp, "admin", false) }
	s := &usersServers{}
	var minter *eddsaMinter
	s.realOff, minter = adminConfigServerWith(t, nil, withUsersCellCallers)
	s.realOffC = usersCallers(t, minter)
	s.realOn, minter = adminConfigServerWith(t, on, withUsersCellCallers)
	s.realOnC = usersCallers(t, minter)
	s.echoOff, minter = adminConfigServerOver(t, nil, withUsersCellCallers, usersEchoRoutes)
	s.echoOffC = usersCallers(t, minter)
	s.echoOn, minter = adminConfigServerOver(t, on, withUsersCellCallers, usersEchoRoutes)
	s.echoOnC = usersCallers(t, minter)
	for _, set := range [][]usersCaller{s.realOffC, s.realOnC, s.echoOffC, s.echoOnC} {
		pdp.registerUsers(set)
	}
	return s
}

// usersCellDo sends key with token ("" = no Authorization header) and the route's body.
func usersCellDo(h http.Handler, key, token string) cellAnswer {
	method, _, _ := strings.Cut(key, " ")
	var body io.Reader
	if raw, ok := adminUsersBodies[key]; ok {
		body = strings.NewReader(raw)
	}
	request := httptest.NewRequest(method, cellPath(key), body)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	return cellAnswer{status: recorder.Code, body: recorder.Body.String(), header: recorder.Header()}
}

// usersAnswersDiffer is the one cell comparison: status and body byte-equal.
func usersAnswersDiffer(a, b cellAnswer) bool { return a.status != b.status || a.body != b.body }

func usersRead(key string) bool { return strings.HasPrefix(key, "GET ") }

const (
	orgContextBody     = `{"detail":"Organization context required"}`
	impersonationBody  = `{"detail":"Not allowed in an impersonation session"}`
	usersAdminRequired = adminRequiredBody
)

// The profile names admin.users on exactly the six /admin/users rows, and the guard's action map reads each as a
// platform row.
func TestAdminUsersRowsNameAdminUsers(t *testing.T) {
	var document struct {
		Rows []struct {
			Service, Method, Route string
			Action                 *string
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, row := range document.Rows {
		if row.Service != goAPIAccessService || (row.Route != "/api/v1/admin/users" && !strings.HasPrefix(row.Route, "/api/v1/admin/users/")) {
			continue
		}
		action := ""
		if row.Action != nil {
			action = *row.Action
		}
		got[row.Method+" "+row.Route] = action
	}
	keys := make([]string, 0, len(got))
	for key := range got {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if !slices.Equal(keys, adminUsersKeys) {
		t.Fatalf("/admin/users rows %v, want %v", keys, adminUsersKeys)
	}
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range adminUsersKeys {
		if got[key] != policy.ActionAdminUsers {
			t.Errorf("%s: action %q, want %s", key, got[key], policy.ActionAdminUsers)
		}
		if row, ok := rows[key]; !ok || row.Action != policy.ActionAdminUsers || row.ResourceType != "platform" {
			t.Errorf("%s: guard row %+v (present %v), want admin.users on the platform", key, row, ok)
		}
	}
}

// legacyClass is the legacy answer class of each caller on the /admin/users rows: "handler" (the guard let it through),
// or the exact refusal.
var usersLegacyClass = map[string]string{
	"platform admin, no membership":  "handler",
	"platform admin, no org claim":   "handler",
	"platform admin + org member":    "handler",
	"org owner":                      "handler",
	"org admin":                      "handler",
	"org member":                     usersAdminRequired,
	"org viewer":                     usersAdminRequired,
	"no platform role, no org claim": orgContextBody,
	"impersonating platform admin":   "handler",
	"retired HS256 token":            "401",
	"service key bearer":             "401",
	"anonymous":                      "401",
}

func answerClass(a cellAnswer) string {
	switch {
	case a.status == http.StatusUnauthorized:
		return "401"
	case a.body == usersAdminRequired, a.body == orgContextBody, a.body == impersonationBody:
		return a.body
	case strings.Contains(a.body, "Authorization service temporarily unavailable"):
		return "503 no verdict"
	}
	return "handler"
}

// Every caller class on every /admin/users row, on the real routes and over the echo, gets the same status and body
// with the admin cell on as with it off, and that answer is the legacy class (usersLegacyClass). The impersonating
// platform admin's writes are the one accepted narrowing (D6833) and have their own cell; its reads are here. Each
// cell is logged as "CELL server|route|caller|status|body" so a run on the base commit records today's answers.
func TestAdminUsersRowsAnswerAsLegacyWithTheAdminCellOn(t *testing.T) {
	pdp := newCellPDP(t)
	s := newUsersServers(t, pdp)
	for _, server := range []struct {
		name      string
		off, on   http.Handler
		offC, onC []usersCaller
	}{
		{"real", s.realOff, s.realOn, s.realOffC, s.realOnC},
		{"echo", s.echoOff, s.echoOn, s.echoOffC, s.echoOnC},
	} {
		for _, key := range adminUsersKeys {
			for i, caller := range server.offC {
				if caller.impersonating && !usersRead(key) {
					continue
				}
				pdp.reset("evaluate")
				off := usersCellDo(server.off, key, caller.token)
				on := usersCellDo(server.on, key, server.onC[i].token)
				t.Logf("CELL %s|%s|%s|off %d %s|on %d %s", server.name, key, caller.name, off.status, strings.TrimSpace(off.body), on.status, strings.TrimSpace(on.body))
				if usersAnswersDiffer(off, on) {
					t.Errorf("%s %s / %s: cell off %d %q, on %d %q; want byte-equal", server.name, key, caller.name, off.status, off.body, on.status, on.body)
				}
				if got, want := answerClass(off), usersLegacyClass[caller.name]; got != want {
					t.Errorf("%s %s / %s: legacy class %q, want %q", server.name, key, caller.name, got, want)
				}
			}
		}
	}
}

// With the admin cell on, the decision point decides every /admin/users row for each judged caller that is not in an
// impersonation session: the request asks admin.users on the platform (and members.manage in the token's organization
// when the token names one) first; a credential it cannot judge is never asked about.
func TestAdminUsersRowsAreDecidedByTheDecisionPointWithTheAdminCellOn(t *testing.T) {
	pdp := newCellPDP(t)
	s := newUsersServers(t, pdp)
	for _, key := range adminUsersKeys {
		for _, caller := range s.realOnC {
			pdp.reset("evaluate")
			usersCellDo(s.realOn, key, caller.token)
			var asked []string
			for _, call := range pdp.calls() {
				asked = append(asked, call.Action+"@"+call.ResourceType)
			}
			want := []string(nil)
			switch {
			case !caller.judged || caller.impersonating:
			case caller.name == "platform admin, no org claim" || caller.name == "no platform role, no org claim":
				want = []string{"admin.users@platform"}
			default:
				want = []string{"admin.users@platform", "members.manage@organization"}
			}
			// The route's own batch comes first; a handler may then ask its own platform question (the create route's
			// ensureOrgAdminAccess asks admin.orgs).
			extra := asked[min(len(want), len(asked)):]
			if !slices.Equal(asked[:min(len(want), len(asked))], want) || len(asked) < len(want) ||
				slices.ContainsFunc(extra, func(q string) bool { return !strings.HasSuffix(q, "@platform") }) {
				t.Errorf("%s / %s: asked %v, want %v first", key, caller.name, asked, want)
			}
		}
	}
}

// The cell comparison sees a planted divergence: a decision point that holds the org admin to be a member refuses it
// with the admin cell on, where the legacy guard (its admin membership row) lets it through.
func TestAdminUsersRowsCellCompareSeesAPlantedMismatch(t *testing.T) {
	pdp := newCellPDP(t)
	s := newUsersServers(t, pdp)
	idx := slices.IndexFunc(s.realOnC, func(c usersCaller) bool { return c.name == "org admin" })
	pdp.mu.Lock()
	pdp.callers[s.realOnC[idx].token] = cellSnapshot{orgRole: "member"}
	pdp.callers[s.echoOnC[idx].token] = cellSnapshot{orgRole: "member"}
	pdp.mu.Unlock()
	for _, key := range adminUsersKeys {
		pdp.reset("evaluate")
		off := usersCellDo(s.realOff, key, s.realOffC[idx].token)
		on := usersCellDo(s.realOn, key, s.realOnC[idx].token)
		if !usersAnswersDiffer(off, on) || on.status != http.StatusForbidden || on.body != usersAdminRequired {
			t.Errorf("%s: off %d %q, on %d %q; want the planted 403 %s with the cell on only", key, off.status, off.body, on.status, on.body, usersAdminRequired)
		}
	}
}

// D6833: with the admin cell on, a platform admin with a live go-api impersonation row is refused the four
// /admin/users writes (403, before any decision point call), a narrowing pm lists in the read-only impersonation flaw;
// with the cell off the writes reach the handler as on main. Its reads keep the legacy answer on and off: a read is
// never refused.
func TestAdminUsersRowsImpersonationWritesAreRefusedAndReadsKeepTheLegacyAnswer(t *testing.T) {
	pdp := newCellPDP(t)
	s := newUsersServers(t, pdp)
	idx := slices.IndexFunc(s.realOnC, func(c usersCaller) bool { return c.impersonating })
	for _, server := range []struct {
		name      string
		off, on   http.Handler
		offC, onC []usersCaller
	}{
		{"real", s.realOff, s.realOn, s.realOffC, s.realOnC},
		{"echo", s.echoOff, s.echoOn, s.echoOffC, s.echoOnC},
	} {
		for _, key := range adminUsersKeys {
			pdp.reset("evaluate")
			off := usersCellDo(server.off, key, server.offC[idx].token)
			on := usersCellDo(server.on, key, server.onC[idx].token)
			t.Logf("CELL %s|%s|impersonating platform admin|off %d %s|on %d %s", server.name, key, off.status, strings.TrimSpace(off.body), on.status, strings.TrimSpace(on.body))
			if answerClass(off) != "handler" {
				t.Errorf("%s %s: cell off %d %q, want the handler's answer", server.name, key, off.status, off.body)
			}
			if usersRead(key) {
				if usersAnswersDiffer(off, on) {
					t.Errorf("%s %s read: off %d %q, on %d %q; want the legacy answer", server.name, key, off.status, off.body, on.status, on.body)
				}
			} else if on.status != http.StatusForbidden || on.body != impersonationBody {
				t.Errorf("%s %s write: on %d %q, want 403 %s", server.name, key, on.status, on.body, impersonationBody)
			}
			if calls := pdp.calls(); len(calls) != 0 {
				t.Errorf("%s %s: %d decision point calls, want 0", server.name, key, len(calls))
			}
		}
	}
}
