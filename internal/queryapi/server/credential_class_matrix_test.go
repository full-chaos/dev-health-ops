package server

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-go/authverify"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/wrongclass"
)

// matrixCounts counts every read a credential can cause past the class
// decision: a signing-key read and a store read.
type matrixCounts struct {
	mu    sync.Mutex
	keys  int
	store int
}

func (c *matrixCounts) addKey()   { c.mu.Lock(); c.keys++; c.mu.Unlock() }
func (c *matrixCounts) addStore() { c.mu.Lock(); c.store++; c.mu.Unlock() }
func (c *matrixCounts) reset()    { c.mu.Lock(); c.keys, c.store = 0, 0; c.mu.Unlock() }
func (c *matrixCounts) snapshot() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.keys, c.store
}

type matrixStoreBase struct {
	*accessStore
	counts *matrixCounts
}

func (s matrixStoreBase) UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	s.counts.addStore()
	return s.accessStore.UserState(ctx, id)
}

func (s matrixStoreBase) Membership(ctx context.Context, user, org uuid.UUID) (string, bool, error) {
	s.counts.addStore()
	return s.accessStore.Membership(ctx, user, org)
}

func (s matrixStoreBase) ActiveImpersonation(ctx context.Context, admin uuid.UUID) (*policy.Impersonation, error) {
	s.counts.addStore()
	return s.accessStore.ActiveImpersonation(ctx, admin)
}

func (s matrixStoreBase) SessionState(ctx context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	s.counts.addStore()
	return s.accessStore.SessionState(ctx, id)
}

func (s matrixStoreBase) AccessUser(ctx context.Context, id uuid.UUID) (policy.AccessUserRow, bool, error) {
	s.counts.addStore()
	return s.accessStore.AccessUser(ctx, id)
}

type matrixKeySource struct {
	inner  authverify.StaticKeys
	counts *matrixCounts
}

func (k matrixKeySource) PublicKey(kid string) (ed25519.PublicKey, bool) {
	k.counts.addKey()
	return k.inner.PublicKey(kid)
}

type matrixClassDef struct {
	id      string
	markers map[string][]string
}

func (c matrixClassDef) has(kind, name string) bool { return slices.Contains(c.markers[kind], name) }

func loadQueryMatrixClasses(t *testing.T) []matrixClassDef {
	t.Helper()
	var document struct {
		Classes []struct {
			ClassID string `json:"class_id"`
			Markers []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"markers"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(authv1.CredentialClasses, &document); err != nil {
		t.Fatal(err)
	}
	var out []matrixClassDef
	for _, entry := range document.Classes {
		class := matrixClassDef{id: entry.ClassID, markers: map[string][]string{}}
		for _, marker := range entry.Markers {
			class.markers[marker.Kind] = append(class.markers[marker.Kind], marker.Name)
		}
		out = append(out, class)
	}
	return out
}

// queryMatrixNotDriven are the classes the matrix sends no request head for: wrongclass derives the carrier of each
// class from its credential-classes.json markers, and a class with no carrier, or one carried in a body, has no head.
func queryMatrixNotDriven(t *testing.T) []string {
	t.Helper()
	classes, err := wrongclass.LoadClasses(authv1.CredentialClasses)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, class := range classes {
		carrier, ok, err := wrongclass.CarrierFor(class, wrongclass.Options{Mint: func(string, string) string { return "h.p.s" }})
		if err != nil {
			t.Fatal(err)
		}
		if !ok || carrier.Header == "" {
			out = append(out, class.ID)
		}
	}
	if len(out) == 0 || len(out) == len(classes) {
		t.Fatalf("%d of %d classes have no request head", len(out), len(classes))
	}
	return out
}

var queryMatrixParam = regexp.MustCompile(`\{[^}]*\}`)

type queryMatrixCell struct {
	bearer  string
	headers map[string]string
}

// TestCredentialClassMatrixQueryAPI drives every protected query-api
// endpoint-profile row, on each listener its row names, with every credential
// class of credential-classes.json through the real listener handlers. A class
// the row does not accept must reach no signing-key read and no store read; a
// class it accepts must not be refused.
func TestCredentialClassMatrixQueryAPI(t *testing.T) {
	counts := &matrixCounts{}
	minter := newAccessMinter(t)
	base := matrixStoreBase{accessStore: newAccessStore(), counts: counts}
	setup := testAccess(matrixKeySource{inner: minter.keys, counts: counts}, nil, accessTestNow)
	stack := newAccessStackOn(t, queryAPIUserStore{queryAPIStoreBase: base}, setup)
	stack.user = minter.mint(ecUser, nil, nil)

	// The REST routes' builders need ClickHouse, a registry and JWKS (their
	// real plane is an integration test), so the REST rows are driven through
	// the real REST authenticator over the production route table
	// (RESTRoutes), mounted on a mux with the same patterns.
	restVerifier, restPriv := iaVerifier(t)
	edge, err := principal.NewEdgeVerifier(queryAPIUserStore{queryAPIStoreBase: base})
	if err != nil {
		t.Fatal(err)
	}
	if err := routeEdgeVerifier(edge, setup.opts); err != nil {
		t.Fatal(err)
	}
	restMux := http.NewServeMux()
	restPaths := map[string]bool{}
	for _, route := range RESTRoutes() {
		if restPaths[route.Pattern] {
			continue
		}
		restPaths[route.Pattern] = true
		restMux.HandleFunc(route.Pattern, func(w http.ResponseWriter, r *http.Request) {
			if _, ok := authenticateRESTRequest(w, r, restVerifier, edge, "matrix"); ok {
				w.WriteHeader(http.StatusOK)
			}
		})
	}
	restEnvelope := iaEnvelope(t, restPriv, principal.Claims{OrgID: ecOrg.String(), Role: "admin"})

	filler := strings.Repeat("A", 43)
	carrierFor := func(class matrixClassDef, envelope string) (queryMatrixCell, bool) {
		switch {
		case len(class.markers["credential_prefix"]) > 0:
			return queryMatrixCell{bearer: class.markers["credential_prefix"][0] + filler}, true
		case class.has("authorization_scheme", "Bearer") && class.has("jwt_header_alg", "HS256"):
			return queryMatrixCell{bearer: stack.edge}, true
		case class.has("authorization_scheme", "Bearer") && class.has("jwt_header_alg", "EdDSA"):
			switch typ := class.markers["jwt_header_typ"][0]; typ {
			case "at+jwt":
				return queryMatrixCell{bearer: stack.user}, true
			case "jwt":
				return queryMatrixCell{bearer: envelope}, true
			default:
				return queryMatrixCell{bearer: minter.mint(ecUser, nil, map[string]any{"typ": typ})}, true
			}
		case len(class.markers["dedicated_header"]) > 0:
			// Every dedicated header of the class (acr_web_assertion has two) is on the request.
			headers := map[string]string{}
			for _, name := range class.markers["dedicated_header"] {
				headers[name] = ecOrg.String()
			}
			return queryMatrixCell{headers: headers}, true
		case len(class.markers["signature_header"]) > 0:
			return queryMatrixCell{headers: map[string]string{class.markers["signature_header"][0]: "sha256=" + filler}}, true
		}
		return queryMatrixCell{}, false
	}

	type cell struct {
		row      profileRow
		listener string
	}
	var cells []cell
	for _, rows := range queryAPIRowsByRoute(t) {
		for _, row := range rows {
			if row.public || row.method == "api_route" {
				continue
			}
			for _, listener := range row.listeners {
				cells = append(cells, cell{row, listener})
			}
		}
	}
	if len(cells) == 0 {
		t.Fatal("no protected query-api row")
	}
	slices.SortFunc(cells, func(a, b cell) int { return strings.Compare(a.row.id+a.listener, b.row.id+b.listener) })

	var notDriven []string
	for _, class := range loadQueryMatrixClasses(t) {
		carrier, driven := carrierFor(class, stack.envelope)
		restCarrier, _ := carrierFor(class, restEnvelope)
		if !driven {
			notDriven = append(notDriven, class.id)
			continue
		}
		t.Run(class.id, func(t *testing.T) {
			accepts := 0
			for _, c := range cells {
				path := queryMatrixParam.ReplaceAllString(c.row.route, "x")
				counts.reset()
				var got answer
				if restPaths[c.row.route] {
					got = serveREST(restMux, c.row.method, path, restCarrier)
				} else {
					got = stack.sendWith(c.listener, c.row.method, path, "", carrier.bearer, carrier.headers)
				}
				keys, store := counts.snapshot()
				if slices.Contains(c.row.classes, class.id) && platformAdminOnly[c.row.method+" "+c.row.route] && class.id == "user_access_token" {
					accepts++
					if got.Status != http.StatusUnauthorized || keys+store == 0 {
						t.Errorf("%s @%s: class %s of an organization admin answered %d after %d lookups, want 401 after a live read: the row serves a platform admin only", c.row.id, c.listener, class.id, got.Status, keys+store)
					}
					continue
				}
				if slices.Contains(c.row.classes, class.id) {
					accepts++
					if got.Status == http.StatusUnauthorized || !served(got.Status) {
						t.Errorf("%s @%s: accepted class %s answered %d", c.row.id, c.listener, class.id, got.Status)
					}
					if class.id != "effective_principal_envelope" && keys+store == 0 {
						t.Errorf("%s @%s: accepted class %s made no lookup: the counters do not see it", c.row.id, c.listener, class.id)
					}
					continue
				}
				if keys != 0 || store != 0 {
					t.Errorf("%s @%s: class %s reached %d key reads and %d store reads (status %d)", c.row.id, c.listener, class.id, keys, store, got.Status)
				}
			}
			t.Logf("class %s: %d cells, %d accept cells", class.id, len(cells), accepts)
		})
	}
	slices.Sort(notDriven)
	want := queryMatrixNotDriven(t)
	slices.Sort(want)
	if !slices.Equal(notDriven, want) {
		t.Errorf("classes with no carrier %v, want %v", notDriven, want)
	}
}

// platformAdminOnly are the rows that accept user_access_token for a platform admin only (authenticateBuildInfo):
// the matrix's organization-admin token is refused there after its live users-row read.
var platformAdminOnly = map[string]bool{"GET /buildinfo": true}

func serveREST(handler http.Handler, method, path string, carrier queryMatrixCell) answer {
	request := httptest.NewRequest(method, path, nil)
	if carrier.bearer != "" {
		request.Header.Set("Authorization", "Bearer "+carrier.bearer)
	}
	for name, value := range carrier.headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return answer{recorder.Code, recorder.Body.String()}
}
