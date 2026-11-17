//go:build integration

package authruntime

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadinput"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// CHAOS-13945 RC10: the X-DH-Internal-* header carrier that names a principal (X-DH-Internal-Principal-Id) is decided
// by the real auth-service decision point as that principal, and gets the decision the same user's own access token
// gets on /graphql: per caller and per MCP root, served / refused / unavailable agree across /graphql (user token),
// the MCP listener and /query/run-operation (header carrier + principal). The one exception is the platform role
// (acp Q5): the named principal is decided without it, so a platform admin who is not a member is refused on the
// carrier. One query-api Build: PDP mode enforce, the user cells and the header cells (analytics, metrics, git) on;
// auth-service with the named principal kind bound to query-api's workload principal.

const hpHeaderCells = "analytics,metrics,git"

type hpCaller struct {
	name      string
	user      uuid.UUID
	userToken func(*http.Request)
	// platformNonMember: a platform admin with no membership in org A; the carrier must refuse it (acp Q5).
	platformNonMember bool
}

func TestHeaderCarrierPrincipalParity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	v, b := newHPVenue(t, ctx)
	ops := qpGraphQLOperations(t, v.orgA.String())
	allowed := mcpclass.AllowedRoots()
	var roots []qpOperation
	for _, op := range ops.roots {
		_, field, _ := strings.Cut(op.key, ".")
		if op.registered && allowed[field] && strings.Contains(","+hpHeaderCells+",", ","+op.namespace+",") {
			roots = append(roots, op)
		}
	}
	if len(roots) == 0 {
		t.Fatal("no registered MCP root in the header cells: the parity would measure nothing")
	}
	callers := v.hpCallers(t, ctx)
	cells, carrierAsks, servedSame := 0, 0, 0
	for _, c := range callers {
		carrier := hpCarrier(v.orgA, c.user)
		for _, op := range roots {
			user := v.send(t, b, qpClass{name: c.name, carry: c.userToken},
				qpRow{listener: "public", method: http.MethodPost, target: "/graphql", body: op.bodyBytes}, uuid.NewString())
			before := b.pdp.count()
			mcp := v.send(t, b, qpClass{name: c.name, carry: carrier},
				qpRow{listener: "mcp", method: http.MethodPost, target: "/query", body: op.bodyBytes}, uuid.NewString())
			runOp := v.send(t, b, qpClass{name: c.name, carry: carrier},
				qpRow{listener: "internal", method: http.MethodPost, target: "/query/run-operation", body: op.bodyBytes}, uuid.NewString())
			carrierAsks += b.pdp.count() - before
			cells++
			uv, mv, rv := hpVerdict(user), hpVerdict(mcp), hpVerdict(runOp)
			t.Logf("%-22s %-34s user=%-28s mcp=%-28s run-operation=%s", c.name, op.key, uv, mv, rv)
			switch {
			case c.platformNonMember:
				if !strings.HasPrefix(mv, "refused") || !strings.HasPrefix(rv, "refused") {
					t.Errorf("%s %s: the carrier answered mcp=%s run-operation=%s, want refused (no platform bypass, acp Q5)\n mcp: %s\n run-operation: %s",
						c.name, op.key, mv, rv, mcp, runOp)
				}
			case uv != mv || uv != rv:
				t.Errorf("%s %s: user=%s mcp=%s run-operation=%s, want the same verdict\n user: %s\n mcp: %s\n run-operation: %s",
					c.name, op.key, uv, mv, rv, user, mcp, runOp)
			case uv == "served":
				// A served cell answers the same rows: the principal's grants narrow the carrier as they narrow the token.
				// The MCP listener ends its body with a newline; that framing byte is not compared.
				if u, m, r := hpBody(user), hpBody(mcp), hpBody(runOp); !u.equal(m) || !u.equal(r) {
					t.Errorf("%s %s: served on all three, bodies differ\n user: %s\n mcp: %s\n run-operation: %s", c.name, op.key, u, m, r)
				} else {
					servedSame++
				}
			}
		}
	}
	if carrierAsks == 0 {
		t.Fatal("the header carrier never asked the decision point: the cells are not decided")
	}
	t.Logf("%d cells over %d callers and %d roots; the carrier asked the decision point %d times; %d served cells byte-equal on all three",
		cells, len(callers), len(roots), carrierAsks, servedSame)
}

// hpCarrier is acr's real header set (role viewer, no elevated flag) for org, naming user.
func hpCarrier(org, user uuid.UUID) func(*http.Request) {
	return func(r *http.Request) {
		r.Header.Set(internalidentity.HeaderOrgID, org.String())
		r.Header.Set(internalidentity.HeaderRole, "viewer")
		r.Header.Set(internalidentity.HeaderSuperuser, "false")
		r.Header.Set(internalidentity.HeaderImpersonationActive, "false")
		r.Header.Set(internalidentity.HeaderPrincipalID, session.WirePrincipalID(user))
	}
}

func hpBody(a qpAnswer) qpAnswer {
	a = a.masked()
	a.body = bytes.TrimRight(a.body, "\n")
	return a
}

var hpReason = regexp.MustCompile(`"reason":"([a-z_]+)"`)

// hpVerdict is the decision an answer carries: served, refused (401, 403, FORBIDDEN, a legacy AUTHORIZATION_ERROR) or
// unavailable with its reason (503, SERVICE_UNAVAILABLE).
func hpVerdict(a qpAnswer) string {
	body := string(a.body)
	reason := ""
	if m := hpReason.FindStringSubmatch(body); m != nil {
		reason = ":" + m[1]
	}
	switch {
	case a.status == http.StatusServiceUnavailable || strings.Contains(body, `"SERVICE_UNAVAILABLE"`):
		return "unavailable" + reason
	case a.status == http.StatusUnauthorized || a.status == http.StatusForbidden || strings.Contains(body, `"code":"FORBIDDEN"`):
		return "refused"
	case strings.Contains(body, "AUTHORIZATION_ERROR"):
		return "refused:legacy"
	case a.status == http.StatusOK && !strings.Contains(body, `"errors"`):
		return "served"
	}
	var parsed map[string]any
	if json.Unmarshal(a.body, &parsed) == nil && parsed["data"] != nil {
		return "served:field-errors"
	}
	return "other:" + http.StatusText(a.status)
}

// hpUser is a go-api users row (email at example.com) projected into auth.
func (v *qpVenue) hpUser(t *testing.T, ctx context.Context, superuser bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	v.az.exec(t, ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, $3, 0, now(), now())`, id, "hp-"+id.String()[:8]+"@example.com", superuser)
	if err := v.az.identity.Project(ctx, id); err != nil {
		t.Fatalf("project the principal: %v", err)
	}
	return id
}

func (v *qpVenue) hpMember(t *testing.T, ctx context.Context, user uuid.UUID, role string) {
	t.Helper()
	v.az.armed(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, $3, now(), now())`,
		user, v.orgA, role)
}

func (v *qpVenue) hpCallers(t *testing.T, ctx context.Context) []hpCaller {
	t.Helper()
	org := v.orgA
	v.az.exec(t, ctx, `INSERT INTO auth.roles (role_key, name, scope) VALUES ('platform_admin', 'platform_admin', 'platform') ON CONFLICT (role_key) DO NOTHING`)
	var callers []hpCaller
	add := func(name string, user uuid.UUID, token func(*http.Request), platformNonMember bool) {
		callers = append(callers, hpCaller{name: name, user: user, userToken: token, platformNonMember: platformNonMember})
	}
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		user := v.hpUser(t, ctx, false)
		v.hpMember(t, ctx, user, role)
		add("member_"+role, user, v.bearer(t, ctx, user, &org), false)
	}
	// A member whose reads are narrowed to two repositories by resource grants.
	granted := v.hpUser(t, ctx, false)
	v.hpMember(t, ctx, granted, "member")
	for _, key := range []string{"acme/web", "acme/api"} {
		v.az.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id)
VALUES ($1, $2, 'member', 'repository', $3)`, granted, org, key)
	}
	add("member_repo_grant", granted, v.bearer(t, ctx, granted, &org), false)
	// Not a member of org A: the forged-org case (the carrier names org A for a principal that has no row there).
	outsider := v.hpUser(t, ctx, false)
	add("outsider_forged_org", outsider, v.bearer(t, ctx, outsider, &org), false)
	// Deactivated after its session started.
	deactivated := v.hpUser(t, ctx, false)
	v.hpMember(t, ctx, deactivated, "member")
	deactivatedToken := v.bearer(t, ctx, deactivated, &org)
	v.az.exec(t, ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, deactivated)
	if err := v.az.identity.Project(ctx, deactivated); err != nil {
		t.Fatalf("project the deactivated principal: %v", err)
	}
	add("deactivated", deactivated, deactivatedToken, false)
	// Membership removed after its session started.
	removed := v.hpUser(t, ctx, false)
	v.hpMember(t, ctx, removed, "member")
	removedToken := v.bearer(t, ctx, removed, &org)
	v.az.armed(t, ctx, `DELETE FROM public.memberships WHERE user_id = $1 AND org_id = $2`, removed, org)
	add("membership_removed", removed, removedToken, false)
	// Platform admins (is_superuser): one a viewer of org A, one with no membership there.
	adminMember := v.hpUser(t, ctx, true)
	v.hpMember(t, ctx, adminMember, "viewer")
	add("platform_admin_member", adminMember, v.bearer(t, ctx, adminMember, &org), false)
	adminOutsider := v.hpUser(t, ctx, true)
	add("platform_admin_non_member", adminOutsider, v.bearer(t, ctx, adminOutsider, &org), true)
	return callers
}

// newHPVenue is newQPVenue with the named principal kind on and one enforce build with the header cells on.
func newHPVenue(t *testing.T, ctx context.Context) (*qpVenue, *qpBuild) {
	t.Helper()
	qpQuietLogs(t)
	issuerFront := httptest.NewUnstartedServer(nil)
	issuer := "https://" + issuerFront.Listener.Addr().String()
	workloadID := uuid.New()
	caller := session.WirePrincipalID(workloadID)
	az := newAuthorizeRouterEnv(t, ctx, map[string]string{
		authconfig.EnvTokenIssuerURL:                issuer,
		authconfig.EnvAuthorizeCallerSubjects:       caller,
		authconfig.EnvAuthorizeNamedPrincipalCaller: caller,
		authconfig.EnvWorkloadBootstrap:             "true",
	})
	az.caller = caller
	api := az.front.Config.Handler
	issuerFront.Config.Handler = api
	issuerFront.TLS = &tls.Config{Certificates: []tls.Certificate{authruntimeTestIssuerTLS}}
	issuerFront.StartTLS()
	t.Cleanup(issuerFront.Close)
	qpTrustGuard(t)

	v := &qpVenue{az: az, issuer: issuer, client: &http.Client{Timeout: 60 * time.Second}}
	v.orgA = az.azOrg(t, ctx, true, true)
	bootstrap := v.seedWorkload(t, ctx, workloadID)
	for _, root := range mcpclass.SortedRoots() {
		pgseed.ClassDecision(ctx, t, az.admin, mcpclass.Operation(root), "primary")
	}
	jwksPath := v.envelopeJWKS(t)
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = ch.Close(closeCtx)
	})
	chschema.Apply(ctx, t, ch)
	settings := map[string]string{
		"CLICKHOUSE_URI":               ch.URI,
		"GO_API_ENVELOPE_JWKS_PATH":    jwksPath,
		"GO_API_ENVELOPE_ISSUER":       qpEnvelopeIssuer,
		"GO_API_ENVELOPE_AUDIENCE":     qpEnvelopeAudience,
		"DEV_HEALTH_ENV":               "ci",
		"AUTH_ACCESS_TOKEN_ISSUER":     issuer,
		workloadinput.EnvIssuer:        issuer,
		workloadinput.EnvBootstrapFile: bootstrap,
	}
	for _, name := range qpRouteSwitches(t) {
		settings[name] = "true"
	}
	// Production query-api runs with auth-service owning sessions (deploy values.prod.yaml): its access-token path
	// reads the auth sessions table, and its role holds that read.
	settings["AUTH_PROXY_SESSIONS"] = "true"
	settings["GO_API_REGISTRY_POSTGRES_URI"] = hpQueryAPIRole(t, ctx, az, settings)
	counter := &qpCounter{next: api, revisions: map[int]int{}}
	proxy := httptest.NewServer(counter)
	t.Cleanup(proxy.Close)
	settings = qpMerge(settings, map[string]string{
		pdp.EnvMode:                        pdp.ModeEnforce,
		pdp.EnvAuthProxyURL:                proxy.URL,
		authzenforce.QueryAPI.Env():        qpEnforceCells,
		authzenforce.QueryAPIHeaders.Env(): hpHeaderCells,
	})
	return v, v.build(t, ctx, "H", settings, counter)
}

// hpQueryAPIRole is query-api's production role shape on the venue database: a LOGIN NOSUPERUSER role with exactly
// the grants of postgresstore.QueryAPIPosture, applied through the pinned river migrations as the deploy hook does.
// While auth-service owns sessions (AUTH_PROXY_SESSIONS) the role also holds the auth session read, and the platform
// role read while AUTH_PLATFORM_ROLES_OWNED, as rivermigrate grants them. It returns that role's DSN after the posture
// check passes on it.
func hpQueryAPIRole(t *testing.T, ctx context.Context, az *azRouterEnv, settings map[string]string) string {
	t.Helper()
	suffix := strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	domain, queue, role := "hp_domain_"+suffix, "hp_queue_"+suffix, "hp_query_api_"+suffix
	password := testRolePassword(t)
	database := az.admin.Config().ConnConfig.Database
	statements := []string{"REVOKE TEMPORARY ON DATABASE " + database + " FROM PUBLIC"}
	for _, name := range []string{domain, queue, role} {
		statements = append(statements,
			"CREATE ROLE "+name+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+password+"'",
			"GRANT CONNECT ON DATABASE "+database+" TO "+name)
	}
	for _, statement := range statements {
		if _, err := az.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", strings.ReplaceAll(statement, password, "<password>"), err)
		}
	}
	var grants []riverstore.TableGrant
	for _, table := range postgresstore.QueryAPIPosture().RequiredTables {
		grants = append(grants, riverstore.TableGrant{TableName: table.TableName,
			AllowInsert: table.AllowInsert, AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete})
	}
	var columns []riverstore.ColumnGrant
	for _, column := range postgresstore.QueryAPIPosture().ColumnScoped {
		columns = append(columns, riverstore.ColumnGrant{TableName: column.TableName, ColumnName: column.ColumnName, Privilege: column.Privilege})
	}
	var sessions *postgresstore.QueryAPISessionRead
	var sessionGrant *riverstore.SessionReadGrant
	if settings["AUTH_PROXY_SESSIONS"] == "true" {
		read := postgresstore.QueryAPISessionReadFor("auth", settings["AUTH_PLATFORM_ROLES_OWNED"] == "true")
		sessions = &read
		sessionGrant = &riverstore.SessionReadGrant{Schema: read.Schema, Table: read.Table, Columns: read.Columns}
		for _, table := range read.Also {
			sessionGrant.Also = append(sessionGrant.Also, riverstore.SessionReadTable{Table: table.Table, Columns: table.Columns})
		}
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, az.admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domain, QueueRole: queue,
		QueryAPIRole: role, QueryAPIGrants: grants, QueryAPIColumnGrants: columns,
		QueryAPISessionRead: sessionGrant, AuthSchema: "auth",
	}); err != nil {
		t.Fatalf("apply the pinned river migrations with the query-api leg: %v", err)
	}
	dsn, err := url.Parse(az.admin.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	dsn.User = url.UserPassword(role, password)
	pool, err := pgxpool.New(ctx, dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var superuser bool
	if err := pool.QueryRow(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = current_user`).Scan(&superuser); err != nil || superuser {
		t.Fatalf("the query-api login must be a NOSUPERUSER role (superuser=%v, err=%v)", superuser, err)
	}
	t.Logf("query-api login %s: NOSUPERUSER, posture grants, auth session read %v (AUTH_PROXY_SESSIONS=%q, AUTH_PLATFORM_ROLES_OWNED=%q)",
		"hp_query_api_<id>", sessions != nil, settings["AUTH_PROXY_SESSIONS"], settings["AUTH_PLATFORM_ROLES_OWNED"])
	if err := postgresstore.CheckQueryAPIAuthorizationWithSessions(ctx, pool, role, "river", sessions); err != nil {
		t.Fatalf("the query-api role must satisfy its posture: %v", err)
	}
	return dsn.String()
}
