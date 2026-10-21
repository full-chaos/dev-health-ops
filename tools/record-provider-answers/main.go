// record-provider-answers records the real answers of Linear and the Atlassian
// Teams gateway to the reads a per-kind absence prover needs (CHAOS-9124):
// one FOUND and one NOT FOUND answer of each read, as status + body SHAPE.
//
// It is a one-off recorder, not a product command: it is not under cmd/ and no
// image builds it. It runs inside the stack that holds the integration's stored
// credential, as a one-off container on the stack's network, and uses the
// product's own credential path (providerfoundation.CredentialResolver over the
// PostgresCredentialRepository, the stack's SETTINGS_ENCRYPTION_KEY) and the
// product's own clients (providerfoundation HTTP client for Linear, the
// vendored atlassian graph client for the gateway). A credential value is never
// printed, logged, put on argv or written: the recorder reads the environment
// by NAME inside the process.
//
// Read-only: every call is a GraphQL query or a GET. The probes of a NOT FOUND
// answer name an id the recorder makes up (a random UUID / a random team ARI),
// so the body of such an answer holds nothing of the organization.
//
// Output (counts only on stdout): one JSON file per recorded read under -out:
//
//	{"case", "method", "path", "status", "content_type", "request_shape", "body_shape", "body_verbatim"}
//
// body_shape keeps every key and the type of every value (strings become
// "<string>", numbers "<number>", an array keeps its first element only).
// body_verbatim is set ONLY for a NOT FOUND probe, whose text is the
// provider's own wording and holds no value of the organization.
//
// -dry resolves the credential and prints the NAMES of the credential keys it
// will use (and whether each is configured), the environment variable NAMES it
// reads and the number of provider calls it will make; it makes no provider
// call.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

const (
	envPostgres = "POSTGRES_URI"
	envKey      = "SETTINGS_ENCRYPTION_KEY"
	envSalt     = "SETTINGS_ENCRYPTION_SALT"
	envOrg      = "RECORD_ORG_ID"

	// The calls each provider takes, counted from the plan below (one HTTP call
	// each; a page of a paged read is one call).
	linearCalls    = 8
	atlassianCalls = 11
)

type recording struct {
	Case         string          `json:"case"`
	Method       string          `json:"method"`
	Path         string          `json:"path"`
	Status       int             `json:"status"`
	ContentType  string          `json:"content_type"`
	RequestShape json.RawMessage `json:"request_shape,omitempty"`
	BodyShape    json.RawMessage `json:"body_shape,omitempty"`
	// BodyVerbatim is set only for a NOT FOUND probe.
	BodyVerbatim string `json:"body_verbatim,omitempty"`
}

// recorder wraps a transport and keeps the last response of each call.
type recorder struct {
	inner http.RoundTripper
	last  *capture
}

type capture struct {
	method, path, contentType string
	status                    int
	request, body             []byte
}

func (r *recorder) RoundTrip(request *http.Request) (*http.Response, error) {
	var requestBody []byte
	if request.Body != nil {
		requestBody, _ = io.ReadAll(request.Body)
		request.Body = io.NopCloser(bytes.NewReader(requestBody))
	}
	inner := r.inner
	if inner == nil {
		inner = httpguard.SharedTransport()
	}
	response, err := inner.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	response.Body = io.NopCloser(bytes.NewReader(body))
	r.last = &capture{
		method: request.Method, path: request.URL.Path, contentType: response.Header.Get("Content-Type"),
		status: response.StatusCode, request: requestBody, body: body,
	}
	return response, nil
}

// client is the doer the product's clients accept: the shared no-redirect client
// (httpguard.NewClient) whose transport is the recorder.
func (r *recorder) client() *http.Client {
	client := httpguard.NewClient(45 * time.Second)
	client.Transport = r
	return client
}

// shape replaces every value by its type and keeps the first element of an array.
func shape(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = shape(item)
		}
		return out
	case []any:
		if len(typed) == 0 {
			return []any{}
		}
		return []any{shape(typed[0])}
	case string:
		return "<string>"
	case float64:
		return "<number>"
	case bool:
		return "<bool>"
	case nil:
		return nil
	}
	return "<unknown>"
}

func shapeOf(raw []byte) json.RawMessage {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return json.RawMessage(`"<not json>"`)
	}
	out, _ := json.Marshal(shape(value))
	return out
}

func randomUUID() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16])
}

type session struct {
	out    string
	count  int
	failed []string
}

func (s *session) save(name string, provider string, c *capture, verbatim bool) error {
	if c == nil {
		return errors.New("no call was captured for " + name)
	}
	entry := recording{
		Case: name, Method: c.method, Path: c.path, Status: c.status, ContentType: c.contentType,
		RequestShape: shapeOf(c.request), BodyShape: shapeOf(c.body),
	}
	if verbatim {
		entry.BodyVerbatim = string(c.body)
	}
	raw, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Join(s.out, provider)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	s.count++
	return os.WriteFile(filepath.Join(dir, name+".json"), append(raw, '\n'), 0o644)
}

func main() {
	provider := flag.String("provider", "", "linear | atlassian")
	out := flag.String("out", "", "directory the recordings are written to")
	dry := flag.Bool("dry", false, "resolve the credential and print the credential key NAMES and the call count; make no provider call")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := run(ctx, *provider, *out, *dry); err != nil {
		fmt.Fprintln(os.Stderr, "record-provider-answers:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, provider, out string, dry bool) error {
	provider = strings.ToLower(strings.TrimSpace(provider))
	dbKey := provider
	if provider == "atlassian" {
		dbKey = "jira"
	}
	if provider != "linear" && provider != "atlassian" {
		return errors.New("-provider must be linear or atlassian")
	}
	if !dry && strings.TrimSpace(out) == "" {
		return errors.New("-out is required")
	}
	dsn := secrets.GetenvDSN(envPostgres)
	if strings.TrimSpace(dsn) == "" {
		return errors.New(envPostgres + " is not set")
	}
	keyValue := secrets.GetenvSecret(envKey)
	if strings.TrimSpace(keyValue) == "" {
		return errors.New(envKey + " is not set")
	}
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(keyValue), os.Getenv(envSalt))
	if err != nil {
		return errors.New("the encryption key is not usable")
	}
	dsn = strings.Replace(strings.Replace(dsn, "postgresql+asyncpg://", "postgresql://", 1), "postgres+asyncpg://", "postgresql://", 1)
	// The boundary of the connection redacts the login and the password it resolves, in any error text.
	boundary := pgstorage.Boundary(dsn)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("the database connection could not be opened: %w", boundary.Redact(err))
	}
	defer pool.Close()

	orgID, integrationID, credentialID, siblings, err := pickIntegration(ctx, pool, dbKey, os.Getenv(envOrg))
	if err != nil {
		return err
	}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	resolver := providerfoundation.CredentialResolver{
		Repository: providerfoundation.PostgresCredentialRepository{Pool: pool}, Decryptor: decryptor,
	}
	credential, err := resolver.Resolve(ctx, lease, providerfoundation.TenantScope{
		OrgID: orgID, Provider: dbKey, IntegrationID: integrationID, CredentialID: credentialID,
	})
	if err != nil {
		return fmt.Errorf("the stored %s credential could not be resolved", dbKey)
	}
	names := credentialKeyNames(provider)
	if dry {
		fmt.Printf("provider=%s\n", provider)
		fmt.Printf("environment variable names read: %s, %s, %s (salt optional), %s (optional)\n", envPostgres, envKey, envSalt, envOrg)
		fmt.Printf("active %s integrations of the organization: %d (the first by creation is used)\n", dbKey, siblings)
		for _, name := range names {
			_, ok := credential.Secret(name)
			fmt.Printf("credential key name %q configured=%t\n", name, ok)
		}
		calls := linearCalls
		if provider == "atlassian" {
			calls = atlassianCalls
		}
		fmt.Printf("provider calls: %d (read-only; no call is made by -dry)%s\n", calls, map[bool]string{true: ", plus one more call for each further page of a team with more than 50 members or containers", false: ""}[provider == "atlassian"])
		return nil
	}
	s := &session{out: out}
	var runErr error
	switch provider {
	case "linear":
		runErr = recordLinear(ctx, s, credential, lease)
	default:
		runErr = recordAtlassian(ctx, s, credential, lease)
	}
	fmt.Printf("provider=%s recordings=%d failed_steps=%d\n", provider, s.count, len(s.failed))
	for _, step := range s.failed {
		fmt.Println("failed step:", step)
	}
	return runErr
}

func credentialKeyNames(provider string) []string {
	if provider == "linear" {
		return []string{"api_key"}
	}
	return []string{"email", "api_token", "apiToken", "token"}
}

func pickIntegration(ctx context.Context, pool *pgxpool.Pool, provider, orgID string) (org, integration, credential string, siblings int, err error) {
	rows, err := pool.Query(ctx, `SELECT org_id::text, id::text, COALESCE(credential_id::text, '')
FROM integrations WHERE lower(provider) = $1 AND is_active = TRUE AND ($2 = '' OR org_id::text = $2)
ORDER BY created_at ASC`, provider, orgID)
	if err != nil {
		return "", "", "", 0, errors.New("the integrations could not be read")
	}
	defer rows.Close()
	for rows.Next() {
		var o, i, c string
		if err := rows.Scan(&o, &i, &c); err != nil {
			return "", "", "", 0, errors.New("the integrations could not be read")
		}
		if siblings == 0 {
			org, integration, credential = o, i, c
		}
		siblings++
	}
	if err := rows.Err(); err != nil || siblings == 0 {
		return "", "", "", 0, fmt.Errorf("no active %s integration was found", provider)
	}
	return org, integration, credential, siblings, nil
}

// --- Linear ------------------------------------------------------------------

type linearCall struct {
	name, query string
	verbatim    bool
	after       func(body []byte, carry map[string]string)
}

func firstString(body []byte, path ...string) string {
	var value any
	if json.Unmarshal(body, &value) != nil {
		return ""
	}
	for _, key := range path {
		switch typed := value.(type) {
		case map[string]any:
			value = typed[key]
		case []any:
			if len(typed) == 0 {
				return ""
			}
			value = typed[0]
			if object, ok := value.(map[string]any); ok {
				value = object[key]
			}
		default:
			return ""
		}
	}
	text, _ := value.(string)
	return text
}

func recordLinear(ctx context.Context, s *session, credential providerfoundation.Credential, lease providerfoundation.LeaseGuard) error {
	rec := &recorder{}
	client, err := providerfoundation.NewLinearClient(credential, rec.client(), providerfoundation.DefaultRetryPolicy(), lease)
	if err != nil {
		return errors.New("the linear client could not be built")
	}
	carry := map[string]string{}
	unknownKey := "ZZ" + strings.ToUpper(strings.ReplaceAll(randomUUID(), "-", ""))[:6]
	unknownID := randomUUID()
	q := func(format string, args ...any) string { return fmt.Sprintf(format, args...) }
	steps := []linearCall{
		{name: "teams_list_found", query: `query { teams(first: 2) { nodes { id key } pageInfo { hasNextPage endCursor } } }`,
			after: func(body []byte, c map[string]string) { c["key"] = firstString(body, "data", "teams", "nodes", "key") }},
		{name: "projects_list_found", query: `query { projects(first: 2) { nodes { id } pageInfo { hasNextPage endCursor } } }`,
			after: func(body []byte, c map[string]string) {
				c["project"] = firstString(body, "data", "projects", "nodes", "id")
			}},
	}
	run := func(step linearCall, query string) error {
		payload, _ := json.Marshal(map[string]any{"query": query})
		response, err := client.Do(ctx, http.MethodPost, "/graphql", bytes.NewReader(payload))
		if err != nil {
			s.failed = append(s.failed, step.name)
			return nil
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if err := s.save(step.name, "linear", rec.last, step.verbatim); err != nil {
			return err
		}
		if step.after != nil {
			step.after(rec.last.body, carry)
		}
		return nil
	}
	for _, step := range steps {
		if err := run(step, step.query); err != nil {
			return err
		}
	}
	if carry["key"] == "" || carry["project"] == "" {
		return errors.New("the found team or project could not be picked from the first answers")
	}
	later := []linearCall{
		{name: "team_by_key_found", query: q(`query { team(id: %q) { id key name } }`, carry["key"])},
		{name: "team_by_key_not_found", query: q(`query { team(id: %q) { id key name } }`, unknownKey), verbatim: true},
		{name: "teams_filter_key_found", query: q(`query { teams(filter: {key: {eq: %q}}) { nodes { id key } } }`, carry["key"])},
		{name: "teams_filter_key_not_found", query: q(`query { teams(filter: {key: {eq: %q}}) { nodes { id key } } }`, unknownKey), verbatim: true},
		{name: "project_by_id_found", query: q(`query { project(id: %q) { id name teams { nodes { key } } } }`, carry["project"])},
		{name: "project_by_id_not_found", query: q(`query { project(id: %q) { id name teams { nodes { key } } } }`, unknownID), verbatim: true},
	}
	for _, step := range later {
		if err := run(step, step.query); err != nil {
			return err
		}
	}
	return nil
}

// --- Atlassian Teams gateway -------------------------------------------------

func recordAtlassian(ctx context.Context, s *session, credential providerfoundation.Credential, lease providerfoundation.LeaseGuard) error {
	rec := &recorder{}
	client, err := providerfoundation.NewJiraClient(credential, rec.client(), providerfoundation.DefaultRetryPolicy(), lease)
	if err != nil || client.BaseURL == nil {
		return errors.New("the jira client could not be built")
	}
	tenant, err := url.Parse(strings.TrimRight(client.BaseURL.String(), "/"))
	if err != nil || tenant.Host == "" {
		return errors.New("the stored base url is not usable")
	}
	tenant = &url.URL{Scheme: "https", Host: tenant.Host}
	cloudID := strings.TrimSpace(credential.Config["atlassian_cloud_id"])
	if cloudID == "" {
		if cloudID, err = atlassianteams.ResolveCloudID(ctx, rec.client(), tenant); err != nil {
			return errors.New("the cloud id could not be resolved")
		}
		if err := s.save("tenant_info_found", "atlassian", rec.last, false); err != nil {
			return err
		}
	}
	email, _ := credential.Secret("email")
	token := ""
	for _, alias := range credentialKeyNames("atlassian")[1:] {
		if value, ok := credential.Secret(alias); ok && value.Configured() {
			token = value.Reveal()
			break
		}
	}
	if !email.Configured() || token == "" {
		return errors.New("the stored credential has no email or api token")
	}
	gatewayURL := tenant.String() + "/gateway/api"
	auth := atlassian.BasicAPITokenAuth{Email: email.Reveal(), Token: token}
	gatewayClient := httpguard.NewClient(45 * time.Second)
	gatewayClient.Transport = &recorder{inner: atlassianteams.CompletePagesOnly(nil)}
	gateway := &graph.Client{
		BaseURL: gatewayURL, Auth: auth, Strict: true,
		HTTPClient: gatewayClient,
	}
	gatewayRecorder := gateway.HTTPClient.Transport.(*recorder)
	organizationID := strings.TrimSpace(credential.Config["atlassian_organization_id"])
	if organizationID == "" {
		if organizationID, err = atlassianteams.ResolveOrganizationID(ctx, gateway, cloudID); err != nil {
			return errors.New("the organization id could not be resolved")
		}
		if err := s.save("tenant_contexts_found", "atlassian", gatewayRecorder.last, false); err != nil {
			return err
		}
	}
	organizationARI := organizationID
	if !strings.HasPrefix(strings.ToLower(organizationARI), "ari:") {
		organizationARI = "ari:cloud:platform::org/" + organizationID
	}
	siteContext := cloudID
	if !strings.HasPrefix(strings.ToLower(siteContext), "ari:") {
		siteContext = "ari:cloud:platform::site/" + cloudID
	}
	siteCtx := graph.WithQueryContext(ctx, siteContext)

	record := func(name string, verbatim bool, call func() error) error {
		gatewayRecorder.last = nil
		if err := call(); err != nil {
			// A failed call is a recorded answer too (the not-found probe answers an error).
			if gatewayRecorder.last == nil {
				s.failed = append(s.failed, name)
				return nil
			}
		}
		return s.save(name, "atlassian", gatewayRecorder.last, verbatim)
	}
	var foundTeam string
	if err := record("team_search_found", false, func() error {
		teams, err := gateway.SearchTeams(ctx, organizationARI, cloudID, "", 2)
		if len(teams) > 0 {
			foundTeam = teams[0].ID
		}
		return err
	}); err != nil {
		return err
	}
	if foundTeam == "" {
		return errors.New("no team was found to probe")
	}
	unknownTeam := "ari:cloud:identity::team/" + randomUUID()
	// foundUser is kept in memory only (never written): the first member of the found team.
	var foundUser string
	steps := []struct {
		name     string
		verbatim bool
		call     func() error
	}{
		{"team_users_found", false, func() error {
			relations, err := gateway.IterTeamUsers(siteCtx, foundTeam, 50)
			for _, relation := range relations {
				if relation.RelationType == "TEAM_MEMBER" && strings.TrimSpace(relation.SubjectUserID) != "" {
					foundUser = relation.SubjectUserID
					break
				}
			}
			return err
		}},
		{"team_users_not_found", true, func() error { _, err := gateway.IterTeamUsers(siteCtx, unknownTeam, 50); return err }},
		{"team_containers_found", false, func() error { _, err := gateway.IterTeamConnectedContainers(siteCtx, foundTeam, 50); return err }},
		{"team_containers_not_found", true, func() error { _, err := gateway.IterTeamConnectedContainers(siteCtx, unknownTeam, 50); return err }},
		{"team_by_id_found", false, func() error { _, err := gateway.GetTeamByID(ctx, foundTeam, cloudID); return err }},
		{"team_by_id_not_found", true, func() error { _, err := gateway.GetTeamByID(ctx, unknownTeam, cloudID); return err }},
		{"user_teams_found", false, func() error {
			if foundUser == "" {
				return errors.New("no member to probe")
			}
			_, err := gateway.IterUserTeams(siteCtx, foundUser, 50)
			return err
		}},
		{"user_teams_not_found", true, func() error {
			_, err := gateway.IterUserTeams(siteCtx, "ari:cloud:identity::user/"+randomUUID(), 50)
			return err
		}},
	}
	for _, step := range steps {
		if err := record(step.name, step.verbatim, step.call); err != nil {
			return err
		}
	}
	return nil
}
