// Package synccli is the `sync` group of dho: verbs that pull an external
// system's structure into ClickHouse. Its first verb, `sync teams`, reads the
// organization's Atlassian Teams (structure, members, active projects) and
// writes them to the ClickHouse team dimensions (internal/atlassianteams).
//
// `--provider jira` keeps the name Python users know. It is Atlassian Teams,
// not Jira projects standing in for teams: the project-as-team rows the Jira
// catalog writes are a separate option and are never touched.
package synccli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// Environment variable NAMES the verb reads; their values are never logged.
const (
	ClickHouseURIKey  = "CLICKHOUSE_URI"
	OrganizationIDKey = "ATLASSIAN_ORGANIZATION_ID"
	CloudIDKey        = "ATLASSIAN_CLOUD_ID"
	emailKey          = "ATLASSIAN_EMAIL"
	legacyEmailKey    = "JIRA_EMAIL"
	apiTokenKey       = "ATLASSIAN_API_TOKEN"
	legacyAPITokenKey = "JIRA_API_TOKEN"
	baseURLKey        = "ATLASSIAN_JIRA_BASE_URL"
	legacyBaseURLKey  = "JIRA_BASE_URL"
	gatewayPath       = "/gateway/api"
	teamsUsage        = "usage: dho sync teams --provider jira --org <org-id> [--structure] [--members] [--projects]\n\n" +
		"Syncs the organization's Atlassian Teams into ClickHouse. With none of --structure,\n" +
		"--members and --projects, all three are synced. Members and project links a team no longer has are\n" +
		"retracted (closed); an empty result is refused, so a permissions problem retracts nothing, unless --allow-empty.\n\n" +
		"environment (names only):\n" +
		"  CLICKHOUSE_URI (or _FILE)             ClickHouse DSN\n" +
		"  ATLASSIAN_ORGANIZATION_ID             the Atlassian organization id\n" +
		"  ATLASSIAN_CLOUD_ID                    the Atlassian cloud (site) id; else the subdomain of the Jira base URL\n" +
		"  ATLASSIAN_EMAIL, ATLASSIAN_API_TOKEN  gateway credentials (fallback JIRA_EMAIL, JIRA_API_TOKEN; _FILE accepted)\n" +
		"  ATLASSIAN_JIRA_BASE_URL               the tenant URL (fallback JIRA_BASE_URL)\n"
)

// Command is the `sync` group.
func Command() cli.Command {
	return cli.Command{
		Name:    "sync",
		Summary: "pull an external system's structure into ClickHouse",
		Kind:    cli.Group,
		Children: []cli.Command{{
			Name:    "teams",
			Summary: "sync the organization's Atlassian Teams (structure, members, active projects)",
			Kind:    cli.Verb,
			Run:     func(ctx context.Context, env cli.Env) int { return runTeams(ctx, env, defaultDeps()) },
		}},
	}
}

// deps are the verb's two outside connections, replaceable in tests.
type deps struct {
	newClient func(gatewayURL string, auth atlassian.AuthProvider) atlassianteams.Client
	openStore func(ctx context.Context, dsn string) (driver.Conn, error)
	now       func() time.Time
}

func defaultDeps() deps {
	return deps{
		newClient: func(gatewayURL string, auth atlassian.AuthProvider) atlassianteams.Client {
			// Strict: GraphQL errors next to partial data fail the read (the
			// client otherwise returns the partial data). CompletePagesOnly: a
			// page that promises a next page without a cursor fails it too.
			return &graph.Client{
				BaseURL: gatewayURL, Auth: auth, Strict: true,
				HTTPClient: &http.Client{Timeout: 30 * time.Second, Transport: atlassianteams.CompletePagesOnly(nil)},
			}
		},
		openStore: func(ctx context.Context, dsn string) (driver.Conn, error) {
			return clickhousestore.Open(ctx, clickhousestore.DefaultConfig(dsn))
		},
		now: time.Now,
	}
}

func runTeams(ctx context.Context, env cli.Env, d deps) int {
	flags := flag.NewFlagSet("dho sync teams", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	flags.Usage = func() { fmt.Fprint(env.Stderr, teamsUsage) }
	provider := flags.String("provider", "", "the team source: jira")
	org := flags.String("org", "", "the organization id the rows are written under")
	structure := flags.Bool("structure", false, "sync the teams")
	members := flags.Bool("members", false, "sync team memberships")
	projects := flags.Bool("projects", false, "sync the projects each team works on")
	allowEmpty := flags.Bool("allow-empty", false, "accept an organization with no Atlassian teams (retracts every member and link)")
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted")
		return cli.ExitUsage
	}
	if *provider != "jira" {
		fmt.Fprintln(env.Stderr, "argument error: --provider must be jira")
		return cli.ExitUsage
	}
	orgID := strings.TrimSpace(*org)
	if orgID == "" {
		fmt.Fprintln(env.Stderr, "argument error: --org is required")
		return cli.ExitUsage
	}
	selections := atlassianteams.Selections{Structure: *structure, Members: *members, Projects: *projects}
	if !selections.Structure && !selections.Members && !selections.Projects {
		selections = atlassianteams.Selections{Structure: true, Members: true, Projects: true}
	}
	if env.Lookup == nil {
		env.Lookup = func(string) (string, bool) { return "", false }
	}

	settings, err := readSettings(env.Lookup)
	if err != nil {
		return writeError(env.Stderr, cli.ExitRefused, "configuration", err.Error())
	}
	dsn, configured, err := platformconfig.ResolveDSN(env.Lookup, ClickHouseURIKey, platformconfig.ClickHouseSpec)
	if err != nil || !configured {
		detail := ClickHouseURIKey + " is not set"
		if err != nil {
			detail = err.Error()
		}
		return writeError(env.Stderr, cli.ExitRefused, "configuration", detail)
	}
	boundary := secrets.NewBoundary(dsn.Reveal())
	redact := func(err error) string { return boundary.Redact(settings.redact(err)).Error() }

	conn, err := d.openStore(ctx, dsn.Reveal())
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "clickhouse_unavailable", redact(err))
	}
	defer func() { _ = conn.Close() }()

	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	started := time.Now()
	client := d.newClient(settings.gatewayURL, atlassian.BasicAPITokenAuth{Email: settings.email, Token: settings.token})
	rows, err := atlassianteams.Collect(ctx, client, atlassianteams.Params{
		OrgID: orgID, OrganizationID: settings.organizationID, SiteID: settings.cloudID,
		Selections: selections, Now: d.now(),
	})
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "read_failed", redact(err))
	}
	// An empty answer is far more often a permissions or configuration problem
	// than an organization with no teams, and writing it would retract every
	// member and link: refuse it unless the operator says it is real.
	if len(rows.Teams) == 0 && !*allowEmpty {
		return writeError(env.Stderr, cli.ExitFailure, "empty_result",
			"the gateway returned no Atlassian teams; nothing was written. Check the organization id, the cloud id and the credentials, or pass --allow-empty if the organization really has no teams")
	}
	result, err := atlassianteams.Write(ctx, conn, orgID, rows, selections)
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "write_failed", redact(err))
	}
	logger.Info("atlassian teams synced", "org_id", orgID, "teams", len(rows.Teams), "memberships", len(rows.Memberships),
		"project_links", len(rows.Ownership), "skipped_project_links", rows.SkippedProjects,
		"expired_memberships", result.ExpiredMemberships, "expired_project_links", result.ExpiredOwnership, "deactivated_teams", result.DeactivatedTeams, "duration_ms", time.Since(started).Milliseconds())
	if _, err := fmt.Fprintf(env.Stdout, "teams=%d memberships=%d project_links=%d expired_memberships=%d expired_project_links=%d deactivated_teams=%d\n",
		len(rows.Teams), len(rows.Memberships), len(rows.Ownership), result.ExpiredMemberships, result.ExpiredOwnership, result.DeactivatedTeams); err != nil {
		return cli.ExitFailure
	}
	return cli.ExitOK
}

// settings are the Atlassian inputs of one run.
type settings struct {
	organizationID string
	cloudID        string
	email          string
	token          string
	gatewayURL     string
}

// redact removes the credential from an error text: the gateway can echo a
// rejected request.
func (s settings) redact(err error) error {
	return secrets.NewBoundary(s.token).Redact(err)
}

func readSettings(lookup secrets.LookupEnv) (settings, error) {
	first := func(keys ...string) (string, error) {
		for _, key := range keys {
			value, ok, err := secrets.Resolve(key, lookup)
			if err != nil {
				return "", err
			}
			if ok && strings.TrimSpace(value.Reveal()) != "" {
				return strings.TrimSpace(value.Reveal()), nil
			}
		}
		return "", nil
	}
	var s settings
	var err error
	if s.organizationID, err = first(OrganizationIDKey); err != nil {
		return settings{}, err
	}
	if s.email, err = first(emailKey, legacyEmailKey); err != nil {
		return settings{}, err
	}
	if s.token, err = first(apiTokenKey, legacyAPITokenKey); err != nil {
		return settings{}, err
	}
	base, err := first(baseURLKey, legacyBaseURLKey)
	if err != nil {
		return settings{}, err
	}
	if s.cloudID, err = first(CloudIDKey); err != nil {
		return settings{}, err
	}
	var missing []string
	if s.organizationID == "" {
		missing = append(missing, OrganizationIDKey)
	}
	if s.email == "" {
		missing = append(missing, emailKey)
	}
	if s.token == "" {
		missing = append(missing, apiTokenKey)
	}
	if base == "" {
		missing = append(missing, baseURLKey)
	}
	if len(missing) > 0 {
		return settings{}, fmt.Errorf("required settings are not set: %s", strings.Join(missing, ", "))
	}
	tenant, err := normalizeBase(base)
	if err != nil {
		return settings{}, fmt.Errorf("%s is not a valid URL", baseURLKey)
	}
	s.gatewayURL = tenant.String() + gatewayPath
	if s.cloudID == "" {
		// The Python integration derives the site from the tenant subdomain.
		host := tenant.Hostname()
		if i := strings.Index(host, "."); i > 0 {
			s.cloudID = host[:i]
		}
	}
	if s.cloudID == "" {
		return settings{}, fmt.Errorf("%s is not set and the site cannot be derived from %s", CloudIDKey, baseURLKey)
	}
	return s, nil
}

// normalizeBase is the Python compat layer's URL normalization: https, no
// trailing slash.
func normalizeBase(value string) (*url.URL, error) {
	value = strings.TrimRight(strings.TrimSpace(value), "/")
	switch {
	case strings.HasPrefix(value, "http://"):
		value = "https://" + strings.TrimPrefix(value, "http://")
	case !strings.HasPrefix(value, "https://"):
		value = "https://" + strings.TrimLeft(value, "/")
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return nil, errors.New("invalid url")
	}
	return &url.URL{Scheme: parsed.Scheme, Host: parsed.Host}, nil
}

func writeError(stderr io.Writer, exit int, code, detail string) int {
	fmt.Fprintf(stderr, "{\"error\":{\"code\":%q,\"detail\":%q}}\n", code, detail)
	return exit
}
