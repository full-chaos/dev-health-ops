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
	"github.com/jackc/pgx/v5/pgxpool"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// Environment variable NAMES the verb reads; their values are never logged.
// For --provider jira, these are an OPTIONAL OVERRIDE at most (D2770): the
// canonical path resolves the org's stored jira integration credential --
// the same one work-items sync and the worker's post-sync team_autoimport
// job already use -- from Postgres (POSTGRES_URI). Every ATLASSIAN_* var
// below is set only to override one field of that resolution (or, when ALL
// of ATLASSIAN_EMAIL/API_TOKEN/JIRA_BASE_URL are set, to run fully
// env-configured for an offline/test path with no stored credential at all).
const (
	ClickHouseURIKey  = "CLICKHOUSE_URI"
	PostgresURIKey    = "POSTGRES_URI"
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
		"--provider github|gitlab --org <org-id> --owner <org-or-group> [--auth <token>] runs that\n" +
		"provider's own team catalog instead of Atlassian Teams; see docs/reference/cli/index.md for\n" +
		"its exact flags, refusals and env vars, which differ per provider.\n\n" +
		"--provider jira resolves the org's stored jira integration credential from Postgres\n" +
		"(--db, else POSTGRES_URI or _FILE) by default -- the atlassian_organization_id (required) and\n" +
		"atlassian_cloud_id (optional; else derived live from the tenant) come from that same\n" +
		"integration's config. The environment below overrides individual fields, or (when\n" +
		"ATLASSIAN_EMAIL, ATLASSIAN_API_TOKEN and ATLASSIAN_JIRA_BASE_URL are ALL set) replaces\n" +
		"the stored credential entirely, for an offline/test path with no database at all:\n\n" +
		"environment (names only):\n" +
		"  CLICKHOUSE_URI (or _FILE)             ClickHouse DSN\n" +
		"  POSTGRES_URI (or _FILE)               the domain database the stored jira credential is read from\n" +
		"  ATLASSIAN_ORGANIZATION_ID             overrides the stored atlassian_organization_id\n" +
		"  ATLASSIAN_CLOUD_ID                    overrides the stored/derived atlassian cloud (site) id\n" +
		"  ATLASSIAN_EMAIL, ATLASSIAN_API_TOKEN  gateway credentials (fallback JIRA_EMAIL, JIRA_API_TOKEN; _FILE accepted)\n" +
		"  ATLASSIAN_JIRA_BASE_URL               the tenant URL (fallback JIRA_BASE_URL)\n"
)

// Command is the `sync` group.
func Command() cli.Command {
	return cli.Command{
		Name:    "sync",
		Summary: "pull an external system's structure into ClickHouse",
		Kind:    cli.Group,
		Children: append([]cli.Command{{
			Name:    "teams",
			Summary: "sync the organization's Atlassian Teams (structure, members, active projects)",
			Kind:    cli.Verb,
			Run:     func(ctx context.Context, env cli.Env) int { return runTeams(ctx, env, defaultDeps()) },
		}}, TargetCommands(InlineExecutor(InlineDeps{}))...),
	}
}

// deps are the verb's outside connections, replaceable in tests.
type deps struct {
	newClient func(gatewayURL string, auth atlassian.AuthProvider) atlassianteams.Client
	openStore func(ctx context.Context, dsn string) (driver.Conn, error)
	now       func() time.Time
	// doer is the HTTP client of a catalog provider, and (for --provider
	// jira) the stored-credential resolution's jira/tenant-info client (nil:
	// a 45 s client); tests replace it.
	doer providerfoundation.HTTPDoer
	// openPostgres opens the domain database the stored jira credential is
	// resolved from; tests replace it (no live Postgres in unit tests).
	openPostgres func(ctx context.Context, dsn string) (*pgxpool.Pool, error)
	// decryptor decrypts the resolved credential's ciphertext
	// (SETTINGS_ENCRYPTION_KEY); tests replace it.
	decryptor func(env cli.Env) (providerfoundation.CredentialDecryptor, error)
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
		now:          time.Now,
		openPostgres: openPostgresPool,
		decryptor: func(env cli.Env) (providerfoundation.CredentialDecryptor, error) {
			decryptor, err := settingsDecryptor(env)
			if err != nil {
				return nil, err
			}
			return decryptor, nil
		},
	}
}

func runTeams(ctx context.Context, env cli.Env, d deps) int {
	flags := flag.NewFlagSet("dho sync teams", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	flags.Usage = func() { fmt.Fprint(env.Stderr, teamsUsage) }
	provider := flags.String("provider", "", "the team source: jira, github, gitlab or linear")
	owner := flags.String("owner", "", "the GitHub organization (github) or GitLab group path (gitlab); not used for linear")
	auth := flags.String("auth", "", "the provider token (github/gitlab/linear; else GITHUB_TOKEN/GITLAB_TOKEN/LINEAR_API_KEY)")
	org := flags.String("org", "", "the organization id the rows are written under")
	db := flags.String("db", "", "the domain database DSN the stored jira credential is resolved from (jira; else "+PostgresURIKey+")")
	structure := flags.Bool("structure", false, "sync the teams")
	members := flags.Bool("members", false, "sync team memberships")
	projects := flags.Bool("projects", false, "sync the projects each team works on")
	allowEmpty := flags.Bool("allow-empty", false, "accept an empty result (jira: retracts every member and link; github/gitlab/linear: exit 0 instead of refusing)")
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
	if *provider != "jira" && !isCatalogProvider(*provider) {
		fmt.Fprintln(env.Stderr, "argument error: --provider must be jira, github, gitlab or linear")
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

	if isCatalogProvider(*provider) {
		dsn, configured, err := platformconfig.ResolveDSN(env.Lookup, ClickHouseURIKey, platformconfig.ClickHouseSpec)
		if err != nil || !configured {
			detail := ClickHouseURIKey + " is not set"
			if err != nil {
				detail = err.Error()
			}
			return writeError(env.Stderr, cli.ExitRefused, "configuration", detail)
		}
		return runCatalogTeams(ctx, env, d, catalogRequest{
			provider: *provider, orgID: orgID, owner: *owner, auth: *auth, allowEmpty: *allowEmpty,
			structure: selections.Structure, members: selections.Members, projects: selections.Projects, dsn: dsn.Reveal(),
		})
	}
	settings, err := resolveTeamsSettings(ctx, env, d, orgID, *db)
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

// envOverrides is what the ATLASSIAN_* environment carries for one run. Every
// field is "" when unset -- unlike the old readSettings, nothing here is
// required: resolveTeamsSettings decides what "complete" means.
type envOverrides struct {
	organizationID, cloudID, email, token, base string
}

func readEnvOverrides(lookup secrets.LookupEnv) (envOverrides, error) {
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
	var o envOverrides
	var err error
	if o.organizationID, err = first(OrganizationIDKey); err != nil {
		return envOverrides{}, err
	}
	if o.cloudID, err = first(CloudIDKey); err != nil {
		return envOverrides{}, err
	}
	if o.email, err = first(emailKey, legacyEmailKey); err != nil {
		return envOverrides{}, err
	}
	if o.token, err = first(apiTokenKey, legacyAPITokenKey); err != nil {
		return envOverrides{}, err
	}
	if o.base, err = first(baseURLKey, legacyBaseURLKey); err != nil {
		return envOverrides{}, err
	}
	return o, nil
}

// fullyConfigured reports whether the environment alone can run the verb
// with no stored credential at all -- the offline/test path D2770 keeps.
func (o envOverrides) fullyConfigured() bool {
	return o.email != "" && o.token != "" && o.base != ""
}

// settingsFromEnv is the OLD readSettings' env-derivation, kept verbatim for
// the fully-env-configured path: https-normalize the base URL, and derive
// the cloud id from the tenant subdomain when not given explicitly (the
// Python-integration-compatible approximation this path has always used).
func settingsFromEnv(o envOverrides) (settings, error) {
	tenant, err := normalizeBase(o.base)
	if err != nil {
		return settings{}, fmt.Errorf("%s is not a valid URL", baseURLKey)
	}
	s := settings{organizationID: o.organizationID, cloudID: o.cloudID, email: o.email, token: o.token, gatewayURL: tenant.String() + gatewayPath}
	if s.cloudID == "" {
		host := tenant.Hostname()
		if i := strings.Index(host, "."); i > 0 {
			s.cloudID = host[:i]
		}
	}
	if s.cloudID == "" {
		return settings{}, fmt.Errorf("%s is not set and the site cannot be derived from %s", CloudIDKey, baseURLKey)
	}
	if s.organizationID == "" {
		return settings{}, fmt.Errorf("%s is not set", OrganizationIDKey)
	}
	return s, nil
}

// resolveTeamsSettings is the D2770 entry point: the stored jira integration
// credential is canonical; the full ATLASSIAN_EMAIL/API_TOKEN/JIRA_BASE_URL
// triple is an offline/test escape hatch with no database at all; either
// way, ATLASSIAN_ORGANIZATION_ID/ATLASSIAN_CLOUD_ID individually override
// whatever was resolved.
func resolveTeamsSettings(ctx context.Context, env cli.Env, d deps, orgID string, dbFlag string) (settings, error) {
	overrides, err := readEnvOverrides(env.Lookup)
	if err != nil {
		return settings{}, err
	}
	var s settings
	if overrides.fullyConfigured() {
		s, err = settingsFromEnv(overrides)
		if err != nil {
			return settings{}, err
		}
	} else {
		if d.openPostgres == nil || d.decryptor == nil {
			return settings{}, errors.New("no stored-credential resolution is configured")
		}
		// --db, like every sibling sync verb's own --db (internal/synccli/
		// target.go's dbValue), wins over POSTGRES_URI when given.
		pgDSNValue := strings.TrimSpace(dbFlag)
		if pgDSNValue == "" {
			pgDSN, configured, err := platformconfig.ResolveDSN(env.Lookup, PostgresURIKey, platformconfig.DomainDatabaseSpec)
			if err != nil || !configured {
				detail := PostgresURIKey + " is not set"
				if err != nil {
					detail = err.Error()
				}
				return settings{}, fmt.Errorf("%s (needed to resolve the stored jira credential; alternatively set %s, %s and %s together)", detail, emailKey, apiTokenKey, baseURLKey)
			}
			pgDSNValue = pgDSN.Reveal()
		}
		pool, err := d.openPostgres(ctx, pgDSNValue)
		if err != nil {
			// openPostgresPool (the default d.openPostgres) already redacts via
			// pgstorage.Boundary before returning; wrap without redacting again.
			return settings{}, fmt.Errorf("open postgres: %w", err)
		}
		defer pool.Close()
		decryptor, err := d.decryptor(env)
		if err != nil {
			return settings{}, err
		}
		s, err = resolveJiraStoredSettings(ctx, pool, decryptor, d.doer, orgID)
		if err != nil {
			return settings{}, err
		}
	}
	if overrides.organizationID != "" {
		s.organizationID = overrides.organizationID
	}
	if overrides.cloudID != "" {
		s.cloudID = overrides.cloudID
	}
	// Every ATLASSIAN_* field is documented as an INDIVIDUAL override on top
	// of whichever settings source resolved (stored credential or the fully-
	// env-configured offline path), not only the org/cloud id pair above --
	// e.g. ATLASSIAN_API_TOKEN alone must override just the token, matching
	// the stored credential's own email/base URL.
	if overrides.email != "" {
		s.email = overrides.email
	}
	if overrides.token != "" {
		s.token = overrides.token
	}
	if overrides.base != "" {
		tenant, err := normalizeBase(overrides.base)
		if err != nil {
			return settings{}, fmt.Errorf("%s is not a valid URL", baseURLKey)
		}
		s.gatewayURL = tenant.String() + gatewayPath
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
