package adminops

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/artifactexclude"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// `admin sync-configs artifact-excludes set|show` write and read the ci_artifact_exclude_prefixes list of a GitHub sync
// configuration's integration with artifactexclude.Parse and artifactexclude.WriteTx, the validator and writer of
// PATCH /api/v1/admin/sync-configs/{id}. The list applies to every sync config of this integration, and a write
// replaces it whole. The organization is the configuration's own; ids are printed as 8-character prefixes only. Unlike
// the PATCH, the verb does not snapshot the GitHub work-item options from its environment: it changes the list only.
func syncConfigsGroup() cli.Command {
	return cli.Command{
		Name: "sync-configs", Summary: "sync configuration settings", Kind: cli.Group,
		Children: []cli.Command{{
			Name: "artifact-excludes", Summary: "CI artifact name prefixes the GitHub tests sync skips (applies to every sync config of this integration)", Kind: cli.Group,
			Children: []cli.Command{
				{Name: "set", Summary: "replace or clear the artifact name prefixes of a sync configuration's integration (applies to every sync config of this integration)", Kind: cli.Verb, Run: runArtifactExcludesSet},
				{Name: "show", Summary: "show the artifact name prefixes of a sync configuration's integration (applies to every sync config of this integration)", Kind: cli.Verb, Run: runArtifactExcludesShow},
			},
		}},
	}
}

type repeatedString []string

func (r *repeatedString) String() string { return strings.Join(*r, ",") }
func (r *repeatedString) Set(v string) error {
	*r = append(*r, v)
	return nil
}

func parseConfigID(env cli.Env, typed optString) (uuid.UUID, bool) {
	if !typed.set {
		fmt.Fprintln(env.Stderr, "argument error: --config-id is required")
		return uuid.UUID{}, false
	}
	id, err := uuid.Parse(typed.value)
	if err != nil {
		fmt.Fprintln(env.Stderr, "argument error: --config-id must be a UUID")
		return uuid.UUID{}, false
	}
	return id, true
}

func runArtifactExcludesSet(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin sync-configs artifact-excludes set")
	var configID, reason optString
	var prefixes repeatedString
	flags.Var(&configID, "config-id", "sync configuration id, a UUID (required)")
	flags.Var(&prefixes, "prefix", "an artifact name prefix; repeat for each (or --clear). The list replaces the integration's list whole and applies to every sync config of this integration")
	clearAll := flags.Bool("clear", false, "remove the integration's prefixes (from every sync config of this integration)")
	dryRun := flags.Bool("dry-run", false, "validate and report what would change, then roll back")
	flags.Var(&reason, "reason", "why the prefixes change (required, printed with the result as audit evidence)")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	id, ok := parseConfigID(env, configID)
	if !ok {
		return cli.ExitUsage
	}
	if (len(prefixes) > 0) == *clearAll {
		fmt.Fprintln(env.Stderr, "argument error: give --prefix (one or more) or --clear")
		return cli.ExitUsage
	}
	if !reason.set || strings.TrimSpace(reason.value) == "" {
		fmt.Fprintln(env.Stderr, "argument error: --reason is required")
		return cli.ExitUsage
	}
	var value pyjson.Value
	if !*clearAll {
		items := make([]pyjson.Value, len(prefixes))
		for index, prefix := range prefixes {
			items[index] = prefix
		}
		value = items
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	tx, err := op.Pool.Begin(ctx)
	if err != nil {
		return databaseFailure(env, err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	result, err := setArtifactExcludesTx(ctx, tx, id, value, op.Now())
	if err != nil {
		return artifactExcludeFailure(env, err)
	}
	if !*dryRun {
		if err := tx.Commit(ctx); err != nil {
			return databaseFailure(env, err)
		}
	}
	action := "set"
	if *clearAll {
		action = "clear"
	}
	return printJSONLine(env, map[string]any{
		"action": action, "config_id_prefix": result.ConfigIDPrefix, "org_id_prefix": result.OrgIDPrefix,
		"integration_id_prefix": result.IntegrationIDPrefix, "prefix_count": len(prefixes),
		"sync_options_changed": result.SyncOptionsChanged, "integration_config_changed": result.IntegrationConfigChanged,
		"dry_run": *dryRun, "reason": strings.TrimSpace(reason.value),
	})
}

// setArtifactExcludesTx is the verb's write: the configuration's organization and provider, Parse, then WriteTx.
func setArtifactExcludesTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, value pyjson.Value, now time.Time) (artifactexclude.Result, error) {
	var org, provider string
	err := tx.QueryRow(ctx, `SELECT org_id, provider FROM sync_configurations WHERE id = $1`, id).Scan(&org, &provider)
	if errors.Is(err, pgx.ErrNoRows) {
		return artifactexclude.Result{}, artifactexclude.ErrConfigNotFound
	}
	if err != nil {
		return artifactexclude.Result{}, err
	}
	change, err := artifactexclude.Parse(provider, value)
	if err != nil {
		return artifactexclude.Result{}, err
	}
	return artifactexclude.WriteTx(ctx, tx, org, id, change, now)
}

func artifactExcludeFailure(env cli.Env, err error) int {
	var refused *artifactexclude.Refusal
	switch {
	case errors.As(err, &refused):
		return writeError(env.Stderr, "invalid_value", refused.Message)
	case errors.Is(err, artifactexclude.ErrConfigNotFound):
		return writeError(env.Stderr, "not_found", "no sync configuration has this id")
	}
	return databaseFailure(env, err)
}

func runArtifactExcludesShow(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin sync-configs artifact-excludes show")
	var configID optString
	flags.Var(&configID, "config-id", "sync configuration id, a UUID (required)")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	id, ok := parseConfigID(env, configID)
	if !ok {
		return cli.ExitUsage
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	var org string
	var integrationID *uuid.UUID
	var options, config *string
	err := op.Pool.QueryRow(ctx, `SELECT config.org_id, config.integration_id, config.sync_options::text, integration.config::text
FROM sync_configurations AS config LEFT JOIN integrations AS integration
  ON integration.id = config.integration_id AND integration.org_id = config.org_id
WHERE config.id = $1`, id).Scan(&org, &integrationID, &options, &config)
	if errors.Is(err, pgx.ErrNoRows) {
		return artifactExcludeFailure(env, artifactexclude.ErrConfigNotFound)
	}
	if err != nil {
		return databaseFailure(env, err)
	}
	integrationPrefix := ""
	if integrationID != nil {
		integrationPrefix = integrationID.String()[:8]
	}
	return printJSONLine(env, map[string]any{
		"config_id_prefix": id.String()[:8], "org_id_prefix": prefixOf(org), "integration_id_prefix": integrationPrefix,
		"sync_options_prefixes": storedPrefixes(options), "integration_config_prefixes": storedPrefixes(config),
	})
}

func prefixOf(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// storedPrefixes is the stored list as it is (nil when the key is absent or the document is not an object).
func storedPrefixes(text *string) any {
	if text == nil {
		return nil
	}
	value, err := pyjson.DecodeString(*text)
	if err != nil {
		return nil
	}
	document, ok := value.(*pyjson.Object)
	if !ok {
		return nil
	}
	stored, _ := document.Get(artifactexclude.Key)
	return stored
}
