package adminops

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/entitlements"
)

// `admin org entitlements list|grant|revoke` manage the per-organization feature
// overrides through internal/entitlements, the code the admin API also calls.
// Each action prints one JSON object per line (list: one per override, none when
// the organization has none); a message that is not JSON goes to stderr.

func orgGroup() cli.Command {
	return cli.Command{
		Name: "org", Summary: "single-organization administration", Kind: cli.Group,
		Children: []cli.Command{{
			Name: "entitlements", Summary: "per-organization feature entitlements (overrides)", Kind: cli.Group,
			Children: []cli.Command{
				{Name: "list", Summary: "list the feature overrides of an organization", Kind: cli.Verb, Run: runEntitlementsList},
				{Name: "grant", Summary: "grant an organization a feature (same as bundles assign-org)", Kind: cli.Verb, Run: runEntitlementsGrant},
				{Name: "revoke", Summary: "remove an organization's override of a feature (idempotent)", Kind: cli.Verb, Run: runEntitlementsRevoke},
			},
		}},
	}
}

func printJSONLine(env cli.Env, value map[string]any) int {
	line, err := json.Marshal(value)
	if err != nil {
		fmt.Fprintln(env.Stderr, "could not encode the result")
		return cli.ExitFailure
	}
	fmt.Fprintf(env.Stdout, "%s\n", line)
	return cli.ExitOK
}

func parseOrgID(env cli.Env, text string) (uuid.UUID, bool) {
	org, err := parseDriverUUID(text)
	if err != nil {
		fmt.Fprintf(env.Stderr, "argument error: --org-id: %v\n", err)
		return uuid.Nil, false
	}
	return org, true
}

func runEntitlementsList(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin org entitlements list")
	var orgID optString
	flags.Var(&orgID, "org-id", "organization id, a UUID (required)")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	if !orgID.set {
		fmt.Fprintln(env.Stderr, "argument error: --org-id is required")
		return cli.ExitUsage
	}
	org, ok := parseOrgID(env, orgID.value)
	if !ok {
		return cli.ExitUsage
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	list, err := entitlements.List(ctx, op.Pool, org)
	if err != nil {
		return databaseFailure(env, err)
	}
	for _, o := range list {
		line := map[string]any{
			"action": "list", "org_id": o.OrgID.String(), "feature_key": o.FeatureKey, "is_enabled": o.IsEnabled,
			"expires_at": nil, "reason": o.Reason,
		}
		if o.ExpiresAt != nil {
			line["expires_at"] = isoUTC(*o.ExpiresAt)
		}
		if code := printJSONLine(env, line); code != cli.ExitOK {
			return code
		}
	}
	return cli.ExitOK
}

func runEntitlementsGrant(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin org entitlements grant")
	var orgID, featureKey, reason, expiresAt optString
	flags.Var(&orgID, "org-id", "organization id, a UUID (required)")
	flags.Var(&featureKey, "feature-key", "feature flag key (required)")
	flags.Var(&reason, "reason", "why the entitlement exists")
	flags.Var(&expiresAt, "expires-at", "when the entitlement ends, an RFC 3339 time such as 2026-12-31T00:00:00Z")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	if !orgID.set || !featureKey.set {
		fmt.Fprintln(env.Stderr, "argument error: --org-id and --feature-key are required")
		return cli.ExitUsage
	}
	org, ok := parseOrgID(env, orgID.value)
	if !ok {
		return cli.ExitUsage
	}
	var expires *time.Time
	if expiresAt.set {
		moment, err := time.Parse(time.RFC3339, expiresAt.value)
		if err != nil {
			fmt.Fprintln(env.Stderr, "argument error: --expires-at must be an RFC 3339 time")
			return cli.ExitUsage
		}
		moment = moment.UTC()
		expires = &moment
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	featureID, found, err := entitlements.FeatureIDByKey(ctx, op.Pool, featureKey.value)
	if err != nil {
		return databaseFailure(env, err)
	}
	if !found {
		fmt.Fprintf(env.Stdout, "Error: Feature flag '%s' not found\n", featureKey.value)
		return cli.ExitFailure
	}
	if err := entitlements.Grant(ctx, op.Pool, entitlements.GrantParams{
		ID: uuid.New(), OrgID: org, FeatureID: featureID, IsEnabled: true, ExpiresAt: expires, Reason: reason.ptr(), Now: time.Now(),
	}); err != nil {
		return databaseFailure(env, err)
	}
	line := map[string]any{"action": "grant", "org_id": org.String(), "feature_key": featureKey.value, "expires_at": nil}
	if expires != nil {
		line["expires_at"] = isoUTC(*expires)
	}
	return printJSONLine(env, line)
}

func runEntitlementsRevoke(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin org entitlements revoke")
	var orgID, featureKey optString
	flags.Var(&orgID, "org-id", "organization id, a UUID (required)")
	flags.Var(&featureKey, "feature-key", "feature flag key (required)")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	if !orgID.set || !featureKey.set || featureKey.value == "" {
		fmt.Fprintln(env.Stderr, "argument error: --org-id and --feature-key are required")
		return cli.ExitUsage
	}
	org, ok := parseOrgID(env, orgID.value)
	if !ok {
		return cli.ExitUsage
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	removed, err := entitlements.Revoke(ctx, op.Pool, org, nil, featureKey.value)
	if err != nil {
		return databaseFailure(env, err)
	}
	result := "not_present"
	if removed > 0 {
		result = "removed"
	}
	return printJSONLine(env, map[string]any{"action": "revoke", "org_id": org.String(), "feature_key": featureKey.value, "result": result})
}
