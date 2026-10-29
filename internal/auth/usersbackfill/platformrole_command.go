package usersbackfill

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func platformRoleVerbs() []cli.Command {
	return []cli.Command{{
		Name:    "platform-role-grant",
		Kind:    cli.Verb,
		Summary: "grant a platform role to a user principal (refused unless AUTH_PLATFORM_ROLES_OWNED is true; the granter must hold it now)",
		Run:     runPlatformRoleGrant,
	}, {
		Name:    "platform-role-revoke",
		Kind:    cli.Verb,
		Summary: "revoke a platform role (refused unless AUTH_PLATFORM_ROLES_OWNED is true; never the last active platform_admin)",
		Run:     runPlatformRoleRevoke,
	}, {
		Name:    "platform-role-list",
		Kind:    cli.Verb,
		Summary: "list platform role assignments and whether each is active now (read-only; ids only)",
		Run:     runPlatformRoleList,
	}}
}

// platformRoleOwnedGate refuses a write before any database access while
// public users.is_superuser is still the authority.
func platformRoleOwnedGate(env cli.Env, verb string) int {
	owned, err := platformrole.OwnedFromLookup(env.Lookup)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %s must be true or false\n", verb, platformrole.OwnedEnv)
		return cli.ExitFailure
	}
	if !owned {
		fmt.Fprintf(env.Stderr, "%s: refused: %s is not true, public users.is_superuser is still the platform authority\n", verb, platformrole.OwnedEnv)
		return cli.ExitRefused
	}
	return cli.ExitOK
}

func parseUUIDFlag(env cli.Env, name, value string) (uuid.UUID, bool) {
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil {
		fmt.Fprintf(env.Stderr, "argument error: -%s must be a principal uuid\n", name)
		return uuid.Nil, false
	}
	return id, true
}

func openPlatformRoles(ctx context.Context, env cli.Env, verb string) (*platformrole.Store, func(), int) {
	pool, schema, code := connectFn(ctx, env, verb)
	if code != cli.ExitOK {
		return nil, func() {}, code
	}
	store, err := platformrole.New(platformrole.Config{Pool: pool, Schema: schema, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		pool.Close()
		fmt.Fprintf(env.Stderr, "%s: could not build the platform role store\n", verb)
		return nil, func() {}, cli.ExitFailure
	}
	return store, pool.Close, cli.ExitOK
}

// platformRoleResult prints a refusal by name (exit 3) or a store failure
// (exit 1).
func platformRoleResult(env cli.Env, verb string, err error) int {
	if errors.Is(err, platformrole.ErrStore) {
		fmt.Fprintf(env.Stderr, "%s: the store failed; nothing was changed\n", verb)
		return cli.ExitFailure
	}
	fmt.Fprintf(env.Stderr, "%s: refused: %v\n", verb, err)
	return cli.ExitRefused
}

func runPlatformRoleGrant(ctx context.Context, env cli.Env) int {
	const verb = "platform-role-grant"
	flags := flag.NewFlagSet("dho auth "+verb, flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	principal := flags.String("principal", "", "principal uuid that receives the role")
	role := flags.String("role", "", "platform role of the catalog (platform_admin)")
	grantedBy := flags.String("granted-by", "", "principal uuid of the granter; it must hold the role's grant action now")
	expiresAt := flags.String("expires-at", "", "optional RFC 3339 expiry, in the future")
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if flags.NArg() != 0 || *role == "" {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted and -role is required")
		return cli.ExitUsage
	}
	target, ok := parseUUIDFlag(env, "principal", *principal)
	if !ok {
		return cli.ExitUsage
	}
	actor, ok := parseUUIDFlag(env, "granted-by", *grantedBy)
	if !ok {
		return cli.ExitUsage
	}
	in := platformrole.GrantInput{Actor: actor, Principal: target, Role: *role}
	if *expiresAt != "" {
		at, err := time.Parse(time.RFC3339, *expiresAt)
		if err != nil {
			fmt.Fprintln(env.Stderr, "argument error: -expires-at must be RFC 3339")
			return cli.ExitUsage
		}
		in.ExpiresAt = &at
	}
	if code := platformRoleOwnedGate(env, verb); code != cli.ExitOK {
		return code
	}
	store, closePool, code := openPlatformRoles(ctx, env, verb)
	defer closePool()
	if code != cli.ExitOK {
		return code
	}
	id, err := store.Grant(ctx, in)
	if err != nil {
		return platformRoleResult(env, verb, err)
	}
	fmt.Fprintf(env.Stdout, "platform_role_granted assignment=%s principal=%s role=%s\n", id, target, *role)
	return cli.ExitOK
}

func runPlatformRoleRevoke(ctx context.Context, env cli.Env) int {
	const verb = "platform-role-revoke"
	flags := flag.NewFlagSet("dho auth "+verb, flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	principal := flags.String("principal", "", "principal uuid that holds the role")
	role := flags.String("role", "", "platform role of the catalog (platform_admin)")
	revokedBy := flags.String("revoked-by", "", "principal uuid of the revoker; it must hold the role's grant action now")
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if flags.NArg() != 0 || *role == "" {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted and -role is required")
		return cli.ExitUsage
	}
	target, ok := parseUUIDFlag(env, "principal", *principal)
	if !ok {
		return cli.ExitUsage
	}
	actor, ok := parseUUIDFlag(env, "revoked-by", *revokedBy)
	if !ok {
		return cli.ExitUsage
	}
	if code := platformRoleOwnedGate(env, verb); code != cli.ExitOK {
		return code
	}
	store, closePool, code := openPlatformRoles(ctx, env, verb)
	defer closePool()
	if code != cli.ExitOK {
		return code
	}
	if err := store.Revoke(ctx, platformrole.RevokeInput{Actor: actor, Principal: target, Role: *role}); err != nil {
		return platformRoleResult(env, verb, err)
	}
	fmt.Fprintf(env.Stdout, "platform_role_revoked principal=%s role=%s\n", target, *role)
	return cli.ExitOK
}

func runPlatformRoleList(ctx context.Context, env cli.Env) int {
	const verb = "platform-role-list"
	flags := flag.NewFlagSet("dho auth "+verb, flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
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
	store, closePool, code := openPlatformRoles(ctx, env, verb)
	defer closePool()
	if code != cli.ExitOK {
		return code
	}
	assignments, err := store.List(ctx)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: reading the assignments failed\n", verb)
		return cli.ExitFailure
	}
	for _, a := range assignments {
		grantedBy, expires := "none", "none"
		if a.GrantedBy != nil {
			grantedBy = a.GrantedBy.String()
		}
		if a.ExpiresAt != nil {
			expires = a.ExpiresAt.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(env.Stdout, "assignment=%s principal=%s role=%s granted_by=%s expires_at=%s active=%t\n",
			a.ID, a.PrincipalID, a.Role, grantedBy, expires, a.Active)
	}
	fmt.Fprintf(env.Stdout, "platform_role_assignments=%d\n", len(assignments))
	return cli.ExitOK
}
