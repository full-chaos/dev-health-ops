package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	platformOwnerGrantVerb  = "platform-owner-grant"
	platformOwnerRevokeVerb = "platform-owner-revoke"
)

func platformOwnerUsage(verb string) string {
	return "usage: auth-migrate " + verb + " -principal <uuid> -confirm"
}

// parsePlatformOwnerArgs refuses a bad command line before any environment or
// database is read. -confirm is required: both verbs write.
func parsePlatformOwnerArgs(verb string, args []string, stderr io.Writer) (uuid.UUID, error) {
	flags := flag.NewFlagSet(serviceName+" "+verb, flag.ContinueOnError)
	flags.SetOutput(stderr)
	principal := flags.String("principal", "", "the user principal whose platform_owner role changes")
	confirm := flags.Bool("confirm", false, "required: the verb writes the platform_owner assignment")
	if err := flags.Parse(args); err != nil {
		return uuid.Nil, err
	}
	if flags.NArg() != 0 {
		return uuid.Nil, errors.New("positional arguments are not accepted")
	}
	id, err := uuid.Parse(*principal)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, errors.New("-principal must be a principal uuid")
	}
	if !*confirm {
		return uuid.Nil, errors.New("-confirm is required")
	}
	return id, nil
}

// executePlatformOwnerGrant runs `auth-migrate platform-owner-grant`: the only
// grant path of platform_owner.
func executePlatformOwnerGrant(parent context.Context, args []string, lookup secrets.LookupEnv, stdout, stderr io.Writer) int {
	return executePlatformOwner(parent, platformOwnerGrantVerb, args, lookup, stdout, stderr,
		func(ctx context.Context, store *platformrole.Store, principal uuid.UUID) (string, error) {
			id, err := store.GrantOwner(ctx, principal)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("platform_owner_granted assignment=%s principal=%s", id, principal), nil
		})
}

// executePlatformOwnerRevoke runs `auth-migrate platform-owner-revoke`: the only
// revoke path of platform_owner. It has no last-owner guard.
func executePlatformOwnerRevoke(parent context.Context, args []string, lookup secrets.LookupEnv, stdout, stderr io.Writer) int {
	return executePlatformOwner(parent, platformOwnerRevokeVerb, args, lookup, stdout, stderr,
		func(ctx context.Context, store *platformrole.Store, principal uuid.UUID) (string, error) {
			if err := store.RevokeOwner(ctx, principal); err != nil {
				return "", err
			}
			return fmt.Sprintf("platform_owner_revoked principal=%s", principal), nil
		})
}

// executePlatformOwner connects as the migration role, refuses to run as the
// runtime role, runs one owner verb and prints ids only. Exit 0 done, 1 a
// configuration or store failure, 2 a bad command line, 3 a refusal.
func executePlatformOwner(parent context.Context, verb string, args []string, lookup secrets.LookupEnv, stdout, stderr io.Writer,
	run func(context.Context, *platformrole.Store, uuid.UUID) (string, error)) int {
	principal, err := parsePlatformOwnerArgs(verb, args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "argument error: %v\n%s\n", err, platformOwnerUsage(verb))
		return 2
	}
	ctx, store, cleanup, code := openPlatformRoleStoreAsMigrationRole(parent, lookup, stderr, verb, "the owner verb")
	if code != 0 {
		return code
	}
	defer cleanup()
	line, err := run(ctx, store, principal)
	if err != nil {
		if errors.Is(err, platformrole.ErrStore) {
			fmt.Fprintf(stderr, "%s error: the store failed; nothing was written\n", verb)
			return 1
		}
		fmt.Fprintf(stderr, "%s refused: %v\n", verb, err)
		return 3
	}
	fmt.Fprintln(stdout, line)
	return 0
}
