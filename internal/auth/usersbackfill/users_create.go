package usersbackfill

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

const usersCreateVerb = "users-create"

// maxPasswordLine bounds the password line read from stdin.
const maxPasswordLine = 4096

func usersCreateCommand() cli.Command {
	return cli.Command{
		Name:    usersCreateVerb,
		Kind:    cli.Verb,
		Summary: "create a verified local user while auth-service owns users (refused unless AUTH_USERS_OWNED is true; the password comes on stdin only; never grants a platform role; prints the principal id)",
		Run:     runUsersCreate,
	}
}

func runUsersCreate(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho auth "+usersCreateVerb, flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	email := flags.String("email", "", "login email of the new user")
	fullName := flags.String("full-name", "", "display name (optional)")
	superuser := flags.Bool("superuser", false, "refused: this verb never grants a platform role; the refusal names the next step")
	passwordStdin := flags.Bool("password-stdin", false, "read the password from the first line of stdin (required)")
	if code, ok := cli.ParseFlags(flags, env.Stderr, env.Args); !ok {
		return code
	}
	owned, err := authconfig.UsersOwnedFromLookup(env.Lookup)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: AUTH_USERS_OWNED must be true or false\n", usersCreateVerb)
		return cli.ExitFailure
	}
	if !owned {
		fmt.Fprintf(env.Stderr, "%s: refused: AUTH_USERS_OWNED is not true; use `dho admin users create`, which writes public.users while go-api owns users\n", usersCreateVerb)
		return cli.ExitRefused
	}
	if flags.NArg() != 0 || strings.TrimSpace(*email) == "" || !*passwordStdin {
		fmt.Fprintln(env.Stderr, "argument error: -email and -password-stdin are required and positional arguments are not accepted")
		return cli.ExitUsage
	}
	password, err := readPasswordLine(env.Stdin)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: the password must be one line on stdin\n", usersCreateVerb)
		return cli.ExitUsage
	}
	if violations := passwordpolicy.ValidateForHash(password); len(violations) > 0 {
		fmt.Fprintf(env.Stderr, "%s: refused: password validation failed: %s\n", usersCreateVerb, strings.Join(violations, "; "))
		return cli.ExitRefused
	}
	hash, err := passwordhash.Hash(password)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: refused: the password cannot be hashed (longer than 72 bytes?)\n", usersCreateVerb)
		return cli.ExitRefused
	}
	if *superuser {
		if rolesOwned, err := platformrole.OwnedFromLookup(env.Lookup); err != nil {
			fmt.Fprintf(env.Stderr, "%s: %s must be true or false\n", usersCreateVerb, platformrole.OwnedEnv)
			return cli.ExitFailure
		} else if !rolesOwned {
			fmt.Fprintf(env.Stderr, "%s: refused: --superuser needs %s=true; while it is not, public users.is_superuser is the platform authority and the auth runtime role cannot write it. Create the user without --superuser, then set it with the owner (migration) login (runbook auth-platform-roles-flip-and-rollback, Rollback step 1) or flip platform roles to owned\n", usersCreateVerb, platformrole.OwnedEnv)
			return cli.ExitRefused
		}
	}
	pool, schema, code := connectFn(ctx, env, usersCreateVerb)
	if code != cli.ExitOK {
		return code
	}
	defer pool.Close()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	if *superuser {
		roles, err := platformrole.New(platformrole.Config{Pool: pool, Schema: schema, Logger: quiet})
		if err != nil {
			fmt.Fprintf(env.Stderr, "%s: could not build the platform role store\n", usersCreateVerb)
			return cli.ExitFailure
		}
		assignments, err := roles.List(ctx)
		if err != nil {
			fmt.Fprintf(env.Stderr, "%s: reading the platform role assignments failed; nothing was written\n", usersCreateVerb)
			return cli.ExitFailure
		}
		for _, a := range assignments {
			if a.Active && a.Role == platformrole.PlatformAdmin {
				fmt.Fprintf(env.Stderr, "%s: refused: this verb never grants a platform role and an active platform_admin exists. Run it without --superuser, then `dho auth platform-role-grant -principal <id> -role platform_admin -granted-by <admin id>`; nothing was written\n", usersCreateVerb)
				return cli.ExitRefused
			}
		}
		fmt.Fprintf(env.Stderr, "%s: refused: this verb never grants a platform role and no active platform_admin exists. Run it without --superuser, then `auth-migrate platform-role-bootstrap -principal <id> -confirm`; nothing was written\n", usersCreateVerb)
		return cli.ExitRefused
	}
	store, err := identity.New(identity.Config{Pool: pool, Schema: schema, UsersOwnedByAuth: true, Logger: quiet})
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: could not build the identity store\n", usersCreateVerb)
		return cli.ExitFailure
	}
	req := identity.CLIUserRequest{Email: *email, PasswordHash: hash}
	if *fullName != "" {
		req.FullName = fullName
	}
	id, err := store.ProvisionCLIUser(ctx, req)
	switch {
	case err == nil:
		fmt.Fprintf(env.Stdout, "user_created principal=%s\n", id)
		return cli.ExitOK
	case errors.Is(err, identity.ErrEmailTaken):
		fmt.Fprintf(env.Stderr, "%s: refused: a user with this email exists; nothing was written\n", usersCreateVerb)
		return cli.ExitRefused
	case errors.Is(err, identity.ErrInvalidEmail):
		fmt.Fprintf(env.Stderr, "%s: refused: the email is not a valid login address; nothing was written\n", usersCreateVerb)
		return cli.ExitRefused
	}
	fmt.Fprintf(env.Stderr, "%s: the store failed; nothing was written\n", usersCreateVerb)
	return cli.ExitFailure
}

// readPasswordLine reads the first line of r without its line end. A missing
// reader, an empty line, or a line over maxPasswordLine is an error.
func readPasswordLine(r io.Reader) (string, error) {
	if r == nil {
		return "", errors.New("no stdin")
	}
	line, err := bufio.NewReaderSize(io.LimitReader(r, maxPasswordLine+2), maxPasswordLine+2).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" || len(line) > maxPasswordLine {
		return "", errors.New("bad password line")
	}
	return line, nil
}
