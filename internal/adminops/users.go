package adminops

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The verbs below port `dev-hops admin users create|list|update` and
// `admin orgs create|list`. They print what the Python verbs printed: results
// and refusals ("Error: ...") both go to stdout, a refusal exits 1.

func usersGroup() cli.Command {
	return cli.Command{
		Name: "users", Summary: "user management", Kind: cli.Group,
		Children: []cli.Command{
			{Name: "create", Summary: "create a new user", Kind: cli.Verb, Run: runUsersCreate},
			{Name: "list", Summary: "list users", Kind: cli.Verb, Run: runUsersList},
			{Name: "update", Summary: "update an existing user", Kind: cli.Verb, Run: runUsersUpdate},
		},
	}
}

func orgsGroup() cli.Command {
	return cli.Command{
		Name: "orgs", Summary: "organization management", Kind: cli.Group,
		Children: []cli.Command{
			{Name: "create", Summary: "create a new organization", Kind: cli.Verb, Run: runOrgsCreate},
			{Name: "list", Summary: "list organizations", Kind: cli.Verb, Run: runOrgsList},
		},
	}
}

// optString is a string flag that knows whether it was given: Python's
// `is not None`, where an empty value is a value.
type optString struct {
	value string
	set   bool
}

func (o *optString) String() string { return o.value }
func (o *optString) Set(v string) error {
	o.value, o.set = v, true
	return nil
}
func (o *optString) ptr() *string {
	if !o.set {
		return nil
	}
	v := o.value
	return &v
}

// optBool is Python's BooleanOptionalAction: --name sets true, --no-name sets
// false, absent stays nil.
type optBool struct {
	value bool
	set   bool
}

func (o *optBool) set2(v bool) { o.value, o.set = v, true }
func (o *optBool) ptr() *bool {
	if !o.set {
		return nil
	}
	v := o.value
	return &v
}

type boolFlag struct {
	target *optBool
	on     bool
}

func (b boolFlag) String() string { return "" }
func (b boolFlag) Set(string) error {
	b.target.set2(b.on)
	return nil
}
func (b boolFlag) IsBoolFlag() bool { return true }

func optionalBool(flags *flag.FlagSet, name, usage string) *optBool {
	target := &optBool{}
	flags.Var(boolFlag{target: target, on: true}, name, usage)
	flags.Var(boolFlag{target: target, on: false}, "no-"+name, "unset --"+name)
	return target
}

const dbEnvHelp = "\nEnvironment:\n" +
	"  MIGRATION_DATABASE_URI (or _FILE, or the DEV_HEALTH_MIGRATION_PG_* component form)   the database\n" +
	"  POSTGRES_URI (or _FILE)   used when MIGRATION_DATABASE_URI is not configured\n"

func newFlags(env cli.Env, name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	defaultUsage := flags.Usage
	flags.Usage = func() {
		defaultUsage()
		fmt.Fprint(env.Stderr, dbEnvHelp)
	}
	return flags
}

// parseArgs parses the verb's flags: ok false means the exit code in code.
func parseArgs(flags *flag.FlagSet, env cli.Env) (code int, ok bool) {
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK, false
		}
		return cli.ExitUsage, false
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted")
		return cli.ExitUsage, false
	}
	return 0, true
}

// operator opens the pool and returns the Operator; the returned close func is
// always safe to call.
func operator(ctx context.Context, env cli.Env) (admin.Operator, func(), int) {
	dsn, _, ok := config.ResolveMigrationDatabase(env.Lookup, env.Stderr, true)
	if !ok {
		return admin.Operator{}, func() {}, cli.ExitFailure
	}
	boundary := secrets.NewBoundary(dsn.Reveal())
	pool, err := pgxpool.New(ctx, dsn.Reveal())
	if err != nil {
		return admin.Operator{}, func() {}, writeError(env.Stderr, "postgres_unavailable", boundary.Redact(err).Error())
	}
	return admin.Operator{Pool: pool, Now: time.Now}, pool.Close, 0
}

// report prints an operator-facing refusal the way the Python verbs did
// ("Error: ..." on stdout, exit 1); any other error is a failure on stderr.
func report(env cli.Env, boundary func(error) error, err error) int {
	var refusal *admin.OperatorError
	if errors.As(err, &refusal) {
		fmt.Fprintf(env.Stdout, "Error: %s\n", refusal.Message)
		return cli.ExitFailure
	}
	return writeError(env.Stderr, "admin_failed", boundary(err).Error())
}

func redactor(env cli.Env) func(error) error {
	dsn, _, ok := config.ResolveMigrationDatabase(env.Lookup, io.Discard, false)
	if !ok {
		return func(err error) error { return errors.New("redacted") }
	}
	boundary := secrets.NewBoundary(dsn.Reveal())
	return boundary.Redact
}

func runUsersCreate(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin users create")
	var email, password, username, fullName optString
	flags.Var(&email, "email", "user email address (required)")
	flags.Var(&password, "password", "user password, at least 8 characters (required)")
	flags.Var(&username, "username", "optional username")
	flags.Var(&fullName, "full-name", "the user's full name")
	superuser := flags.Bool("superuser", false, "grant superuser privileges")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	if !email.set || !password.set {
		fmt.Fprintln(env.Stderr, "argument error: --email and --password are required")
		return cli.ExitUsage
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	user, err := op.CreateUser(ctx, admin.CreateUserInput{Email: email.value, Password: password.value,
		Username: username.ptr(), FullName: fullName.ptr(), Superuser: *superuser})
	if err != nil {
		return report(env, redactor(env), err)
	}
	fmt.Fprintf(env.Stdout, "Created user: %s (id: %s)\n", user.Email, user.ID)
	if *superuser {
		fmt.Fprintln(env.Stdout, "  [superuser]")
	}
	return cli.ExitOK
}

// pad is Python's f"{text:<width}": left-aligned, padded to width code points.
func pad(text string, width int) string {
	if n := len([]rune(text)); n < width {
		return text + strings.Repeat(" ", width-n)
	}
	return text
}

func yesNo(value bool) string {
	if value {
		return "Yes"
	}
	return "No"
}

func runUsersList(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin users list")
	limit := flags.Int("limit", 100, "max users to list")
	includeInactive := flags.Bool("include-inactive", false, "include inactive users")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	users, err := op.ListUsers(ctx, *limit, *includeInactive)
	if err != nil {
		return report(env, redactor(env), err)
	}
	if len(users) == 0 {
		fmt.Fprintln(env.Stdout, "No users found.")
		return cli.ExitOK
	}
	fmt.Fprintf(env.Stdout, "%s %s %s %s %s\n", pad("ID", 40), pad("Email", 30), pad("Username", 20), pad("Superuser", 10), pad("Active", 8))
	fmt.Fprintln(env.Stdout, strings.Repeat("-", 108))
	for _, user := range users {
		username := ""
		if user.Username != nil {
			username = *user.Username
		}
		fmt.Fprintf(env.Stdout, "%s %s %s %s %s\n", pad(user.ID.String(), 40), pad(user.Email, 30), pad(username, 20),
			pad(yesNo(user.IsSuperuser), 10), pad(yesNo(user.IsActive), 8))
	}
	return cli.ExitOK
}

func runUsersUpdate(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin users update")
	var id, email, username, newEmail, newUsername, fullName, password, org, role, removeFromOrg optString
	flags.Var(&id, "id", "user id to update (identifier)")
	flags.Var(&email, "email", "email of the user to update (identifier)")
	flags.Var(&username, "username", "username of the user to update (identifier)")
	flags.Var(&newEmail, "new-email", "set a new email address")
	flags.Var(&newUsername, "new-username", "set a new username (an empty value clears it)")
	flags.Var(&fullName, "full-name", "set the user's full name")
	flags.Var(&password, "password", "set a new password, at least 8 characters; revokes existing sessions")
	verified := optionalBool(flags, "verified", "set verified status (--verified / --no-verified)")
	superuser := optionalBool(flags, "superuser", "set superuser status (--superuser / --no-superuser)")
	active := optionalBool(flags, "active", "set active status (--active / --no-active)")
	flags.Var(&org, "org", "org slug or id: add the user to this org, or update their role")
	flags.Var(&role, "role", "membership role to set with --org: owner, admin, member or viewer")
	flags.Var(&removeFromOrg, "remove-from-org", "org slug or id: remove the user's membership from this org")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	if role.set {
		switch role.value {
		case "owner", "admin", "member", "viewer":
		default:
			fmt.Fprintf(env.Stderr, "argument error: --role must be one of owner, admin, member, viewer\n")
			return cli.ExitUsage
		}
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	result, err := op.UpdateUser(ctx, admin.UpdateUserInput{
		ID: id.ptr(), Email: email.ptr(), Username: username.ptr(),
		NewEmail: newEmail.ptr(), NewUsername: newUsername.ptr(), FullName: fullName.ptr(), Password: password.ptr(),
		Active: active.ptr(), Verified: verified.ptr(), Superuser: superuser.ptr(),
		MembershipOrg: org.ptr(), Role: role.ptr(), RemoveFromOrg: removeFromOrg.ptr(),
	})
	if err != nil {
		return report(env, redactor(env), err)
	}
	fmt.Fprintf(env.Stdout, "Updated user: %s (id: %s)\n", result.Email, result.ID)
	for _, change := range result.Changes {
		fmt.Fprintf(env.Stdout, "  %s\n", change)
	}
	return cli.ExitOK
}

func runOrgsCreate(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin orgs create")
	var name, slug, description, ownerEmail optString
	flags.Var(&name, "name", "organization name (required)")
	flags.Var(&slug, "slug", "URL-safe slug (generated when omitted)")
	flags.Var(&description, "description", "organization description")
	tier := flags.String("tier", "community", "subscription tier")
	flags.Var(&ownerEmail, "owner-email", "email of the initial owner")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	if !name.set {
		fmt.Fprintln(env.Stderr, "argument error: --name is required")
		return cli.ExitUsage
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	result, err := op.CreateOrg(ctx, admin.CreateOrgInput{Name: name.value, Slug: slug.ptr(), Description: description.ptr(),
		Tier: *tier, OwnerEmail: ownerEmail.ptr()})
	if err != nil {
		return report(env, redactor(env), err)
	}
	fmt.Fprintf(env.Stdout, "Created organization: %s (slug: %s, id: %s)\n", result.Org.Name, result.Org.Slug, result.Org.ID)
	if result.OwnerEmail != "" {
		fmt.Fprintf(env.Stdout, "  Owner: %s\n", result.OwnerEmail)
	}
	return cli.ExitOK
}

func runOrgsList(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin orgs list")
	limit := flags.Int("limit", 100, "max organizations to list")
	includeInactive := flags.Bool("include-inactive", false, "include inactive organizations")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	orgs, err := op.ListOrgs(ctx, *limit, *includeInactive)
	if err != nil {
		return report(env, redactor(env), err)
	}
	if len(orgs) == 0 {
		fmt.Fprintln(env.Stdout, "No organizations found.")
		return cli.ExitOK
	}
	fmt.Fprintf(env.Stdout, "%s %s %s %s %s\n", pad("ID", 40), pad("Slug", 20), pad("Name", 30), pad("Tier", 10), pad("Active", 8))
	fmt.Fprintln(env.Stdout, strings.Repeat("-", 108))
	for _, org := range orgs {
		fmt.Fprintf(env.Stdout, "%s %s %s %s %s\n", pad(org.ID.String(), 40), pad(org.Slug, 20), pad(org.Name, 30), pad(org.Tier, 10), pad(yesNo(org.IsActive), 8))
	}
	return cli.ExitOK
}
