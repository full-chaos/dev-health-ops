package authschema

import (
	"errors"
	"fmt"
)

// RepoIdentityFunction is the one function the worker domain login may execute for repository identities (migration
// 0056), and RepoIdentityFunctionSignature its argument list. It runs as RepoIdentityWriterRole and appends one
// repository_identity.observed event to the outbox under the commit guard.
const (
	RepoIdentityFunction          = "append_repository_identity_event"
	RepoIdentityFunctionSignature = "(uuid, text, text, text, timestamptz)"
)

// RepoIdentityCallerCount is the number of logins that may execute the function: the worker domain login.
const RepoIdentityCallerCount = 1

// ErrRepoIdentityWriter reports a repository identity writer role that is missing or not set up as the migration
// needs; the message says which step to run.
var ErrRepoIdentityWriter = errors.New("repository identity writer role")

// repoIdentityWriterTables and repoIdentityWriterSequences are the writer role's complete privilege sets: the outbox
// append and the revision counter. It holds nothing on repository_identities; the auth service writes that table.
var (
	repoIdentityWriterTables    = map[string][]string{"auth_outbox_events": {"INSERT"}, "aggregate_revisions": {"INSERT", "SELECT", "UPDATE"}}
	repoIdentityWriterSequences = map[string][]string{"auth_outbox_events_id_seq": {"USAGE"}}
)

func (o Options) validateRepoIdentity(migrationRole string) error {
	if o.RepoIdentityWriterRole == "" {
		if len(o.RepoIdentityCallerRoles) > 0 {
			return fmt.Errorf("%w: repository identity caller roles are set without a writer role", ErrInvalidOptions)
		}
		return nil
	}
	if err := ValidateIdentifier(o.RepoIdentityWriterRole); err != nil {
		return err
	}
	if n := len(o.RepoIdentityCallerRoles); n != RepoIdentityCallerCount {
		return fmt.Errorf("%w: the repository identity writer role needs exactly %d caller role (the worker domain login), got %d",
			ErrInvalidOptions, RepoIdentityCallerCount, n)
	}
	w := o.RepoIdentityWriterRole
	if w == o.RuntimeRole || (migrationRole != "" && w == migrationRole) || w == o.PushLifecycleWriterRole {
		return fmt.Errorf("%w: the repository identity writer role %q must differ from the runtime role, the migration role and the push lifecycle writer role",
			ErrInvalidOptions, w)
	}
	if listContains(o.PushLifecycleCallerRoles, w) {
		return fmt.Errorf("%w: the repository identity writer role %q must differ from every push lifecycle caller role", ErrInvalidOptions, w)
	}
	for _, caller := range o.RepoIdentityCallerRoles {
		if err := ValidateIdentifier(caller); err != nil {
			return err
		}
		if caller == w || caller == o.RuntimeRole || (migrationRole != "" && caller == migrationRole) || caller == o.PushLifecycleWriterRole {
			return fmt.Errorf("%w: repository identity caller role %q must differ from both writer roles, the runtime role and the migration role",
				ErrInvalidOptions, caller)
		}
	}
	return nil
}
