// Package usersbackfill copies public.users into auth.principals, auth.users
// and auth.external_identities. It is a pre-cutover tool: public.users is the
// source. It reports counts only; no id, address or verifier reaches a log,
// an error or the report.
package usersbackfill

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// Row is the part of one public.users row the backfill reads.
type Row struct {
	ID             uuid.UUID
	Email          string
	FullName       *string
	PasswordHash   *string
	Active         bool
	Verified       bool
	AuthProvider   *string
	AuthProviderID *string
}

// Skip classes. A skipped user is counted, not written, and fails the run.
const (
	SkipUnknownHashFormat   = "unknown_hash_format"
	SkipEmailCollision      = "email_collision"
	SkipIdentityConflict    = "identity_conflict"
	SkipProviderAlreadyLink = "provider_already_linked"
	SkipInvalidIdentity     = "invalid_identity"
	SkipStoreError          = "store_error"
)

// NotCarried names the public.users fields that have no auth-side column.
var NotCarried = []string{"is_superuser", "token_version", "last_login_at"}

// Source pages public.users in id order.
type Source interface {
	Page(ctx context.Context, after uuid.UUID, limit int) ([]Row, error)
}

// Sink writes one user. *identity.Store satisfies it.
type Sink interface {
	BackfillUser(ctx context.Context, t identity.BackfillTarget, dryRun bool) (identity.BackfillResult, error)
}

// Report is the outcome of a run.
type Report struct {
	DryRun           bool
	Seen             int
	Inserted         int
	Updated          int
	Unchanged        int
	IdentityLinked   int
	IdentityNotCarry int
	DisplayNameDrift int
	Skipped          map[string]int
}

// SkippedTotal is the number of users that were not written.
func (r Report) SkippedTotal() int {
	n := 0
	for _, c := range r.Skipped {
		n += c
	}
	return n
}

// Lines renders the report as stable key=value lines.
func (r Report) Lines() []string {
	mode := "write"
	if r.DryRun {
		mode = "dry-run"
	}
	lines := []string{
		"mode=" + mode,
		fmt.Sprintf("seen=%d", r.Seen),
		fmt.Sprintf("inserted=%d", r.Inserted),
		fmt.Sprintf("updated=%d", r.Updated),
		fmt.Sprintf("unchanged=%d", r.Unchanged),
		fmt.Sprintf("identity_linked=%d", r.IdentityLinked),
		fmt.Sprintf("identity_not_carried=%d", r.IdentityNotCarry),
		fmt.Sprintf("display_name_drift=%d", r.DisplayNameDrift),
		fmt.Sprintf("skipped=%d", r.SkippedTotal()),
	}
	classes := make([]string, 0, len(r.Skipped))
	for c := range r.Skipped {
		classes = append(classes, c)
	}
	sort.Strings(classes)
	for _, c := range classes {
		lines = append(lines, fmt.Sprintf("skipped.%s=%d", c, r.Skipped[c]))
	}
	lines = append(lines, "not_carried="+strings.Join(NotCarried, ","))
	return lines
}

// Options configures Run.
type Options struct {
	DryRun    bool
	BatchSize int
}

// Run copies every user. Per-user failures are counted by class and the run
// continues; a failure of the source, or a cancelled context, stops it.
func Run(ctx context.Context, src Source, sink Sink, opts Options) (Report, error) {
	batch := opts.BatchSize
	if batch <= 0 {
		batch = 500
	}
	rep := Report{DryRun: opts.DryRun, Skipped: map[string]int{}}
	after := uuid.Nil
	for {
		rows, err := src.Page(ctx, after, batch)
		if err != nil {
			return rep, errors.New("usersbackfill: reading public.users failed")
		}
		if len(rows) == 0 {
			return rep, nil
		}
		for _, row := range rows {
			if err := ctx.Err(); err != nil {
				return rep, err
			}
			rep.Seen++
			target, class, notCarried := Map(row)
			if class != "" {
				rep.Skipped[class]++
				continue
			}
			if notCarried {
				rep.IdentityNotCarry++
			}
			res, err := sink.BackfillUser(ctx, target, opts.DryRun)
			if err != nil {
				rep.Skipped[classify(err)]++
				continue
			}
			switch {
			case res.Inserted:
				rep.Inserted++
			case res.Updated:
				rep.Updated++
			default:
				rep.Unchanged++
			}
			if res.IdentityLinked {
				rep.IdentityLinked++
			}
			if res.DisplayNameDrift {
				rep.DisplayNameDrift++
			}
		}
		after = rows[len(rows)-1].ID
	}
}

func classify(err error) string {
	switch {
	case errors.Is(err, identity.ErrEmailCollision):
		return SkipEmailCollision
	case errors.Is(err, identity.ErrIdentityConflict):
		return SkipIdentityConflict
	case errors.Is(err, identity.ErrProviderAlreadyLinked):
		return SkipProviderAlreadyLink
	default:
		return SkipStoreError
	}
}

// Map turns a row into a write target. class is a skip class (empty when the
// row is writable); notCarried is set when the user's provider identity is not
// copied (any provider outside the social set, whose issuer is fixed).
func Map(row Row) (target identity.BackfillTarget, class string, notCarried bool) {
	target = identity.BackfillTarget{ID: row.ID, Email: row.Email, Active: row.Active, Verified: row.Verified}
	if row.FullName != nil {
		target.DisplayName = *row.FullName
	}
	if row.PasswordHash != nil {
		if !isBcrypt(*row.PasswordHash) {
			return target, SkipUnknownHashFormat, false
		}
		target.PasswordHash = row.PasswordHash
	}
	provider, subject := "", ""
	if row.AuthProvider != nil {
		provider = *row.AuthProvider
	}
	if row.AuthProviderID != nil {
		subject = *row.AuthProviderID
	}
	switch {
	case provider == "" || provider == "local":
	case identity.Provider(provider).IsSocial():
		issuer, _ := identity.SocialIssuer(identity.Provider(provider))
		key := identity.ExternalKey{Issuer: issuer, Provider: identity.Provider(provider), Subject: subject}
		if key.Validate() != nil {
			return target, SkipInvalidIdentity, false
		}
		target.Identity = &key
	default:
		notCarried = true
	}
	return target, "", notCarried
}

func isBcrypt(hash string) bool {
	for _, prefix := range []string{"$2a$", "$2b$", "$2y$"} {
		if strings.HasPrefix(hash, prefix) {
			return identity.ValidBcryptHash(hash)
		}
	}
	return false
}
