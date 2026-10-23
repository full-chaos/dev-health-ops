package usersbackfill

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// AuthRow is the part of one auth.principals row (with its auth.users row and
// platform role) that the reconciliation compares.
type AuthRow struct {
	ID      uuid.UUID
	Kind    string
	HasUser bool
	// Email and EmailLower are auth.users.email and email_lower.
	Email      string
	EmailLower string
	Active     bool
	Verified   bool
	// PasswordHash is nil for a user without a local credential.
	PasswordHash *string
	TokenVersion int64
	LastLoginAt  *time.Time
	DisplayName  string
	// RoleHeld is any platform_admin assignment; RoleBackfilled one with no granter.
	RoleHeld       bool
	RoleBackfilled bool
}

// Target pages the auth principals in id order and answers who holds an
// external identity. Both calls read the same snapshot as the Source.
type Target interface {
	Page(ctx context.Context, after *uuid.UUID, limit int) ([]AuthRow, error)
	IdentityHolder(ctx context.Context, key identity.ExternalKey) (holder uuid.UUID, found bool, err error)
}

// Gated diff classes. Any count above zero fails the gate.
const (
	DiffMissingInAuth    = "missing_in_auth"
	DiffExtraInAuth      = "extra_in_auth"
	DiffPrincipalKind    = "principal_kind_diff"
	DiffEmail            = "email_diff"
	DiffActive           = "active_diff"
	DiffVerified         = "verified_diff"
	DiffPasswordHash     = "password_hash_diff"
	DiffPlatformRole     = "platform_role_diff"
	DiffTokenVersion     = "token_version_diff"
	DiffIdentityMissing  = "identity_missing"
	DiffIdentityConflict = "identity_conflict"
)

// GatedClasses lists every class the gate counts, in report order: the diff
// classes above, then the backfill skip classes a user can fall into.
var GatedClasses = []string{
	DiffMissingInAuth, DiffExtraInAuth, DiffPrincipalKind, DiffEmail, DiffActive, DiffVerified,
	DiffPasswordHash, DiffPlatformRole, DiffTokenVersion, DiffIdentityMissing, DiffIdentityConflict,
	SkipUnknownHashFormat, SkipInvalidIdentity,
}

// Reconciliation is the outcome of a read-only comparison of public.users with
// the auth schema.
type Reconciliation struct {
	// Seen is the number of public.users rows read; Matched is how many of them
	// have no gated diff.
	Seen    int
	Matched int
	// Diffs counts users per gated class (a user with two diffs is in two classes).
	Diffs map[string]int
	// Informational counts never fail the gate.
	LastLoginDrift     int
	DisplayNameDrift   int
	IdentityNotCarried int
}

// GatedTotal is the number of gated diffs: zero is the gate.
func (r Reconciliation) GatedTotal() int {
	n := 0
	for _, c := range r.Diffs {
		n += c
	}
	return n
}

// Lines renders the report as stable key=value lines: class names and counts
// only, never a value, address or id.
func (r Reconciliation) Lines() []string {
	lines := []string{
		"mode=read-only",
		fmt.Sprintf("seen=%d", r.Seen),
		fmt.Sprintf("matched=%d", r.Matched),
		fmt.Sprintf("gated_diffs=%d", r.GatedTotal()),
	}
	for _, class := range GatedClasses {
		lines = append(lines, fmt.Sprintf("diff.%s=%d", class, r.Diffs[class]))
	}
	lines = append(lines,
		fmt.Sprintf("info.last_login_drift=%d", r.LastLoginDrift),
		fmt.Sprintf("info.display_name_drift=%d", r.DisplayNameDrift),
		fmt.Sprintf("info.identity_not_carried=%d", r.IdentityNotCarried),
	)
	return lines
}

// Reconcile compares every public.users row with its auth rows and every auth
// user with its public row, writing nothing. A failure of either side stops it
// with an error that names no value.
func Reconcile(ctx context.Context, src Source, tgt Target, batch int) (Reconciliation, error) {
	if batch <= 0 {
		batch = 500
	}
	rep := Reconciliation{Diffs: map[string]int{}}
	pub := &pubCursor{src: src, batch: batch}
	auth := &authCursor{tgt: tgt, batch: batch}
	for {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		row, haveRow, err := pub.peek(ctx)
		if err != nil {
			return rep, errors.New("usersbackfill: reading public.users failed")
		}
		a, haveAuth, err := auth.peek(ctx)
		if err != nil {
			return rep, errors.New("usersbackfill: reading the auth schema failed")
		}
		switch {
		case !haveRow && !haveAuth:
			return rep, nil
		case haveRow && (!haveAuth || bytes.Compare(row.ID[:], a.ID[:]) < 0):
			rep.Seen++
			skips, _, _, notCarried := rowFacts(row)
			if notCarried {
				rep.IdentityNotCarried++
			}
			for _, class := range skips {
				rep.Diffs[class]++
			}
			if len(skips) == 0 {
				rep.Diffs[DiffMissingInAuth]++
			}
			pub.advance()
		case haveAuth && (!haveRow || bytes.Compare(a.ID[:], row.ID[:]) < 0):
			if a.Kind == "user" || a.HasUser {
				rep.Diffs[DiffExtraInAuth]++
			}
			auth.advance()
		default:
			rep.Seen++
			if err := compareOne(ctx, tgt, row, a, &rep); err != nil {
				return rep, errors.New("usersbackfill: reading the auth schema failed")
			}
			pub.advance()
			auth.advance()
		}
	}
}

// rowFacts is what a public row expects of the auth schema, whatever else is
// wrong with the row: the backfill skip classes it falls into (the hash and the
// identity are judged independently), its write target, and its social identity
// read from the row itself rather than from the mapper's early return.
func rowFacts(row Row) (skips []string, target identity.BackfillTarget, key *identity.ExternalKey, notCarried bool) {
	target, hashSkip, _ := Map(row)
	if hashSkip != "" {
		skips = append(skips, hashSkip)
	}
	key, idSkip, notCarried := mapIdentity(row)
	if idSkip != "" && idSkip != hashSkip {
		skips = append(skips, idSkip)
	}
	return skips, target, key, notCarried
}

func compareOne(ctx context.Context, tgt Target, row Row, a AuthRow, rep *Reconciliation) error {
	skips, target, key, notCarried := rowFacts(row)
	if notCarried {
		rep.IdentityNotCarried++
	}
	classes := append([]string(nil), skips...)
	if a.Kind != "user" {
		classes = append(classes, DiffPrincipalKind)
	}
	// Every independent field is compared on any matched row that has an
	// auth.users row, whatever its principal kind or skip class.
	if !a.HasUser {
		if a.Kind == "user" {
			classes = append(classes, DiffPrincipalKind)
		}
	} else {
		if a.Email != row.Email || a.EmailLower != identity.StoredLoginKey(row.Email) {
			classes = append(classes, DiffEmail)
		}
		if a.Active != row.Active {
			classes = append(classes, DiffActive)
		}
		if a.Verified != row.Verified {
			classes = append(classes, DiffVerified)
		}
		// An unmappable verifier is its own skip class, not a password diff.
		if !contains(skips, SkipUnknownHashFormat) && differsString(a.PasswordHash, target.PasswordHash) {
			classes = append(classes, DiffPasswordHash)
		}
		if (row.Superuser && !a.RoleHeld) || (!row.Superuser && a.RoleBackfilled) {
			classes = append(classes, DiffPlatformRole)
		}
		if a.TokenVersion != row.TokenVersion {
			classes = append(classes, DiffTokenVersion)
		}
		if key != nil {
			holder, found, err := tgt.IdentityHolder(ctx, *key)
			if err != nil {
				return err
			}
			switch {
			case !found:
				classes = append(classes, DiffIdentityMissing)
			case holder != row.ID:
				classes = append(classes, DiffIdentityConflict)
			}
		}
		if !sameTime(a.LastLoginAt, row.LastLoginAt) {
			rep.LastLoginDrift++
		}
		if a.DisplayName != target.DisplayName {
			rep.DisplayNameDrift++
		}
	}
	if len(classes) == 0 {
		rep.Matched++
		return nil
	}
	for _, c := range classes {
		rep.Diffs[c]++
	}
	return nil
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func differsString(a, b *string) bool {
	if a == nil || b == nil {
		return a != b
	}
	return *a != *b
}

func sameTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

type pubCursor struct {
	src   Source
	batch int
	buf   []Row
	last  *uuid.UUID
	done  bool
}

func (c *pubCursor) peek(ctx context.Context) (Row, bool, error) {
	if len(c.buf) == 0 && !c.done {
		rows, err := c.src.Page(ctx, c.last, c.batch)
		if err != nil {
			return Row{}, false, err
		}
		if len(rows) == 0 {
			c.done = true
		}
		c.buf = rows
	}
	if len(c.buf) == 0 {
		return Row{}, false, nil
	}
	return c.buf[0], true, nil
}

func (c *pubCursor) advance() {
	id := c.buf[0].ID
	c.last = &id
	c.buf = c.buf[1:]
}

type authCursor struct {
	tgt   Target
	batch int
	buf   []AuthRow
	last  *uuid.UUID
	done  bool
}

func (c *authCursor) peek(ctx context.Context) (AuthRow, bool, error) {
	if len(c.buf) == 0 && !c.done {
		rows, err := c.tgt.Page(ctx, c.last, c.batch)
		if err != nil {
			return AuthRow{}, false, err
		}
		if len(rows) == 0 {
			c.done = true
		}
		c.buf = rows
	}
	if len(c.buf) == 0 {
		return AuthRow{}, false, nil
	}
	return c.buf[0], true, nil
}

func (c *authCursor) advance() {
	id := c.buf[0].ID
	c.last = &id
	c.buf = c.buf[1:]
}
