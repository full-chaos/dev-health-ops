package usersbackfill

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// OAuthSSOCandidate is one (user, active OAuth provider row) pair: the user's
// public.users auth_provider is the row's OAuth type and the user is a member
// of the row's organization. BaseURL is the row's config base_url ("" if none).
type OAuthSSOCandidate struct {
	UserID       uuid.UUID
	AuthProvider string
	Subject      string
	RowID        uuid.UUID
	BaseURL      string
}

// OAuthSSOKey returns the row key for a candidate: the issuer is the fixed social issuer of its type, except for a
// GitLab row, whose issuer is the lowercase scheme and host of the row's configured base_url (identity.OAuthIssuer),
// so a self-hosted instance is its own namespace and a numeric id on it is never the gitlab.com account. A base_url
// that names no host is never carried (key nil). public.users has no column that tells an OAuth-SSO user from a plain
// social user (go-api writes the same auth_provider and subject for both, sso_provider_id NULL on gitlab.com): a
// social user in the row's org gets the row key too, which only ever matches the account it already owns.
func OAuthSSOKey(c OAuthSSOCandidate) *identity.ExternalKey {
	provider := identity.Provider(c.AuthProvider)
	if !provider.IsSocial() {
		return nil
	}
	issuer, err := identity.OAuthIssuer(provider, c.BaseURL)
	if err != nil {
		return nil
	}
	k := identity.ExternalKey{Issuer: issuer, Provider: identity.Provider(c.RowID.String()), Subject: c.Subject}
	if k.Validate() != nil {
		return nil
	}
	return &k
}

// OAuthSSOCursor is the keyset position of a candidate page.
type OAuthSSOCursor struct{ User, Row uuid.UUID }

// OAuthSSOSource pages candidates in (user id, row id) order.
type OAuthSSOSource interface {
	Page(ctx context.Context, after *OAuthSSOCursor, limit int) ([]OAuthSSOCandidate, error)
}

// OAuthSSOSink writes one extra identity. *identity.Store satisfies it.
type OAuthSSOSink interface {
	CarryExtraIdentity(ctx context.Context, principal uuid.UUID, key identity.ExternalKey, dryRun bool) (bool, error)
}

// OAuthSSOReport is the outcome of RunOAuthSSO. Counts only.
type OAuthSSOReport struct {
	DryRun    bool
	Users     int
	Linked    int
	Ambiguous int
	Skipped   map[string]int
}

// Lines renders the report as stable key=value lines.
func (r OAuthSSOReport) Lines() []string {
	lines := []string{
		fmt.Sprintf("oauth_sso_users=%d", r.Users),
		fmt.Sprintf("oauth_sso_keys_linked=%d", r.Linked),
		fmt.Sprintf("oauth_sso_ambiguous_row=%d", r.Ambiguous),
	}
	classes := make([]string, 0, len(r.Skipped))
	for c := range r.Skipped {
		classes = append(classes, c)
	}
	sort.Strings(classes)
	for _, c := range classes {
		lines = append(lines, fmt.Sprintf("skipped.%s=%d", c, r.Skipped[c]))
	}
	return lines
}

// RunOAuthSSO carries the row key of every OAuth-SSO user that has EXACTLY ONE
// active candidate row (any organization it belongs to). Two or more rows are
// ambiguous: nothing is written and the user is counted. It runs after Run: the
// principal and its social key exist. Per-key failures are counted by class and
// the run continues; a source failure stops it.
func RunOAuthSSO(ctx context.Context, src OAuthSSOSource, sink OAuthSSOSink, opts Options) (OAuthSSOReport, error) {
	batch := opts.BatchSize
	if batch <= 0 {
		batch = 500
	}
	rep := OAuthSSOReport{DryRun: opts.DryRun, Skipped: map[string]int{}}
	var group []OAuthSSOCandidate
	flush := func() {
		if len(group) == 0 {
			return
		}
		rep.Users++
		switch {
		case len(group) > 1:
			rep.Ambiguous++
		default:
			if key := OAuthSSOKey(group[0]); key != nil {
				linked, err := sink.CarryExtraIdentity(ctx, group[0].UserID, *key, opts.DryRun)
				switch {
				case err != nil:
					rep.Skipped[classify(err)]++
				case linked:
					rep.Linked++
				}
			}
		}
		group = group[:0]
	}
	var after *OAuthSSOCursor
	for {
		rows, err := src.Page(ctx, after, batch)
		if err != nil {
			return rep, errors.New("usersbackfill: reading oauth sso candidates failed")
		}
		if len(rows) == 0 {
			flush()
			return rep, nil
		}
		for _, c := range rows {
			if err := ctx.Err(); err != nil {
				return rep, err
			}
			if len(group) > 0 && group[0].UserID != c.UserID {
				flush()
			}
			group = append(group, c)
		}
		last := rows[len(rows)-1]
		after = &OAuthSSOCursor{User: last.UserID, Row: last.RowID}
	}
}

// oauthSSOPageSQL reads only columns in authschema.PublicReadColumns; never a
// secret column.
const oauthSSOPageSQL = `SELECT u.id, u.auth_provider, u.auth_provider_id, sp.id, coalesce(sp.config::jsonb->>'base_url', '')
FROM public.users u
JOIN public.memberships m ON m.user_id = u.id
JOIN public.sso_providers sp ON sp.org_id = m.org_id AND sp.status = 'active' AND sp.protocol = 'oauth_' || u.auth_provider
WHERE u.auth_provider IN ('github', 'gitlab', 'google') AND coalesce(u.auth_provider_id, '') <> ''
  AND ($1::uuid IS NULL OR (u.id, sp.id) > ($1::uuid, $2::uuid))
ORDER BY u.id, sp.id LIMIT $3`

// PGOAuthSSOSource reads candidates from public.users, memberships and sso_providers.
type PGOAuthSSOSource struct{ Pool *pgxpool.Pool }

func (s PGOAuthSSOSource) Page(ctx context.Context, after *OAuthSSOCursor, limit int) ([]OAuthSSOCandidate, error) {
	var user, row *uuid.UUID
	if after != nil {
		user, row = &after.User, &after.Row
	}
	rs, err := s.Pool.Query(ctx, oauthSSOPageSQL, user, row, limit)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []OAuthSSOCandidate
	for rs.Next() {
		var c OAuthSSOCandidate
		var sub *string
		if err := rs.Scan(&c.UserID, &c.AuthProvider, &sub, &c.RowID, &c.BaseURL); err != nil {
			return nil, err
		}
		if sub != nil {
			c.Subject = *sub
		}
		out = append(out, c)
	}
	return out, rs.Err()
}
