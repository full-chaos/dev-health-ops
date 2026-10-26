package usersbackfill

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

// OAuthSSOKey returns the row key for a candidate. A key is carried only when
// the row's issuer is the social issuer of its type: the row's subject
// namespace is then the social namespace, so the key can only match the
// account the user already holds. A row with another base_url (self-hosted) is
// never carried: selfHosted is set and key is nil.
func OAuthSSOKey(c OAuthSSOCandidate) (key *identity.ExternalKey, selfHosted bool) {
	issuer, ok := identity.SocialIssuer(identity.Provider(c.AuthProvider))
	if !ok {
		return nil, false
	}
	if base := strings.TrimRight(c.BaseURL, "/"); base != "" && base != issuer {
		return nil, true
	}
	k := identity.ExternalKey{Issuer: issuer, Provider: identity.Provider(c.RowID.String()), Subject: c.Subject}
	if k.Validate() != nil {
		return nil, false
	}
	return &k, false
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
	DryRun               bool
	Users                int
	Linked               int
	SelfHostedUnbindable int
	Skipped              map[string]int
}

// Lines renders the report as stable key=value lines.
func (r OAuthSSOReport) Lines() []string {
	lines := []string{
		fmt.Sprintf("oauth_sso_users=%d", r.Users),
		fmt.Sprintf("oauth_sso_keys_linked=%d", r.Linked),
		fmt.Sprintf("oauth_sso_selfhosted_unbindable=%d", r.SelfHostedUnbindable),
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

// RunOAuthSSO carries the row key of every OAuth-SSO user. It runs after Run:
// the principal and its social key exist. Per-key failures are counted by
// class and the run continues; a source failure stops it.
func RunOAuthSSO(ctx context.Context, src OAuthSSOSource, sink OAuthSSOSink, opts Options) (OAuthSSOReport, error) {
	batch := opts.BatchSize
	if batch <= 0 {
		batch = 500
	}
	rep := OAuthSSOReport{DryRun: opts.DryRun, Skipped: map[string]int{}}
	var after *OAuthSSOCursor
	var lastUser uuid.UUID
	var seen, lastSelfHosted bool
	for {
		rows, err := src.Page(ctx, after, batch)
		if err != nil {
			return rep, errors.New("usersbackfill: reading oauth sso candidates failed")
		}
		if len(rows) == 0 {
			return rep, nil
		}
		for _, c := range rows {
			if err := ctx.Err(); err != nil {
				return rep, err
			}
			if !seen || c.UserID != lastUser {
				seen, lastUser, lastSelfHosted = true, c.UserID, false
				rep.Users++
			}
			key, selfHosted := OAuthSSOKey(c)
			if selfHosted && !lastSelfHosted {
				lastSelfHosted = true
				rep.SelfHostedUnbindable++
			}
			if key == nil {
				continue
			}
			linked, err := sink.CarryExtraIdentity(ctx, c.UserID, *key, opts.DryRun)
			if err != nil {
				rep.Skipped[classify(err)]++
				continue
			}
			if linked {
				rep.Linked++
			}
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

// OAuthSSOReadColumns lists the public columns oauthSSOPageSQL and
// OAuthSSOCountSQL read, by table.
func OAuthSSOReadColumns() map[string][]string {
	return map[string][]string{
		"users":         {"id", "auth_provider", "auth_provider_id"},
		"memberships":   {"user_id", "org_id"},
		"sso_providers": {"id", "org_id", "protocol", "status", "config"},
	}
}

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

// OAuthSSOCount is the pre-flip count line of the users-owned runbook. Counts
// only: no id, e-mail or organization.
type OAuthSSOCount struct {
	Total, Keyed, Missing, SelfHostedUnbindable int64
}

// Line renders the count as one stable line.
func (c OAuthSSOCount) Line() string {
	return fmt.Sprintf("oauth_sso total=%d keyed=%d missing=%d selfhosted_unbindable=%d",
		c.Total, c.Keyed, c.Missing, c.SelfHostedUnbindable)
}

// OAuthSSOCountSQL is the read-only count statement. quotedSchema is the quoted
// auth schema. Parameters $1..$3 are the github, gitlab and google issuers.
// total: users with an active OAuth row in a member org. keyed: every
// social-namespace row has its key. missing: some social-namespace row has no
// key. selfhosted_unbindable: some row has a self-hosted base_url (not carried).
func OAuthSSOCountSQL(quotedSchema string) string {
	return `WITH cand AS (
  SELECT u.id AS uid, u.auth_provider_id AS sub, sp.id AS spid,
         rtrim(coalesce(sp.config::jsonb->>'base_url', ''), '/') AS base,
         CASE sp.protocol WHEN 'oauth_github' THEN $1::text WHEN 'oauth_gitlab' THEN $2::text WHEN 'oauth_google' THEN $3::text END AS iss
  FROM public.users u
  JOIN public.memberships m ON m.user_id = u.id
  JOIN public.sso_providers sp ON sp.org_id = m.org_id AND sp.status = 'active' AND sp.protocol = 'oauth_' || u.auth_provider
  WHERE u.auth_provider IN ('github', 'gitlab', 'google') AND coalesce(u.auth_provider_id, '') <> ''
), flag AS (
  SELECT c.uid,
         bool_or(c.base <> '' AND c.base <> c.iss) AS selfh,
         bool_or(NOT (c.base <> '' AND c.base <> c.iss)) AS social,
         bool_or(NOT (c.base <> '' AND c.base <> c.iss) AND e.id IS NULL) AS missing
  FROM cand c
  LEFT JOIN ` + quotedSchema + `.external_identities e
    ON e.principal_id = c.uid AND e.issuer = c.iss AND e.provider = c.spid::text AND e.subject = c.sub
  GROUP BY c.uid
)
SELECT count(*), count(*) FILTER (WHERE social AND NOT missing), count(*) FILTER (WHERE missing), count(*) FILTER (WHERE selfh) FROM flag`
}

type countQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// ReadOAuthSSOCount runs OAuthSSOCountSQL. The issuers come from the identity
// package, the one definition the carry uses.
func ReadOAuthSSOCount(ctx context.Context, db countQuerier, quotedSchema string) (OAuthSSOCount, error) {
	var c OAuthSSOCount
	gh, _ := identity.SocialIssuer(identity.ProviderGitHub)
	gl, _ := identity.SocialIssuer(identity.ProviderGitLab)
	gg, _ := identity.SocialIssuer(identity.ProviderGoogle)
	if err := db.QueryRow(ctx, OAuthSSOCountSQL(quotedSchema), gh, gl, gg).Scan(&c.Total, &c.Keyed, &c.Missing, &c.SelfHostedUnbindable); err != nil {
		return OAuthSSOCount{}, errors.New("usersbackfill: reading the oauth sso count failed")
	}
	return c, nil
}
