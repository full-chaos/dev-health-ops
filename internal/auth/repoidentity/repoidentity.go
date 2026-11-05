// Package repoidentity copies the provider repository ids of an organization from the repository source
// (ClickHouse repos, the authority) into auth.repository_identities, and gives each live repository grant of
// that organization the id of its path (resource_grants.resource_repo_id). One run is idempotent. A grant whose
// path resolves to no single repository stays path-only and is listed in the report (count and ids, no keys).
package repoidentity

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/repokey"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// MaxListedGrants bounds the unresolved grant ids a report carries.
const MaxListedGrants = 100

var (
	providerShape = regexp.MustCompile(`^[a-z]{1,16}$`)
	repoIDShape   = regexp.MustCompile(`^[0-9]{1,20}$`)
)

// Repo is one repository of the source: the organization it belongs to, the provider id and the live path.
type Repo struct {
	OrgID          string
	Provider       string
	ProviderRepoID string
	Path           string
}

// Source lists the repositories of every organization.
type Source interface {
	Repos(ctx context.Context) ([]Repo, error)
}

// OrgResult is what a sink did for one organization.
type OrgResult struct {
	Changed    bool
	Unresolved []uuid.UUID
	// UnresolvedTotal is the full count; Unresolved holds at most MaxListedGrants ids.
	UnresolvedTotal int
}

// Sink writes the identities of one organization and backfills its grants.
type Sink interface {
	// OrgsWithIdentities lists the organizations that hold identity rows, so an organization the source no longer
	// lists is given an empty set.
	OrgsWithIdentities(ctx context.Context) ([]uuid.UUID, error)
	Apply(ctx context.Context, org uuid.UUID, repos []Repo) (OrgResult, error)
}

// Report is the outcome of one run.
type Report struct {
	Orgs            int
	Repos           int
	SkippedInvalid  int
	OrgsFailed      int
	OrgsChanged     int
	UnresolvedTotal int
	Unresolved      []uuid.UUID
}

// Lines is the report as text: counts and grant ids only.
func (r Report) Lines() []string {
	lines := []string{fmt.Sprintf("repo-identities: orgs=%d repos=%d skipped_invalid=%d orgs_failed=%d orgs_changed=%d unresolved_grants=%d",
		r.Orgs, r.Repos, r.SkippedInvalid, r.OrgsFailed, r.OrgsChanged, r.UnresolvedTotal)}
	for _, id := range r.Unresolved {
		lines = append(lines, "repo-identities: unresolved grant "+id.String())
	}
	return lines
}

// Group keeps the valid repositories, one per (organization, provider, id), by organization. A row with a
// malformed organization, provider, id or path is counted and dropped.
func Group(repos []Repo) (map[uuid.UUID][]Repo, int) {
	type key struct {
		org      uuid.UUID
		provider string
		id       string
	}
	seen := map[key]Repo{}
	skipped := 0
	for _, r := range repos {
		org, err := uuid.Parse(r.OrgID)
		if err != nil || !providerShape.MatchString(r.Provider) || !repoIDShape.MatchString(r.ProviderRepoID) || !repokey.Valid(r.Path) || r.Path == "*" {
			skipped++
			continue
		}
		seen[key{org, r.Provider, r.ProviderRepoID}] = r
	}
	out := map[uuid.UUID][]Repo{}
	for k, r := range seen {
		out[k.org] = append(out[k.org], r)
	}
	for _, rs := range out {
		sort.Slice(rs, func(i, j int) bool {
			if rs[i].Provider != rs[j].Provider {
				return rs[i].Provider < rs[j].Provider
			}
			return rs[i].ProviderRepoID < rs[j].ProviderRepoID
		})
	}
	return out, skipped
}

// Run reads the source and applies each organization. A failed organization is counted and the run goes on.
func Run(ctx context.Context, src Source, sink Sink) (Report, error) {
	repos, err := src.Repos(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("repoidentity: read source: %w", err)
	}
	groups, skipped := Group(repos)
	rep := Report{SkippedInvalid: skipped}
	stale, err := sink.OrgsWithIdentities(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("repoidentity: list organizations: %w", err)
	}
	for _, org := range stale {
		if _, ok := groups[org]; !ok {
			groups[org] = nil
		}
	}
	orgs := make([]uuid.UUID, 0, len(groups))
	for org := range groups {
		orgs = append(orgs, org)
	}
	sort.Slice(orgs, func(i, j int) bool { return orgs[i].String() < orgs[j].String() })
	for _, org := range orgs {
		rep.Orgs++
		rep.Repos += len(groups[org])
		res, err := sink.Apply(ctx, org, groups[org])
		if err != nil {
			rep.OrgsFailed++
			continue
		}
		if res.Changed {
			rep.OrgsChanged++
		}
		rep.UnresolvedTotal += res.UnresolvedTotal
		for _, id := range res.Unresolved {
			if len(rep.Unresolved) < MaxListedGrants {
				rep.Unresolved = append(rep.Unresolved, id)
			}
		}
	}
	return rep, nil
}

// PGSink writes through audit.Commit as the auth runtime role.
type PGSink struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
}

// OrgsWithIdentities reads the distinct organizations of the identity table.
func (s PGSink) OrgsWithIdentities(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := s.Pool.Query(ctx, `SELECT DISTINCT organization_id FROM `+authschema.Quote(s.Schema)+`.repository_identities`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// Apply upserts the organization's identities, removes the ones the source no longer lists, backfills the live
// repository grants whose path names exactly one repository, and reads the grants still without an id.
func (s PGSink) Apply(ctx context.Context, org uuid.UUID, repos []Repo) (OrgResult, error) {
	q := authschema.Quote(s.Schema)
	providers, ids, paths := make([]string, len(repos)), make([]string, len(repos)), make([]string, len(repos))
	for i, r := range repos {
		providers[i], ids[i], paths[i] = r.Provider, r.ProviderRepoID, r.Path
	}
	orgText := org.String()
	err := audit.Commit(ctx, s.Pool, s.Schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			// The commit guard counts a row when its BEFORE INSERT trigger fires, also when ON CONFLICT then does
			// nothing. So a changed path is an UPDATE of the matched row, and the INSERT proposes only missing rows:
			// a replay with the same input writes no row and no audit row.
			if _, err := tx.Exec(ctx, `UPDATE `+q+`.repository_identities t SET current_path = u.c, updated_at = now()
FROM unnest($2::text[], $3::text[], $4::text[]) AS u(p, i, c)
WHERE t.organization_id = $1 AND t.provider = u.p AND t.provider_repo_id = u.i AND t.current_path IS DISTINCT FROM u.c`, org, providers, ids, paths); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO `+q+`.repository_identities (organization_id, provider, provider_repo_id, current_path)
SELECT $1, u.p, u.i, u.c FROM unnest($2::text[], $3::text[], $4::text[]) AS u(p, i, c)
WHERE NOT EXISTS (SELECT 1 FROM `+q+`.repository_identities r WHERE r.organization_id = $1 AND r.provider = u.p AND r.provider_repo_id = u.i)
ON CONFLICT (organization_id, provider, provider_repo_id) DO NOTHING`, org, providers, ids, paths); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM `+q+`.repository_identities WHERE organization_id = $1
AND (provider, provider_repo_id) NOT IN (SELECT t.p, t.i FROM unnest($2::text[], $3::text[]) AS t(p, i))`, org, providers, ids); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE `+q+`.resource_grants g SET resource_repo_id = u.rid, updated_at = now()
FROM (SELECT current_path, min(provider || ':' || provider_repo_id) AS rid FROM `+q+`.repository_identities
      WHERE organization_id = $1 GROUP BY current_path HAVING count(*) = 1) u
WHERE g.organization_id = $1 AND g.resource_kind = 'repository' AND g.resource_id <> '*'
AND g.revoked_at IS NULL AND g.resource_repo_id IS NULL AND g.resource_id = u.current_path`, org)
			return err
		},
		Audit:     audit.AuditEvent{EventType: "auth.repository_identity.synced", Outcome: audit.OutcomeAllowed, OrganizationID: &orgText},
		OffStream: audit.OffStreamRepositoryIdentity,
	})
	res := OrgResult{Changed: err == nil}
	if errors.Is(err, audit.ErrNoStateChange) {
		err = nil
	}
	if err != nil {
		return OrgResult{}, err
	}
	rows, err := s.Pool.Query(ctx, `SELECT id FROM `+q+`.resource_grants WHERE organization_id = $1 AND resource_kind = 'repository'
AND resource_id <> '*' AND revoked_at IS NULL AND resource_repo_id IS NULL ORDER BY id`, org)
	if err != nil {
		return OrgResult{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return OrgResult{}, err
		}
		res.UnresolvedTotal++
		if len(res.Unresolved) < MaxListedGrants {
			res.Unresolved = append(res.Unresolved, id)
		}
	}
	return res, rows.Err()
}
