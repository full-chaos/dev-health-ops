package usersbackfill

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// Diff classes of the organization reconciliation, in report order.
const (
	ClassOrgMissingInAuth         = "org_missing_in_auth"
	ClassOrgExtraInAuth           = "org_extra_in_auth"
	ClassOrgSlug                  = "org_slug_diff"
	ClassOrgName                  = "org_name_diff"
	ClassOrgActive                = "org_active_diff"
	ClassMemberPublicMissing      = "membership_public_missing"
	ClassMemberInactiveInPublic   = "membership_inactive_in_public"
	ClassMemberAuthMissing        = "membership_auth_missing"
	ClassMemberRole               = "membership_role_diff"
	ClassMemberDuplicatePublicSrc = "membership_duplicate_public_source"
	defaultGatedClassCount        = 7
)

// OrgClasses lists every class in report order. The first defaultGatedClassCount classes fail the default
// gate: the organization diffs, the auth-only memberships (the 4.6 stop signal: auth holds a membership public does
// not) and a membership auth holds inactive while public still has the member. The rest are printed in the default
// mode (go-api still changes roles and removes members in public until the flip) and fail only the -exact gate.
var OrgClasses = []string{
	ClassOrgMissingInAuth, ClassOrgExtraInAuth, ClassOrgSlug, ClassOrgName, ClassOrgActive,
	ClassMemberPublicMissing, ClassMemberInactiveInPublic,
	ClassMemberAuthMissing, ClassMemberRole, ClassMemberDuplicatePublicSrc,
}

// OrgMismatch is one organization with at least one diff class: its id and the class names, never a value.
type OrgMismatch struct {
	OrgID   uuid.UUID
	Classes []string
}

// OrgDiffs are the counts of one read-only snapshot of public.organizations and public.memberships against the
// auth schema. Membership counts are over (organization, user) pairs; Members is read over the auth principals of
// kind user.
type OrgDiffs struct {
	PublicOrgs, AuthOrgs, Matched                              int
	MissingInAuth, ExtraInAuth, SlugDiff, NameDiff, ActiveDiff int
	Members                                                    MembershipDiffs
	DuplicatePublicSource                                      int
	NonUserPrincipalMemberships                                int
	PublicMemberships, AuthMemberships                         int
	// Mismatches lists every organization with a diff, in id order.
	Mismatches []OrgMismatch
}

// OrgsTarget reads the snapshot counts.
type OrgsTarget interface {
	OrgDiffs(ctx context.Context) (OrgDiffs, error)
}

// OrgMismatchCap is the number of mismatching organization ids a report lists.
const OrgMismatchCap = 50

// OrgsReconciliation is the outcome of a read-only comparison of the organization aggregates.
type OrgsReconciliation struct {
	Exact             bool
	Diffs             OrgDiffs
	Listed            []OrgMismatch
	MismatchOrgsTotal int
}

// ReconcileOrgs reads one snapshot and builds the report. A failed read stops it with an error that names no value.
func ReconcileOrgs(ctx context.Context, tgt OrgsTarget, exact bool) (OrgsReconciliation, error) {
	d, err := tgt.OrgDiffs(ctx)
	if err != nil {
		return OrgsReconciliation{Exact: exact}, errors.New("usersbackfill: reading the organization aggregates failed")
	}
	rep := OrgsReconciliation{Exact: exact, Diffs: d, MismatchOrgsTotal: len(d.Mismatches)}
	n := len(d.Mismatches)
	if n > OrgMismatchCap {
		n = OrgMismatchCap
	}
	rep.Listed = d.Mismatches[:n]
	return rep, nil
}

func (d OrgDiffs) count(class string) int {
	switch class {
	case ClassOrgMissingInAuth:
		return d.MissingInAuth
	case ClassOrgExtraInAuth:
		return d.ExtraInAuth
	case ClassOrgSlug:
		return d.SlugDiff
	case ClassOrgName:
		return d.NameDiff
	case ClassOrgActive:
		return d.ActiveDiff
	case ClassMemberPublicMissing:
		return d.Members.PublicMissing
	case ClassMemberInactiveInPublic:
		return d.Members.InactiveInPublic
	case ClassMemberAuthMissing:
		return d.Members.AuthMissing
	case ClassMemberRole:
		return d.Members.RoleDiff
	case ClassMemberDuplicatePublicSrc:
		return d.DuplicatePublicSource
	}
	return 0
}

// GatedTotal is the number of gated diffs: zero is the gate. The default mode gates the first defaultGatedClassCount
// classes, -exact gates every class.
func (r OrgsReconciliation) GatedTotal() int {
	gated := OrgClasses[:defaultGatedClassCount]
	if r.Exact {
		gated = OrgClasses
	}
	n := 0
	for _, c := range gated {
		n += r.Diffs.count(c)
	}
	return n
}

// Lines renders the report as stable key=value lines: class names, counts and organization ids only, never a
// slug, name, email or any other value.
func (r OrgsReconciliation) Lines() []string {
	gate := "default"
	if r.Exact {
		gate = "exact"
	}
	d := r.Diffs
	lines := []string{
		"mode=read-only",
		"gate=" + gate,
		fmt.Sprintf("orgs_public=%d", d.PublicOrgs),
		fmt.Sprintf("orgs_auth=%d", d.AuthOrgs),
		fmt.Sprintf("orgs_matched=%d", d.Matched),
		fmt.Sprintf("memberships_public=%d", d.PublicMemberships),
		fmt.Sprintf("memberships_auth=%d", d.AuthMemberships),
	}
	for _, c := range OrgClasses {
		lines = append(lines, fmt.Sprintf("diff.%s=%d", c, d.count(c)))
	}
	lines = append(lines,
		fmt.Sprintf("info.membership_non_user_principals=%d", d.NonUserPrincipalMemberships),
		fmt.Sprintf("gated_diffs=%d", r.GatedTotal()),
		fmt.Sprintf("mismatch_orgs_total=%d", r.MismatchOrgsTotal),
		fmt.Sprintf("mismatch_listed=%d", len(r.Listed)),
	)
	for _, m := range r.Listed {
		lines = append(lines, fmt.Sprintf("mismatch.org=%s classes=%s", m.OrgID, strings.Join(m.Classes, ",")))
	}
	return lines
}

// orgsReconcileTargetFn opens the snapshot target of the verb and returns its closer; tests replace it.
var orgsReconcileTargetFn = openOrgsReconcileTarget

func openOrgsReconcileTarget(ctx context.Context, env cli.Env, verb string) (OrgsTarget, func(), int) {
	pool, schema, code := connectFn(ctx, env, verb)
	if code != cli.ExitOK {
		return nil, func() {}, code
	}
	tx, err := OpenSnapshot(ctx, pool)
	if err != nil {
		fmt.Fprintln(env.Stderr, verb+": could not open one read-only snapshot of the public tables and the auth schema; stopping")
		pool.Close()
		return nil, func() {}, cli.ExitFailure
	}
	closer := func() {
		_ = tx.Rollback(context.Background())
		pool.Close()
	}
	return TxTarget{Tx: tx, Schema: schema}, closer, cli.ExitOK
}

// OrgDiffs counts inside the snapshot transaction. Counts and organization ids only: no slug, name or email leaves
// the queries.
func (t TxTarget) OrgDiffs(ctx context.Context) (OrgDiffs, error) {
	q := authschema.Quote(t.Schema)
	var d OrgDiffs
	err := t.Tx.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM public.organizations),
  (SELECT count(*) FROM `+q+`.organizations),
  (SELECT count(*) FROM public.organizations p JOIN `+q+`.organizations a ON a.id = p.id
     WHERE p.slug = a.slug AND p.name = a.name AND p.is_active = (a.disabled_at IS NULL)),
  (SELECT count(*) FROM (SELECT 1 FROM public.memberships GROUP BY org_id, user_id) x),
  (SELECT count(*) FROM `+q+`.memberships m JOIN `+q+`.principals pr ON pr.id = m.principal_id WHERE pr.kind = 'user'),
  (SELECT count(*) FROM `+q+`.memberships m JOIN `+q+`.principals pr ON pr.id = m.principal_id WHERE pr.kind <> 'user')`).
		Scan(&d.PublicOrgs, &d.AuthOrgs, &d.Matched, &d.PublicMemberships, &d.AuthMemberships, &d.NonUserPrincipalMemberships)
	if err != nil {
		return OrgDiffs{}, err
	}
	rows, err := t.Tx.Query(ctx, `WITH pub AS (
  SELECT org_id, user_id, array_agg(DISTINCT role::text) AS roles, count(*) AS copies
  FROM public.memberships GROUP BY org_id, user_id
), au AS (
  SELECT m.organization_id, m.principal_id, m.status, m.role
  FROM `+q+`.memberships m JOIN `+q+`.principals pr ON pr.id = m.principal_id AND pr.kind = 'user'
), pairs AS (
  SELECT COALESCE(a.organization_id, p.org_id) AS org_id,
         a.principal_id IS NOT NULL AS in_auth, p.user_id IS NOT NULL AS in_public, a.status, a.role, p.roles, p.copies
  FROM au a FULL JOIN pub p ON p.org_id = a.organization_id AND p.user_id = a.principal_id
), mem AS (
  SELECT org_id,
    count(*) FILTER (WHERE in_auth AND status = 'active' AND NOT in_public) AS public_missing,
    count(*) FILTER (WHERE in_auth AND status <> 'active' AND in_public) AS inactive_in_public,
    count(*) FILTER (WHERE NOT in_auth AND in_public) AS auth_missing,
    count(*) FILTER (WHERE in_auth AND status = 'active' AND in_public AND (role IS NULL OR roles <> ARRAY[role]::text[])) AS role_diff,
    count(*) FILTER (WHERE in_public AND copies > 1) AS dup_source
  FROM pairs GROUP BY org_id
), org AS (
  SELECT COALESCE(p.id, a.id) AS org_id,
    (p.id IS NOT NULL AND a.id IS NULL) AS missing_in_auth,
    (a.id IS NOT NULL AND p.id IS NULL) AS extra_in_auth,
    (p.id IS NOT NULL AND a.id IS NOT NULL AND p.slug <> a.slug) AS slug_diff,
    (p.id IS NOT NULL AND a.id IS NOT NULL AND p.name <> a.name) AS name_diff,
    (p.id IS NOT NULL AND a.id IS NOT NULL AND p.is_active <> (a.disabled_at IS NULL)) AS active_diff
  FROM public.organizations p FULL JOIN `+q+`.organizations a ON a.id = p.id
)
SELECT COALESCE(o.org_id, m.org_id),
  COALESCE(o.missing_in_auth, false), COALESCE(o.extra_in_auth, false), COALESCE(o.slug_diff, false),
  COALESCE(o.name_diff, false), COALESCE(o.active_diff, false),
  COALESCE(m.public_missing, 0), COALESCE(m.inactive_in_public, 0), COALESCE(m.auth_missing, 0),
  COALESCE(m.role_diff, 0), COALESCE(m.dup_source, 0)
FROM org o FULL JOIN mem m ON m.org_id = o.org_id
WHERE COALESCE(o.missing_in_auth OR o.extra_in_auth OR o.slug_diff OR o.name_diff OR o.active_diff, false)
   OR COALESCE(m.public_missing, 0) > 0 OR COALESCE(m.inactive_in_public, 0) > 0 OR COALESCE(m.auth_missing, 0) > 0
   OR COALESCE(m.role_diff, 0) > 0 OR COALESCE(m.dup_source, 0) > 0
ORDER BY 1`)
	if err != nil {
		return OrgDiffs{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		var missing, extra, slug, name, active bool
		var pubMissing, inactive, authMissing, role, dup int
		if err := rows.Scan(&id, &missing, &extra, &slug, &name, &active, &pubMissing, &inactive, &authMissing, &role, &dup); err != nil {
			return OrgDiffs{}, err
		}
		var classes []string
		add := func(on bool, class string, n *int, by int) {
			if on {
				classes = append(classes, class)
				*n += by
			}
		}
		add(missing, ClassOrgMissingInAuth, &d.MissingInAuth, 1)
		add(extra, ClassOrgExtraInAuth, &d.ExtraInAuth, 1)
		add(slug, ClassOrgSlug, &d.SlugDiff, 1)
		add(name, ClassOrgName, &d.NameDiff, 1)
		add(active, ClassOrgActive, &d.ActiveDiff, 1)
		add(pubMissing > 0, ClassMemberPublicMissing, &d.Members.PublicMissing, pubMissing)
		add(inactive > 0, ClassMemberInactiveInPublic, &d.Members.InactiveInPublic, inactive)
		add(authMissing > 0, ClassMemberAuthMissing, &d.Members.AuthMissing, authMissing)
		add(role > 0, ClassMemberRole, &d.Members.RoleDiff, role)
		add(dup > 0, ClassMemberDuplicatePublicSrc, &d.DuplicatePublicSource, dup)
		d.Mismatches = append(d.Mismatches, OrgMismatch{OrgID: id, Classes: classes})
	}
	if err := rows.Err(); err != nil {
		return OrgDiffs{}, err
	}
	return d, nil
}
