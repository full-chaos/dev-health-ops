package usersbackfill

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// OrgRow is the part of one public.organizations row the backfill reads.
type OrgRow struct {
	ID     uuid.UUID
	Slug   string
	Name   string
	Active bool
}

// MemberRow is the part of one public.memberships row the backfill reads.
// Copies is the number of source rows that share its (org, user) key.
type MemberRow struct {
	OrgID     uuid.UUID
	UserID    uuid.UUID
	Role      *string
	JoinedAt  *time.Time
	CreatedAt *time.Time
	// InvitedByID is public.memberships.invited_by_id.
	InvitedByID *uuid.UUID
	Copies      int
}

// MemberKey is the keyset position of a membership page.
type MemberKey struct{ OrgID, UserID uuid.UUID }

// Skip classes of the organization and membership backfill. A skipped row is
// counted, not written, and fails the run.
const (
	SkipOrgInvalid             = "org_invalid"
	SkipOrgSlugCollision       = "org_slug_collision"
	SkipOrgStoreError          = "org_store_error"
	SkipMembershipUnknownRole  = "membership_unknown_role"
	SkipMembershipOrgMissing   = "membership_org_missing"
	SkipMembershipNoPrincipal  = "membership_principal_missing"
	SkipMembershipDuplicateSrc = "membership_duplicate_source"
	SkipMembershipNoTimestamp  = "membership_no_timestamp"
	SkipMembershipStoreError   = "membership_store_error"
)

// OrgSource pages public.organizations and public.memberships in key order. A
// nil after starts at the first row.
type OrgSource interface {
	PageOrgs(ctx context.Context, after *uuid.UUID, limit int) ([]OrgRow, error)
	PageMemberships(ctx context.Context, after *MemberKey, limit int) ([]MemberRow, error)
}

// OrgSink writes one organization or membership. *identity.Store satisfies it.
type OrgSink interface {
	BackfillOrganization(ctx context.Context, t identity.OrgBackfillTarget, dryRun bool) (identity.OrgBackfillResult, error)
	BackfillMembership(ctx context.Context, t identity.MembershipBackfillTarget, dryRun bool) (identity.MembershipBackfillResult, error)
}

// AuthOnly counts the auth rows that have no public row.
type AuthOnly struct{ Orgs, Memberships int }

// AuthOnlyReader counts auth-only rows once the run has finished.
type AuthOnlyReader interface {
	AuthOnly(ctx context.Context) (AuthOnly, error)
}

// OrgReport is the outcome of a run.
type OrgReport struct {
	DryRun               bool
	OrgsSeen             int
	OrgsInserted         int
	OrgsUpdated          int
	OrgsReactivated      int
	OrgsSuspended        int
	OrgsUnchanged        int
	MembershipsSeen      int
	MembershipsInserted  int
	MembershipsRoleSet   int
	InvitersSet          int
	JoinedSet            int
	InviterUnresolved    int
	MembershipsUnchanged int
	AuthDiverged         int
	AuthOnlyOrgs         int
	AuthOnlyMemberships  int
	Skipped              map[string]int
}

// SkippedTotal is the number of rows that were not written.
func (r OrgReport) SkippedTotal() int {
	n := 0
	for _, c := range r.Skipped {
		n += c
	}
	return n
}

// Lines renders the report as stable key=value lines. auth_only_memberships
// above zero means the auth schema holds a membership public.memberships does
// not: STOP for the cutover until it is explained.
func (r OrgReport) Lines() []string {
	mode := "write"
	if r.DryRun {
		mode = "dry-run"
	}
	lines := []string{
		"mode=" + mode,
		fmt.Sprintf("orgs_seen=%d", r.OrgsSeen),
		fmt.Sprintf("orgs_inserted=%d", r.OrgsInserted),
		fmt.Sprintf("orgs_updated=%d", r.OrgsUpdated),
		fmt.Sprintf("orgs_reactivated=%d", r.OrgsReactivated),
		fmt.Sprintf("orgs_suspended=%d", r.OrgsSuspended),
		fmt.Sprintf("orgs_unchanged=%d", r.OrgsUnchanged),
		fmt.Sprintf("memberships_seen=%d", r.MembershipsSeen),
		fmt.Sprintf("memberships_inserted=%d", r.MembershipsInserted),
		fmt.Sprintf("memberships_role_updated=%d", r.MembershipsRoleSet),
		fmt.Sprintf("memberships_inviter_set=%d", r.InvitersSet),
		fmt.Sprintf("memberships_joined_set=%d", r.JoinedSet),
		fmt.Sprintf("memberships_inviter_unresolved=%d", r.InviterUnresolved),
		fmt.Sprintf("memberships_unchanged=%d", r.MembershipsUnchanged),
		fmt.Sprintf("auth_diverged_memberships=%d", r.AuthDiverged),
		fmt.Sprintf("auth_only_orgs=%d", r.AuthOnlyOrgs),
		fmt.Sprintf("auth_only_memberships=%d", r.AuthOnlyMemberships),
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
	return lines
}

// MapOrg turns a row into a write target; class is a skip class, empty when
// the row is writable.
func MapOrg(row OrgRow) (identity.OrgBackfillTarget, string) {
	t := identity.OrgBackfillTarget{ID: row.ID, Slug: row.Slug, Name: row.Name, Active: row.Active}
	if strings.TrimSpace(row.Slug) == "" || strings.TrimSpace(row.Name) == "" {
		return t, SkipOrgInvalid
	}
	return t, ""
}

// MapMembership turns a row into a write target; class is a skip class, empty
// when the row is writable. An unknown or missing role is refused, never
// coerced, and so is a row with neither joined_at nor created_at (an active
// membership needs a join time).
func MapMembership(row MemberRow) (identity.MembershipBackfillTarget, string) {
	t := identity.MembershipBackfillTarget{OrgID: row.OrgID, UserID: row.UserID, JoinedAt: row.JoinedAt, InvitedBy: row.InvitedByID}
	if t.JoinedAt == nil {
		t.JoinedAt = row.CreatedAt
	}
	if row.Copies > 1 {
		return t, SkipMembershipDuplicateSrc
	}
	if row.Role == nil || !identity.IsOrganizationRole(*row.Role) {
		return t, SkipMembershipUnknownRole
	}
	t.Role = *row.Role
	if t.JoinedAt == nil {
		return t, SkipMembershipNoTimestamp
	}
	return t, ""
}

// RunOrgs copies every organization, then every membership. Per-row failures
// are counted by class and the run continues; a failure of the source, or a
// cancelled context, stops it. In a dry run an organization that would be
// inserted does not count its memberships as missing the organization.
func RunOrgs(ctx context.Context, src OrgSource, sink OrgSink, auth AuthOnlyReader, opts Options) (OrgReport, error) {
	batch := opts.BatchSize
	if batch <= 0 {
		batch = 500
	}
	rep := OrgReport{DryRun: opts.DryRun, Skipped: map[string]int{}}
	// The membership read is tried first, by the same function the run uses, so a missing column grant fails before any write.
	if _, err := src.PageMemberships(ctx, nil, 1); err != nil {
		return rep, errors.New("usersbackfill: reading public.memberships failed")
	}
	pending := map[uuid.UUID]struct{}{}
	var afterOrg *uuid.UUID
	for {
		rows, err := src.PageOrgs(ctx, afterOrg, batch)
		if err != nil {
			return rep, errors.New("usersbackfill: reading public.organizations failed")
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if err := ctx.Err(); err != nil {
				return rep, err
			}
			rep.OrgsSeen++
			target, class := MapOrg(row)
			if class != "" {
				rep.Skipped[class]++
				continue
			}
			res, err := sink.BackfillOrganization(ctx, target, opts.DryRun)
			if err != nil {
				if errors.Is(err, identity.ErrOrganizationSlugCollision) {
					rep.Skipped[SkipOrgSlugCollision]++
				} else {
					rep.Skipped[SkipOrgStoreError]++
				}
				continue
			}
			switch {
			case res.Inserted:
				rep.OrgsInserted++
				pending[row.ID] = struct{}{}
			case res.Updated:
				rep.OrgsUpdated++
				if res.Reactivated {
					rep.OrgsReactivated++
				}
				if res.Suspended {
					rep.OrgsSuspended++
				}
			default:
				rep.OrgsUnchanged++
			}
		}
		last := rows[len(rows)-1].ID
		afterOrg = &last
	}
	var afterMember *MemberKey
	for {
		rows, err := src.PageMemberships(ctx, afterMember, batch)
		if err != nil {
			return rep, errors.New("usersbackfill: reading public.memberships failed")
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if err := ctx.Err(); err != nil {
				return rep, err
			}
			rep.MembershipsSeen++
			target, class := MapMembership(row)
			if class != "" {
				rep.Skipped[class]++
				continue
			}
			res, err := sink.BackfillMembership(ctx, target, opts.DryRun)
			if errors.Is(err, identity.ErrBackfillOrgMissing) && opts.DryRun {
				if _, ok := pending[row.OrgID]; ok {
					res, err = identity.MembershipBackfillResult{Inserted: true}, nil
				}
			}
			if err != nil {
				switch {
				case errors.Is(err, identity.ErrBackfillOrgMissing):
					rep.Skipped[SkipMembershipOrgMissing]++
				case errors.Is(err, identity.ErrBackfillPrincipalMissing):
					rep.Skipped[SkipMembershipNoPrincipal]++
				default:
					rep.Skipped[SkipMembershipStoreError]++
				}
				continue
			}
			switch {
			case res.Inserted:
				rep.MembershipsInserted++
			case res.RoleUpdated:
				rep.MembershipsRoleSet++
			case res.Diverged:
				rep.AuthDiverged++
			case !res.InviterSet && !res.JoinedSet:
				rep.MembershipsUnchanged++
			}
			if res.InviterSet {
				rep.InvitersSet++
			}
			if res.JoinedSet {
				rep.JoinedSet++
			}
			if res.InviterUnresolved {
				rep.InviterUnresolved++
			}
		}
		last := MemberKey{OrgID: rows[len(rows)-1].OrgID, UserID: rows[len(rows)-1].UserID}
		afterMember = &last
	}
	if auth != nil {
		only, err := auth.AuthOnly(ctx)
		if err != nil {
			return rep, errors.New("usersbackfill: counting auth-only rows failed")
		}
		rep.AuthOnlyOrgs, rep.AuthOnlyMemberships = only.Orgs, only.Memberships
	}
	return rep, nil
}
