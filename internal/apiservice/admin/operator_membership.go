package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
)

// ErrNoProvisioner refuses a membership change or a real organization delete
// when the CLI holds no client of auth-service's provisioning routes.
var ErrNoProvisioner = &OperatorError{Message: "membership changes and organization deletes go through the authentication service: AUTH_PROXY_URL and the workload inputs (AUTH_WORKLOAD_SUBJECT_FILE or AUTH_WORKLOAD_BOOTSTRAP_FILE, and AUTH_WORKLOAD_ISSUER) must be set; nothing was written."}

// ErrActorRequired refuses a membership change or a real organization delete with no --actor.
var ErrActorRequired = &OperatorError{Message: "--actor is required: the id or email of an active owner or admin of the organization, or of a superuser (a superuser for an organization delete)."}

// ErrOneMembershipStep refuses an update with both --org and --remove-from-org: a second step could fail after the first one was sent.
var ErrOneMembershipStep = &OperatorError{Message: "run one membership change per update"}

func (o Operator) requireProvisioning(actor string) error {
	if o.Provisioner == nil {
		return ErrNoProvisioner
	}
	if strings.TrimSpace(actor) == "" {
		return ErrActorRequired
	}
	return nil
}

// resolveActor is the actor's id: --actor as an id is used as given, an email is
// read from public.users. Whether the actor exists and may act is auth-service's
// rule, run under its locks.
func (o Operator) resolveActor(ctx context.Context, actor string) (uuid.UUID, error) {
	var db pgDB = o.Pool
	if o.db != nil {
		db = o.db
	}
	return resolveActorOn(ctx, o.store(db), strings.TrimSpace(actor))
}

func resolveActorOn(ctx context.Context, store pgStore, actor string) (uuid.UUID, error) {
	if !strings.Contains(actor, "@") {
		id, err := pyUUID(actor)
		if err != nil {
			return uuid.Nil, refuse("--actor must be an email or a user id.")
		}
		return id, nil
	}
	user, err := store.actorByEmail(ctx, actor)
	if err != nil {
		return uuid.Nil, err
	}
	if user == nil {
		return uuid.Nil, refuse("Actor not found.")
	}
	return user.ID, nil
}

type membershipKind int

const (
	membershipAdd membershipKind = iota
	membershipRole
	membershipRemove
	membershipAbsent
)

type membershipStep struct {
	kind  membershipKind
	org   organization
	role  string
	label string
}

type membershipPlan struct {
	actor uuid.UUID
	steps []membershipStep
}

// planMembershipChanges reads what the update needs (the actor, the
// organizations, the current memberships) inside the update's transaction and
// writes nothing: the writes are auth-service's, after the transaction.
func planMembershipChanges(ctx context.Context, store pgStore, userID uuid.UUID, in UpdateUserInput, plan *membershipPlan) error {
	if in.MembershipOrg != nil {
		org, err := resolveOrg(ctx, store, *in.MembershipOrg)
		if err != nil {
			return err
		}
		if org == nil {
			return refuse("Organization '%s' not found.", *in.MembershipOrg)
		}
		existing, err := store.membershipByOrgUser(ctx, org.ID, userID)
		if err != nil {
			return err
		}
		role := "member"
		kind := membershipAdd
		if existing != nil {
			role, kind = existing.Role, membershipRole
		}
		if in.Role != nil && *in.Role != "" {
			role = *in.Role
		}
		plan.steps = append(plan.steps, membershipStep{kind: kind, org: *org, role: role})
	}
	if in.RemoveFromOrg != nil {
		org, err := resolveOrg(ctx, store, *in.RemoveFromOrg)
		if err != nil {
			return err
		}
		if org == nil {
			return refuse("Organization '%s' not found.", *in.RemoveFromOrg)
		}
		existing, err := store.membershipByOrgUser(ctx, org.ID, userID)
		if err != nil {
			return err
		}
		kind := membershipRemove
		if existing == nil {
			kind = membershipAbsent
		}
		plan.steps = append(plan.steps, membershipStep{kind: kind, org: *org})
	}
	return nil
}

// applyMembershipPlan sends each step to auth-service, which writes both
// planes and the audit in one commit. It returns the changes done so far, and
// the error of the first step that failed.
func (o Operator) applyMembershipPlan(ctx context.Context, plan membershipPlan, userID uuid.UUID) ([]string, error) {
	var changes []string
	for _, step := range plan.steps {
		var err error
		switch step.kind {
		case membershipAdd:
			_, err = o.Provisioner.AddMember(ctx, step.org.ID, userID, plan.actor, step.role, nil)
			changes = append(changes, fmt.Sprintf("added to org '%s' as %s", step.org.Slug, step.role))
		case membershipRole:
			_, err = o.Provisioner.ChangeRole(ctx, step.org.ID, userID, plan.actor, step.role)
			changes = append(changes, fmt.Sprintf("org '%s' role -> %s", step.org.Slug, step.role))
		case membershipRemove:
			err = o.Provisioner.RemoveMember(ctx, step.org.ID, userID, plan.actor)
			changes = append(changes, fmt.Sprintf("removed from org '%s'", step.org.Slug))
		case membershipAbsent:
			changes = append(changes, fmt.Sprintf("not a member of org '%s' (no change)", step.org.Slug))
		}
		if err != nil {
			return changes[:len(changes)-1], provisionError(err, step.role)
		}
	}
	return changes, nil
}

// provisionError is the refusal the operator reads for an auth-service answer.
// An answer that names no refusal below (a 5xx, a lost connection, an unknown
// reason) is returned as it is: a failure, with nothing written.
func provisionError(err error, role string) error {
	var operator *OperatorError
	if errors.As(err, &operator) {
		return operator
	}
	switch authprovision.ReasonOf(err) {
	case "role_invalid":
		return refuse("Invalid role: %s", role)
	case "already_member":
		return refuse("User is already a member of this organization")
	case "membership_not_found":
		return refuse("Membership not found")
	case "last_owner":
		return refuse("Cannot remove the last owner of an organization")
	case "superuser_required":
		return refuse("Superuser access required")
	case "admin_required":
		return refuse("Admin access required for organization")
	case "principal_deactivated":
		return refuse("The actor is deactivated")
	case "organization_not_found":
		return refuse("Organization not found.")
	}
	var refused *authprovision.Refused
	if errors.As(err, &refused) {
		return refuse("The authentication service refused the change (status %d, reason %q); nothing was written.", refused.Status, refused.Reason)
	}
	return fmt.Errorf("the authentication service did not take the change; nothing was written: %w", err)
}

// actorProvisioner reads the actor's id when the deprovision call is made.
type actorProvisioner struct {
	authprovision.Provisioner
	actor   string
	resolve func(context.Context, string) (uuid.UUID, error)
}

func (p actorProvisioner) Deprovision(ctx context.Context, org, _ uuid.UUID) error {
	actor, err := p.resolve(ctx, p.actor)
	if err != nil {
		return err
	}
	return p.Provisioner.Deprovision(ctx, org, actor)
}
