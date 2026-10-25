// Package org is the auth-service Organization aggregate: the write authority
// for auth.organizations (id, slug, name, status). Every verb commits through
// audit.Commit, so the row, its audit row, its provisioning event and its
// revision change together or not at all.
package org

import (
	"errors"
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

type Status string

const (
	StatusActive    Status = "active"
	StatusSuspended Status = "suspended"
)

type verb string

const (
	verbCreate     verb = "create"
	verbRename     verb = "rename"
	verbSuspend    verb = "suspend"
	verbReactivate verb = "reactivate"
)

type Organization struct {
	ID          uuid.UUID
	Slug        string
	Name        string
	Status      Status
	SuspendedAt *time.Time
	Revision    int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type Actor struct {
	PrincipalID *uuid.UUID
	RequestID   *string
}

type state struct {
	name   string
	active bool
}

type decision struct {
	noop bool
	next state
}

var slugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,62}$`)

// ValidateSlug refuses a slug outside the provisioning wire pattern. A slug is
// refused, never rewritten.
func ValidateSlug(slug string) error {
	if !slugPattern.MatchString(slug) {
		return ErrSlugInvalid
	}
	return nil
}

// NormalizeName strips the name and bounds it to 1..100 runes (go-api
// validate_organization_name).
func NormalizeName(name string) (string, error) {
	n := pythonparity.Strip(name)
	if n == "" || utf8.RuneCountInString(n) > 100 {
		return "", ErrNameInvalid
	}
	return n, nil
}

// decide is the whole transition table. A repeat of the current state is a
// no-op; a suspended organization cannot be renamed.
func decide(v verb, cur state, newName string) (decision, error) {
	switch v {
	case verbRename:
		if !cur.active {
			return decision{}, ErrSuspended
		}
		if cur.name == newName {
			return decision{noop: true, next: cur}, nil
		}
		return decision{next: state{name: newName, active: true}}, nil
	case verbSuspend:
		if !cur.active {
			return decision{noop: true, next: cur}, nil
		}
		return decision{next: state{name: cur.name}}, nil
	case verbReactivate:
		if cur.active {
			return decision{noop: true, next: cur}, nil
		}
		return decision{next: state{name: cur.name, active: true}}, nil
	}
	return decision{}, errors.New("org: unknown verb")
}

func checkRevision(current int64, ifRevision *int64) error {
	if ifRevision != nil && *ifRevision != current {
		return ErrRevisionConflict
	}
	return nil
}

// eventFor is the provisioning event of a verb. The name is free text and is
// never carried; the slug rides only on create.
func eventFor(v verb, orgID uuid.UUID, slug string) audit.OutboxEvent {
	change := map[string]any{}
	var eventType string
	switch v {
	case verbCreate:
		eventType = "organization.created"
		change["status"] = "active"
		change["slug"] = slug
	case verbRename:
		eventType = "organization.updated"
	case verbSuspend:
		eventType = "organization.suspended"
		change["status"] = "suspended"
	case verbReactivate:
		eventType = "organization.reactivated"
		change["status"] = "active"
	}
	return audit.OutboxEvent{AggregateType: "organization",
		AggregateID: provisioning.WireID("organization", orgID.String()), EventType: eventType, Change: change}
}

func auditFor(v verb, orgID uuid.UUID, a Actor, prev, next Status) audit.AuditEvent {
	org := orgID.String()
	ev := audit.AuditEvent{Outcome: audit.OutcomeAllowed, OrganizationID: &org, RequestID: a.RequestID,
		Attributes: map[string]any{"target_kind": "organization", "target_id": org}}
	if a.PrincipalID != nil {
		actor := a.PrincipalID.String()
		ev.ActorPrincipalID = &actor
	}
	switch v {
	case verbCreate:
		ev.EventType = "auth.organization.created"
		ev.Attributes["reason_code"] = "organization_created"
		ev.Attributes["new_status"] = string(next)
	case verbRename:
		ev.EventType = "auth.organization.renamed"
		ev.Attributes["reason_code"] = "organization_updated"
		ev.Attributes["changed_fields"] = []string{"display_name"}
	case verbSuspend:
		ev.EventType = "auth.organization.suspended"
		ev.Attributes["reason_code"] = "organization_updated"
		ev.Attributes["changed_fields"] = []string{"status", "disabled_at"}
		ev.Attributes["previous_status"] = string(prev)
		ev.Attributes["new_status"] = string(next)
	case verbReactivate:
		ev.EventType = "auth.organization.reactivated"
		ev.Attributes["reason_code"] = "organization_updated"
		ev.Attributes["changed_fields"] = []string{"status", "disabled_at"}
		ev.Attributes["previous_status"] = string(prev)
		ev.Attributes["new_status"] = string(next)
	}
	return ev
}

// outcomeLabel maps an error to the closed outcome label set of the mutation
// counter. No id, slug or name reaches a label.
func outcomeLabel(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, audit.ErrNoStateChange):
		return "noop"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrSlugTaken):
		return "slug_taken"
	case errors.Is(err, ErrRevisionConflict):
		return "conflict"
	case errors.Is(err, ErrSlugInvalid), errors.Is(err, ErrNameInvalid):
		return "invalid"
	case errors.Is(err, ErrSuspended):
		return "suspended"
	}
	return "error"
}
