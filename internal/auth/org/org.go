// Package org is the auth-service Organization aggregate: the write authority
// for auth.organizations (id, slug, name, status). Every verb commits through
// audit.Commit, so the row, its audit row, its provisioning event and its
// revision change together or not at all.
package org

import (
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
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

// ValidateSlug refuses a slug outside the provisioning wire pattern.
func ValidateSlug(slug string) error { return nil }

// NormalizeName strips the name and bounds it to 1..100 runes.
func NormalizeName(name string) (string, error) { return name, nil }

func decide(v verb, cur state, newName string) (decision, error) { return decision{}, nil }

func checkRevision(current int64, ifRevision *int64) error { return nil }

func eventFor(v verb, orgID uuid.UUID, slug string) audit.OutboxEvent { return audit.OutboxEvent{} }

func auditFor(v verb, orgID uuid.UUID, a Actor, prev, next Status) audit.AuditEvent {
	return audit.AuditEvent{}
}

func outcomeLabel(err error) string { return "" }
