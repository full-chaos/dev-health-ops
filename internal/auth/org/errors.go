package org

import "errors"

var (
	ErrNotFound         = errors.New("org: organization not found")
	ErrSlugInvalid      = errors.New("org: the slug is outside the organization slug pattern")
	ErrNameInvalid      = errors.New("org: the name is empty or longer than 100 characters")
	ErrSlugTaken        = errors.New("org: the slug belongs to another organization")
	ErrSuspended        = errors.New("org: the organization is suspended")
	ErrDenied           = errors.New("org: the actor may not make this change")
	ErrRevisionConflict = errors.New("org: the organization changed since the given revision")
	ErrStore            = errors.New("org: store unavailable")
)
