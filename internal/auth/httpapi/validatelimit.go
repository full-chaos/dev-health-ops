package httpapi

import "time"

// The rate limit of POST /api/v1/auth/validate, one count for go-api and for auth-service's compatibility route.
//
// web checks each signed-in session every ValidateWebCheckInterval, once per web process: its memo is per process and per
// token. The limit is keyed by the token, so every web process that holds the session spends from the same budget. A
// process makes Window/interval checks in a window and one more when a window edge falls between two checks; the budget
// must cover the most web processes the chart can run (web.autoscaling.maxReplicas in deploy/helm/dev-health/values.yaml).
// A refused check is a transient on web's side and widens the freshness bound, so the budget is sized for the checks, not
// for abuse. TestValidateLimitCoversTheWebCheckers pins the ratio.
const (
	ValidateLimitCount  = 100
	ValidateLimitWindow = 15 * time.Minute
	// ValidateWebCheckInterval is the interval of web's session check (src/lib/authValidationMemo.ts in dev-health-web). It is a
	// number the web repository owns; this constant is the copy the ratio test uses and must move with it.
	ValidateWebCheckInterval = 60 * time.Second
)
