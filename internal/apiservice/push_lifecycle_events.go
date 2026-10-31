package apiservice

import (
	"fmt"
	"strconv"
	"strings"
)

// EnvPushLifecycleEvents turns on the External Push credential lifecycle
// events (external-push-lifecycle-event.v1): each customer-push token create,
// rotate and revoke also appends its events to the auth outbox, in the same
// transaction. It defaults OFF, and with it off the routes write nothing new
// and are byte-identical to the routes before the events existed.
//
// DO NOT turn it on before go-api's login holds the outbox grants. go-api's
// Postgres login reads the auth schema column by column and holds no write
// privilege there; with the flag on, a token write needs INSERT on
// auth_outbox_events, INSERT and UPDATE on aggregate_revisions and USAGE on the
// outbox sequence, or it fails as a 500 and rolls the token change back.
const EnvPushLifecycleEvents = "AUTH_PUSH_LIFECYCLE_EVENTS"

// ParsePushLifecycleEvents reads EnvPushLifecycleEvents: unset or empty is off,
// "true"/"false" (strconv.ParseBool) is the value, anything else is a start-up
// error.
func ParsePushLifecycleEvents(lookup func(string) (string, bool)) (bool, error) {
	raw, ok := lookup(EnvPushLifecycleEvents)
	if !ok || strings.TrimSpace(raw) == "" {
		return false, nil
	}
	on, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", EnvPushLifecycleEvents)
	}
	return on, nil
}
