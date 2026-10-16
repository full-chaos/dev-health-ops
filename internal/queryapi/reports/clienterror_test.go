package reports

import (
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// Python answers a malformed report id with the constructor's own message; the
// GraphQL presenter passes a ClientError's text and masks every other error.
func TestRequestDerivedValidationErrorsAreClientErrors(t *testing.T) {
	cases := map[string]error{
		"short id":    second(ParseReportID("nope")),
		"non hex id":  second(ParseReportID("zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz")),
		"int range":   checkInt32("limit", 1<<40),
		"timezone":    validateTimezone("Mars/Base"),
		"cron fields": validateCron("* *", nil, time.Time{}),
	}
	for name, err := range cases {
		var client *policy.ClientError
		if !errors.As(err, &client) || client.Message == "" {
			t.Errorf("%s: %v is not a ClientError with a message", name, err)
		}
	}
	if got := second(ParseReportID("nope")).Error(); got != "badly formed hexadecimal UUID string" {
		t.Errorf("message = %q", got)
	}
}

func second(_ string, err error) error { return err }
