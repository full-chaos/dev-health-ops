package billing

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func itoa(n int) string { return strconv.Itoa(n) }

func joinComma(parts []string) string { return strings.Join(parts, ", ") }

var errNoPool = errors.New("billing: no database pool")

func errUnexpected(value any) error { return fmt.Errorf("billing: unexpected stored value %T", value) }

// stripeFailure labels a Stripe error for the route's log line.
func stripeFailure(call string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("stripe %s: %s", call, pyStripeError(err))
}
