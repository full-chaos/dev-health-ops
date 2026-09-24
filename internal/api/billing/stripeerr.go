package billing

import (
	"errors"

	"github.com/stripe/stripe-go/v86"
)

// pyStripeError is str() of the exception the Python SDK raises for err:
// "Request <id>: <message>" for a Stripe API error (the message, or
// "<empty message>"), else Go's own text for a transport failure, which the
// Python SDK words differently (a named limit of the pull report).
func pyStripeError(err error) string {
	var apiError *stripe.Error
	if errors.As(err, &apiError) {
		message := apiError.Msg
		if message == "" {
			message = "<empty message>"
		}
		if apiError.RequestID != "" {
			return "Request " + apiError.RequestID + ": " + message
		}
		return message
	}
	return err.Error()
}
