// Package stripeclient is the one place `dho api` builds its Stripe client
// (github.com/stripe/stripe-go), the Go counterpart of the Python api's
// api/billing/stripe_client.py get_stripe_client().
//
// stripe-go is pinned (go.mod) to the release whose API version equals the
// one the Python SDK in uv.lock sends, so both planes ask Stripe for the same
// object shapes; TestPinnedAPIVersionMatchesPythonSDK holds the two together.
package stripeclient

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/stripe/stripe-go/v86"
)

// ErrKeyMissing is get_stripe_client's RuntimeError text when
// STRIPE_SECRET_KEY is unset; the checkout and portal routes answer it as
// their 500 detail.
var ErrKeyMissing = errors.New("STRIPE_SECRET_KEY environment variable is not set")

// Provider builds Stripe clients. The zero value (no key) refuses every
// request with ErrKeyMissing, as the Python api does without the key.
type Provider struct {
	key     string
	backend *stripe.Backends
}

// Options configure New.
type Options struct {
	// Key is the Stripe secret key ("" = not configured).
	Key string
	// BaseURL overrides Stripe's API base. Only a test sets it (a fake
	// Stripe server); production always talks to Stripe itself.
	BaseURL string
	// HTTPClient is the transport (nil = an 80 s timeout, the Python SDK's).
	// Whatever is given is wrapped by pythonIdempotency.
	HTTPClient *http.Client
}

// New returns a Provider for options.
func New(options Options) *Provider {
	if options.Key == "" {
		return &Provider{}
	}
	client := &http.Client{Timeout: 80 * time.Second}
	if options.HTTPClient != nil {
		copied := *options.HTTPClient
		client = &copied
	}
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.Transport = pythonIdempotency{base: base}
	config := &stripe.BackendConfig{
		HTTPClient: client,
		// The SDK's own logger writes to stderr; route failures are logged
		// by the caller with the route's fields instead.
		LeveledLogger: &stripe.LeveledLogger{Level: stripe.LevelNull},
	}
	if options.BaseURL != "" {
		config.URL = stripe.String(options.BaseURL)
	}
	return &Provider{key: options.Key, backend: stripe.NewBackendsWithConfig(config)}
}

// Client returns a client, or ErrKeyMissing.
func (p *Provider) Client() (*stripe.Client, error) {
	if p == nil || p.key == "" {
		return nil, ErrKeyMissing
	}
	return stripe.NewClient(p.key, stripe.WithBackends(p.backend)), nil
}

// RawGet is a GET of path (with its query) on Stripe's API as raw JSON: the request
// the typed client would send, with none of the SDK's decoding. A caller that reads
// only some fields of an object needs it: the typed decoder refuses the whole
// response for one field of the wrong type, where a reader of raw JSON never looks
// at that field. A Stripe error answer is the returned error, as for a typed call.
func (p *Provider) RawGet(ctx context.Context, path string) ([]byte, error) {
	if p == nil || p.key == "" {
		return nil, ErrKeyMissing
	}
	backend, ok := p.backend.API.(stripe.RawRequestBackend)
	if !ok {
		return nil, errors.New("stripe backend cannot make raw requests")
	}
	response, err := backend.RawRequest(http.MethodGet, path, p.key, "", &stripe.RawParams{Params: stripe.Params{Context: ctx}})
	if err != nil {
		return nil, err
	}
	return response.RawJSON, nil
}

// pythonIdempotency sends the Idempotency-Key header the way the Python SDK
// does: on POST only. stripe-go also adds one to DELETE (a subscription
// cancel); Stripe applies a DELETE once either way, and dropping it keeps
// both planes' requests identical.
type pythonIdempotency struct{ base http.RoundTripper }

func (p pythonIdempotency) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method != http.MethodPost && request.Header.Get("Idempotency-Key") != "" {
		request = request.Clone(request.Context())
		request.Header.Del("Idempotency-Key")
	}
	return p.base.RoundTrip(request)
}

// APIVersion is the Stripe API version every request carries.
const APIVersion = stripe.APIVersion
