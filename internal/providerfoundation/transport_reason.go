package providerfoundation

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"net"
	"syscall"

	"github.com/full-chaos/dev-health-ops/internal/egressguard"
)

// IsEgressRefused reports whether err is a provider call the dial-time egress guard refused, before or after
// providerfoundation classified it.
func IsEgressRefused(err error) bool {
	if errors.Is(err, egressguard.ErrRefused) {
		return true
	}
	var providerErr *ProviderError
	return errors.As(err, &providerErr) && providerErr.Reason == TransportReasonEgressRefused
}

const (
	TransportReasonTLSUnknownAuthority = "tls_unknown_authority"
	TransportReasonTLSCertInvalid      = "tls_cert_invalid"
	TransportReasonDNS                 = "dns"
	TransportReasonTimeout             = "timeout"
	TransportReasonConnRefused         = "conn_refused"
	TransportReasonCanceled            = "canceled"
	TransportReasonOther               = "other"
	// TransportReasonEgressRefused: the dial-time guard refused the address the stored provider URL resolves to
	// (egressguard.Wrap). Permanent: the same URL is refused on every attempt.
	TransportReasonEgressRefused = "egress_refused"
)

// classifyTransportReason maps a transport error to a closed-set reason. The
// error text is never returned: it can carry a URL with userinfo.
func classifyTransportReason(err error) string {
	var unknownAuthority x509.UnknownAuthorityError
	var certInvalid x509.CertificateInvalidError
	var hostname x509.HostnameError
	var verification *tls.CertificateVerificationError
	var dnsErr *net.DNSError
	var netErr net.Error
	switch {
	case err == nil:
		return TransportReasonOther
	case errors.Is(err, egressguard.ErrRefused):
		return TransportReasonEgressRefused
	case errors.As(err, &unknownAuthority):
		return TransportReasonTLSUnknownAuthority
	case errors.As(err, &verification), errors.As(err, &certInvalid), errors.As(err, &hostname):
		return TransportReasonTLSCertInvalid
	case errors.Is(err, context.Canceled):
		return TransportReasonCanceled
	case errors.As(err, &dnsErr):
		return TransportReasonDNS
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return TransportReasonTimeout
	case errors.Is(err, syscall.ECONNREFUSED):
		return TransportReasonConnRefused
	default:
		return TransportReasonOther
	}
}

// transportFailure builds the transient error for a request that got no HTTP
// response and logs its reason; neither the cause text nor the request path appears in the error or log (a path can carry a member login).
func (c *HTTPClient) transportFailure(ctx context.Context, cause error, attempt int) *ProviderError {
	return newTransportError(c.Provider, routeTemplate(ctx), cause, attempt)
}

func transportProviderError(provider string, cause error, attempt int) *ProviderError {
	return newTransportError(provider, "", cause, attempt)
}

func newTransportError(provider, route string, cause error, attempt int) *ProviderError {
	reason := classifyTransportReason(cause)
	attributes := []any{"provider", provider, "attempt", attempt, "reason", reason}
	if route != "" {
		attributes = append(attributes, "route", route)
	}
	slog.Warn("provider transport error", attributes...)
	if reason == TransportReasonEgressRefused {
		return &ProviderError{Class: ErrorPermanent, Reason: reason, Route: route}
	}
	return &ProviderError{Class: ErrorTransient, Reason: reason, Route: route}
}
