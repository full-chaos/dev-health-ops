package providerfoundation

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"net"
	"syscall"
)

const (
	TransportReasonTLSUnknownAuthority = "tls_unknown_authority"
	TransportReasonTLSCertInvalid      = "tls_cert_invalid"
	TransportReasonDNS                 = "dns"
	TransportReasonTimeout             = "timeout"
	TransportReasonConnRefused         = "conn_refused"
	TransportReasonCanceled            = "canceled"
	TransportReasonOther               = "other"
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
func (c *HTTPClient) transportFailure(cause error, attempt int) *ProviderError {
	return transportProviderError(c.Provider, cause, attempt)
}

func transportProviderError(provider string, cause error, attempt int) *ProviderError {
	reason := classifyTransportReason(cause)
	slog.Warn("provider transport error",
		"provider", provider,
		"attempt", attempt,
		"reason", reason,
	)
	return &ProviderError{Class: ErrorTransient, Reason: reason}
}
