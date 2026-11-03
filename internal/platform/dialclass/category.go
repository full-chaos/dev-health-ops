package dialclass

import (
	"errors"
	"io"
	"net"
	"syscall"

	"github.com/full-chaos/dev-health-ops/internal/platform/timeoutclass"
	"github.com/jackc/pgx/v5/pgconn"
)

// Category names the class of a connect failure for a log line. It reads the
// type and code only, never the text, so the result carries no host, login or
// server message.
func Category(err error) string {
	if err == nil {
		return "none"
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "57P03" {
			return "server_starting"
		}
		if IsTransient(pgErr) {
			return "server_unavailable"
		}
		return "server"
	}
	var dnsErr *net.DNSError
	switch {
	case errors.As(err, &dnsErr):
		return "dns"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused"
	case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return "no_route"
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return "reset"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "eof"
	case timeoutclass.IsTimeout(err):
		return "timeout"
	}
	return "other"
}
