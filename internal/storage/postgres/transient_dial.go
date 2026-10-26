package postgres

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"syscall"

	"github.com/jackc/pgx/v5/pgconn"
)

// IsTransientDialError classifies err by type and code only, never by text.
// Auth (28P01, 28000), permission (42501), other server answers, TLS and
// configuration failures are final.
func IsTransientDialError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "57P03", "57P01", "53300", "08P01":
			return true
		}
		return len(pgErr.Code) >= 2 && pgErr.Code[:2] == "08"
	}
	var dnsErr *net.DNSError
	switch {
	case errors.As(err, &dnsErr),
		errors.Is(err, syscall.ECONNREFUSED),
		errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.EPIPE),
		errors.Is(err, syscall.EHOSTUNREACH),
		errors.Is(err, syscall.ENETUNREACH),
		errors.Is(err, io.EOF),
		errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, os.ErrDeadlineExceeded):
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
