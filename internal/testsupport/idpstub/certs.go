package idpstub

import "errors"

import (
	"crypto/ecdsa"

	"crypto/x509"

	"math/big"

	"time"
)

// CA is a throwaway certificate authority for the stub's own TLS listener. It is separate from the provider
// stub's CA: whoever talks to the stub trusts ca.pem next to the provider stub's.
type CA struct {
	Cert    *x509.Certificate
	CertPEM []byte
	key     *ecdsa.PrivateKey
}

// NewCA creates a CA valid for lifetime.
func NewCA(lifetime time.Duration) (*CA, error) {
	return *new(*CA), errors.New("idpstub: not implemented")
}

func serialNumber() (*big.Int, error) {
	return *new(*big.Int), errors.New("idpstub: not implemented")
}

// IssueServer issues a server certificate and key (both PEM) for the given lower-case DNS names. An IP address,
// a wildcard, or an empty list is refused.
func (ca *CA) IssueServer(hosts []string, lifetime time.Duration) (certPEM, keyPEM []byte, err error) {
	return *new([]byte), *new([]byte), errors.New("idpstub: not implemented")
}
