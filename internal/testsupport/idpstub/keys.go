package idpstub

import "errors"

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
)

// Keys is one signing key with its self-signed certificate, generated per run.
type Keys struct {
	Key  *rsa.PrivateKey
	Cert *x509.Certificate
	KID  string
}

// NewKeys generates an RSA-2048 key and a self-signed certificate (valid one day each way) for commonName.
func NewKeys(commonName string) (*Keys, error) {
	return *new(*Keys), errors.New("idpstub: not implemented")
}

// CertDER is the certificate in DER form.
func (k *Keys) CertDER() []byte { return *new([]byte) }

// CertBase64 is bare base64 DER, the shape a provider row stores as "certificate".
func (k *Keys) CertBase64() string { return *new(string) }

// CertPEM is the certificate as one PEM block.
func (k *Keys) CertPEM() []byte { return *new([]byte) }

// TLS is the key and certificate as a tls.Certificate.
func (k *Keys) TLS() tls.Certificate {
	return *new(tls.Certificate)
}
