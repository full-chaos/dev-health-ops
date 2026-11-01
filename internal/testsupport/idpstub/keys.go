package idpstub

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"time"
)

// Keys is one signing key with its self-signed certificate, generated per run.
type Keys struct {
	Key  *rsa.PrivateKey
	Cert *x509.Certificate
	KID  string
}

// NewKeys generates an RSA-2048 key and a self-signed certificate (valid one day each way) for commonName.
func NewKeys(commonName string) (*Keys, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-24 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &Keys{Key: key, Cert: cert, KID: "idpstub-1"}, nil
}

// CertDER is the certificate in DER form.
func (k *Keys) CertDER() []byte { return k.Cert.Raw }

// CertBase64 is bare base64 DER, the shape a provider row stores as "certificate".
func (k *Keys) CertBase64() string { return base64.StdEncoding.EncodeToString(k.Cert.Raw) }

// CertPEM is the certificate as one PEM block.
func (k *Keys) CertPEM() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: k.Cert.Raw})
}

// TLS is the key and certificate as a tls.Certificate.
func (k *Keys) TLS() tls.Certificate {
	return tls.Certificate{Certificate: [][]byte{k.Cert.Raw}, PrivateKey: k.Key}
}
