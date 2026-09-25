package providerstub

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// CA is a throwaway certificate authority for the venue: the two api containers
// trust its certificate (SSL_CERT_FILE / REQUESTS_CA_BUNDLE), the stub serves a
// certificate it issued for every provider host. It exists only in the venue's
// volume; nothing outside the venue trusts it.
type CA struct {
	Cert    *x509.Certificate
	CertPEM []byte
	key     *ecdsa.PrivateKey
}

// NewCA creates a CA valid for the given lifetime.
func NewCA(lifetime time.Duration) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "providerstub venue CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(lifetime),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &CA{Cert: cert, CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), key: key}, nil
}

// IssueServer issues a server certificate and key (both PEM) for the given DNS
// names. Every name must be a provider host or a Jira tenant (*.atlassian.net):
// the certificate never covers anything else, and carries no IP address.
func (ca *CA) IssueServer(hosts []string, lifetime time.Duration) (certPEM, keyPEM []byte, err error) {
	if len(hosts) == 0 {
		return nil, nil, fmt.Errorf("providerstub: no hosts for the server certificate")
	}
	for _, host := range hosts {
		if strings.Contains(host, ":") || host != strings.ToLower(host) || strings.HasSuffix(host, ".") || ProviderFor(host) == "" {
			return nil, nil, fmt.Errorf("providerstub: %q is not a provider host or a Jira tenant (*.atlassian.net)", host)
		}
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return nil, nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: hosts[0]},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(lifetime),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: hosts,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.Cert, &key.PublicKey, ca.key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), nil
}
