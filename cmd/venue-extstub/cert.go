package main

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"slices"
	"time"
)

// readOwnerOnly reads a regular file that no group or other can read.
func readOwnerOnly(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, errors.Unwrap(err))
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%s: want an owner-only (0600) regular file", path)
	}
	return os.ReadFile(path)
}

// leafCert signs, with the venue CA, a one-day leaf for every stub host on a key made in memory. The CA key is a
// PKCS #8 PEM file of mode 0600; no key is ever written.
func leafCert(caCertPEM []byte, caKeyPath string, now time.Time) (tls.Certificate, error) {
	block, _ := pem.Decode(caCertPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return tls.Certificate{}, errors.New("ca cert: no CERTIFICATE PEM block")
	}
	ca, err := x509.ParseCertificate(block.Bytes)
	if err != nil || !ca.IsCA {
		return tls.Certificate{}, errors.New("ca cert: not a CA certificate")
	}
	rawKey, err := readOwnerOnly(caKeyPath)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("ca key: %w", err)
	}
	keyBlock, _ := pem.Decode(rawKey)
	if keyBlock == nil || keyBlock.Type != "PRIVATE KEY" {
		return tls.Certificate{}, errors.New("ca key: no PKCS #8 PRIVATE KEY PEM block")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		return tls.Certificate{}, errors.New("ca key: unreadable PKCS #8 key")
	}
	caKey, ok := parsed.(crypto.Signer)
	if !ok {
		return tls.Certificate{}, errors.New("ca key: not a signing key")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return tls.Certificate{}, err
	}
	names := make([]string, 0, len(Hosts))
	for host := range Hosts {
		names = append(names, host)
	}
	slices.Sort(names)
	tmpl := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "venue-extstub"}, DNSNames: names,
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("leaf: %w", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}
