package main

import (
	"crypto/tls"
	"errors"
	"time"
)

// leafCert: skeleton, no behaviour yet.
func leafCert(caCertPEM []byte, caKeyPath string, now time.Time) (tls.Certificate, error) {
	return tls.Certificate{}, errors.New("skeleton")
}
