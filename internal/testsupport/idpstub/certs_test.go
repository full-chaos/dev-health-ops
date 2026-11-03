package idpstub_test

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/idpstub"
)

func TestCAIssuesServerCertForNamedHostsOnly(t *testing.T) {
	ca, err := idpstub.NewCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]string{nil, {""}, {"*.example.test"}, {"Upper.example.test"}, {"10.0.0.1:443"}, {"host.example.test."}} {
		if _, _, err := ca.IssueServer(bad, time.Hour); err == nil {
			t.Errorf("hosts %q were accepted", bad)
		}
	}
	certPEM, keyPEM, err := ca.IssueServer([]string{"idp.kit.test"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	conn, err := tls.Dial("tcp", srv.Listener.Addr().String(), &tls.Config{RootCAs: pool, ServerName: "idp.kit.test"})
	if err != nil {
		t.Fatalf("handshake with the issued certificate: %v", err)
	}
	conn.Close()
	if _, err := tls.Dial("tcp", srv.Listener.Addr().String(), &tls.Config{RootCAs: pool, ServerName: "other.kit.test"}); err == nil {
		t.Fatal("the certificate verified for a host it was not issued for")
	}
}
