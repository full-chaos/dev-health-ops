package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func TestCmdIdpstubCertsServeOIDCAndSAML(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "certs")
	if err := certs([]string{"-out", dir, "-host", "localhost"}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "server.key"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("server.key: %v %v", info, err)
	}
	if err := certs([]string{"-out", dir}); err == nil {
		t.Fatal("certs with no -host succeeded")
	}
	addr := freePort(t)
	base := "https://localhost:" + addr[strings.LastIndex(addr, ":")+1:]
	done := make(chan error, 1)
	go func() {
		done <- serve("serve", []string{"-listen", addr, "-cert", filepath.Join(dir, "server.pem"), "-key", filepath.Join(dir, "server.key"),
			"-base-url", base, "-user", "carol,carol@kit.test,Carol"})
	}()
	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("ca.pem is not a certificate")
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}, Timeout: 5 * time.Second}
	var res *http.Response
	for i := 0; i < 100; i++ {
		select {
		case err := <-done:
			t.Fatalf("serve returned early: %v", err)
		default:
		}
		if res, err = client.Get(base + "/t/.well-known/openid-configuration"); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("discovery over the stub's own CA: %v", err)
	}
	var doc map[string]any
	_ = json.NewDecoder(res.Body).Decode(&doc)
	res.Body.Close()
	if doc["issuer"] != base+"/t" || doc["authorization_endpoint"] != base+"/authorize" {
		t.Fatalf("discovery = %v", doc)
	}
	res, err = client.Get(base + "/saml/metadata")
	if err != nil {
		t.Fatal(err)
	}
	md, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if !strings.Contains(string(md), `entityID="`+base+`/saml/metadata"`) || !strings.Contains(string(md), base+"/saml/sso") {
		t.Fatalf("metadata = %s", md)
	}
}

func TestCmdIdpstubRefusesMissingFlagsAndBadUsers(t *testing.T) {
	if err := serve("serve", nil); err == nil {
		t.Error("serve with no flags succeeded")
	}
	if _, _, err := parseUsers([]string{"only-sub"}); err == nil {
		t.Error("a -user without an email was accepted")
	}
}
