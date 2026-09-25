package main

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func TestCertsWritesAVerifiableChainForEveryProviderHostAndTheJiraTenants(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "certs")
	if err := certs([]string{"-out", dir, "-jira-host", "zz-venue.atlassian.net"}); err != nil {
		t.Fatal(err)
	}
	read := func(name string) []byte {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	info, err := os.Stat(filepath.Join(dir, "server.key"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("server.key mode = %v, %v; want 0600", info, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(read("ca.pem")) {
		t.Fatal("ca.pem is not a certificate")
	}
	block, _ := pem.Decode(read("server.pem"))
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"api.github.com", "github.com", "gitlab.com", "api.linear.app", "api.pagerduty.com", "api.eu.pagerduty.com", "identity.pagerduty.com", "zz-venue.atlassian.net"} {
		if _, err := cert.Verify(x509.VerifyOptions{DNSName: host, Roots: roots}); err != nil {
			t.Errorf("server.pem does not verify for %s: %v", host, err)
		}
	}
	if _, err := cert.Verify(x509.VerifyOptions{DNSName: "example.com", Roots: roots}); err == nil {
		t.Error("the certificate must not cover hosts the venue does not stub")
	}
	if err := certs([]string{}); err == nil {
		t.Error("certs without -out must be refused")
	}
}

func TestServeRefusesMissingFlags(t *testing.T) {
	if err := serve([]string{"-fixtures", "x"}); err == nil {
		t.Fatal("serve without -cert and -key must be refused")
	}
}

func TestCertsRefusesANonJiraHostAndTightensAnExistingKeyFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "certs")
	if err := certs([]string{"-out", dir, "-jira-host", "example.com"}); err == nil {
		t.Fatal("-jira-host example.com must be refused: only *.atlassian.net tenants")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "server.key"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := certs([]string{"-out", dir, "-jira-host", "zz-venue.atlassian.net"}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "server.key"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("server.key mode = %v, %v; want 0600 even when the file pre-existed 0644", info, err)
	}
}

func TestCertsNeverWritesThroughAPreExistingSymlink(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "certs")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "victim")
	if err := os.WriteFile(target, []byte("victim-original"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "server.key")); err != nil {
		t.Skip("no symlinks")
	}
	if err := certs([]string{"-out", dir}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(target); string(got) != "victim-original" {
		t.Fatalf("certs wrote the private key through a symlink: %q", got[:min(len(got), 30)])
	}
	info, err := os.Lstat(filepath.Join(dir, "server.key"))
	if err != nil || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 {
		t.Fatalf("server.key = %v, %v; want a regular 0600 file", info, err)
	}
}

func TestServeDefaultsTheRecorderToLoopback(t *testing.T) {
	if defaultAdminAddr != "127.0.0.1:9090" {
		t.Fatalf("default recorder address = %q, want loopback only", defaultAdminAddr)
	}
}
