// Command idpstub runs the test-support identity provider (see the idpstub package). It is a test tool: no product
// image holds it, and its signing keys are generated at every start.
//
//	idpstub certs -out DIR -host NAME [-host NAME ...]      write ca.pem, server.pem, server.key
//	idpstub serve|oidc|saml -listen :443 -cert F -key F -base-url URL [flags]
//
// serve answers both protocols on one TLS listener (SAML under /saml/, OIDC everywhere else); oidc and saml answer one.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/idpstub"
)

type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: idpstub certs|serve|oidc|saml [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "certs":
		err = certs(os.Args[2:])
	case "serve", "oidc", "saml":
		err = serve(os.Args[1], os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q (want certs, serve, oidc or saml)", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "idpstub:", err)
		os.Exit(1)
	}
}

func certs(args []string) error {
	fs := flag.NewFlagSet("certs", flag.ContinueOnError)
	out := fs.String("out", "", "directory to write ca.pem, server.pem and server.key into (created 0700)")
	var hosts stringList
	fs.Var(&hosts, "host", "a DNS name for the server certificate (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("-out is required")
	}
	ca, err := idpstub.NewCA(30 * 24 * time.Hour)
	if err != nil {
		return err
	}
	certPEM, keyPEM, err := ca.IssueServer(hosts, 30*24*time.Hour)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0o700); err != nil {
		return err
	}
	for name, data := range map[string][]byte{"ca.pem": ca.CertPEM, "server.pem": certPEM, "server.key": keyPEM} {
		mode := os.FileMode(0o644)
		if name == "server.key" {
			mode = 0o600
		}
		if err := writeFresh(filepath.Join(*out, name), data, mode); err != nil {
			return err
		}
	}
	fmt.Println("wrote ca.pem, server.pem, server.key to", *out)
	return nil
}

func parseUsers(specs []string) ([]idpstub.OIDCUser, []idpstub.SAMLUser, error) {
	var oidcUsers []idpstub.OIDCUser
	var samlUsers []idpstub.SAMLUser
	yes := true
	for _, spec := range specs {
		parts := strings.SplitN(spec, ",", 3)
		if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
			return nil, nil, fmt.Errorf("-user %q: want sub,email[,name]", spec)
		}
		name := ""
		if len(parts) == 3 {
			name = parts[2]
		}
		oidcUsers = append(oidcUsers, idpstub.OIDCUser{Sub: parts[0], Email: parts[1], Name: name, EmailVerified: &yes})
		samlUsers = append(samlUsers, idpstub.SAMLUser{NameID: idpstub.NameID{Format: idpstub.NameIDPersistent, Value: parts[0]}, Email: parts[1], FullName: name})
	}
	return oidcUsers, samlUsers, nil
}

func serve(mode string, args []string) error {
	fs := flag.NewFlagSet(mode, flag.ContinueOnError)
	certFile := fs.String("cert", "", "server certificate PEM")
	keyFile := fs.String("key", "", "server key PEM")
	listen := fs.String("listen", ":443", "TLS listen address")
	base := fs.String("base-url", "", "public base URL of this listener, e.g. https://idp.kit.test")
	clientID := fs.String("client-id", "", "OIDC client id to require (empty accepts any)")
	clientSecret := fs.String("client-secret", "", "OIDC client secret to require (empty accepts any)")
	var redirects, users, acs stringList
	fs.Var(&redirects, "redirect-uri", "an OIDC redirect URI to allow (repeatable; none allows any)")
	fs.Var(&acs, "acs-url", "a SAML ACS URL to allow (repeatable; none allows any)")
	fs.Var(&users, "user", "sub,email[,name] (repeatable; the first is the default user; none gives alice and bob)")
	spEntity := fs.String("sp-entity-id", "", "SAML SP entity id used for responses built without a request")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *certFile == "" || *keyFile == "" || *base == "" {
		return errors.New("-cert, -key and -base-url are required")
	}
	baseURL := strings.TrimSuffix(*base, "/")
	oidcUsers, samlUsers, err := parseUsers(users)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	if mode == "serve" || mode == "oidc" {
		o, err := idpstub.NewOIDC(idpstub.OIDCOptions{ClientID: *clientID, ClientSecret: *clientSecret, RedirectURIs: redirects, Users: oidcUsers})
		if err != nil {
			return err
		}
		o.SetBaseURL(baseURL)
		mux.Handle("/", o.Handler())
	}
	if mode == "serve" || mode == "saml" {
		s, err := idpstub.NewSAML(idpstub.SAMLOptions{EntityID: baseURL + "/saml/metadata", SSOURL: baseURL + "/saml/sso",
			SPEntityID: *spEntity, AllowedACS: acs, Users: samlUsers})
		if err != nil {
			return err
		}
		mux.Handle("/saml/", s.Handler())
	}
	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		return err
	}
	logged := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("idpstub:", r.Method, r.URL.Path) // path only: no query, no values
		mux.ServeHTTP(w, r)
	})
	server := &http.Server{Addr: *listen, Handler: logged, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}}
	errs := make(chan error, 1)
	go func() { errs <- server.ListenAndServeTLS("", "") }()
	fmt.Println("idpstub:", mode, "on", *listen)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errs:
		return err
	case <-stop:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(ctx)
}

// writeFresh writes data to a NEW regular file; a file already there is removed first, never followed or reused.
func writeFresh(path string, data []byte, mode os.FileMode) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode) // #nosec G304 -- operator-chosen output directory
	if err != nil {
		return err
	}
	if err := f.Chmod(mode); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
