package livee2e

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/ssoseed"
)

// ssoDB is what the seed verb needs of a database.
type ssoDB interface {
	ssoseed.Execer
	ssoseed.Querier
}

// openSSODB connects with the standard PG* environment (the variables the stack script's psql verbs use).
func openSSODB(ctx context.Context) (ssoDB, func(), error) {
	pool, err := pgxpool.New(ctx, "")
	if err != nil {
		return nil, nil, err
	}
	return pool, pool.Close, nil
}

// seedSSOProvider is `cigate live-e2e seed-sso-provider EMAIL PROTOCOL [flags]`: a TEST-ONLY stand-in for the provider
// create route go-api does not have (CHAOS-13636). args is EMAIL, PROTOCOL, flags. It refuses an unknown protocol before it
// reads a secret or opens a database, and it prints the provider id only, never the client secret or the sealed blob.
func seedSSOProvider(ctx context.Context, args []string, getenv func(string) string, stdout io.Writer, open func(context.Context) (ssoDB, func(), error)) error {
	if len(args) < 2 {
		return errors.New("usage: seed-sso-provider EMAIL PROTOCOL [flags]")
	}
	email, protocol := args[0], args[1]
	if !ssoseed.ValidProtocol(protocol) {
		return fmt.Errorf("unknown protocol %q (want oidc or saml)", protocol)
	}
	fs := flag.NewFlagSet("seed-sso-provider", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	name := fs.String("name", "Live E2E SSO", "provider name")
	role := fs.String("default-role", "member", "role of a user the provider creates")
	autoProvision := fs.Bool("auto-provision", true, "create users on first sign-in")
	issuer := fs.String("issuer", "", "OIDC issuer")
	clientID := fs.String("client-id", "", "OIDC client id")
	authorizationEndpoint := fs.String("authorization-endpoint", "", "OIDC authorization endpoint")
	tokenEndpoint := fs.String("token-endpoint", "", "OIDC token endpoint")
	userinfoEndpoint := fs.String("userinfo-endpoint", "", "OIDC userinfo endpoint")
	jwksURI := fs.String("jwks-uri", "", "OIDC jwks uri")
	entityID := fs.String("entity-id", "", "SAML IdP entity id")
	ssoURL := fs.String("sso-url", "", "SAML IdP sign-on url")
	certFile := fs.String("cert-file", "", "SAML IdP certificate (PEM file)")
	spEntityID := fs.String("sp-entity-id", "", "SAML SP entity id")
	acsBase := fs.String("acs-base", "", "SAML SP ACS base (scheme and host); sp_acs_url is built from it and the provider id")
	if err := fs.Parse(args[2:]); err != nil {
		return fmt.Errorf("flags: %w", err)
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected arguments after the flags")
	}
	key := getenv("SETTINGS_ENCRYPTION_KEY")
	if key == "" {
		return errors.New("SETTINGS_ENCRYPTION_KEY is not set")
	}
	providerID := uuid.NewString()
	var config string
	var secretsJSON *string
	var err error
	switch protocol {
	case "oidc":
		secret := getenv("SSO_CLIENT_SECRET")
		if secret == "" {
			return errors.New("SSO_CLIENT_SECRET is not set")
		}
		if config, err = ssoseed.OIDCConfig(*issuer, *clientID, *authorizationEndpoint, *tokenEndpoint, *userinfoEndpoint, *jwksURI); err != nil {
			return err
		}
		sealed, err := ssoseed.Seal(key, getenv("SETTINGS_ENCRYPTION_SALT"), secret)
		if err != nil {
			return err
		}
		secretsJSON = &sealed
	case "saml":
		pem, err := os.ReadFile(*certFile)
		if err != nil {
			return fmt.Errorf("cert-file: %w", err)
		}
		if config, err = ssoseed.SAMLConfig(providerID, *entityID, *ssoURL, string(pem), *spEntityID, *acsBase); err != nil {
			return err
		}
	}
	db, closeDB, err := open(ctx)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer closeDB()
	org, err := ssoseed.OrgIDForEmail(ctx, db, email)
	if err != nil {
		return err
	}
	domains := "[]"
	now := time.Now().UTC()
	provider := ssoseed.Provider{
		ID: providerID, OrgID: org, Name: *name, Protocol: protocol, Status: "active", DefaultRole: *role,
		AutoProvision: *autoProvision, Config: &config, AllowedDomains: &domains, CreatedAt: now, UpdatedAt: now,
		EncryptedSecrets: secretsJSON,
	}
	if err := ssoseed.Insert(ctx, db, provider); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "seeded %s provider %s\n", protocol, provider.ID)
	return nil
}
