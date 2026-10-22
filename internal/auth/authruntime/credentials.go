package authruntime

import (
	"context"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/linkmail"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// linkMailComponent closes the link-mail pool on shutdown. It sits after the
// store in the component list, so it stops before the pool closes and after
// the API listener has drained.
type linkMailComponent struct{ sender *linkmail.Sender }

func (linkMailComponent) Name() string                         { return "auth-linkmail" }
func (linkMailComponent) Start(context.Context) error          { return nil }
func (c linkMailComponent) Shutdown(ctx context.Context) error { return c.sender.Close(ctx) }

// withheldRecipient hides the recipient from the console provider, which logs
// it: a development stack must not write an address to a log line.
type withheldRecipient struct{ mail.Sender }

func (w withheldRecipient) Send(ctx context.Context, message mail.Message) error {
	message.To = "recipient-withheld"
	return w.Sender.Send(ctx, message)
}

// quietProvider wraps the console provider and leaves every other one alone.
func quietProvider(provider mail.Sender) mail.Sender {
	if provider.Name() == "console" {
		return withheldRecipient{provider}
	}
	return provider
}

// credentialRoutes mounts go-api's password and e-mail verification routes
// when the session secrets and AUTH_USERS_OWNED are set, and nothing
// otherwise. The returned component closes the link-mail sender.
func credentialRoutes(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
) ([]httpapi.Route, []lifecycle.Component, error) {
	if !cfg.Session.Enabled || !cfg.UsersOwned {
		return nil, nil, nil
	}
	sessions, err := session.New(session.Config{
		Pool: pool, Schema: schema, Logger: logger,
		SuccessorKey: contracts.NewSecretString(cfg.Session.SuccessorKey.Reveal()),
	})
	if err != nil {
		return nil, nil, err
	}
	issuer, err := authapi.NewIssuer(cfg.Session.AccessSecret.Reveal(), cfg.Session.AccessIssuer, cfg.Session.AccessAudience)
	if err != nil {
		return nil, nil, err
	}
	identities, err := identity.New(identity.Config{Pool: pool, Schema: schema, Logger: logger, UsersOwnedByAuth: true})
	if err != nil {
		return nil, nil, err
	}
	provider, err := mail.NewSenderFromEnv(nil)
	if err != nil {
		return nil, nil, err
	}
	baseURL, baseURLSet := os.LookupEnv("APP_BASE_URL")
	sender, err := linkmail.New(linkmail.Config{
		Mail:   quietProvider(provider),
		Links:  authmail.Config{AppBaseURL: baseURL, AppBaseURLSet: baseURLSet},
		Logger: logger,
	})
	if err != nil {
		return nil, nil, err
	}
	links := local.NewPGLinks(pool, schema)
	flows, err := local.NewFlows(local.FlowsConfig{
		Accounts: local.IdentityAccounts{Store: identities, RequestID: requestid.Audit, Sessions: sessions, Links: links},
		Links:    links, Sessions: sessions, Mail: sender, Locker: links,
		UsersOwnedByAuth: true,
	})
	if err != nil {
		_ = sender.Close(context.Background())
		return nil, nil, err
	}
	routes, err := authapi.CredentialRoutes(authapi.CredentialDeps{
		Flows: flows, Sessions: sessions, Users: authapi.PGUsers{Pool: pool}, Issuer: issuer, Logger: logger,
	})
	if err != nil {
		_ = sender.Close(context.Background())
		return nil, nil, err
	}
	return routes, []lifecycle.Component{linkMailComponent{sender: sender}}, nil
}
