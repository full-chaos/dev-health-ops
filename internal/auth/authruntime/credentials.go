package authruntime

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"strings"

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

// EnvMailAllowConsole is the explicit development override: with it exactly "true", users-owned start-up accepts
// the console mail provider, which delivers nothing. The chart never renders it.
const EnvMailAllowConsole = "AUTH_MAIL_ALLOW_CONSOLE"

// ownedUsersMailProvider builds go-api's mail provider for the verify and reset mails and refuses to start while
// they could not reach a user: no real provider, a provider missing its settings, no from-address, or an
// APP_BASE_URL that is not an absolute https URL (http only under the console override; an unset one would
// point the links at the built-in localhost default). Every refusal names a variable and never a value, and carries
// mailConfigReason as its DependencyReason so the start log names it.
func ownedUsersMailProvider(logger *slog.Logger) (mail.Sender, error) {
	provider, err := buildOwnedUsersMailProvider(logger)
	if err != nil {
		return nil, mailConfigInvalid{err}
	}
	return provider, nil
}

// mailConfigReason is the stable start-log reason for an owned-users mail refusal.
const mailConfigReason = "auth_mail_config_invalid"

type mailConfigInvalid struct{ error }

func (e mailConfigInvalid) Unwrap() error          { return e.error }
func (mailConfigInvalid) DependencyReason() string { return mailConfigReason }

func buildOwnedUsersMailProvider(logger *slog.Logger) (mail.Sender, error) {
	if from, set := os.LookupEnv("EMAIL_FROM_ADDRESS"); !set || strings.TrimSpace(from) == "" {
		return nil, errors.New("auth mail: EMAIL_FROM_ADDRESS is required while auth-service owns users")
	}
	if err := checkAppBaseURL(os.Getenv("APP_BASE_URL"), os.Getenv(EnvMailAllowConsole) == "true"); err != nil {
		return nil, err
	}
	if raw, set := os.LookupEnv("EMAIL_PROVIDER"); set {
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "smtp", "resend", "console":
		default:
			return nil, errors.New("auth mail: EMAIL_PROVIDER is not a supported provider")
		}
	}
	provider, err := mail.NewSenderFromEnv(nil, mail.Redacted())
	if err != nil {
		return nil, mailConfigError(err)
	}
	switch provider.Name() {
	case "console":
		if os.Getenv(EnvMailAllowConsole) != "true" {
			return nil, errors.New("auth mail: EMAIL_PROVIDER must name a real provider (smtp or resend) while auth-service owns users; the console provider delivers no mail (development only: " + EnvMailAllowConsole + "=true)")
		}
		logger.Warn("auth mail: " + EnvMailAllowConsole + " is set: the console provider is in use and no verify or reset mail is delivered")
	case "smtp":
		if host, set := os.LookupEnv("SMTP_HOST"); !set || strings.TrimSpace(host) == "" {
			return nil, errors.New("auth mail: SMTP_HOST is required while EMAIL_PROVIDER=smtp and auth-service owns users")
		}
	}
	return provider, nil
}

// mailConfigError names the variable a constructor fault is about and never carries a value.
func mailConfigError(err error) error {
	switch {
	case errors.Is(err, mail.ErrProviderUnsupported):
		return errors.New("auth mail: EMAIL_PROVIDER is not a supported provider")
	case errors.Is(err, mail.ErrSMTPPortInvalid):
		return errors.New("auth mail: SMTP_PORT is not a valid port")
	case errors.Is(err, mail.ErrSMTPCAFileInvalid):
		return errors.New("auth mail: SMTP_TLS_CA_FILE is invalid")
	case errors.Is(err, mail.ErrResendKeyMissing):
		return errors.New("auth mail: EMAIL_API_KEY is required while EMAIL_PROVIDER=resend")
	}
	return errors.New("auth mail: the mail settings do not load")
}

// checkAppBaseURL: APP_BASE_URL is an origin only (path empty or "/", no query, no fragment); the link builder appends a route and a query to it.
func checkAppBaseURL(raw string, allowHTTP bool) error {
	parsed, err := url.Parse(raw)
	if err != nil || raw != strings.TrimSpace(raw) || strings.ContainsAny(raw, "?#") || (parsed.Path != "" && parsed.Path != "/") || (parsed.Scheme != "https" && !(allowHTTP && parsed.Scheme == "http")) || parsed.Hostname() == "" || parsed.User != nil {
		return errors.New("auth mail: APP_BASE_URL must be an https origin (no credentials, path, query or fragment) while auth-service owns users (http only under " + EnvMailAllowConsole + "=true)")
	}
	return nil
}

// credentialRoutes mounts go-api's password and e-mail verification routes
// when the session secrets and AUTH_USERS_OWNED are set, and nothing
// otherwise. It refuses to start (error naming the variable) without a usable
// mail setting; see ownedUsersMailProvider. The returned component closes the
// link-mail sender.
func credentialRoutes(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	limits *authapi.Limits,
) ([]httpapi.Route, []lifecycle.Component, error) {
	if !cfg.Session.Enabled {
		return nil, nil, nil
	}
	if !cfg.UsersOwned {
		return authapi.NotOwnedCredentialRoutes(), nil, nil
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
	provider, err := ownedUsersMailProvider(logger)
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
		Flows: flows, Sessions: sessions, Users: authapi.PGUsers{Pool: pool}, Issuer: issuer, Logger: logger, Limits: limits,
	})
	if err != nil {
		_ = sender.Close(context.Background())
		return nil, nil, err
	}
	return routes, []lifecycle.Component{linkMailComponent{sender: sender}}, nil
}
