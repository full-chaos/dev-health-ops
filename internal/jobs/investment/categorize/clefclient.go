package categorize

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// ClefProviderName is the name the clef backend goes by in errors, logs and
// the budget guard. It is not a ProviderKind: clef is a SystemOne decision
// model that only an organization's categorize configuration can name.
const ClefProviderName ProviderKind = ProviderKind(llmorgsettings.ProviderClef)

// ClefClientConfig configures the client of one organization's Cloudflare
// Workers AI clef account. The token and the account come from the
// organization's saved BYO configuration only, never from the environment.
type ClefClientConfig struct {
	// Token is the Cloudflare API token, sent as the bearer.
	Token secrets.Hidden `json:"-"`
	// BaseURL is the saved base_url: https://api.cloudflare.com/client/v4/accounts/<id>/ai.
	BaseURL string
	// Model is the saved model: clef or clef-flash.
	Model string
	// Timeout bounds one HTTP attempt. Zero keeps the shared hardened client's 60 s.
	Timeout time.Duration
	// HTTPClient replaces the SSRF-guarded client; a test supplies a fake round
	// tripper. The URL rule holds for a supplied client too.
	HTTPClient *http.Client
	Logger     *slog.Logger
}

// ErrClefTarget is the one error of a base_url or model the client refuses. It
// holds no part of the configured value: it may carry a credential.
var ErrClefTarget = errors.New("clef client: the base URL must be the organization's Cloudflare account URL and the model clef or clef-flash")

// NewClefClient builds the client of one clef account. It sends nothing. The
// POST URL is derived from the validated account id and the closed model map;
// it is the only URL the client can send to, and api.typesafe.ai is not it.
func NewClefClient(cfg ClefClientConfig) (*TypeSafeClient, error) {
	token := secrets.NewHidden(strings.TrimSpace(cfg.Token.Reveal()))
	if token.Reveal() == "" {
		return nil, errors.New("clef client: an API token is required")
	}
	if len(token.Reveal()) < minFragmentLen {
		return nil, errors.New("clef client: the API token is too short to be protected from a peer echo")
	}
	postURL, ok := llmorgsettings.ClefPostURL(cfg.BaseURL, cfg.Model)
	if !ok {
		return nil, ErrClefTarget
	}
	client := cfg.HTTPClient
	if client == nil {
		client = orgHTTPClient(cfg.BaseURL, nil)
		if cfg.Timeout > 0 {
			client.Timeout = cfg.Timeout
		}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	typed := TypeSafeClientConfig{Model: cfg.Model, Timeout: cfg.Timeout, HTTPClient: client, Logger: logger}
	target := systemOneTarget{provider: ClefProviderName, url: postURL, bearer: token}
	return &TypeSafeClient{cfg: typed, target: target, client: client, logger: logger, sleep: sleepForRetry}, nil
}
