package categorize

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// OrgTypeSafeClientConfig configures the client of one organization's own
// TypeSafe account. The key and the model come from the organization's saved
// BYO configuration only, never from the environment, and there is no base
// URL: the client sends to llmorgsettings.TypeSafePostURL only.
type OrgTypeSafeClientConfig struct {
	APIKey secrets.Hidden `json:"-"`
	// Model is the saved model; it must be on llmorgsettings' closed list.
	Model string
	// Timeout bounds one HTTP attempt. Zero keeps the shared hardened client's 60 s.
	Timeout time.Duration
	// HTTPClient replaces the SSRF-guarded client; a test supplies a fake round
	// tripper. The host rule holds for a supplied client too.
	HTTPClient *http.Client
	Logger     *slog.Logger
}

// ErrOrgTypeSafeModel is the error of a model the org list does not hold. It
// holds no part of the configured value.
var ErrOrgTypeSafeModel = errors.New("typesafe org client: the model is not on the list of versioned jev models")

// NewOrgTypeSafeClient builds the client of one org's TypeSafe account. It
// sends nothing and reads no environment: TYPESAFE_API_KEY, TYPESAFE_BASE_URL
// and TYPESAFE_MODEL belong to the platform backend only.
func NewOrgTypeSafeClient(cfg OrgTypeSafeClientConfig) (*TypeSafeClient, error) {
	if !llmorgsettings.TypeSafeModelAllowed(cfg.Model) {
		return nil, ErrOrgTypeSafeModel
	}
	client := cfg.HTTPClient
	if client == nil {
		client = orgHTTPClient(DefaultTypeSafeBaseURL, nil)
		if cfg.Timeout > 0 {
			client.Timeout = cfg.Timeout
		}
	}
	return NewTypeSafeClient(TypeSafeClientConfig{
		APIKey: cfg.APIKey, Model: cfg.Model, Timeout: cfg.Timeout, HTTPClient: client, Logger: cfg.Logger,
	})
}
