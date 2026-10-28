package authapi

import (
	"context"
	"crypto/ed25519"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

const (
	AccessService       = "dev-health-auth-service"
	AccessTokenAudience = "auth-service"
)

var errAccessNotBuilt = errors.New("authapi: access check is not built")

type AccessCheck struct{}

type AccessCheckConfig struct {
	IssuerURL string
	Keys      KeySetSource
	Legacy    *Issuer
	Rows      []accessverify.Row
	Meter     metric.Meter
	Logger    *slog.Logger
	Now       func() time.Time
}

func NewAccessCheck(AccessCheckConfig) (*AccessCheck, error) { return nil, errAccessNotBuilt }

func (i *Issuer) WithAccessCheck(*AccessCheck) *Issuer { return i }

func AuthServiceAccessRows() ([]accessverify.Row, error) { return nil, errAccessNotBuilt }

var _ = context.Background

type publishedKeys struct{ source KeySetSource }

func (publishedKeys) ResolveKey(string) (ed25519.PublicKey, error) { return nil, errAccessNotBuilt }
