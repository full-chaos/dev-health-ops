package oidc

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestSuccessLogNamesNoAccount(t *testing.T) {
	var out bytes.Buffer
	counter, err := noop.NewMeterProvider().Meter("test").Int64Counter("auth.oidc.outcomes")
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{cfg: Config{Logger: slog.New(slog.NewJSONHandler(&out, nil))}, outcomes: counter}
	for _, route := range []string{routeCallback, routeOAuthCallback, routeAuthorize} {
		s.succeeded(context.Background(), route, uuid.New())
	}
	if text := out.String(); !strings.Contains(text, "request succeeded") || strings.Contains(text, "principal") {
		t.Fatalf("success log: %s", text)
	}
}
