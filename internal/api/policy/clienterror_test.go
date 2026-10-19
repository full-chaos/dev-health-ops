package policy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const providerText = "upstream-body: token ghp_unit_marker rejected by host.internal"

func TestMapErrorNeverCarriesInternalText(t *testing.T) {
	typed := &ClientError{Status: http.StatusConflict, Message: "Name already in use", Cause: errors.New(providerText)}
	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantText   string
		wantClient bool
	}{
		{"typed client error", typed, http.StatusConflict, "Name already in use", true},
		{"wrapped typed client error", fmt.Errorf("step failed: %w", typed), http.StatusConflict, "Name already in use", true},
		{"provider error with body text", ProviderFailure("AuthenticationException", errors.New(providerText)), http.StatusUnauthorized, "The provider rejected the credential.", true},
		{"provider rate limit", ProviderFailure("RateLimitException", errors.New(providerText)), http.StatusTooManyRequests, "The provider rate limit was reached. Try again later.", true},
		{"provider api failure", ProviderFailure("APIException", errors.New(providerText)), http.StatusBadGateway, "The provider request failed.", true},
		{"raw provider error text", errors.New(providerText), http.StatusInternalServerError, "Internal Server Error", false},
		{"wrapped exception", fmt.Errorf("decode: %w", errors.New(providerText)), http.StatusInternalServerError, "Internal Server Error", false},
		{"context canceled", fmt.Errorf("%s: %w", providerText, context.Canceled), http.StatusInternalServerError, "Internal Server Error", false},
		{"deadline exceeded", fmt.Errorf("%s: %w", providerText, context.DeadlineExceeded), http.StatusInternalServerError, "Internal Server Error", false},
		{"unknown provider class", func() error {
			if failure := ProviderFailure("SomethingElse", errors.New(providerText)); failure != nil {
				return failure
			}
			return errors.New(providerText)
		}(), http.StatusInternalServerError, "Internal Server Error", false},
		{"typed error with empty message", &ClientError{Status: http.StatusBadRequest, Cause: errors.New(providerText)}, http.StatusInternalServerError, "Internal Server Error", false},
		{"nil", nil, http.StatusInternalServerError, "Internal Server Error", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, text, client := MapError(tc.err)
			if status != tc.wantStatus || text != tc.wantText || client != tc.wantClient {
				t.Fatalf("MapError = %d %q %v, want %d %q %v", status, text, client, tc.wantStatus, tc.wantText, tc.wantClient)
			}
			if strings.Contains(text, "ghp_unit_marker") || strings.Contains(text, "host.internal") {
				t.Fatalf("mapped text carries internal text: %q", text)
			}
		})
	}
}

func TestWriteErrorLogsTheCauseAndKeepsItOutOfTheBody(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	for name, err := range map[string]error{
		"provider": ProviderFailure("AuthenticationException", errors.New(providerText)),
		"unknown":  errors.New(providerText),
	} {
		logs.Reset()
		recorder := httptest.NewRecorder()
		recorder.Header().Set(RequestIDHeader, "req-1")
		request := httptest.NewRequest(http.MethodGet, "/x", nil)
		WriteError(recorder, request, logger, "unit step", err)
		body := recorder.Body.String()
		if strings.Contains(body, "ghp_unit_marker") || strings.Contains(body, "host.internal") || strings.Contains(body, "upstream-body") {
			t.Fatalf("%s: body carries internal text: %s", name, body)
		}
		for _, want := range []string{"level=ERROR", "request_id=req-1", "ghp_unit_marker", "step=\"unit step\"", "api error mapped"} {
			if !strings.Contains(logs.String(), want) {
				t.Fatalf("%s: log lacks %q: %s", name, want, logs.String())
			}
		}
		if recorder.Header().Get(RequestIDHeader) != "req-1" {
			t.Fatalf("%s: request id header lost", name)
		}
	}
}

func TestPlanGateKeepsTheSentenceTheWebMatches(t *testing.T) {
	status, text, client := MapError(fmt.Errorf("outer: %w", PlanGate("Team plan or higher.", errors.New(providerText))))
	if status != http.StatusForbidden || !client || text != "This feature requires the Team plan or higher." {
		t.Fatalf("MapError = %d %q %v", status, text, client)
	}
}
