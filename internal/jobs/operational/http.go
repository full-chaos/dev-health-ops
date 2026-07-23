package operational

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxResponseBytes = 4 * 1024

type HTTPDispatcherConfig struct {
	WebhookEndpoint string
	BillingEndpoint string
	BearerToken     string
}

type bridgeResponse struct {
	Status string `json:"status"`
}

// HTTPDispatcher is the production bridge to the internal provider and email
// services during coexistence. Only durable identities and bounded routing
// metadata cross the boundary; the services reload authoritative rows.
type HTTPDispatcher struct {
	client          *http.Client
	webhookEndpoint string
	billingEndpoint string
	token           string
}

func NewHTTPDispatcher(client *http.Client, config HTTPDispatcherConfig) (*HTTPDispatcher, error) {
	if client == nil || client.Timeout < 100*time.Millisecond || client.Timeout > 30*time.Second ||
		strings.TrimSpace(config.BearerToken) == "" ||
		!validInternalEndpoint(config.WebhookEndpoint) || !validInternalEndpoint(config.BillingEndpoint) {
		return nil, errors.New("operational HTTP dispatcher configuration is invalid")
	}
	return &HTTPDispatcher{
		client: client, webhookEndpoint: config.WebhookEndpoint,
		billingEndpoint: config.BillingEndpoint, token: config.BearerToken,
	}, nil
}

func (dispatcher *HTTPDispatcher) DispatchWebhook(ctx context.Context, delivery WebhookDelivery) error {
	return dispatcher.post(ctx, dispatcher.webhookEndpoint, map[string]string{
		"delivery_id": delivery.ID,
		"provider":    delivery.Provider,
		"event_type":  delivery.EventType,
	}, "success", "skipped")
}

func (dispatcher *HTTPDispatcher) DispatchBilling(ctx context.Context, notification BillingNotification) error {
	return dispatcher.post(ctx, dispatcher.billingEndpoint, map[string]string{
		"notification_id":   notification.ID,
		"organization_id":   notification.OrganizationID,
		"notification_type": notification.NotificationType,
	}, "sent")
}

func (dispatcher *HTTPDispatcher) post(
	ctx context.Context,
	endpoint string,
	value map[string]string,
	successStatuses ...string,
) error {
	if dispatcher == nil || dispatcher.client == nil {
		return errors.New("operational HTTP dispatcher is unavailable")
	}
	body, err := json.Marshal(value)
	if err != nil {
		return errors.New("operational request encoding failed")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return errors.New("operational request construction failed")
	}
	request.Header.Set("Authorization", "Bearer "+dispatcher.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := dispatcher.client.Do(request)
	if err != nil {
		return errors.New("operational service unavailable")
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if readErr != nil || len(body) > maxResponseBytes {
		return errors.New("operational service response unavailable")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if response.StatusCode >= 400 && response.StatusCode < 500 &&
			response.StatusCode != http.StatusRequestTimeout &&
			response.StatusCode != http.StatusTooManyRequests {
			return ErrDispatchPermanent
		}
		return errors.New("operational service rejected request")
	}
	var result bridgeResponse
	if json.Unmarshal(body, &result) != nil {
		return errors.New("operational service response invalid")
	}
	for _, allowed := range successStatuses {
		if result.Status == allowed {
			return nil
		}
	}
	return ErrDispatchPermanent
}

func validInternalEndpoint(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	if parsed.Scheme == "https" && parsed.Host != "" {
		return true
	}
	host := strings.ToLower(parsed.Hostname())
	return parsed.Scheme == "http" && (host == "127.0.0.1" || host == "::1" || host == "localhost")
}
