package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// postWorkerBridge posts a JSON payload to the operational bridge at path,
// authenticating with the token resolved from tokenEnvKey. It is the shared
// client every operator repair verb uses (CHAOS-5042's workgraph/metric
// execution repairs, plus the pre-existing `metrics daily-redrive` ledger
// call) so the auth/error/timeout shape stays in exactly one place --
// mirrors what redriveDailyMetricsLedgerChunk did inline before this file
// existed (WORKER_OPERATIONAL_BRIDGE_URL base, Bearer token, 30s timeout,
// 64KiB response cap).
//
// It returns the HTTP status code and the decoded JSON body regardless of
// status -- CHAOS-5042's repair verbs print the bridge's response verbatim
// on every outcome (including 401/409/422/500), not just 2xx, so an operator
// can see exactly why a repair was refused. err is non-nil only for a
// transport-level failure (bad config, network error, undecodable body) that
// never produced a real bridge response to show.
//
// The token itself is NEVER included in the returned error or logged --
// resolveRequired's platformsecrets.Value keeps it out of %v/%s formatting,
// and Reveal() is called only to build the Authorization header below.
func postWorkerBridge(
	ctx context.Context, tokenEnvKey, path string, payload map[string]any,
) (statusCode int, body map[string]any, err error) {
	baseURL, ok := resolveRequired("WORKER_OPERATIONAL_BRIDGE_URL", os.LookupEnv)
	if !ok {
		return 0, nil, errors.New("WORKER_OPERATIONAL_BRIDGE_URL is not configured")
	}
	token, ok := resolveRequired(tokenEnvKey, os.LookupEnv)
	if !ok {
		return 0, nil, fmt.Errorf("%s is not configured", tokenEnvKey)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, err
	}
	requestURL := strings.TrimRight(baseURL.Reveal(), "/") + path
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(encoded))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token.Reveal())
	client := &http.Client{Timeout: 30 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = response.Body.Close() }()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<16))
	if err != nil {
		return 0, nil, err
	}
	var decoded map[string]any
	if len(responseBody) > 0 {
		if unmarshalErr := json.Unmarshal(responseBody, &decoded); unmarshalErr != nil {
			// The bridge is expected to always answer JSON, even on error
			// (FastAPI's own {"detail": "..."} shape) -- but never crash the
			// CLI on a body that for some reason isn't, surface it as text
			// instead of discarding it.
			decoded = map[string]any{"raw_response": string(responseBody)}
		}
	}
	return response.StatusCode, decoded, nil
}
