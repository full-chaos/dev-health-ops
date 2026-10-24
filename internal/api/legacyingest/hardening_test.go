package legacyingest

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

var hardeningNow = time.Unix(1_800_000_000, 0)

func hardenedRequest(t *testing.T, getenv map[string]string, options Options, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	routes := Harden(Routes(Deps{Getenv: env(getenv), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}), options)
	var target http.Handler
	for _, route := range routes {
		if route.Pattern == "/api/v1/ingest/commits" {
			target = route.Handler
		}
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/commits", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	target.ServeHTTP(recorder, request)
	return recorder
}

func timestampedSignature(secret, timestamp, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "." + body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func timestampOptions() Options {
	return Options{RequireSignatureTimestamp: true, Now: func() time.Time { return hardeningNow }}
}

func at(offset int64) string { return strconv.FormatInt(hardeningNow.Unix()+offset, 10) }

func TestLegacyIngestHMACRawBytes(t *testing.T) {
	const secret = "raw-byte-secret"
	now := at(0)
	valid := timestampedSignature(secret, now, commitsBody)
	reencoded := strings.Replace(commitsBody, `"org_id":"org-1"`, `"org_id": "org-1"`, 1)
	flipped := strings.Replace(commitsBody, `"hash":"abc"`, `"hash":"abd"`, 1)
	for _, item := range []struct {
		name    string
		body    string
		headers map[string]string
		status  int
	}{
		{"missing signature", commitsBody, map[string]string{SignatureTimestampHeader: now}, 401},
		{"malformed prefix", commitsBody, map[string]string{SignatureTimestampHeader: now, "X-Signature-256": strings.TrimPrefix(valid, "sha256=")}, 401},
		{"malformed hex", commitsBody, map[string]string{SignatureTimestampHeader: now, "X-Signature-256": "sha256=zz" + valid[len("sha256=zz"):]}, 401},
		{"malformed odd length", commitsBody, map[string]string{SignatureTimestampHeader: now, "X-Signature-256": valid[:len(valid)-1]}, 401},
		{"malformed non-ASCII", commitsBody, map[string]string{SignatureTimestampHeader: now, "X-Signature-256": "sha256=\xe9"}, 401},
		{"wrong secret", commitsBody, map[string]string{SignatureTimestampHeader: now, "X-Signature-256": timestampedSignature("other", now, commitsBody)}, 401},
		{"one body byte changed", flipped, map[string]string{SignatureTimestampHeader: now, "X-Signature-256": valid}, 401},
		{"body re-encoded with the same meaning", reencoded, map[string]string{SignatureTimestampHeader: now, "X-Signature-256": valid}, 401},
		{"signature of the re-encoded bytes", reencoded, map[string]string{SignatureTimestampHeader: now, "X-Signature-256": timestampedSignature(secret, now, reencoded)}, 202},
		{"valid", commitsBody, map[string]string{SignatureTimestampHeader: now, "X-Signature-256": valid}, 202},
	} {
		t.Run(item.name, func(t *testing.T) {
			got := hardenedRequest(t, map[string]string{"INGEST_SIGNING_SECRET": secret}, timestampOptions(), item.body, item.headers)
			if got.Code != item.status {
				t.Fatalf("status = %d, want %d: %s", got.Code, item.status, got.Body.String())
			}
			if item.status == 401 && got.Body.String() != `{"detail":"Invalid signature"}` {
				t.Fatalf("body = %s", got.Body.String())
			}
		})
	}
}

func TestLegacyIngestHMACTimestamp(t *testing.T) {
	const secret = "timestamp-secret"
	for _, item := range []struct {
		name    string
		headers map[string]string
		status  int
	}{
		{"missing timestamp", map[string]string{"X-Signature-256": timestampedSignature(secret, at(0), commitsBody)}, 401},
		{"non-integer timestamp", map[string]string{SignatureTimestampHeader: "12.5", "X-Signature-256": timestampedSignature(secret, "12.5", commitsBody)}, 401},
		{"signed timestamp", map[string]string{SignatureTimestampHeader: "+" + at(0), "X-Signature-256": timestampedSignature(secret, "+"+at(0), commitsBody)}, 401},
		{"timestamp 301 s ahead", map[string]string{SignatureTimestampHeader: at(301), "X-Signature-256": timestampedSignature(secret, at(301), commitsBody)}, 401},
		{"timestamp 301 s behind", map[string]string{SignatureTimestampHeader: at(-301), "X-Signature-256": timestampedSignature(secret, at(-301), commitsBody)}, 401},
		{"timestamp 300 s behind", map[string]string{SignatureTimestampHeader: at(-300), "X-Signature-256": timestampedSignature(secret, at(-300), commitsBody)}, 202},
		{"valid timestamp with the old body-only signature", map[string]string{SignatureTimestampHeader: at(0), "X-Signature-256": sign(secret, commitsBody)}, 401},
		{"timestamp swapped after signing", map[string]string{SignatureTimestampHeader: at(10), "X-Signature-256": timestampedSignature(secret, at(0), commitsBody)}, 401},
		{"valid", map[string]string{SignatureTimestampHeader: at(0), "X-Signature-256": timestampedSignature(secret, at(0), commitsBody)}, 202},
	} {
		t.Run(item.name, func(t *testing.T) {
			got := hardenedRequest(t, map[string]string{"INGEST_SIGNING_SECRET": secret}, timestampOptions(), commitsBody, item.headers)
			if got.Code != item.status {
				t.Fatalf("status = %d, want %d: %s", got.Code, item.status, got.Body.String())
			}
		})
	}
	t.Run("api key mode ignores the timestamp", func(t *testing.T) {
		got := hardenedRequest(t, map[string]string{"INGEST_API_KEYS": devKey}, timestampOptions(), commitsBody, map[string]string{"X-API-Key": devKey})
		if got.Code != 202 {
			t.Fatalf("status = %d: %s", got.Code, got.Body.String())
		}
	})
}

func TestLegacyIngestDeprecationHeaders(t *testing.T) {
	sunset := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	options := Options{Deprecation: true, Sunset: sunset}
	for _, item := range []struct {
		name    string
		env     map[string]string
		body    string
		headers map[string]string
		status  int
	}{
		{"accepted", map[string]string{"INGEST_API_KEYS": devKey}, commitsBody, map[string]string{"X-API-Key": devKey}, 202},
		{"refused", map[string]string{"INGEST_API_KEYS": devKey}, commitsBody, nil, 401},
		{"undecodable body", map[string]string{"INGEST_API_KEYS": devKey}, "{", map[string]string{"X-API-Key": devKey}, 422},
	} {
		t.Run(item.name, func(t *testing.T) {
			got := hardenedRequest(t, item.env, options, item.body, item.headers)
			if got.Code != item.status {
				t.Fatalf("status = %d, want %d", got.Code, item.status)
			}
			if got.Header().Get("Deprecation") != "true" || got.Header().Get("Sunset") != "Tue, 01 Dec 2026 00:00:00 GMT" {
				t.Fatalf("headers = %v", got.Header())
			}
		})
	}
	t.Run("no sunset configured sends no Sunset", func(t *testing.T) {
		got := hardenedRequest(t, map[string]string{"INGEST_API_KEYS": devKey}, Options{Deprecation: true}, commitsBody, map[string]string{"X-API-Key": devKey})
		if got.Header().Get("Deprecation") != "true" || got.Header().Get("Sunset") != "" {
			t.Fatalf("headers = %v", got.Header())
		}
	})
}

func TestLegacyIngestEmptyKeyEntriesNeverMatch(t *testing.T) {
	for _, item := range []struct {
		name    string
		keys    string
		headers map[string]string
		status  int
	}{
		{"lone comma, no header", ",", nil, 401},
		{"lone comma, empty header", ",", map[string]string{"X-API-Key": ""}, 401},
		{"trailing comma, empty header", "a,", map[string]string{"X-API-Key": ""}, 401},
		{"leading comma, space header", ",a", map[string]string{"X-API-Key": " "}, 401},
		{"blank entries only, space header", " , ", map[string]string{"X-API-Key": " "}, 401},
		{"double comma, empty header", "a,,b", map[string]string{"X-API-Key": ""}, 401},
		{"double comma, key a", "a,,b", map[string]string{"X-API-Key": "a"}, 202},
		{"double comma, key b", "a,,b", map[string]string{"X-API-Key": "b"}, 202},
		{"trailing comma, key a", "a,", map[string]string{"X-API-Key": "a"}, 202},
	} {
		t.Run(item.name, func(t *testing.T) {
			got := serve(t, map[string]string{"INGEST_API_KEYS": item.keys}, commitsBody, item.headers)
			if got.Code != item.status {
				t.Fatalf("status = %d, want %d", got.Code, item.status)
			}
		})
	}
}

func TestAPIKeysDropsBlankEntries(t *testing.T) {
	for raw, want := range map[string]int{"": 0, ",": 0, " , ": 0, "a,": 1, ",a": 1, "a,,b": 2, " a , b ": 2} {
		if got := apiKeys(env(map[string]string{"INGEST_API_KEYS": raw})); len(got) != want {
			t.Errorf("apiKeys(%q) = %q, want %d entries", raw, got, want)
		}
	}
}
