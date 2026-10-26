package workloadexchange

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func rewrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func urlEscape(s string) string { return url.QueryEscape(s) }

// collectCounter returns every data point of the named Int64 sum as
// "outcome/reason/audience" -> value.
func collectCounter(t *testing.T, reader *sdkmetric.ManualReader, name string) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want an Int64 sum", name, m.Data)
			}
			for _, point := range sum.DataPoints {
				get := func(key string) string {
					v, _ := point.Attributes.Value(attribute.Key(key))
					return v.AsString()
				}
				if point.Attributes.Len() != 3 {
					t.Fatalf("%s point carries %d attributes, want outcome, reason, audience", name, point.Attributes.Len())
				}
				out[get("outcome")+"/"+get("reason")+"/"+get("audience")] = point.Value
			}
		}
	}
	return out
}

// dumpMetrics renders every exported metric, name, attribute and value.
func dumpMetrics(t *testing.T, reader *sdkmetric.ManualReader) string {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(rm.ScopeMetrics)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	b.Write(raw)
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			b.WriteString(m.Name)
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, point := range sum.DataPoints {
					for _, kv := range point.Attributes.ToSlice() {
						fmt.Fprintf(&b, " %s=%s", kv.Key, kv.Value.Emit())
					}
				}
			}
		}
	}
	return b.String()
}

// TestNoCredentialByteLeavesTheRoute plants a marker in the subject token, the
// reviewer token and (by value) the signing key, drives every refusal, every
// outage and a success, and greps the logs, the metrics, every refusal body
// and the audit rows for each marker in raw, base64url, std base64 and hex
// forms. The minted token may appear only in the 200 body.
func TestNoCredentialByteLeavesTheRoute(t *testing.T) {
	type run struct {
		name  string
		setup func(h *harness) string
	}
	runs := []run{{"success", func(h *harness) string { return form(h.validSubject(), "auth-service", testAction).Encode() }}}
	for _, tc := range refusalCases() {
		runs = append(runs, run{tc.name, tc.setup})
	}
	runs = append(runs,
		run{"shape fault", func(h *harness) string { return "subject_token=" + url.QueryEscape(h.validSubject()) }},
		run{"review 500", func(h *harness) string {
			h.review.set(func(w http.ResponseWriter, _ *http.Request, _ reviewSpec) {
				w.WriteHeader(http.StatusInternalServerError)
			})
			return form(h.validSubject(), "auth-service", "").Encode()
		}},
		run{"review echoes the token", func(h *harness) string {
			h.review.set(func(w http.ResponseWriter, _ *http.Request, spec reviewSpec) {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"message":"` + spec.Token + `"}`))
			})
			return form(h.validSubject(), "auth-service", "").Encode()
		}},
		run{"audit down", func(h *harness) string {
			h.auditErr = fmt.Errorf("audit down")
			return form(h.validSubject(), "auth-service", "").Encode()
		}},
	)
	for _, r := range runs {
		t.Run(r.name, func(t *testing.T) {
			h := newHarness(t)
			body := r.setup(h)
			rec := h.post(body)
			var minted string
			if rec.Code == http.StatusOK {
				minted = decodeAnswer(t, rec).AccessToken
			}
			markers := map[string]string{"subject": subjectMarker, "reviewer": reviewerMarker}
			seed := h.keySeed
			surfaces := map[string]string{
				"logs":    h.logs.String(),
				"metrics": dumpMetrics(t, h.reader),
				"audit":   mustJSON(t, h.auditRows()),
			}
			if rec.Code != http.StatusOK {
				surfaces["body"] = rec.Body.String()
				surfaces["headers"] = fmt.Sprint(rec.Header())
			}
			for surface, text := range surfaces {
				for name, marker := range markers {
					for _, form := range []string{
						marker,
						base64.RawURLEncoding.EncodeToString([]byte(marker)),
						base64.StdEncoding.EncodeToString([]byte(marker)),
						hex.EncodeToString([]byte(marker)),
					} {
						if strings.Contains(text, form) {
							t.Errorf("%s carries the %s marker (%q)", surface, name, form)
						}
					}
				}
				for _, form := range []string{
					base64.RawURLEncoding.EncodeToString(seed), base64.StdEncoding.EncodeToString(seed), hex.EncodeToString(seed),
				} {
					if strings.Contains(text, form) {
						t.Errorf("%s carries the signing key", surface)
					}
				}
				if minted != "" {
					for _, part := range strings.Split(minted, ".") {
						if strings.Contains(text, part) {
							t.Errorf("%s carries a segment of the minted token", surface)
						}
					}
				}
				for _, segment := range strings.Split(h.validSubject(), ".") {
					if strings.Contains(text, segment) {
						t.Errorf("%s carries a segment of the subject token", surface)
					}
				}
			}
		})
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// Every series of the closed reason set is declared at 0 before any request,
// and nothing outside it is ever exported.
func TestEveryCounterSeriesIsDeclaredAtZero(t *testing.T) {
	h := newHarness(t)
	points := h.metricPoints()
	want := map[string]bool{}
	for _, reason := range []string{
		"subject_token_invalid", "unknown", "disabled", "uid_mismatch", "no_audience", "store_error", "resolve_refused",
		"audience_refused", "scope_refused", "lifetime_too_short",
	} {
		for _, aud := range []string{"auth-service", "dev-health-api", "none"} {
			want["refused/"+reason+"/"+aud] = true
		}
	}
	for _, reason := range []string{
		"tokenreview_unavailable", "saturated", "signer_unavailable", "audit_unavailable",
		"selfcheck_contract", "selfcheck_unknown_kid", "selfcheck_signature", "selfcheck_issuer",
		"selfcheck_audience", "selfcheck_not_yet_valid", "selfcheck_expired",
	} {
		for _, aud := range []string{"auth-service", "dev-health-api", "none"} {
			want["unavailable/"+reason+"/"+aud] = true
		}
	}
	for _, aud := range []string{"auth-service", "dev-health-api"} {
		want["issued/issued/"+aud] = true
	}
	want["invalid/invalid_request/none"] = true
	var missing, extra []string
	for key := range want {
		if v, ok := points[key]; !ok || v != 0 {
			missing = append(missing, key)
		}
	}
	for key := range points {
		if !want[key] {
			extra = append(extra, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("series not declared at 0: %v; series outside the closed set: %v", missing, extra)
	}
	decodeAnswer(t, h.post(form(h.validSubject(), "auth-service", "").Encode()))
	if got := h.metricPoints()["issued/issued/auth-service"]; got != 1 {
		t.Fatalf("issued counter %d, want 1", got)
	}
}
