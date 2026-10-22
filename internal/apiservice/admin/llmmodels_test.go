package admin

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

type recordedModelRequest struct {
	method string
	url    string
	header http.Header
}

type scriptedModelDoer struct {
	requests  []recordedModelRequest
	responses []string
	status    int
}

func (d *scriptedModelDoer) Do(req *http.Request) (*http.Response, error) {
	d.requests = append(d.requests, recordedModelRequest{req.Method, req.URL.String(), req.Header.Clone()})
	index := len(d.requests) - 1
	body := "{}"
	if index < len(d.responses) {
		body = d.responses[index]
	}
	status := d.status
	if status == 0 {
		status = 200
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
}

func TestListProviderModelsOpenAIUsesBearerAndSortsIDs(t *testing.T) {
	doer := &scriptedModelDoer{responses: []string{`{"object":"list","data":[{"id":"gpt-b"},{"id":"gpt-a"},{"id":"gpt-b"}]}`}}
	ids, reason := listProviderModels(context.Background(), doer, "openai", "", "sk-secret")
	if reason != "" || strings.Join(ids, ",") != "gpt-a,gpt-b" {
		t.Fatalf("ids=%v reason=%q", ids, reason)
	}
	got := doer.requests[0]
	if got.method != "GET" || got.url != "https://api.openai.com/v1/models" || got.header.Get("Authorization") != "Bearer sk-secret" {
		t.Fatalf("request = %+v", got)
	}
}

func TestListProviderModelsAnthropicSendsKeyHeaderAndFollowsCursor(t *testing.T) {
	doer := &scriptedModelDoer{responses: []string{
		`{"data":[{"id":"claude-b"}],"has_more":true,"last_id":"claude-b"}`,
		`{"data":[{"id":"claude-a"}],"has_more":false,"last_id":"claude-a"}`,
	}}
	ids, reason := listProviderModels(context.Background(), doer, "anthropic", "", "ak-secret")
	if reason != "" || strings.Join(ids, ",") != "claude-b,claude-a" {
		t.Fatalf("ids=%v reason=%q", ids, reason)
	}
	if len(doer.requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(doer.requests))
	}
	first, second := doer.requests[0], doer.requests[1]
	if first.url != "https://api.anthropic.com/v1/models?limit=1000" ||
		first.header.Get("x-api-key") != "ak-secret" ||
		first.header.Get("anthropic-version") != "2023-06-01" ||
		first.header.Get("Authorization") != "" {
		t.Fatalf("first request = %+v", first)
	}
	if !strings.Contains(second.url, "after_id=claude-b") {
		t.Fatalf("second url = %s", second.url)
	}
}

func TestListProviderModelsGeminiKeepsGenerateContentModelsOnly(t *testing.T) {
	doer := &scriptedModelDoer{responses: []string{
		`{"models":[{"name":"models/gemini-2-pro","supportedGenerationMethods":["generateContent"]},` +
			`{"name":"models/embed-1","supportedGenerationMethods":["embedContent"]}],"nextPageToken":"t2"}`,
		`{"models":[{"name":"models/gemini-2-flash","supportedGenerationMethods":["generateContent","countTokens"]}]}`,
	}}
	ids, reason := listProviderModels(context.Background(), doer, "gemini", "", "gk-secret")
	if reason != "" || strings.Join(ids, ",") != "gemini-2-flash,gemini-2-pro" {
		t.Fatalf("ids=%v reason=%q", ids, reason)
	}
	first, second := doer.requests[0], doer.requests[1]
	if first.url != "https://generativelanguage.googleapis.com/v1beta/models?pageSize=1000" || first.header.Get("x-goog-api-key") != "gk-secret" {
		t.Fatalf("first request = %+v", first)
	}
	if !strings.Contains(second.url, "pageToken=t2") {
		t.Fatalf("second url = %s", second.url)
	}
	for _, r := range doer.requests {
		if strings.Contains(r.url, "gk-secret") {
			t.Fatalf("key in url: %s", r.url)
		}
	}
}

func TestListProviderModelsCompatibleProviderNeedsBaseURL(t *testing.T) {
	doer := &scriptedModelDoer{}
	if ids, reason := listProviderModels(context.Background(), doer, "qwen", "", "k"); ids != nil || reason != llmModelsReasonBaseURLRequired || len(doer.requests) != 0 {
		t.Fatalf("ids=%v reason=%q calls=%d", ids, reason, len(doer.requests))
	}
	doer = &scriptedModelDoer{responses: []string{`{"data":[{"id":"qwen-max"}]}`}}
	ids, reason := listProviderModels(context.Background(), doer, "qwen", "https://qwen.example.invalid/v1/", "k")
	if reason != "" || len(ids) != 1 || doer.requests[0].url != "https://qwen.example.invalid/v1/models" {
		t.Fatalf("ids=%v reason=%q req=%+v", ids, reason, doer.requests)
	}
}

func TestListProviderModelsReasonsCarryNoProviderText(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		want   string
	}{
		{401, `{"error":"bad key sk-secret"}`, llmModelsReasonProviderRejected},
		{500, `upstream sk-secret`, llmModelsReasonProviderDown},
		{200, `not json sk-secret`, llmModelsReasonInvalidResponse},
	} {
		doer := &scriptedModelDoer{status: tc.status, responses: []string{tc.body}}
		ids, reason := listProviderModels(context.Background(), doer, "openai", "", "sk-secret")
		if ids != nil || reason != tc.want {
			t.Fatalf("status %d: ids=%v reason=%q want %q", tc.status, ids, reason, tc.want)
		}
	}
}

func TestLLMModelListCacheExpiresAndInvalidatesPerOrg(t *testing.T) {
	cache := newLLMModelListCache()
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	keyA := llmModelListKey("org-a", "openai", "", "k1")
	keyB := llmModelListKey("org-b", "openai", "", "k1")
	cache.put(keyA, llmModelListEntry{orgID: "org-a", models: []string{"m"}, fetchedAt: now}, now)
	cache.put(keyB, llmModelListEntry{orgID: "org-b", models: []string{"m"}, fetchedAt: now}, now)

	if _, ok := cache.get(keyA, now.Add(59*time.Minute)); !ok {
		t.Fatal("entry must be served inside the TTL")
	}
	if _, ok := cache.get(keyA, now.Add(time.Hour)); ok {
		t.Fatal("entry must expire at the TTL")
	}
	cache.put(keyA, llmModelListEntry{orgID: "org-a", models: []string{"m"}, fetchedAt: now}, now)
	cache.invalidateOrg("org-a")
	if _, ok := cache.get(keyA, now); ok {
		t.Fatal("org-a entry must be gone after invalidation")
	}
	if _, ok := cache.get(keyB, now); !ok {
		t.Fatal("org-b entry must survive org-a invalidation")
	}
}

func TestLLMModelListKeyChangesWithKeyAndBaseURLAndHoldsNoSecret(t *testing.T) {
	a := llmModelListKey("org", "openai", "", "sk-one")
	if a == llmModelListKey("org", "openai", "", "sk-two") || a == llmModelListKey("org", "openai", "https://x.invalid/v1", "sk-one") {
		t.Fatal("key must change with api key and base url")
	}
	if strings.Contains(a, "sk-one") {
		t.Fatalf("cache key holds the secret: %s", a)
	}
}

func TestListProviderModelsDropsIDsThatCarryTheSavedKeyOrAreOversized(t *testing.T) {
	long := strings.Repeat("m", llmModelIDMaxLen+1)
	for _, provider := range []string{"openai", "anthropic", "gemini"} {
		body := `{"data":[{"id":"ok-model"},{"id":"sk-secret"},{"id":"prefix-sk-secret-suffix"},{"id":"` + long + `"}],"models":[` +
			`{"name":"models/ok-model","supportedGenerationMethods":["generateContent"]},` +
			`{"name":"models/sk-secret","supportedGenerationMethods":["generateContent"]}]}`
		doer := &scriptedModelDoer{responses: []string{body}}
		ids, reason := listProviderModels(context.Background(), doer, provider, "", "sk-secret")
		if reason != "" || len(ids) != 1 || ids[0] != "ok-model" {
			t.Fatalf("%s: ids=%v reason=%q", provider, ids, reason)
		}
	}
}

func TestListProviderModelsDropsSecretShapedIDs(t *testing.T) {
	body := `{"data":[{"id":"gpt-5"},{"id":"sk-proj-abcdefghijklmnopqrstuvwxyz0123"},` +
		`{"id":"AIzaSyA-abcdefghijklmnopqrstuvwxyz0123456"},{"id":"Bearer abc.def"}]}`
	doer := &scriptedModelDoer{responses: []string{body}}
	ids, reason := listProviderModels(context.Background(), doer, "openai", "", "a-different-key")
	if reason != "" || len(ids) != 1 || ids[0] != "gpt-5" {
		t.Fatalf("ids=%v reason=%q", ids, reason)
	}
}

func TestListProviderModelsDropsEncodedAndSplitKeyMaterial(t *testing.T) {
	key := strings.Join([]string{"my-private", "api-key", "0123456789"}, "-")
	forms := map[string]string{
		"base64":      base64.StdEncoding.EncodeToString([]byte(key)),
		"base64url":   base64.RawURLEncoding.EncodeToString([]byte(key)),
		"base64shift": "x" + base64.RawStdEncoding.EncodeToString([]byte("zz" + key))[3:],
		"hex":         hex.EncodeToString([]byte(key)),
		"upper":       strings.ToUpper(key),
		"percent":     url.QueryEscape(key + "/"),
	}
	for name, form := range forms {
		body := `{"data":[{"id":"gpt-5"},{"id":"` + form + `"}]}`
		doer := &scriptedModelDoer{responses: []string{body}}
		ids, reason := listProviderModels(context.Background(), doer, "openai", "", key)
		if reason != "" || len(ids) != 1 || ids[0] != "gpt-5" {
			t.Fatalf("%s: ids=%v reason=%q", name, ids, reason)
		}
	}

	half := len(key) / 2
	doer := &scriptedModelDoer{responses: []string{
		`{"data":[{"id":"gpt-5"},{"id":"` + key[:half] + `"}],"has_more":true,"last_id":"x"}`,
		`{"data":[{"id":"` + key[half:] + `"}]}`,
	}}
	ids, reason := listProviderModels(context.Background(), doer, "anthropic", "", key)
	if len(ids) != 0 || reason != llmModelsReasonInvalidResponse {
		t.Fatalf("split key: ids=%v reason=%q", ids, reason)
	}
}

func TestListProviderModelsNeverPutsTheKeyInAURL(t *testing.T) {
	key := strings.Join([]string{"my-private", "api-key", "0123456789"}, "-")
	doer := &scriptedModelDoer{responses: []string{
		`{"data":[{"id":"claude-a"}],"has_more":true,"last_id":"` + key + `"}`,
		`{"data":[{"id":"claude-b"}]}`,
	}}
	ids, _ := listProviderModels(context.Background(), doer, "anthropic", "", key)
	for _, r := range doer.requests {
		if strings.Contains(r.url, key) {
			t.Fatalf("key in url: %s", r.url)
		}
	}
	if len(ids) != 1 || ids[0] != "claude-a" {
		t.Fatalf("ids=%v", ids)
	}

	doer = &scriptedModelDoer{}
	ids, reason := listProviderModels(context.Background(), doer, "qwen", "https://provider.example.invalid/v1/"+key, key)
	if ids != nil || reason != llmModelsReasonInvalidBaseURL || len(doer.requests) != 0 {
		t.Fatalf("base url with key: ids=%v reason=%q calls=%d", ids, reason, len(doer.requests))
	}
}

func TestListProviderModelsAllowlistsIDsAndCursors(t *testing.T) {
	body := `{"data":[{"id":"gpt-5"},{"id":"ft:gpt-4o:org:name:abc"},{"id":"qwen_max_2"},{"id":"org@v1+beta"},{"id":"a b"},{"id":"a=b"},{"id":"../x"},{"id":"_x"}],"has_more":true,"last_id":"cur sor"}`
	doer := &scriptedModelDoer{responses: []string{body, `{"data":[{"id":"never"}]}`}}
	ids, reason := listProviderModels(context.Background(), doer, "anthropic", "", "a-key-of-some-length")
	if reason != "" || strings.Join(ids, ",") != "gpt-5,ft:gpt-4o:org:name:abc,qwen_max_2,org@v1+beta" {
		t.Fatalf("ids=%v reason=%q", ids, reason)
	}
	if len(doer.requests) != 1 {
		t.Fatalf("a cursor outside the allowlist must stop paging, requests=%d", len(doer.requests))
	}
}

func TestBaseURLCarriesKeyAtWriteTime(t *testing.T) {
	if !baseURLCarriesKey("https://p.example.invalid/v1/sk-live-0123456789", "sk-live-0123456789") {
		t.Fatal("a base url holding the key must be refused")
	}
	if baseURLCarriesKey("https://p.example.invalid/v1", "sk-live-0123456789") || baseURLCarriesKey("https://p.example.invalid/v1", "") {
		t.Fatal("a clean base url must pass")
	}
}

func TestListProviderModelsRefusesAResponseWithoutTheListField(t *testing.T) {
	for _, provider := range []string{"openai", "anthropic", "gemini"} {
		for _, body := range []string{`{}`, `{"object":"list"}`, `{"data":[]}`, `{"models":[]}`} {
			doer := &scriptedModelDoer{responses: []string{body}}
			ids, reason := listProviderModels(context.Background(), doer, provider, "", "a-key-of-some-length")
			if ids != nil || reason != llmModelsReasonInvalidResponse {
				t.Fatalf("%s %s: ids=%v reason=%q", provider, body, ids, reason)
			}
		}
	}
}

func TestCheckBaseURLRefusesAKeyBeforeAnyResolution(t *testing.T) {
	calls := 0
	spy := func(context.Context, string) (bool, string, error) { calls++; return true, "", nil }
	stored, fresh := "sk-stored-"+"0123456789", "sk-fresh-"+"0123456789"
	for name, tc := range map[string]struct {
		url  string
		keys []string
	}{
		"new key in host":    {"https://" + fresh + ".example.invalid/v1", []string{fresh, stored}},
		"stored key in host": {"https://" + stored + ".example.invalid/v1", []string{"", stored}},
		"stored key in path": {"https://p.example.invalid/" + stored, []string{stored}},
	} {
		valid, _, keyHit, err := checkBaseURL(context.Background(), tc.url, tc.keys, spy)
		if err != nil || valid || !keyHit || calls != 0 {
			t.Fatalf("%s: valid=%v keyHit=%v err=%v resolver calls=%d", name, valid, keyHit, err, calls)
		}
	}
	valid, _, keyHit, err := checkBaseURL(context.Background(), "https://p.example.invalid/v1", []string{stored}, spy)
	if err != nil || !valid || keyHit || calls != 1 {
		t.Fatalf("clean url: valid=%v keyHit=%v err=%v calls=%d", valid, keyHit, err, calls)
	}
}

type resolverSpy struct {
	calls int
	valid bool
}

func (s *resolverSpy) validate(context.Context, string) (bool, string, error) {
	s.calls++
	return s.valid, "spy", nil
}

func newKeyGuardHandlers(spy *resolverSpy, stored string) *handlers {
	return &handlers{
		modelCache:  newLLMModelListCache(),
		validateURL: spy.validate,
		storedKey:   func(context.Context, string) (string, error) { return stored, nil },
	}
}

func TestApplyLLMSettingsRefusesAKeyInTheBaseURLBeforeResolution(t *testing.T) {
	stored, fresh := "sk-stored-"+"0123456789", "sk-fresh-"+"0123456789"
	for name, tc := range map[string]struct {
		in     llmUpsert
		stored string
	}{
		"full PUT, new key in the host": {
			in: llmUpsert{provider: "openai", apiKey: &fresh, baseURL: strPtr("https://" + fresh + ".example.invalid/v1")},
		},
		"partial PUT, stored key in the host": {
			in:     llmUpsert{provider: "openai", baseURL: strPtr("https://" + stored + ".example.invalid/v1")},
			stored: stored,
		},
		"full PUT with another key, stored key in the path": {
			in:     llmUpsert{provider: "openai", apiKey: &fresh, baseURL: strPtr("https://p.example.invalid/" + stored)},
			stored: stored,
		},
	} {
		spy := &resolverSpy{}
		h := newKeyGuardHandlers(spy, tc.stored)
		out, refusal, err := h.applyLLMSettings(context.Background(), "org", tc.in)
		if err != nil || out != nil || refusal == nil || refusal.status != 400 || spy.calls != 0 {
			t.Fatalf("%s: out=%v refusal=%v err=%v resolver calls=%d", name, out, refusal, err, spy.calls)
		}
	}
}

func TestModelListRefusesAKeyInTheBaseURLBeforeResolution(t *testing.T) {
	key := "sk-listed-" + "0123456789"
	spy := &resolverSpy{}
	h := newKeyGuardHandlers(spy, "")
	doer := &scriptedModelDoer{}
	h.upstreamDoer = doer
	cfg := readinessBYOConfig{provider: "openai", apiKey: secrets.NewHidden(key), baseURL: "https://" + key + ".example.invalid/v1"}
	out, err := h.modelList(context.Background(), "org", cfg, "")
	if err != nil || spy.calls != 0 || len(doer.requests) != 0 {
		t.Fatalf("err=%v resolver calls=%d provider calls=%d", err, spy.calls, len(doer.requests))
	}
	body, _ := out.MarshalJSON()
	if !strings.Contains(string(body), `"reason":"invalid_base_url"`) || strings.Contains(string(body), key) {
		t.Fatalf("body=%s", body)
	}

	clean := readinessBYOConfig{provider: "openai", apiKey: secrets.NewHidden(key), baseURL: "https://p.example.invalid/v1"}
	doer.responses = []string{`{"data":[{"id":"gpt-5"}]}`}
	spy.valid = true
	out, err = h.modelList(context.Background(), "org", clean, "")
	body, _ = out.MarshalJSON()
	if err != nil || spy.calls != 1 || len(doer.requests) != 1 || !strings.Contains(string(body), `"source":"live"`) {
		t.Fatalf("clean: err=%v resolver calls=%d provider calls=%d body=%s", err, spy.calls, len(doer.requests), body)
	}
}

func strPtr(s string) *string { return &s }

func TestListProviderModelsRefusesALaterPageWithoutTheListField(t *testing.T) {
	doer := &scriptedModelDoer{responses: []string{
		`{"data":[{"id":"claude-a"}],"has_more":true,"last_id":"claude-a"}`,
		`{}`,
	}}
	ids, reason := listProviderModels(context.Background(), doer, "anthropic", "", "a-key-of-some-length")
	if ids != nil || reason != llmModelsReasonInvalidResponse || len(doer.requests) != 2 {
		t.Fatalf("ids=%v reason=%q requests=%d", ids, reason, len(doer.requests))
	}
}

type modelListView struct {
	provider string
	source   string
	reason   string
	items    []string
}

func viewModelList(t *testing.T, out interface{ MarshalJSON() ([]byte, error) }) (modelListView, string) {
	t.Helper()
	raw, err := out.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Provider *string `json:"provider"`
		Source   string  `json:"source"`
		Reason   *string `json:"reason"`
		Models   []struct {
			ID       string `json:"id"`
			Source   string `json:"source"`
			Verified bool   `json:"verified"`
		} `json:"models"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("%s: %v", raw, err)
	}
	view := modelListView{source: decoded.Source}
	if decoded.Provider != nil {
		view.provider = *decoded.Provider
	}
	if decoded.Reason != nil {
		view.reason = *decoded.Reason
	}
	for _, m := range decoded.Models {
		view.items = append(view.items, m.ID+"/"+m.Source+"/"+strconv.FormatBool(m.Verified))
	}
	return view, string(raw)
}

func newModelListHandlers() (*handlers, *scriptedModelDoer) {
	spy := &resolverSpy{valid: true}
	h := newKeyGuardHandlers(spy, "")
	doer := &scriptedModelDoer{}
	h.upstreamDoer = doer
	return h, doer
}

func TestModelListWithNoKeyIsTheCatalog(t *testing.T) {
	h, doer := newModelListHandlers()
	out, err := h.modelList(context.Background(), "org", readinessBYOConfig{provider: "anthropic", model: "claude-haiku-5-5"}, "")
	if err != nil {
		t.Fatal(err)
	}
	view, raw := viewModelList(t, out)
	if view.provider != "anthropic" || view.source != "catalog" || view.reason != "" || len(doer.requests) != 0 {
		t.Fatalf("view=%+v calls=%d body=%s", view, len(doer.requests), raw)
	}
	if got := strings.Join(view.items, ","); got != "claude-haiku-5-5/catalog/false,claude-opus-5-5/catalog/false,claude-sonnet-5-5/catalog/false" {
		t.Fatalf("items=%s", got)
	}
	if !strings.Contains(raw, `"fetched_at":null`) {
		t.Fatalf("body=%s", raw)
	}
}

func TestModelListNeverSendsTheSavedKeyToAnotherProvider(t *testing.T) {
	h, doer := newModelListHandlers()
	key := "sk-saved-" + "0123456789"
	cfg := readinessBYOConfig{provider: "openai", model: "gpt-5-mini", apiKey: secrets.NewHidden(key), baseURL: "https://gateway.example.invalid/v1"}
	out, err := h.modelList(context.Background(), "org", cfg, "anthropic")
	if err != nil {
		t.Fatal(err)
	}
	view, raw := viewModelList(t, out)
	if len(doer.requests) != 0 || view.provider != "anthropic" || view.source != "catalog" || view.reason != "" {
		t.Fatalf("view=%+v calls=%d", view, len(doer.requests))
	}
	for _, item := range view.items {
		if strings.HasPrefix(item, "gpt-5-mini/") {
			t.Fatalf("the saved openai model is listed under anthropic: %s", raw)
		}
	}
	if strings.Contains(raw, key) {
		t.Fatalf("body carries the key: %s", raw)
	}
}

func TestModelListMergesTheLiveListIntoTheCatalog(t *testing.T) {
	h, doer := newModelListHandlers()
	doer.responses = []string{`{"data":[{"id":"claude-sonnet-4-5"},{"id":"claude-haiku-5-5"}],"has_more":false}`}
	cfg := readinessBYOConfig{provider: "anthropic", model: "my-finetune", apiKey: secrets.NewHidden("sk-ant-" + "0123456789abcdef")}
	out, err := h.modelList(context.Background(), "org", cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	view, raw := viewModelList(t, out)
	if view.source != "live" || view.reason != "" || len(doer.requests) != 1 {
		t.Fatalf("view=%+v calls=%d body=%s", view, len(doer.requests), raw)
	}
	want := "claude-haiku-5-5/live/true,claude-sonnet-4-5/live/true,claude-opus-5-5/catalog/false,claude-sonnet-5-5/catalog/false,my-finetune/saved/false"
	if got := strings.Join(view.items, ","); got != want {
		t.Fatalf("items=%s\nwant %s", got, want)
	}
	cached, err := h.modelList(context.Background(), "org", cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	view, _ = viewModelList(t, cached)
	if view.source != "cache" || strings.Join(view.items, ",") != want || len(doer.requests) != 1 {
		t.Fatalf("cached view=%+v calls=%d", view, len(doer.requests))
	}
}

func TestModelListKeepsTheCatalogWhenTheLiveListFails(t *testing.T) {
	h, doer := newModelListHandlers()
	doer.status = 401
	cfg := readinessBYOConfig{provider: "anthropic", apiKey: secrets.NewHidden("sk-ant-" + "0123456789abcdef")}
	out, err := h.modelList(context.Background(), "org", cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	view, raw := viewModelList(t, out)
	if view.source != "catalog" || view.reason != "provider_rejected" || len(view.items) != 3 {
		t.Fatalf("view=%+v body=%s", view, raw)
	}
}

func TestModelListIsUnavailableOnlyWhenThereIsNothingToList(t *testing.T) {
	h, _ := newModelListHandlers()
	for name, tc := range map[string]struct {
		cfg    readinessBYOConfig
		reason string
	}{
		"no provider":                     {readinessBYOConfig{}, "not_configured"},
		"uncatalogued provider, no key":   {readinessBYOConfig{provider: "ollama"}, "base_url_required"},
		"uncatalogued, keyed, no baseurl": {readinessBYOConfig{provider: "azure-openai", apiKey: secrets.NewHidden("k-0123456789")}, "base_url_required"},
	} {
		out, err := h.modelList(context.Background(), "org", tc.cfg, "")
		if err != nil {
			t.Fatal(err)
		}
		view, raw := viewModelList(t, out)
		if view.source != "unavailable" || view.reason != tc.reason || len(view.items) != 0 || !strings.Contains(raw, `"models":[]`) {
			t.Errorf("%s: view=%+v body=%s", name, view, raw)
		}
	}
}

func TestModelListListsTheSavedModelOfAnUncataloguedProvider(t *testing.T) {
	h, _ := newModelListHandlers()
	out, err := h.modelList(context.Background(), "org", readinessBYOConfig{provider: "ollama", model: "llama3"}, "")
	if err != nil {
		t.Fatal(err)
	}
	view, _ := viewModelList(t, out)
	if view.source != "catalog" || strings.Join(view.items, ",") != "llama3/saved/false" {
		t.Fatalf("view=%+v", view)
	}
}

func TestMergeLLMModelsOrdersVerifiedFirstThenAlphabetical(t *testing.T) {
	items := mergeLLMModels([]string{"z-live", "b-both"}, []string{"b-both", "a-cat", "y-cat"}, "m-saved")
	var got []string
	for _, item := range items {
		got = append(got, item.id+"/"+item.source)
	}
	want := "b-both/live,z-live/live,a-cat/catalog,m-saved/saved,y-cat/catalog"
	if strings.Join(got, ",") != want {
		t.Fatalf("got %v want %s", got, want)
	}
	if again := mergeLLMModels([]string{"z-live", "b-both"}, []string{"b-both", "a-cat", "y-cat"}, "a-cat"); len(again) != 4 {
		t.Fatalf("a saved model already in the catalog must not repeat: %v", again)
	}
}

func TestSavedModelListableFiltersLikeProviderIDs(t *testing.T) {
	key := "sk-listed-" + "0123456789"
	for model, want := range map[string]bool{
		"gpt-5-mini": true, "": false, "has space": false, "sk-" + "abcdefghijklmnopqrst": false,
		"Bearer x": false, strings.Repeat("a", 129): false, "prefix-" + key: false,
	} {
		if got := savedModelListable(model, key); got != want {
			t.Errorf("%q: got %v want %v", model, got, want)
		}
	}
}
