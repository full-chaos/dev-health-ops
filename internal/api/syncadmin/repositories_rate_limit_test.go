package syncadmin

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

type scriptedGitLab struct {
	status  int
	headers http.Header
	calls   int
}

func (s *scriptedGitLab) RoundTrip(request *http.Request) (*http.Response, error) {
	s.calls++
	return &http.Response{StatusCode: s.status, Header: s.headers.Clone(), Request: request,
		Body: io.NopCloser(strings.NewReader(`{"message":"gitlab-body-marker internal.example"}`))}, nil
}

const planted429Detail = "The provider rate limit was reached. Try again later."

func rateLimitHandlers(transport http.RoundTripper) (*handlers, *bytes.Buffer) {
	logs := &bytes.Buffer{}
	return &handlers{logger: slog.New(slog.NewTextHandler(logs, nil)), decryptor: tokenDecryptor{},
		gitlabHTTP: &http.Client{Transport: transport}}, logs
}

func resolveWith(h *handlers, repos []string) error {
	credentialID := "11111111-1111-1111-1111-111111111111"
	options := pyjson.NewObject()
	options.Set("group", "platform")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, err := h.resolveGitLabProjects(ctx, credentialTx{}, "org-1", &credentialID, options, repos)
	return err
}

func TestAGitLabRateLimitOnANamedRepoAnswers429AtOnce(t *testing.T) {
	transport := &scriptedGitLab{status: http.StatusTooManyRequests, headers: http.Header{"Retry-After": {"86400"}}}
	h, logs := rateLimitHandlers(transport)
	err := resolveWith(h, []string{"api"})
	var refusal *answer
	if !errors.As(err, &refusal) || refusal.status != http.StatusTooManyRequests {
		t.Fatalf("err = %v, WANT a 429 refusal", err)
	}
	if refusal.detail != planted429Detail {
		t.Fatalf("detail %v, want the fixed text", refusal.detail)
	}
	if transport.calls != 1 {
		t.Fatalf("%d requests, WANT 1", transport.calls)
	}
	if !strings.Contains(logs.String(), "gitlab-body-marker") && !strings.Contains(logs.String(), "rate limit") {
		t.Fatalf("log lacks the provider cause: %s", logs.String())
	}
	recorder := httptest.NewRecorder()
	h.answerOrFail(recorder, httptest.NewRequest(http.MethodPut, "/x", nil), "replace_repository_selection", err)
	if want := `{"detail":"` + planted429Detail + `"}`; recorder.Code != http.StatusTooManyRequests || recorder.Body.String() != want {
		t.Fatalf("rendered %d %q, want 429 %q", recorder.Code, recorder.Body.String(), want)
	}
}

func TestAGitLabErrorThatIsNotARateLimitKeepsTheParity400(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		transport := &scriptedGitLab{status: code, headers: http.Header{}}
		h, _ := rateLimitHandlers(transport)
		err := resolveWith(h, []string{"api"})
		var refusal *answer
		if !errors.As(err, &refusal) || refusal.status != http.StatusBadRequest {
			t.Fatalf("GitLab %d: err = %v, want the 400 refusal", code, err)
		}
		if refusal.detail != "Failed to list GitLab projects for group 'platform'" {
			t.Fatalf("GitLab %d: detail %v", code, refusal.detail)
		}
	}
}

func TestAGitLabRateLimitOnNumericEntriesOnlyStillTreatsThemAsIds(t *testing.T) {
	transport := &scriptedGitLab{status: http.StatusTooManyRequests, headers: http.Header{"Retry-After": {"86400"}}}
	h, logs := rateLimitHandlers(transport)
	err := resolveWith(h, []string{"123"})
	if err != nil {
		t.Fatalf("err = %v, want none: the listing failure is logged and the entries are ids", err)
	}
	if !strings.Contains(logs.String(), "treating them as project ids") {
		t.Fatalf("log lacks the numeric fallback line: %s", logs.String())
	}
}
