package providersync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func objectTooLargeStrings(value reflect.Value, name string, depth int, out map[string]string) {
	if depth > 4 || !value.IsValid() {
		return
	}
	switch value.Kind() {
	case reflect.String:
		out[name] = value.String()
	case reflect.Pointer, reflect.Interface:
		if !value.IsNil() {
			objectTooLargeStrings(value.Elem(), name, depth+1, out)
		}
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			objectTooLargeStrings(value.Field(index), name+"."+value.Type().Field(index).Name, depth+1, out)
		}
	}
}

// The cap error keeps the cap and the content length for the walk fallback
// and nothing of the request path (owner, repository, sha, query).
func TestObjectTooLargeErrorHoldsNoRawRequestPath(t *testing.T) {
	t.Parallel()
	body := strings.Repeat("a", nativeMaxObjectBytes+1)
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Content-Type": []string{"application/json"}},
			Body:          io.NopCloser(strings.NewReader(body)),
			ContentLength: int64(len(body)),
			Request:       request,
		}, nil
	})
	client := gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com")
	var target any
	err := fetchObjectRoute(context.Background(), client, githubTreeRoute, "/repos/acme/secret/git/trees/deadbeef?recursive=true", &target)
	var tooLarge *providerfoundation.ObjectTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("error=%v, want *providerfoundation.ObjectTooLargeError", err)
	}
	if tooLarge.CapBytes != nativeMaxObjectBytes || tooLarge.ContentLength != int64(len(body)) {
		t.Fatalf("cap=%d length=%d, want the cap and the content length kept", tooLarge.CapBytes, tooLarge.ContentLength)
	}
	held := map[string]string{}
	objectTooLargeStrings(reflect.ValueOf(tooLarge), "ObjectTooLargeError", 0, held)
	for name, text := range held {
		for _, raw := range []string{"acme", "secret", "deadbeef", "recursive"} {
			if strings.Contains(text, raw) {
				t.Errorf("%s = %q holds raw request path text %q", name, text, raw)
			}
		}
	}
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Route != githubTreeRoute {
		t.Fatalf("error=%v, want the response failure to name route %s", err, githubTreeRoute)
	}
}
