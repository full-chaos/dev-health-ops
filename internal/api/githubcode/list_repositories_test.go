package githubcode

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

// The venue oracle's httpx.MockTransport answers any URL, so it cannot show
// what the real transport does with a Link header naming a scheme it cannot
// send: httpx raises UnsupportedProtocol before a request leaves, which the
// core does not catch (the route's generic 500). Here the same Link header
// must stop the listing with that exception, with no second request sent.
func TestListRepositoriesRefusesAnUnsupportedNextScheme(t *testing.T) {
	transport := &scriptedTransport{responses: []scripted{
		ok(page(repoItem(1, "api")), [2]string{"Link", `<ftp://files.test/x>; rel="next"`}),
		ok(page(repoItem(2, "web"))),
	}}
	client := Client{Token: "tok", HTTP: &http.Client{Transport: transport}}
	_, err := client.ListRepositories(context.Background(), ListOptions{Org: "acme"})
	typed, isError := err.(*Error)
	if !isError || typed.Class != "UnsupportedProtocol" || !strings.Contains(typed.Message, "unsupported protocol 'ftp://'") {
		t.Fatalf("error %v (%T), want UnsupportedProtocol", err, err)
	}
	if len(transport.seen) != 1 {
		t.Errorf("%d requests sent, want 1", len(transport.seen))
	}
}
