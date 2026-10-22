package docstools

import (
	"io"
	"net/http"
	"time"
)

// FreshnessRunForTest is check-freshness with a fixed date.today().
func FreshnessRunForTest(args []string, stdout, stderr io.Writer, today time.Time) int {
	return freshnessRun(args, stdout, stderr, today)
}

// ExternalLinksRunForTest is check-external-links with a fixed date.today() and the given fetcher in place of urllib.
func ExternalLinksRunForTest(args []string, stdout, stderr io.Writer, today time.Time, fetch func(string) (bool, string, error)) int {
	return externalLinksRun(args, stdout, stderr, today, fetch)
}

// HTTPFetcherForTest is the production fetcher over the given round tripper.
func HTTPFetcherForTest(rt http.RoundTripper) func(string) (bool, string, error) {
	return elHTTPFetcher(rt, time.Second)
}

// SlugifyForTest is the anchor slug of a raw Markdown heading.
func SlugifyForTest(heading string) string { return dlSlugify(heading) }

// YAMLReprForTest is repr(yaml.safe_load(text)), or the refusal.
func YAMLReprForTest(text string) (string, error) {
	v, err := ylLoad(text)
	if err != nil {
		return "", err
	}
	return v.repr(), nil
}
