package providerfoundation

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type callbackDoerFunc func(*http.Request) (*http.Response, error)

func (f callbackDoerFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
}

func TestPagerDutyTokensFromPayloadMatchesTokens(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 600, time.UTC)
	cases := []struct {
		name      string
		body      string
		wantErr   error
		access    string
		refresh   string
		scopes    string
		expiresIn int64
	}{
		{"full", `{"access_token":"a","refresh_token":"r","scope":"b.read a.read a.read","expires_in":120}`, nil, "a", "r", "a.read,b.read", 120},
		{"no expires_in is an hour", `{"access_token":"a"}`, nil, "a", "", "", 3600},
		{"string expires_in", `{"access_token":"a","expires_in":" 7 "}`, nil, "a", "", "", 7},
		{"underscored string expires_in", `{"access_token":"a","expires_in":"1_0"}`, nil, "a", "", "", 10},
		{"float expires_in is an hour", `{"access_token":"a","expires_in":1.5}`, nil, "a", "", "", 3600},
		{"null expires_in is an hour", `{"access_token":"a","expires_in":null}`, nil, "a", "", "", 3600},
		{"list expires_in is an hour", `{"access_token":"a","expires_in":[1]}`, nil, "a", "", "", 3600},
		{"true is one second", `{"access_token":"a","expires_in":true}`, nil, "a", "", "", 1},
		{"negative is not clamped", `{"access_token":"a","expires_in":-30}`, nil, "a", "", "", -30},
		{"empty refresh_token is none", `{"access_token":"a","refresh_token":""}`, nil, "a", "", "", 3600},
		{"unparseable expires_in string", `{"access_token":"a","expires_in":"abc"}`, ErrPagerDutyExchangeMalformed, "", "", "", 0},
		{"beyond datetime", `{"access_token":"a","expires_in":9000000000000}`, ErrPagerDutyExchangeMalformed, "", "", "", 0},
		{"missing access_token", `{"scope":"a"}`, ErrPagerDutyExchangeMalformed, "", "", "", 0},
		{"not json", `nope`, ErrPagerDutyExchangeMalformed, "", "", "", 0},
		{"a list", `[]`, ErrPagerDutyExchangeMalformed, "", "", "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pagerDutyTokensFromPayload([]byte(tc.body), now)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr != nil {
				return
			}
			if got.AccessToken != tc.access || got.RefreshToken != tc.refresh || strings.Join(got.GrantedScopes, ",") != tc.scopes {
				t.Fatalf("got %+v", got)
			}
			if want := now.Add(time.Duration(tc.expiresIn) * time.Second); !got.ExpiresAt.Equal(want) {
				t.Fatalf("expires_at %v, want %v", got.ExpiresAt, want)
			}
		})
	}
}

func TestExchangePagerDutyAuthorizationCodeStatusClasses(t *testing.T) {
	for status, want := range map[int]error{
		200: nil, 302: ErrPagerDutyExchangeBadStatus, 400: ErrPagerDutyExchangeRejected, 499: ErrPagerDutyExchangeRejected,
		500: ErrPagerDutyExchangeBadStatus, 199: ErrPagerDutyExchangeBadStatus,
	} {
		doer := callbackDoerFunc(func(r *http.Request) (*http.Response, error) {
			return jsonResponse(status, `{"access_token":"a"}`), nil
		})
		_, err := ExchangePagerDutyAuthorizationCode(context.Background(), doer, PagerDutyRevokeConfig{ClientID: "id"}, "code", "verifier", time.Now())
		if !errors.Is(err, want) && !(want == nil && err == nil) {
			t.Errorf("status %d: err = %v, want %v", status, err, want)
		}
	}
	failing := callbackDoerFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("down") })
	if _, err := ExchangePagerDutyAuthorizationCode(context.Background(), failing, PagerDutyRevokeConfig{}, "c", "v", time.Now()); !errors.Is(err, ErrPagerDutyExchangeUnavailable) {
		t.Fatalf("transport failure: %v", err)
	}
}

func TestPagerDutyAccountIdentityMatchesPython(t *testing.T) {
	cases := []struct {
		name, body, id, display, sub string
		fail                         bool
	}{
		{"account", `{"services":[{"account":{"id":" A1 ","subdomain":" Acme ","name":" Acme Co "}}]}`, "A1", "Acme Co", "Acme", false},
		{"name falls back to the subdomain", `{"services":[{"account":{"id":"A1","subdomain":"acme","name":"  "}}]}`, "A1", "acme", "acme", false},
		{"html_url when the account is incomplete", `{"services":[{"account":{"id":"A1"},"html_url":"https://Foo.pagerduty.com/x"}]}`, "foo", "foo", "foo", false},
		{"html_url on the api host", `{"services":[{"html_url":"https://api.pagerduty.com/x"}]}`, "", "", "", true},
		{"html_url elsewhere", `{"services":[{"html_url":"https://foo.example.com/x"}]}`, "", "", "", true},
		{"html_url with too few labels", `{"services":[{"html_url":"https://pagerduty.com/x"}]}`, "", "", "", true},
		{"no services", `{"services":[]}`, "", "", "", true},
		{"services not a list", `{"services":"x"}`, "", "", "", true},
		{"a list", `[]`, "", "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, display, sub, err := pagerDutyAccountIdentity([]byte(tc.body))
			if (err != nil) != tc.fail {
				t.Fatalf("err = %v", err)
			}
			if id != tc.id || display != tc.display || sub != tc.sub {
				t.Fatalf("got %q %q %q", id, display, sub)
			}
		})
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestExchangePagerDutyAuthorizationCodeBodyReadFailureIsUnavailable(t *testing.T) {
	for _, status := range []int{200, 403, 500} {
		doer := callbackDoerFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(failingReader{})}, nil
		})
		_, err := ExchangePagerDutyAuthorizationCode(context.Background(), doer, PagerDutyRevokeConfig{ClientID: "id"}, "c", "v", time.Now())
		if !errors.Is(err, ErrPagerDutyExchangeUnavailable) {
			t.Errorf("status %d with a body that fails to read: err = %v, want unavailable", status, err)
		}
	}
}

func TestRevokePagerDutyOAuthTokenRefusesAnyNon2xx(t *testing.T) {
	for status, wantErr := range map[int]bool{200: false, 204: false, 199: true, 301: true, 302: true, 307: true, 400: true, 500: true} {
		doer := callbackDoerFunc(func(*http.Request) (*http.Response, error) { return jsonResponse(status, ``), nil })
		err := RevokePagerDutyOAuthToken(context.Background(), doer, PagerDutyRevokeConfig{ClientID: "id"}, "tok")
		if (err != nil) != wantErr {
			t.Errorf("status %d: err = %v, want error %v", status, err, wantErr)
		}
	}
}
