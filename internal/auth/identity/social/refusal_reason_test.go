package social

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// The wire reason set is closed, each value is an auth-outcome.v1 reason, and the conflicts whose answer must not
// tell accounts apart share one value.
func TestRefusalReasonsAreAClosedSubsetOfTheReasonVocabulary(t *testing.T) {
	want := []string{"malformed", "invalid_credentials", "expired", "principal_not_found", "principal_deactivated",
		"email_unverified", "identity_conflict", "signup_unavailable", "dependency_unavailable", "last_sign_in_method"}
	if got := RefusalReasons(); !slices.Equal(got, want) {
		t.Fatalf("RefusalReasons() = %v, want %v", got, want)
	}
	for _, wire := range RefusalReasons() {
		if !slices.Contains(contracts.AuthOutcomeReasonCodes, wire) {
			t.Errorf("wire reason %q is not an auth-outcome.v1 reason", wire)
		}
	}
	for _, reason := range []Reason{ReasonEmailCollision, ReasonProviderLinked, ReasonIdentityConflict} {
		if got := RefusalReason(reason); got != string(ReasonIdentityConflict) {
			t.Errorf("RefusalReason(%s) = %q, want the one conflict value %q", reason, got, ReasonIdentityConflict)
		}
	}
	for _, reason := range []Reason{ReasonAccepted, ReasonAudience, ReasonStoreUnavailable} {
		if got := RefusalReason(reason); got != "" {
			t.Errorf("RefusalReason(%s) = %q, want none", reason, got)
		}
	}
}

// classifiedFailures is one error of every class writeError answers.
var classifiedFailures = []error{
	&ConflictError{ExistingMethod: "password"}, ErrProviderUnavailable, ErrProviderNotConfigured, ErrProviderRefused,
	ErrInvalidProviderID, ErrEmailUnverified, ErrUnusableEmail, ErrPrincipalDisabled, identity.ErrSignupNotAvailable,
	ErrReauthenticationRequired, identity.ErrIdentityConflict, ErrProviderLinked, identity.ErrNotFound,
	identity.ErrLastSignInMethod, errors.New("unclassified"), fmt.Errorf("wrapped: %w", ErrProviderRefused),
}

// Every answer writeError gives with a detail object carries the wire reason of the reason Classify names for the
// same error, and a 5xx without a detail object carries none.
func TestEveryRefusalAnswersItsWireReason(t *testing.T) {
	for _, failure := range classifiedFailures {
		rec := httptest.NewRecorder()
		writeError(rec, GitHub, failure)
		status, reason := Classify(failure)
		if rec.Code != status {
			t.Errorf("%v: writeError %d, Classify %d", failure, rec.Code, status)
		}
		detail, ok := refusalDetailOf(t, rec)
		if !ok {
			if rec.Code != http.StatusInternalServerError {
				t.Errorf("%v: %d %s has no detail object", failure, rec.Code, rec.Body)
			}
			continue
		}
		if detail.Reason == nil || *detail.Reason != RefusalReason(reason) || *detail.Reason == "" {
			t.Errorf("%v: body %s, want reason %q", failure, rec.Body, RefusalReason(reason))
		}
	}
}

type wireDetail struct {
	Message string  `json:"message"`
	Reason  *string `json:"reason"`
}

func refusalDetailOf(t *testing.T, rec *httptest.ResponseRecorder) (wireDetail, bool) {
	t.Helper()
	var body struct {
		Detail json.RawMessage `json:"detail"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %s: %v", rec.Body, err)
	}
	var detail wireDetail
	if json.Unmarshal(body.Detail, &detail) != nil {
		return wireDetail{}, false
	}
	return detail, true
}

// refusalRow is one (status, message) refusal answer and its wire reason; <P> stands for the provider name.
type refusalRow struct {
	status  int
	message string
	reason  string
}

// refusalRows are the social refusal answers of both planes. The wire reason must be a function of (status,
// message): it may not tell apart two answers that status and message do not.
var refusalRows = []refusalRow{
	{http.StatusConflict, ConflictMessage, "identity_conflict"},
	{http.StatusConflict, "Cannot remove the last sign-in method", "last_sign_in_method"},
	{http.StatusForbidden, "The provider has not verified this email address", "email_unverified"},
	{http.StatusBadRequest, "The provider returned an invalid user id", "malformed"},
	{http.StatusBadRequest, "The provider returned an unusable email address", "malformed"},
	{http.StatusUnauthorized, "Invalid or expired provider token", "invalid_credentials"},
	{http.StatusServiceUnavailable, "Social login not configured for <P>", "dependency_unavailable"},
	{http.StatusForbidden, "Account is disabled", "principal_deactivated"},
	{http.StatusForbidden, "Sign-up with <P> is not available", "signup_unavailable"},
	{http.StatusForbidden, "Sign in again to change linked accounts", "expired"},
	{http.StatusNotFound, "Identity not found", "principal_not_found"},
}

// The wire reason is a function of (status, message) on every refusal row of auth-service (writeError, the unlink
// id check) and of go-api (each refuseReason call of socialLogin, read from its source): no (status, message) pair
// answers two reasons, and every answer is a row of refusalRows.
func TestTheWireReasonIsAFunctionOfStatusAndMessage(t *testing.T) {
	want := map[[2]string]string{}
	for _, row := range refusalRows {
		key := [2]string{strconv.Itoa(row.status), row.message}
		if _, dup := want[key]; dup {
			t.Fatalf("refusalRows names %v twice", key)
		}
		want[key] = row.reason
	}
	seen := map[[2]string]map[string][]string{}
	note := func(plane string, status int, message, reason string) {
		key := [2]string{strconv.Itoa(status), message}
		if seen[key] == nil {
			seen[key] = map[string][]string{}
		}
		seen[key][reason] = append(seen[key][reason], plane)
	}
	for _, failure := range classifiedFailures {
		rec := httptest.NewRecorder()
		writeError(rec, GitHub, failure)
		if detail, ok := refusalDetailOf(t, rec); ok && detail.Reason != nil {
			note("auth-service "+failure.Error(), rec.Code, strings.ReplaceAll(detail.Message, "github", "<P>"), *detail.Reason)
		}
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/", nil)
	req.SetPathValue("id", "not-a-uuid")
	Handlers{Auth: signedInAs{who: &SignedIn{PrincipalID: uuid.New(), AuthTime: time.Now()}}}.Unlink(rec, req)
	if detail, ok := refusalDetailOf(t, rec); ok && detail.Reason != nil {
		note("auth-service unlink id", rec.Code, detail.Message, *detail.Reason)
	} else {
		t.Errorf("unlink of a non-id: %d %s carries no reason", rec.Code, rec.Body)
	}
	for _, site := range goAPIRefusalSites(t) {
		note(fmt.Sprintf("go-api social.go:%d", site.line), site.status, site.message, site.reason)
	}
	for key, reasons := range seen {
		if len(reasons) != 1 {
			t.Errorf("%v answers %d reasons: %v", key, len(reasons), reasons)
		}
		for reason, planes := range reasons {
			if want[key] != reason {
				t.Errorf("%v answers %q (%v), want %q", key, reason, planes, want[key])
			}
		}
	}
	for key := range want {
		if seen[key] == nil {
			t.Errorf("row %v is answered by no refusal site", key)
		}
	}
}

// goAPIRefusalSite is one refuseReason call of go-api's socialLogin.
type goAPIRefusalSite struct {
	line    int
	status  int
	message string
	reason  string
}

const goAPISocialFile = "../../../api/session/social.go"

var httpStatuses = map[string]int{
	"StatusBadRequest": http.StatusBadRequest, "StatusUnauthorized": http.StatusUnauthorized, "StatusForbidden": http.StatusForbidden,
	"StatusNotFound": http.StatusNotFound, "StatusConflict": http.StatusConflict, "StatusServiceUnavailable": http.StatusServiceUnavailable,
}

// goAPIRefusalSites reads every refuseReason(w, http.Status..., message, social.RefusalReason(social.Reason...)) call
// of go-api's social.go, with its message constants and the reason constants of reason.go resolved.
func goAPIRefusalSites(t *testing.T) []goAPIRefusalSite {
	t.Helper()
	fset := token.NewFileSet()
	file := parseGo(t, fset, goAPISocialFile)
	consts := stringConsts(file)
	reasons := stringConsts(parseGo(t, token.NewFileSet(), "reason.go"))
	var sites []goAPIRefusalSite
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || calleeName(call) != "refuseReason" {
			return true
		}
		line := fset.Position(call.Pos()).Line
		if len(call.Args) != 4 {
			t.Fatalf("social.go:%d: refuseReason with %d arguments", line, len(call.Args))
		}
		status, ok := httpStatuses[selectorName(call.Args[1])]
		if !ok {
			t.Fatalf("social.go:%d: status %s is not a known http.Status constant", line, exprText(fset, call.Args[1]))
		}
		message, ok := messageText(call.Args[2], consts)
		if !ok {
			t.Fatalf("social.go:%d: message %s is not a constant text", line, exprText(fset, call.Args[2]))
		}
		inner, ok := call.Args[3].(*ast.CallExpr)
		if !ok || selectorName(inner.Fun) != "RefusalReason" || len(inner.Args) != 1 {
			t.Fatalf("social.go:%d: the reason %s is not social.RefusalReason(social.Reason...)", line, exprText(fset, call.Args[3]))
		}
		reason, ok := reasons[selectorName(inner.Args[0])]
		if !ok {
			t.Fatalf("social.go:%d: %s is not a Reason constant", line, exprText(fset, inner.Args[0]))
		}
		sites = append(sites, goAPIRefusalSite{line: line, status: status, message: message, reason: RefusalReason(Reason(reason))})
		return true
	})
	if len(sites) == 0 {
		t.Fatal("go-api social.go has no refuseReason call")
	}
	return sites
}

func parseGo(t *testing.T, fset *token.FileSet, path string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

// stringConsts are the file's top-level string constants by name.
func stringConsts(file *ast.File) map[string]string {
	out := map[string]string{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				if i < len(value.Values) {
					if lit, ok := value.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						if text, err := strconv.Unquote(lit.Value); err == nil {
							out[name.Name] = text
						}
					}
				}
			}
		}
	}
	return out
}

// messageText is a message argument as text: a literal, a constant, or a literal + the provider (<P>).
func messageText(expr ast.Expr, consts map[string]string) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		text, err := strconv.Unquote(e.Value)
		return text, err == nil
	case *ast.Ident:
		text, ok := consts[e.Name]
		return text, ok
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return "", false
		}
		left, ok := messageText(e.X, consts)
		if !ok {
			return "", false
		}
		if ident, isIdent := e.Y.(*ast.Ident); isIdent && ident.Name == "provider" {
			return left + "<P>", true
		}
	}
	return "", false
}

func calleeName(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	}
	return ""
}

func selectorName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.SelectorExpr:
		return e.Sel.Name
	case *ast.Ident:
		return e.Name
	}
	return ""
}

func exprText(fset *token.FileSet, expr ast.Expr) string {
	return fmt.Sprintf("the expression at line %d", fset.Position(expr.Pos()).Line)
}

// census is one answer-writing call of a social refusal route: a writer that carries a wire reason, or a named
// exemption (an answer the closed set does not cover, by ruling).
type census struct {
	file  string
	funcs []string
	// carries are writers whose answer has a wire reason (or is a success).
	carries map[string]string
	// exempt are writers whose answer has no wire reason, each with its ruled exemption.
	exempt map[string]string
}

var refusalCensus = []census{
	{
		file:  "handler.go",
		funcs: []string{"readTokenRequest", "Exchange", "Link", "Unlink", "writeError"},
		carries: map[string]string{
			"writeError":   "every class through Classify",
			"WriteSession": "success",
			"WriteHeader":  "204 unlink success",
		},
		exempt: map[string]string{
			"writeInternal": "500 {\"detail\":\"Internal Server Error\"}: no detail object",
			"Admit":         "429 link rate limit, error.v1 with its own reason_code (authapi admit)",
		},
	},
	{
		file:  goAPISocialFile,
		funcs: []string{"socialLogin"},
		carries: map[string]string{
			"refuseReason": "the closed set (TestTheWireReasonIsAFunctionOfStatusAndMessage)",
			"WriteModel":   "success",
		},
		exempt: map[string]string{
			"WriteJSON": "422 request validation (pydantic list)",
			"fail":      "500, or 503 auth-unavailable (sessionissue.UnavailableText)",
			"Write":     "usersowned 409 operation_moved (its own detail.reason)",
		},
	},
	{
		file:  "../../authapi/social.go",
		funcs: []string{"readSocialLogin", "compatSocialLogin", "socialSignedIn"},
		carries: map[string]string{
			"WriteError":        "social.WriteError: every class through Classify",
			"writeCompatModel":  "success",
			"auditSignInFailed": "the auth.login.failed row (Classify's reason), not an answer",
		},
		exempt: map[string]string{
			"readCompatBody":    "the shared compat body read: its transport refusals before the route",
			"WriteJSON":         "422 request validation (pydantic list)",
			"refuseBody":        "422 request validation (pydantic list)",
			"unavailableCompat": "503 auth-unavailable (textAuthUnavailable)",
			"fail":              "compat 409 Organization conflict, 503 auth-unavailable, or 500",
			"failMint":          "503 auth-unavailable or 500",
		},
	},
}

// writerName reports whether a call writes an answer: by name, every writer of these packages starts with one of
// these words, so a new writer cannot slip past the census.
func writerName(name string) bool {
	for _, prefix := range []string{"write", "Write", "refuse", "fail", "unavailable"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// Every answer-writing call of the social refusal routes (auth-service handlers and compat sign-in, go-api
// social-login) carries a wire reason or is a named exemption; in handler.go a writeDetail with no reason is only a
// 422, the 401 "Not authenticated" or the 400 body parse, and refuse (go-api's reasonless writer) is not called.
func TestEverySocialRefusalSiteCarriesAReasonOrANamedExemption(t *testing.T) {
	reasonless := map[string]string{
		"provider and provider_access_token are required": "422 request validation",
		"provider must be one of github, gitlab, google":  "422 request validation",
		"Not authenticated": "401 no session (the v1 routes answer error.v1 before this)",
	}
	for _, c := range refusalCensus {
		fset := token.NewFileSet()
		file := parseGo(t, fset, c.file)
		used := map[string]bool{}
		found := map[string]bool{}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !slices.Contains(c.funcs, fn.Name.Name) {
				continue
			}
			found[fn.Name.Name] = true
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := calleeName(call)
				at := fmt.Sprintf("%s:%d %s", c.file, fset.Position(call.Pos()).Line, name)
				switch {
				case name == "writeDetail":
					if len(call.Args) != 4 {
						t.Errorf("%s: writeDetail with %d arguments", at, len(call.Args))
						return true
					}
					if id, ok := call.Args[3].(*ast.Ident); ok && id.Name == "wire" {
						used["writeDetail"] = true
						return true
					}
					lit, isLit := call.Args[3].(*ast.BasicLit)
					message, _ := messageText(call.Args[2], nil)
					if !isLit || lit.Value != `""` || reasonless[message] == "" {
						t.Errorf("%s: a refusal %q with reason %s is neither a wire reason nor a named exemption", at, message, exprText(fset, call.Args[3]))
					}
				case name == "writeJSON" && selectorName(call.Args[1]) == "StatusOK":
					// the link answer
				case name == "writeJSON" && fn.Name.Name == "readTokenRequest":
					// 400 {"detail":"There was an error parsing the body"}: a text detail, go-api's body parse answer.
				case name == "writeJSON":
					t.Errorf("%s: a bare writeJSON answer that is neither a success nor the body parse", at)
				case name == "refuse":
					t.Errorf("%s: go-api's reasonless refuse on a social refusal route", at)
				case c.carries[name] != "" || c.exempt[name] != "":
					used[name] = true
				case writerName(name):
					t.Errorf("%s: an answer writer the census does not name", at)
				}
				return true
			})
		}
		for _, fn := range c.funcs {
			if !found[fn] {
				t.Errorf("%s: no func %s", c.file, fn)
			}
		}
		for name := range c.exempt {
			if !used[name] {
				t.Errorf("%s: exemption %s is never used; remove it", c.file, name)
			}
		}
	}
}

// outcomeFetcher answers Fetch with err, or with profile; its issuer is issuer.
type outcomeFetcher struct {
	issuer  string
	profile Profile
	err     error
}

func (outcomeFetcher) Configured(Provider) bool { return true }
func (f outcomeFetcher) Issuer(Provider) string { return f.issuer }
func (f outcomeFetcher) Fetch(context.Context, Provider, contracts.SecretString) (Profile, error) {
	return f.profile, f.err
}

// The outcome counter and log of exchange, link and unlink take their reason from one source, the refusal's error:
// the full reason internally (a provider's audience or malformed token, a provider email that is not text) and the
// folded value on the wire (invalid_credentials; no detail object on a 500).
func TestTheOutcomeReasonIsTheWireReasonsSource(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	owner := uuid.New()
	key := identity.ExternalKey{Issuer: SocialIssuer(GitHub), Provider: GitHub, Subject: "42"}
	held := identity.Identity{ID: uuid.New(), Issuer: key.Issuer, Provider: key.Provider, Subject: key.Subject}
	exchange := func(s *Service) error {
		_, err := s.Exchange(context.Background(), GitHub, contracts.NewSecretString("t"), nil)
		return err
	}
	cases := []struct {
		name      string
		operation Operation
		store     Store
		fetcher   ProfileFetcher
		run       func(*Service) error
		outcome   string
		status    int
		wire      string
	}{
		{"link with no sign-in time", OperationLink, stubStore{}, outcomeFetcher{issuer: key.Issuer},
			func(s *Service) error {
				_, err := s.Link(context.Background(), SignedIn{PrincipalID: owner}, GitHub, contracts.NewSecretString("t"))
				return err
			}, "expired", http.StatusForbidden, "expired"},
		{"unlink with no sign-in time", OperationUnlink, stubStore{}, outcomeFetcher{issuer: key.Issuer},
			func(s *Service) error { return s.Unlink(context.Background(), SignedIn{PrincipalID: owner}, held.ID) },
			"expired", http.StatusForbidden, "expired"},
		{"unlink of the last sign-in method", OperationUnlink,
			unlinkStore{principal: owner, identity: held, unlinkErr: identity.ErrLastSignInMethod}, outcomeFetcher{issuer: key.Issuer},
			func(s *Service) error {
				return s.Unlink(context.Background(), SignedIn{PrincipalID: owner, AuthTime: now}, held.ID)
			}, "last_sign_in_method", http.StatusConflict, "last_sign_in_method"},
		{"exchange of a profile of another client or issuer", OperationExchange, stubStore{},
			outcomeFetcher{issuer: "https://other.example", profile: Profile{ExternalKey: key, Email: "a@example.com", EmailVerified: true}},
			exchange, "audience", http.StatusUnauthorized, "invalid_credentials"},
		{"exchange of a token the provider client refuses for its audience", OperationExchange, stubStore{},
			outcomeFetcher{issuer: key.Issuer, err: &ProviderError{Reason: ReasonAudience, Step: "github token check"}},
			exchange, "audience", http.StatusUnauthorized, "invalid_credentials"},
		{"exchange of a token the provider client calls malformed", OperationExchange, stubStore{},
			outcomeFetcher{issuer: key.Issuer, err: &ProviderError{Reason: ReasonMalformed, Step: "token"}},
			exchange, "malformed", http.StatusUnauthorized, "invalid_credentials"},
		{"exchange of a verified provider email that is not text", OperationExchange, stubStore{},
			outcomeFetcher{issuer: key.Issuer, profile: Profile{ExternalKey: key, EmailVerified: true, EmailNotText: true}},
			exchange, "malformed", http.StatusInternalServerError, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var logs bytes.Buffer
			reader := sdkmetric.NewManualReader()
			svc, err := New(Config{Store: c.store, Providers: c.fetcher, Now: func() time.Time { return now },
				Logger: slog.New(slog.NewTextHandler(&logs, nil)),
				Meter:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")})
			if err != nil {
				t.Fatal(err)
			}
			refusal := c.run(svc)
			if refusal == nil {
				t.Fatal("no refusal")
			}
			if !strings.Contains(logs.String(), "reason_code="+c.outcome+"\n") {
				t.Fatalf("log has no reason_code=%s: %s", c.outcome, logs.String())
			}
			if strings.Contains(logs.String(), "social outcome failed") {
				t.Fatalf("a refusal is logged as a failure: %s", logs.String())
			}
			var collected metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &collected); err != nil {
				t.Fatal(err)
			}
			got := map[string]int64{}
			for _, scope := range collected.ScopeMetrics {
				for _, m := range scope.Metrics {
					sum, ok := m.Data.(metricdata.Sum[int64])
					if m.Name != "auth.social.outcomes" || !ok {
						continue
					}
					for _, point := range sum.DataPoints {
						reason, _ := point.Attributes.Value("reason_code")
						operation, _ := point.Attributes.Value("operation")
						got[operation.AsString()+"/"+reason.AsString()] += point.Value
					}
				}
			}
			if want := string(c.operation) + "/" + c.outcome; got[want] != 1 || len(got) != 1 {
				t.Fatalf("outcome counter = %v, want %s: 1", got, want)
			}
			rec := httptest.NewRecorder()
			writeError(rec, GitHub, refusal)
			if rec.Code != c.status {
				t.Fatalf("answer %d %s, want %d", rec.Code, rec.Body, c.status)
			}
			detail, ok := refusalDetailOf(t, rec)
			switch {
			case c.wire == "" && ok:
				t.Fatalf("answer %s carries a detail object, want none", rec.Body)
			case c.wire != "" && (!ok || detail.Reason == nil || *detail.Reason != c.wire):
				t.Fatalf("answer %s, want wire reason %q", rec.Body, c.wire)
			}
		})
	}
}
