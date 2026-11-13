// Package wrongclass builds the two axes of the wrong-credential-class gate from the production contract files: the
// endpoint-profile rows of a service and the credential classes of credential-classes.json, with the request carrier
// each class is presented in. A row answers a class it does not accept the way it answers a garbage credential in the
// same carrier (or no credential, when the carrier is not the Authorization header): Answer and Capture compare those.
package wrongclass

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// Filler is the secret part of every synthetic credential: 43 urlsafe characters, no dot, no prefix.
var Filler = strings.Repeat("A", 43)

// carrierKinds make a request carrier; qualifierKinds only narrow one. Together they must be the whole
// class_disambiguator vocabulary, so a new kind fails LoadClasses until it is given a rule here.
var (
	carrierKinds   = []string{"credential_prefix", "authorization_scheme", "dedicated_header", "signature_header", "body_field"}
	qualifierKinds = []string{"jwt_header_alg", "jwt_header_typ", "content_type", "subject_token_type", "grant_type"}
)

// Marker is one request-head marker of a class.
type Marker struct {
	Kind  string `json:"kind"`
	Name  string `json:"name"`
	Shape string `json:"shape,omitempty"`
}

// Class is one credential-classes.json entry.
type Class struct {
	ID         string   `json:"class_id"`
	Status     string   `json:"status"`
	Markers    []Marker `json:"markers"`
	NoneReason string   `json:"markers_none_reason"`
}

func (c Class) names(kind string) []string {
	var out []string
	for _, marker := range c.Markers {
		if marker.Kind == kind {
			out = append(out, marker.Name)
		}
	}
	return out
}

// LoadClasses parses credential-classes.json. It fails on an empty registry, on a marker kind outside the
// vocabulary, on a vocabulary kind with no carrier or qualifier rule, and on a class with no marker and no reason.
func LoadClasses(raw []byte) ([]Class, error) {
	known := append(slices.Clone(carrierKinds), qualifierKinds...)
	vocabulary := contracts.SelectorKinds()
	slices.Sort(known)
	slices.Sort(vocabulary)
	if !slices.Equal(known, vocabulary) {
		return nil, fmt.Errorf("wrongclass: carrier+qualifier kinds %v, class_disambiguator vocabulary %v", known, vocabulary)
	}
	var document struct {
		Classes []Class `json:"classes"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, err
	}
	if len(document.Classes) == 0 {
		return nil, fmt.Errorf("wrongclass: no credential class")
	}
	for _, class := range document.Classes {
		if class.ID == "" {
			return nil, fmt.Errorf("wrongclass: a class has no class_id")
		}
		if len(class.Markers) == 0 && strings.TrimSpace(class.NoneReason) == "" {
			return nil, fmt.Errorf("wrongclass: class %s has no marker and no markers_none_reason", class.ID)
		}
		for _, marker := range class.Markers {
			if !slices.Contains(known, marker.Kind) {
				return nil, fmt.Errorf("wrongclass: class %s: marker kind %q has no carrier rule", class.ID, marker.Kind)
			}
		}
	}
	return document.Classes, nil
}

// Mint signs a JWT with the given protected-header alg and typ ("" for none) for the service's live probe
// principal, so a refused token is refused for its class and not for its claims. "" means the service cannot.
type Mint func(alg, typ string) string

// Options are the service-specific parts of a carrier.
type Options struct {
	Mint Mint
	// HeaderValue is the value sent in a dedicated header; nil or "" is Filler.
	HeaderValue func(name string) string
}

// Carrier is a class as one request presents it: a header, or a JSON body field.
type Carrier struct {
	Class     string
	Header    string
	Value     string
	BodyField string
}

// Authorization is the header every Bearer carrier uses.
const Authorization = "Authorization"

// CarrierFor builds the carrier of class. ok is false for a class no carrier kind presents (no marker, or only
// qualifiers: the credential rides in a form or a link the request head does not name). An error is a class whose
// markers cannot make a carrier distinct from the baseline: it must fail the gate, not drop out of it.
func CarrierFor(class Class, options Options) (Carrier, bool, error) {
	carrier := Carrier{Class: class.ID}
	switch {
	case len(class.names("credential_prefix")) > 0:
		carrier.Header, carrier.Value = Authorization, "Bearer "+class.names("credential_prefix")[0]+Filler
	case len(class.names("authorization_scheme")) > 0:
		if scheme := class.names("authorization_scheme"); len(scheme) != 1 || scheme[0] != "Bearer" {
			return Carrier{}, false, fmt.Errorf("wrongclass: class %s: authorization scheme %v has no carrier rule", class.ID, scheme)
		}
		algs, typs := class.names("jwt_header_alg"), class.names("jwt_header_typ")
		if len(algs) != 1 || len(typs) > 1 {
			return Carrier{}, false, fmt.Errorf("wrongclass: class %s: a Bearer class needs a prefix or one alg (algs %v, typs %v)", class.ID, algs, typs)
		}
		typ := ""
		if len(typs) == 1 {
			typ = typs[0]
		}
		if options.Mint == nil {
			return Carrier{}, false, fmt.Errorf("wrongclass: class %s: no minter for alg %s typ %q", class.ID, algs[0], typ)
		}
		token := options.Mint(algs[0], typ)
		if strings.Count(token, ".") != 2 {
			return Carrier{}, false, fmt.Errorf("wrongclass: class %s: the minter made no JWT for alg %s typ %q", class.ID, algs[0], typ)
		}
		carrier.Header, carrier.Value = Authorization, "Bearer "+token
	case len(class.names("dedicated_header")) > 0:
		name := class.names("dedicated_header")[0]
		carrier.Header, carrier.Value = name, Filler
		if options.HeaderValue != nil {
			if value := options.HeaderValue(name); value != "" {
				carrier.Value = value
			}
		}
	case len(class.names("signature_header")) > 0:
		carrier.Header, carrier.Value = class.names("signature_header")[0], "sha256="+Filler
	case len(class.names("body_field")) > 0:
		for _, marker := range class.Markers {
			if marker.Kind != "body_field" {
				continue
			}
			value, err := shaped(marker.Shape)
			if err != nil {
				return Carrier{}, false, fmt.Errorf("wrongclass: class %s: %w", class.ID, err)
			}
			carrier.BodyField, carrier.Value = marker.Name, value
			break
		}
	default:
		return Carrier{}, false, nil
	}
	return carrier, true, nil
}

// CarriersFor is CarrierFor with one carrier for EVERY dedicated header of a class, each alone on its request: a
// class presented in two headers (acr_web_assertion: X-ACR-Web-Assertion and X-ACR-Approver-Reference) is refused in
// each. Every other class gives the one CarrierFor carrier.
func CarriersFor(class Class, options Options) ([]Carrier, bool, error) {
	first, ok, err := CarrierFor(class, options)
	if err != nil || !ok {
		return nil, ok, err
	}
	out := []Carrier{first}
	names := class.names("dedicated_header")
	if len(names) < 2 || first.Header != names[0] {
		return out, true, nil
	}
	for _, name := range names[1:] {
		value := Filler
		if options.HeaderValue != nil {
			if v := options.HeaderValue(name); v != "" {
				value = v
			}
		}
		out = append(out, Carrier{Class: class.ID, Header: name, Value: value})
	}
	return out, true, nil
}

// HasHeaderCarrier reports whether carriers present a credential in the named header.
func HasHeaderCarrier(carriers []Carrier, name string) bool {
	return slices.ContainsFunc(carriers, func(c Carrier) bool { return http.CanonicalHeaderKey(c.Header) == http.CanonicalHeaderKey(name) })
}

func shaped(shape string) (string, error) {
	switch {
	case shape == "jwt":
		enc := base64.RawURLEncoding
		return enc.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + enc.EncodeToString([]byte(`{"sub":"wrongclass"}`)) + "." + Filler, nil
	case strings.HasPrefix(shape, "prefix:"):
		return strings.TrimPrefix(shape, "prefix:") + Filler, nil
	case strings.HasPrefix(shape, "opaque:"):
		return strings.TrimPrefix(shape, "opaque:") + Filler, nil
	}
	return "", fmt.Errorf("body_field shape %q has no carrier rule", shape)
}

// Baseline is the carrier's garbage twin: the same Authorization header with a credential of no class, the same
// body field with a value of no class, or no credential at all for any other header.
func (c Carrier) Baseline() Carrier {
	switch {
	case c.Header == Authorization:
		return Carrier{Class: "baseline:authorization", Header: Authorization, Value: "Bearer " + Filler}
	case c.BodyField != "":
		return Carrier{Class: "baseline:body:" + c.BodyField, BodyField: c.BodyField, Value: Filler}
	}
	return Carrier{Class: "baseline:none"}
}

// Request builds method path with the carrier on it.
func (c Carrier) Request(method, path string) *http.Request {
	var body io.Reader
	if c.BodyField != "" {
		raw, _ := json.Marshal(map[string]string{c.BodyField: c.Value})
		body = strings.NewReader(string(raw))
	}
	request := httptest.NewRequest(method, path, body)
	if c.BodyField != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.Header != "" {
		request.Header.Set(c.Header, c.Value)
	}
	return request
}

// Row is one endpoint-profile row.
type Row struct {
	ID             string
	Service        string
	Method         string
	Route          string
	Classification string
	Listeners      []string
	Accepted       []string
}

var listenerTag = regexp.MustCompile(`\[(\S+) @([^\]]+)\]$`)

// LoadRows parses an endpoint-profile file. The listeners come from the row id's "[service @a,b]" tag.
func LoadRows(raw []byte) ([]Row, error) {
	var document struct {
		Rows []struct {
			ID             string   `json:"id"`
			Service        string   `json:"service"`
			Method         string   `json:"method"`
			Route          string   `json:"route"`
			Classification string   `json:"classification"`
			Accepted       []string `json:"accepted_credential_classes"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, err
	}
	if len(document.Rows) == 0 {
		return nil, fmt.Errorf("wrongclass: no endpoint-profile row")
	}
	out := make([]Row, 0, len(document.Rows))
	for _, entry := range document.Rows {
		tag := listenerTag.FindStringSubmatch(entry.ID)
		if tag == nil || tag[1] != entry.Service {
			return nil, fmt.Errorf("wrongclass: row %q has no [%s @listener] tag", entry.ID, entry.Service)
		}
		out = append(out, Row{ID: entry.ID, Service: entry.Service, Method: entry.Method, Route: entry.Route,
			Classification: entry.Classification, Listeners: strings.Split(tag[2], ","), Accepted: entry.Accepted})
	}
	return out, nil
}

// Accepts reports whether the row lists class.
func (r Row) Accepts(class string) bool { return slices.Contains(r.Accepted, class) }

// Protected reports whether the row is classified protected.
func (r Row) Protected() bool { return r.Classification == "protected" }

var pathParam = regexp.MustCompile(`\{[^}]*\}`)

// Path is the row's route with every parameter set to "x".
func (r Row) Path() string { return pathParam.ReplaceAllString(r.Route, "x") }

// comparedHeaders are the response headers an answer is compared on besides status and body.
var comparedHeaders = []string{"Allow", "Content-Type", "Retry-After", "Www-Authenticate"}

// Answer is a response with the request id masked.
type Answer struct {
	Status int
	Body   string
	Header string
}

// Capture reads the recorder's answer; the response's own request id is masked in the body.
func Capture(recorder *httptest.ResponseRecorder) Answer {
	body := recorder.Body.String()
	if id := recorder.Header().Get(httpapi.RequestIDHeader); id != "" {
		body = strings.ReplaceAll(body, id, "<request-id>")
	}
	var headers []string
	for _, name := range comparedHeaders {
		if values := recorder.Header().Values(name); len(values) > 0 {
			headers = append(headers, name+": "+strings.Join(values, ", "))
		}
	}
	return Answer{Status: recorder.Code, Body: body, Header: strings.Join(headers, "; ")}
}

func (a Answer) String() string {
	body := a.Body
	if len(body) > 160 {
		body = body[:160] + "..."
	}
	return fmt.Sprintf("%d [%s] %q", a.Status, a.Header, body)
}

// Census counts what one gate run measured.
type Census struct {
	Service     string
	Rows        int
	Classes     int
	Driven      int
	WrongCells  int
	AcceptCells int
	NotDriven   []string
}

// Line is the census marker a gate run logs: a green test name alone does not show the matrix ran.
func (c Census) Line() string {
	notDriven := slices.Clone(c.NotDriven)
	sort.Strings(notDriven)
	return fmt.Sprintf("WRONG_CLASS_GATE_EXECUTED service=%s rows=%d classes=%d driven=%d wrong_cells=%d accept_cells=%d not_driven=%s",
		c.Service, c.Rows, c.Classes, c.Driven, c.WrongCells, c.AcceptCells, strings.Join(notDriven, ","))
}
