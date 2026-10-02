// Package nilfield is a fixture of the site walker: literals, declarations, embeds and makes of a replaced-module type
// (a "carrier": an exported *http.Client field that defaults to a following client when nil) without a non-nil client.
package nilfield

import (
	nethttp "net/http"

	"github.com/full-chaos/dev-health-ops/internal/httpguard/testdata/redirectsites/outside"
	"github.com/full-chaos/dev-health-ops/internal/httpguard/testdata/redirectsites/outside/sub"
)

func Unset() *outside.Config { return &outside.Config{Name: "a"} }

func New() *outside.Config { return new(outside.Config) }

func Set(c *nethttp.Client) *outside.Config { return &outside.Config{HTTPClient: c} }

func Positional(c *nethttp.Client) outside.Config { return outside.Config{c, "x"} }

func ExplicitNil() *outside.Config { return &outside.Config{HTTPClient: nil, Name: "a"} }

func SubPackage() *sub.Config { return &sub.Config{} }

func Hidden() *outside.Hidden { return &outside.Hidden{Name: "a"} }

var Zero outside.Config

type Wrapper struct {
	outside.Config
	Name string
}

type Named struct {
	Inner outside.Config
	Other int
}

func Embedded() *Wrapper { return &Wrapper{Name: "x"} }

func NamedField() *Named { return &Named{Other: 1} }

func WrapperSet(c *nethttp.Client) *Wrapper { return &Wrapper{Config: outside.Config{HTTPClient: c}} }

func Makes() { _ = make([]outside.Config, 2) }

var Array [1]outside.Config

var PointerOnly *outside.Config

func ArrayLiteral() { _ = [2]outside.Config{{HTTPClient: nethttp.DefaultClient}} }

func Anonymous() { _ = struct{ c outside.Config }{} }

func zeroPtr[T any]() *T { return new(T) }

func Generic() *outside.Config { return zeroPtr[outside.Config]() }

var DeepCarrier [1][1][1][1][1]outside.Config

func TypedNil() *outside.Config { return &outside.Config{HTTPClient: (*nethttp.Client)(nil)} }

var Chan chan outside.Config

var Map map[string]outside.Config
