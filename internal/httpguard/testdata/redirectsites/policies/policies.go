// Package policies is a fixture of the site walker: the class derived from the code of a function.
package policies

import (
	"context"
	"errors"
	"fmt"
	nethttp "net/http"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/httpguard/testdata/redirectsites/fakedefault"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func refuse(*nethttp.Request, []*nethttp.Request) error { return nethttp.ErrUseLastResponse }

func Guarded(c *nethttp.Client) *nethttp.Client { return httpguard.NoRedirects(c) }

func GuardedNew() *nethttp.Client { return httpguard.NewClient(0) }

func RefuseLiteral() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(*nethttp.Request, []*nethttp.Request) error { return nethttp.ErrUseLastResponse }}
}

func RefuseFunction() *nethttp.Client { return &nethttp.Client{CheckRedirect: refuse} }

func RefuseAssign(c *nethttp.Client) { c.CheckRedirect = refuse }

func Drop() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: providerfoundation.DropCredentialsOnHostChange}
}

func Custom() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(*nethttp.Request, []*nethttp.Request) error { return nil }}
}

func Mixed(c *nethttp.Client, d *nethttp.Client) {
	c.CheckRedirect = refuse
	d.CheckRedirect = providerfoundation.DropCredentialsOnHostChange
}

func Bare() *nethttp.Client { return &nethttp.Client{} }

func allow(*nethttp.Request) error { return nil }

func check(*nethttp.Request, []*nethttp.Request) error { return nil }

var errNo = errors.New("no")

func Delegating() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(r *nethttp.Request, via []*nethttp.Request) error { return allow(r) }}
}

func LocalVariable() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(r *nethttp.Request, via []*nethttp.Request) error {
		var err error
		return err
	}}
}

func Conditional() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(r *nethttp.Request, via []*nethttp.Request) error {
		if len(via) > 3 {
			return errNo
		}
		return check(r, via)
	}}
}

func RefuseErrorf() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(*nethttp.Request, []*nethttp.Request) error { return fmt.Errorf("no %d", 1) }}
}

func RefusePackageVar() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(*nethttp.Request, []*nethttp.Request) error { return errNo }}
}

func ForeignCall() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(r *nethttp.Request, via []*nethttp.Request) error { return context.Cause(r.Context()) }}
}

type failure struct{ err error }

func FieldReturn(h failure) *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(r *nethttp.Request, via []*nethttp.Request) error { return h.err }}
}

func ExtraStatement() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(r *nethttp.Request, via []*nethttp.Request) error {
		_ = r
		return nethttp.ErrUseLastResponse
	}}
}

func ForeignError() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(*nethttp.Request, []*nethttp.Request) error { return context.Canceled }}
}

func UnreachableAfter() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(r *nethttp.Request, via []*nethttp.Request) error {
		return nethttp.ErrUseLastResponse
		return nil
	}}
}

func SameNameOtherPackage() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(*nethttp.Request, []*nethttp.Request) error { return fakedefault.ErrUseLastResponse }}
}

func OtherNetHTTPError() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(*nethttp.Request, []*nethttp.Request) error { return nethttp.ErrNotSupported }}
}
