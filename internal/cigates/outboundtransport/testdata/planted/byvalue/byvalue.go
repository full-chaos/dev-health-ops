package byvalue

import "net/http"

type Embedded struct {
	http.Client
	name string
}

type Named struct {
	client http.Client
}

type Defined http.Client

type Alias = http.Client

type Pointer struct {
	client *http.Client
}

func Slice() []http.Client { return nil }

func Map() map[string]http.Client { return nil }

func Array() [2]http.Client { return [2]http.Client{} }

func Chan() chan http.Client { return nil }

func Pointers() []*http.Client { return nil }

func (e Embedded) Name() string { return e.name }
