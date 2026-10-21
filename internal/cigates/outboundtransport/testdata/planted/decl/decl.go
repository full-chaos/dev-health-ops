package decl

import "net/http"

var zero http.Client

func Zero() *http.Client { return &zero }
