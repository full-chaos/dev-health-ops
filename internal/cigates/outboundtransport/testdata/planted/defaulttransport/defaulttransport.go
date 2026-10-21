package defaulttransport

import "net/http"

func Clone() *http.Transport { return http.DefaultTransport.(*http.Transport).Clone() }
