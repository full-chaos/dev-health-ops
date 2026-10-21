package literal

import (
	"net/http"
	"time"
)

func Bare() *http.Client { return &http.Client{} }

func TimeoutOnly() *http.Client { return &http.Client{Timeout: time.Second} }

func NilTransport() *http.Client { return &http.Client{Transport: nil} }
