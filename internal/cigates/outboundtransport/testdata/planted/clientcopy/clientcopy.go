package clientcopy

import (
	"net/http"
	"time"
)

func Copied(client *http.Client) *http.Client {
	copied := *client
	copied.Timeout = time.Second
	return &copied
}
