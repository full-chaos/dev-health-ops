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

func Passed(client *http.Client) *http.Client { return client }

func ByValue(client http.Client) time.Duration { return client.Timeout }
