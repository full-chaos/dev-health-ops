package nilassign

import "net/http"

func Clear(client *http.Client) *http.Client {
	copied := *client
	copied.Transport = nil
	return &copied
}
