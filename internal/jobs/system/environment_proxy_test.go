package system

import (
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestASuppliedHeartbeatClientWithoutATransportGetsOne(t *testing.T) {
	supplied := &http.Client{}
	dispatcher, err := NewNativeHeartbeatDispatcher(new(pgxpool.Pool), supplied, "http://upstream.invalid", "instance", "version")
	if err != nil {
		t.Fatal(err)
	}
	if dispatcher.httpClient.Transport == nil {
		t.Fatal("a supplied heartbeat client with no transport would dial through http.DefaultTransport and its environment proxy")
	}
	if supplied.Transport != nil {
		t.Fatal("the supplied client was modified")
	}
}
