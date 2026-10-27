package smtpcapture

import (
	"net/http/httptest"
	"net/smtp"
	"testing"
)

func send(t *testing.T, addr, body string) {
	t.Helper()
	if err := smtp.SendMail(addr, nil, "from@example.test", []string{"to@example.test"}, []byte(body)); err != nil {
		t.Fatal(err)
	}
}

func TestSinkReadAPIReturnsTheMailByteForByteAndDrainEmptiesIt(t *testing.T) {
	sink, err := NewSink("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	api := httptest.NewServer(sink.Handler())
	defer api.Close()
	remote := NewRemote(api.URL)
	if err := remote.Reachable(); err != nil {
		t.Fatal(err)
	}
	body := "Subject: s\r\n\r\n.leading dot\r\nline two é\r\n"
	send(t, sink.Addr(), body)
	mails := remote.Drain(t)
	if len(mails) != 1 || mails[0].Data != body || len(mails[0].RcptTo) != 1 {
		t.Fatalf("got %q, want one mail with the body %q", mails, body)
	}
	if again := remote.Drain(t); len(again) != 0 {
		t.Fatalf("a second drain returned %q", again)
	}
	var accepts map[string]int
	if err := remote.call("GET", "/accepts", &accepts); err != nil || accepts["accepts"] != 1 {
		t.Fatalf("accepts = %v (%v), want 1", accepts, err)
	}
}

func TestRemoteIsNotReachableWhenTheSinkIsDown(t *testing.T) {
	api := httptest.NewServer(nil)
	url := api.URL
	api.Close()
	if err := NewRemote(url).Reachable(); err == nil {
		t.Fatal("a closed read API must not be reachable")
	}
}
