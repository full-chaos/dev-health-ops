package smtpcapture

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// Sink is the SMTP recorder without a test around it: the in-process Server of a test and the corpus sink container
// (cmd/smtpsink) are the same code. It records every message byte for byte and counts accepted connections.
type Sink struct {
	listener net.Listener
	mu       sync.Mutex
	mails    []Mail
	accepts  int
}

// NewSink listens on addr and records until Close.
func NewSink(addr string) (*Sink, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	sink := &Sink{listener: listener}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			sink.mu.Lock()
			sink.accepts++
			sink.mu.Unlock()
			go sink.serve(conn)
		}
	}()
	return sink, nil
}

// Close stops accepting connections.
func (server *Sink) Close() { _ = server.listener.Close() }

// Addr is the address the sink listens on.
func (server *Sink) Addr() string { return server.listener.Addr().String() }

// takeAll returns every message received so far and forgets them.
func (server *Sink) takeAll() []Mail {
	server.mu.Lock()
	defer server.mu.Unlock()
	out := server.mails
	server.mails = nil
	return out
}

func (server *Sink) peekAll() []Mail {
	server.mu.Lock()
	defer server.mu.Unlock()
	return append([]Mail(nil), server.mails...)
}

func (server *Sink) acceptCount() int {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.accepts
}

// wireMail is a Mail on the read API: the data travels base64 so every byte, CRLF included, is kept.
type wireMail struct {
	MailFrom string   `json:"mail_from"`
	RcptTo   []string `json:"rcpt_to"`
	Data     string   `json:"data_base64"`
}

func toWire(mails []Mail) []wireMail {
	out := make([]wireMail, 0, len(mails))
	for _, mail := range mails {
		out = append(out, wireMail{MailFrom: mail.MailFrom, RcptTo: mail.RcptTo, Data: base64.StdEncoding.EncodeToString([]byte(mail.Data))})
	}
	return out
}

// Handler is the read API of the sink container: GET /mails (peek), POST /drain (take all), GET /accepts.
func (server *Sink) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /mails", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(toWire(server.peekAll()))
	})
	mux.HandleFunc("POST /drain", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(toWire(server.takeAll()))
	})
	mux.HandleFunc("GET /accepts", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int{"accepts": server.acceptCount()})
	})
	return mux
}

// Remote reads the capture of a sink container over its read API.
type Remote struct {
	base   string
	client *http.Client
}

// NewRemote reads the sink whose read API is at base (http://127.0.0.1:PORT).
func NewRemote(base string) *Remote {
	return &Remote{base: strings.TrimRight(base, "/"), client: &http.Client{Timeout: 10 * time.Second}}
}

func (remote *Remote) call(method, path string, into any) error {
	request, err := http.NewRequest(method, remote.base+path, nil)
	if err != nil {
		return err
	}
	response, err := remote.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%s %s: %s", method, path, response.Status)
	}
	return json.NewDecoder(io.LimitReader(response.Body, 64<<20)).Decode(into)
}

// Drain waits until the sink has been quiet for a moment, as Server.Drain does, then takes every message.
func (remote *Remote) Drain(t testing.TB) []Mail {
	t.Helper()
	time.Sleep(500 * time.Millisecond)
	var wire []wireMail
	if err := remote.call(http.MethodPost, "/drain", &wire); err != nil {
		t.Fatalf("drain the corpus mail sink: %v", err)
	}
	out := make([]Mail, 0, len(wire))
	for _, mail := range wire {
		data, err := base64.StdEncoding.DecodeString(mail.Data)
		if err != nil {
			t.Fatalf("the corpus mail sink sent bad data: %v", err)
		}
		out = append(out, Mail{MailFrom: mail.MailFrom, RcptTo: mail.RcptTo, Data: string(data)})
	}
	return out
}

// Reachable is an error unless the read API answers.
func (remote *Remote) Reachable() error {
	var out map[string]int
	return remote.call(http.MethodGet, "/accepts", &out)
}

func (server *Sink) serve(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	reply := func(line string) { _, _ = conn.Write([]byte(line + "\r\n")) }
	reply("220 capture.smtp.test ESMTP")
	var current Mail
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		trimmed := strings.TrimRight(line, "\r\n")
		upper := strings.ToUpper(trimmed)
		switch {
		case strings.HasPrefix(upper, "EHLO"), strings.HasPrefix(upper, "HELO"):
			reply("250 capture.smtp.test")
		case strings.HasPrefix(upper, "MAIL FROM:"):
			current = Mail{MailFrom: trimmed[len("MAIL FROM:"):]}
			reply("250 OK")
		case strings.HasPrefix(upper, "RCPT TO:"):
			current.RcptTo = append(current.RcptTo, trimmed[len("RCPT TO:"):])
			reply("250 OK")
		case upper == "DATA":
			reply("354 go ahead")
			var data bytes.Buffer
			for {
				dataLine, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if dataLine == ".\r\n" {
					break
				}
				data.WriteString(strings.TrimPrefix(dataLine, ".")) // dot-unstuff
			}
			current.Data = data.String()
			server.mu.Lock()
			server.mails = append(server.mails, current)
			server.mu.Unlock()
			current = Mail{}
			reply("250 OK queued")
		case upper == "QUIT":
			reply("221 bye")
			return
		default:
			reply("250 OK")
		}
	}
}
