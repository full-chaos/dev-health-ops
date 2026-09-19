package operational

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestResendMalformedResponsesYieldNoResponseContent drives the real Resend
// sender against a server that answers with a malformed status line or
// header: net/http's own error quotes those bytes, and the sender's error
// text must not.
func TestResendMalformedResponsesYieldNoResponseContent(t *testing.T) {
	for name, reply := range map[string]string{
		"transfer encoding": "HTTP/1.1 200 OK\r\nTransfer-Encoding: canary-encoding\r\n\r\n",
		"status code":       "HTTP/1.1 2x0 canary-status\r\n\r\n",
		"content length":    "HTTP/1.1 200 OK\r\nContent-Length: canary-length\r\n\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			go func() {
				connection, err := listener.Accept()
				if err != nil {
					return
				}
				defer connection.Close()
				_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
				request, err := http.ReadRequest(bufio.NewReader(connection))
				if err == nil {
					_ = request.Body.Close()
				}
				_, _ = connection.Write([]byte(reply))
			}()
			t.Setenv("RESEND_API_BASE_URL", "http://"+listener.Addr().String()+"/v1?token=canary-query")
			sender := &resendEmailSender{from: "billing@example.test", apiKey: "k", client: &http.Client{Timeout: 5 * time.Second}}
			err = sender.Send(context.Background(), EmailMessage{To: "owner@example.test", Subject: "s", HTML: "<p>h</p>"})
			if err == nil {
				t.Fatal("Send() = nil")
			}
			if strings.Contains(err.Error(), "canary") {
				t.Fatalf("Send() error carries response content: %q", err.Error())
			}
		})
	}
}
