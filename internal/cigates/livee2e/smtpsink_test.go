package livee2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/smtp"
	"net/textproto"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func startMailSink(t *testing.T) (string, string, *bytes.Buffer, func()) {
	t.Helper()
	state := t.TempDir()
	dir, port := filepath.Join(state, "tokens"), filepath.Join(state, "port")
	ctx, cancel := context.WithCancel(context.Background())
	var logs bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- ServeSMTPSink(ctx, "127.0.0.1:0", dir, port, &logs) }()
	stopped := false
	stop := func() {
		t.Helper()
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("sink did not stop")
		}
	}
	t.Cleanup(stop)
	deadline := time.Now().Add(30 * time.Second)
	for {
		raw, err := os.ReadFile(port)
		if err == nil {
			n, err := strconv.Atoi(string(raw))
			if err != nil || n < 1 || n > 65535 {
				t.Fatal("invalid port file")
			}
			info, err := os.Stat(port)
			if err != nil || info.Mode().Perm() != 0o644 {
				t.Fatal("port file mode")
			}
			return dir, net.JoinHostPort("127.0.0.1", string(raw)), &logs, stop
		}
		if time.Now().After(deadline) {
			t.Fatal("sink did not publish its port")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func sendSinkMail(t *testing.T, addr string, recipients []string, body string) {
	t.Helper()
	client, err := smtp.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if ok, _ := client.Extension("AUTH"); ok {
		t.Fatal("AUTH advertised")
	}
	if ok, _ := client.Extension("STARTTLS"); ok {
		t.Fatal("STARTTLS advertised")
	}
	if err := client.Mail("sender@example.test"); err != nil {
		t.Fatal(err)
	}
	for _, recipient := range recipients {
		if err := client.Rcpt(recipient); err != nil {
			t.Fatal(err)
		}
	}
	w, err := client.Data()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(w, body); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Quit(); err != nil {
		t.Fatal(err)
	}
}

func TestSMTPSinkMailTokens(t *testing.T) {
	dir, addr, logs, stop := startMailSink(t)
	first, second := "First@Example.test", "second@example.test"
	reset, verify := "reset-secret._~%2F-value", "verify-secret._~%25-value"
	sendSinkMail(t, addr, []string{first, second}, "To: unrelated@example.test\r\nContent-Type: multipart/alternative; boundary=parts\r\n\r\n--parts\r\nContent-Type: text/plain\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n.dot-stuffed\r\nhttps://example.test/auth/reset-password?token=3D"+reset+"\r\n--parts\r\nContent-Type: application/octet-stream\r\n\r\n/auth/verify?token=ignore-attachment\r\n--parts--\r\n")
	sendSinkMail(t, addr, []string{second}, "Content-Type: text/html\r\nContent-Transfer-Encoding: base64\r\n\r\n"+base64.StdEncoding.EncodeToString([]byte(`<a href="https://example.test/auth/verify?token=`+verify+`">verify</a>`))+"\r\n")
	assertToken := func(email, kind, want string) {
		t.Helper()
		path := filepath.Join(dir, kind+"."+hex.EncodeToString([]byte(strings.ToLower(email))))
		raw, err := os.ReadFile(path)
		if err != nil || string(raw) != want {
			t.Fatal("missing or incorrect captured token")
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatal("token mode is not 0600")
		}
	}
	assertToken(first, "reset", reset)
	assertToken(second, "reset", reset)
	assertToken(second, "verify", verify)
	info, err := os.Stat(dir)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatal("directory mode is not 0700")
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 3 {
		t.Fatal("unexpected recipient or attachment token")
	}
	sendSinkMail(t, addr, []string{first}, "Subject: no link\r\n\r\nno token\r\n")
	files, err = os.ReadDir(dir)
	if err != nil || len(files) != 3 {
		t.Fatal("mail without a link changed files")
	}
	sendSinkMail(t, addr, []string{first}, "Subject: replacement\r\n\r\n/auth/reset-password?token=replacement\r\n")
	assertToken(first, "reset", "replacement")
	assertToken(second, "reset", reset)
	assertToken(second, "verify", verify)
	stop()
	for _, secret := range []string{reset, verify, "replacement", first, strings.ToLower(first), second, "reset." + hex.EncodeToString([]byte(strings.ToLower(first)))} {
		if strings.Contains(logs.String(), secret) {
			t.Fatal("SMTP log contains sensitive mail data")
		}
	}
	for _, email := range []string{first, second} {
		for _, kind := range []string{"reset", "verify"} {
			if strings.Contains(logs.String(), kind+"."+hex.EncodeToString([]byte(strings.ToLower(email)))) {
				t.Fatal("SMTP log contains a capture file name")
			}
		}
	}
	if logs.String() != "kind=reset stored\nkind=verify stored\nkind=reset stored\n" {
		t.Fatal("unexpected SMTP log")
	}
}

func TestSMTPSinkRefusesAddressesBeforeWriting(t *testing.T) {
	for _, addr := range []string{":0", "0.0.0.0:0", "[::]:0", "localhost:0", "192.0.2.1:0", "10.0.0.1:0", "127.0.0.1", "", "127.0.0.1:http", "127.0.0.1:65536"} {
		t.Run(addr, func(t *testing.T) {
			state := t.TempDir()
			dir := filepath.Join(state, "tokens")
			port := filepath.Join(state, "port")
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := ServeSMTPSink(ctx, addr, dir, port, io.Discard); err == nil {
				t.Fatal("unsafe address accepted")
			}
			for _, p := range []string{dir, port} {
				if _, err := os.Lstat(p); !os.IsNotExist(err) {
					t.Fatal("unsafe address caused a filesystem write")
				}
			}
		})
	}
}

func TestSMTPSinkRefusesUnsafeDirectories(t *testing.T) {
	for _, kind := range []string{"public", "symlink", "symlink-slash", "file"} {
		t.Run(kind, func(t *testing.T) {
			state := t.TempDir()
			dir := filepath.Join(state, "tokens")
			switch kind {
			case "public":
				if err := os.Mkdir(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(dir, 0o755); err != nil {
					t.Fatal(err)
				}
			case "symlink", "symlink-slash":
				if err := os.Symlink(t.TempDir(), dir); err != nil {
					t.Fatal(err)
				}
			case "file":
				if err := os.WriteFile(dir, []byte("file"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "symlink-slash" {
				dir += "/"
			}
			if err := ServeSMTPSink(context.Background(), "127.0.0.1:0", dir, filepath.Join(state, "port"), io.Discard); err == nil {
				t.Fatal("unsafe directory accepted")
			}
			if _, err := os.Stat(filepath.Join(state, "port")); !os.IsNotExist(err) {
				t.Fatal("unsafe directory published a port")
			}
		})
	}
}

func TestMailTokenCLI(t *testing.T) {
	dir, addr, _, stop := startMailSink(t)
	email := "reader@example.test"
	sendSinkMail(t, addr, []string{email}, "Subject: reset\r\n\r\n/auth/reset-password?token=cli-secret\r\n")
	stop()
	for _, cell := range []struct {
		email, kind string
		ok          bool
	}{{email, "reset", true}, {email, "verify", false}, {email, "other", false}, {"not-email", "reset", false}, {"Name <reader@example.test>", "reset", false}, {"../bad@example.test", "reset", false}} {
		var out, logs bytes.Buffer
		rc := Run([]string{"mail-token", dir, cell.email, cell.kind}, func(string) string { return "" }, &out, &logs)
		if cell.ok {
			if rc != 0 || out.String() != "cli-secret\n" || logs.Len() != 0 {
				t.Fatal("mail-token output contract failed")
			}
		} else if rc != 1 || out.Len() != 0 || logs.Len() == 0 {
			t.Fatal("invalid or missing token accepted")
		}
		if strings.Contains(logs.String(), "cli-secret") || strings.Contains(logs.String(), dir) {
			t.Fatal("mail-token error leaked a token or a path")
		}
	}
	key, err := mailKey("badfile@example.test", "reset")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, key), []byte("not a token hunter2"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out, logs bytes.Buffer
	if rc := Run([]string{"mail-token", dir, "badfile@example.test", "reset"}, func(string) string { return "" }, &out, &logs); rc != 1 || out.Len() != 0 || strings.Contains(logs.String(), "hunter2") {
		t.Fatal("a malformed stored token was printed or leaked in the error")
	}
}

func TestSMTPSinkCommandsAndLimits(t *testing.T) {
	dir, addr, logs, stop := startMailSink(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	r := textproto.NewReader(bufio.NewReader(conn))
	if _, _, err := r.ReadResponse(220); err != nil {
		t.Fatal(err)
	}
	command := func(line string, code int) {
		t.Helper()
		if _, err := fmt.Fprintf(conn, "%s\r\n", line); err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.ReadResponse(code); err != nil {
			t.Fatal(err)
		}
	}
	command("HELO test", 250)
	command("NOOP", 250)
	command("AUTH secret-input", 502)
	command("STARTTLS", 502)
	command("DATA", 503)
	command("MAIL FROM:<sender@example.test>", 250)
	for i := 0; i < 10; i++ {
		command(fmt.Sprintf("RCPT TO:<r%d@example.test>", i), 250)
	}
	command("RCPT TO:<excess@example.test>", 452)
	command("RSET", 250)
	command("DATA", 503)
	command("RCPT TO:<r@example.test>", 250)
	command("DATA", 354)
	command("Subject: large\r\n\r\n"+strings.Repeat("x", smtpMessageLimit)+"\r\n.", 552)
	stop()
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatal("oversize message stored")
	}
	if logs.String() != "SMTP connection error\n" {
		t.Fatal("connection error log exposed input")
	}
}

func TestSMTPSinkLiteralLoopbacks(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:0", "127.255.2.3:65535", "[::1]:0"} {
		if err := sinkAddress(addr); err != nil {
			t.Fatalf("literal loopback refused: %s", addr)
		}
	}
	dir := filepath.Join(t.TempDir(), "tokens")
	if err := ServeSMTPSink(context.Background(), "127.0.0.1:0", dir, filepath.Join(dir, "port"), io.Discard); err == nil {
		t.Fatal("port file inside token directory accepted")
	}
}

func TestSMTPSinkCLIProcess(t *testing.T) {
	if os.Getenv("SMTP_SINK_CLI_HELPER") != "1" {
		return
	}
	var args []string
	for i, arg := range os.Args {
		if arg == "--" {
			args = os.Args[i+1:]
			break
		}
	}
	os.Exit(Run(args, os.Getenv, os.Stdout, os.Stderr))
}

func TestSMTPSinkCLIStopsOnSignals(t *testing.T) {
	for _, signal := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
		t.Run(signal.String(), func(t *testing.T) {
			state := t.TempDir()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			port := filepath.Join(state, "port")
			cmd := exec.Command(executable, "-test.run=^TestSMTPSinkCLIProcess$", "--", "smtp-sink", "127.0.0.1:0", filepath.Join(state, "tokens"), port)
			cmd.Env = append(os.Environ(), "SMTP_SINK_CLI_HELPER=1")
			var out, logs bytes.Buffer
			cmd.Stdout = &out
			cmd.Stderr = &logs
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			finished := false
			t.Cleanup(func() {
				if !finished {
					_ = cmd.Process.Kill()
					<-done
				}
			})
			deadline := time.Now().Add(30 * time.Second)
			for {
				if _, err := os.Stat(port); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("CLI did not start its listener")
				}
				time.Sleep(5 * time.Millisecond)
			}
			if err := cmd.Process.Signal(signal); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				finished = true
				if err != nil {
					t.Fatal("sink did not exit cleanly on signal")
				}
			case <-time.After(30 * time.Second):
				t.Fatal("sink ignored shutdown signal")
			}
			if out.Len() != 0 || logs.Len() != 0 {
				t.Fatal("idle SMTP CLI printed unexpected output")
			}
		})
	}
}
