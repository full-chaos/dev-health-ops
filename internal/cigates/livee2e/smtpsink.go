package livee2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/netip"
	"net/textproto"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const smtpMessageLimit = 1 << 20

var mailLink = regexp.MustCompile(`/auth/(reset-password|verify)\?token=([A-Za-z0-9._~%-]+)`)
var storedMailToken = regexp.MustCompile(`^[A-Za-z0-9._~%-]+$`)
var sinkEmail = regexp.MustCompile(`^[^'\s@]+@[^'\s@]+$`)

func mailKey(email, kind string) (string, error) {
	if kind != "reset" && kind != "verify" {
		return "", errors.New("invalid mail kind")
	}
	address, err := mail.ParseAddress(email)
	if err != nil || address.Address != email || !sinkEmail.MatchString(email) {
		return "", errors.New("invalid e-mail address")
	}
	return kind + "." + hex.EncodeToString([]byte(strings.ToLower(email))), nil
}

func readMailToken(dir, email, kind string) (string, error) {
	key, err := mailKey(email, kind)
	if err != nil {
		return "", err
	}
	raw, err := os.ReadFile(filepath.Join(dir, key))
	if err != nil {
		return "", errors.New("mail token is unavailable")
	}
	if len(raw) == 0 || !storedMailToken.Match(raw) {
		return "", errors.New("invalid stored mail token")
	}
	return string(raw), nil
}

func sinkAddress(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	ip, ipErr := netip.ParseAddr(host)
	n, portErr := strconv.Atoi(port)
	if err != nil || ipErr != nil || !(ip.Is4() && ip.IsLoopback() || ip == netip.IPv6Loopback()) || port == "" || strings.Trim(port, "0123456789") != "" || portErr != nil || n < 0 || n > 65535 {
		return errors.New("SMTP address must be a literal loopback IP and numeric port")
	}
	return nil
}

func prepareMailDir(dir string) error {
	info, err := os.Lstat(dir)
	if err == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0) {
		return errors.New("mail directory must be a private directory")
	}
	if err != nil && !os.IsNotExist(err) {
		return errors.New("cannot inspect mail directory")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return errors.New("cannot create mail directory")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return errors.New("cannot secure mail directory")
	}
	return nil
}

func atomicSinkFile(path string, data []byte, mode os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".smtp-*")
	if err != nil {
		return errors.New("cannot create sink file")
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(mode); err == nil {
		_, err = f.Write(data)
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return errors.New("cannot write sink file")
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return errors.New("cannot replace sink file")
	}
	return nil
}

func ServeSMTPSink(ctx context.Context, addr, dir, portFile string, log io.Writer) error {
	if err := sinkAddress(addr); err != nil {
		return err
	}
	dir = filepath.Clean(dir)
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return errors.New("invalid mail directory")
	}
	absPort, err := filepath.Abs(portFile)
	if err != nil {
		return errors.New("invalid port file")
	}
	rel, err := filepath.Rel(absDir, absPort)
	if err != nil || rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return errors.New("port file must be outside mail directory")
	}
	if err := prepareMailDir(dir); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.New("cannot listen for SMTP")
	}
	defer listener.Close()
	bound, ok := listener.Addr().(*net.TCPAddr)
	if !ok || !bound.IP.IsLoopback() {
		return errors.New("SMTP listener is not loopback")
	}
	if err := atomicSinkFile(portFile, []byte(strconv.Itoa(bound.Port)), 0o644); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()
	var wg sync.WaitGroup
	defer wg.Wait()
	var mu sync.Mutex
	report := func(line string) { mu.Lock(); defer mu.Unlock(); fmt.Fprintln(log, line) }
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return errors.New("SMTP accept failed")
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.Close()
			stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
			defer stop()
			if err := smtpSession(conn, dir, report); err != nil {
				report("SMTP connection error")
			}
		}()
	}
}

func smtpSession(conn net.Conn, dir string, report func(string)) error {
	if err := conn.SetDeadline(time.Now().Add(100 * time.Second)); err != nil {
		return err
	}
	reader := bufio.NewReader(conn)
	reply := func(s string) error { _, err := io.WriteString(conn, s+"\r\n"); return err }
	if err := reply("220 live-e2e SMTP"); err != nil {
		return err
	}
	var recipients []string
	for {
		line, err := reader.ReadSlice('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		command, arg, _ := strings.Cut(strings.TrimSpace(string(line)), " ")
		response := "250 OK"
		switch strings.ToUpper(command) {
		case "EHLO", "HELO", "MAIL", "RSET":
			recipients = nil
		case "NOOP":
		case "RCPT":
			if len(recipients) >= 10 {
				response = "452 too many recipients"
				break
			}
			if len(arg) < 3 || !strings.EqualFold(arg[:3], "TO:") {
				response = "501 invalid recipient"
				break
			}
			email := strings.Trim(strings.TrimSpace(arg[3:]), "<>")
			if _, err := mailKey(email, "reset"); err != nil {
				response = "501 invalid recipient"
				break
			}
			recipients = append(recipients, strings.ToLower(email))
		case "DATA":
			if len(recipients) == 0 {
				response = "503 recipient required"
				break
			}
			if err := reply("354 end with a dot"); err != nil {
				return err
			}
			raw, err := io.ReadAll(io.LimitReader(textproto.NewReader(reader).DotReader(), smtpMessageLimit+1))
			if err != nil {
				return err
			}
			if len(raw) > smtpMessageLimit {
				_ = reply("552 message too large")
				return errors.New("message too large")
			}
			if err := storeMail(dir, recipients, raw, report); err != nil {
				_ = reply("451 cannot store message")
				return err
			}
			recipients = nil
			response = "250 queued"
		case "QUIT":
			return reply("221 bye")
		default:
			response = "502 command not implemented"
		}
		if err := reply(response); err != nil {
			return err
		}
	}
}

func mailText(header textproto.MIMEHeader, body io.Reader, depth int) ([]byte, error) {
	if depth > 20 {
		return nil, errors.New("MIME nesting limit")
	}
	switch strings.ToLower(header.Get("Content-Transfer-Encoding")) {
	case "quoted-printable":
		body = quotedprintable.NewReader(body)
	case "base64":
		body = base64.NewDecoder(base64.StdEncoding, body)
	}
	contentType := header.Get("Content-Type")
	if contentType == "" {
		contentType = "text/plain"
	}
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(mediaType, "multipart/") {
		parts := multipart.NewReader(body, params["boundary"])
		var text bytes.Buffer
		for {
			part, err := parts.NextRawPart()
			if errors.Is(err, io.EOF) {
				return text.Bytes(), nil
			}
			if err != nil {
				return nil, err
			}
			decoded, err := mailText(part.Header, part, depth+1)
			if err != nil {
				return nil, err
			}
			text.Write(decoded)
			text.WriteByte('\n')
		}
	}
	if !strings.HasPrefix(mediaType, "text/") {
		return nil, nil
	}
	return io.ReadAll(body)
}

func storeMail(dir string, recipients []string, raw []byte, report func(string)) error {
	message, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return errors.New("invalid mail")
	}
	text, err := mailText(textproto.MIMEHeader(message.Header), message.Body, 0)
	if err != nil {
		return errors.New("invalid mail body")
	}
	tokens := map[string]string{}
	for _, match := range mailLink.FindAllSubmatch(text, -1) {
		kind := "verify"
		if string(match[1]) == "reset-password" {
			kind = "reset"
		}
		tokens[kind] = string(match[2])
	}
	for kind, token := range tokens {
		for _, email := range recipients {
			key, err := mailKey(email, kind)
			if err != nil {
				return err
			}
			if err := atomicSinkFile(filepath.Join(dir, key), []byte(token), 0o600); err != nil {
				return err
			}
		}
		report("kind=" + kind + " stored")
	}
	return nil
}
