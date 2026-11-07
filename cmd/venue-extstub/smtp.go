package main

import (
	"bufio"
	"net"
	"strings"
	"time"
)

// smtpHost is the host field of a mail hit.
const smtpHost = "smtp"

// serveSMTP accepts plain SMTP on ln until it closes: each message accepted with DATA is one mail hit. No address and no
// message text is logged. STARTTLS and AUTH are not offered (the venue sets SMTP_USE_TLS=false and no SMTP user).
func serveSMTP(ln net.Listener, log *hitLog) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go smtpSession(conn, log)
	}
}

func smtpSession(conn net.Conn, log *hitLog) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
	r := bufio.NewReader(conn)
	reply := func(line string) bool {
		_, err := conn.Write([]byte(line + "\r\n"))
		return err == nil
	}
	if !reply("220 venue-extstub ESMTP") {
		return
	}
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		verb := strings.ToUpper(strings.TrimSpace(line))
		if i := strings.IndexAny(verb, " :"); i >= 0 {
			verb = verb[:i]
		}
		var ok bool
		switch verb {
		case "EHLO", "HELO":
			ok = reply("250 venue-extstub")
		case "MAIL", "RCPT", "RSET", "NOOP":
			ok = reply("250 OK")
		case "DATA":
			if !reply("354 end with <CRLF>.<CRLF>") || !readData(r) {
				return
			}
			if log.record("mail", "DATA", smtpHost, "/") != nil {
				ok = reply("451 venue stub: log failed")
			} else {
				ok = reply("250 OK")
			}
		case "QUIT":
			reply("221 bye")
			return
		default:
			ok = reply("502 not offered")
		}
		if !ok {
			return
		}
	}
}

// readData reads a DATA body up to the line ".".
func readData(r *bufio.Reader) bool {
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return false
		}
		if strings.TrimRight(line, "\r\n") == "." {
			return true
		}
	}
}
