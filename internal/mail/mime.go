package mail

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"net/mail"
	"regexp"
	"strings"
)

// This file reproduces, byte for byte, what Python's
//
//	msg = MIMEMultipart("alternative")
//	msg["From"] = ...; msg["To"] = ...; msg["Subject"] = ...
//	msg.attach(MIMEText(html_content, "html"))
//	smtplib.SMTP.sendmail(from, [to], msg.as_string())
//
// puts on the wire (src/dev_health_ops/api/services/email.py,
// SmtpEmailProvider._send). TestSMTPSenderMatchesLivePythonSMTPProvider runs
// the real Python and this code against the same SMTP server and compares
// the captured bytes; every rule below is one that comparison found.
//
// Not reproduced, deliberately: text_content (the multipart's optional
// text/plain part -- no Go caller supplies one) and the SMTP extension
// parameters the two clients add on their own to MAIL FROM (Python adds
// SIZE=, Go adds BODY=8BITMIME) when a relay advertises them; neither is
// message content.

// errUnsafeHeader is returned for a header value containing a line break.
// Python refuses ASCII values like that (HeaderWriteError/HeaderParseError:
// the message is never sent); for a NON-ASCII value with a line break it
// instead emits a folded continuation. Refusing both is the safe superset:
// a value that reaches this code from user input (an organization name in
// an invite subject) must never be able to start a second header. The
// non-ASCII-with-line-break shape is the one place Go is stricter than
// Python; it is pinned by TestComposeRefusesLineBreaksInHeaderValues.
var errUnsafeHeader = errors.New("mail header value contains a line break")

// nlcre is Generator._write_lines's NLCRE: every kind of line break.
var nlcre = regexp.MustCompile(`\r\n|\r|\n`)

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > 0x7F {
			return false
		}
	}
	return true
}

// encodeHeaderValue is what Python's compat32 policy writes for a header set
// from a str: an all-ASCII value verbatim; any other value as ONE RFC 2047
// encoded word covering the WHOLE string (ASCII parts included), in
// whichever of base64 or quoted-printable is shorter -- charset utf-8's
// SHORTEST header encoding, with quoted-printable winning a tie. Never
// folded: Message.as_string() runs with maxheaderlen=0, which
// email.header.Header.encode treats as "no limit".
func encodeHeaderValue(value string) (string, error) {
	if strings.ContainsAny(value, "\r\n") {
		return "", errUnsafeHeader
	}
	if isASCII(value) {
		return value, nil
	}
	data := []byte(value)

	// email.base64mime.header_length / email.quoprimime.header_length.
	base64Length := (len(data) + 2) / 3 * 4
	quotedLength := 0
	for _, octet := range data {
		quotedLength += len(quotedPrintableHeaderOctet(octet))
	}
	if base64Length < quotedLength {
		return "=?utf-8?b?" + base64.StdEncoding.EncodeToString(data) + "?=", nil
	}
	var encoded strings.Builder
	for _, octet := range data {
		encoded.WriteString(quotedPrintableHeaderOctet(octet))
	}
	return "=?utf-8?q?" + encoded.String() + "?=", nil
}

// quotedPrintableHeaderOctet is email.quoprimime's _QUOPRI_HEADER_MAP: the
// characters RFC 2047 allows unescaped in a header "Q" word, space as "_",
// everything else "=XX".
func quotedPrintableHeaderOctet(octet byte) string {
	switch {
	case octet == ' ':
		return "_"
	case octet == '-' || octet == '!' || octet == '*' || octet == '+' || octet == '/',
		octet >= 'a' && octet <= 'z', octet >= 'A' && octet <= 'Z', octet >= '0' && octet <= '9':
		return string([]byte{octet})
	default:
		return fmt.Sprintf("=%02X", octet)
	}
}

// pythonBoundary is email.generator._make_boundary: fifteen "=", a random
// integer zero-padded to 19 digits, two more "=".
func pythonBoundary(text string) (string, error) {
	limit := new(big.Int).SetUint64(1<<63 - 1) // sys.maxsize
	for {
		token, err := rand.Int(rand.Reader, limit)
		if err != nil {
			return "", fmt.Errorf("mail boundary generation failed: %w", err)
		}
		boundary := fmt.Sprintf("===============%019d==", token)
		if !strings.Contains(text, boundary) {
			return boundary, nil
		}
	}
}

// composeMIME builds the message exactly as Python's as_string() plus
// smtplib's end-of-line normalization would: every line break CRLF, so the
// result can be handed straight to the SMTP DATA writer.
func composeMIME(from, to, subject, html string) ([]byte, error) {
	fromValue, err := encodeHeaderValue(from)
	if err != nil {
		return nil, fmt.Errorf("mail From: %w", err)
	}
	toValue, err := encodeHeaderValue(to)
	if err != nil {
		return nil, fmt.Errorf("mail To: %w", err)
	}
	subjectValue, err := encodeHeaderValue(subject)
	if err != nil {
		return nil, fmt.Errorf("mail Subject: %w", err)
	}
	boundary, err := pythonBoundary(from + to + subject + html)
	if err != nil {
		return nil, err
	}

	// MIMEText(html, "html"): charset us-ascii + 7bit when the text encodes
	// as ASCII, otherwise utf-8 + base64. An ASCII payload is written
	// through Generator._write_lines, which turns any line break (CRLF, CR
	// or LF) into a single LF; a base64 payload is 76-column lines, each
	// LF-terminated.
	var partHeaders, payload string
	if isASCII(html) {
		partHeaders = "Content-Type: text/html; charset=\"us-ascii\"\n" +
			"MIME-Version: 1.0\n" +
			"Content-Transfer-Encoding: 7bit\n"
		lines := nlcre.Split(html, -1)
		var normalized strings.Builder
		for _, line := range lines[:len(lines)-1] {
			normalized.WriteString(line)
			normalized.WriteByte('\n')
		}
		normalized.WriteString(lines[len(lines)-1])
		payload = normalized.String()
	} else {
		partHeaders = "Content-Type: text/html; charset=\"utf-8\"\n" +
			"MIME-Version: 1.0\n" +
			"Content-Transfer-Encoding: base64\n"
		payload = base64Lines([]byte(html))
	}

	var text strings.Builder
	text.WriteString("Content-Type: multipart/alternative; boundary=\"" + boundary + "\"\n")
	text.WriteString("MIME-Version: 1.0\n")
	text.WriteString("From: " + fromValue + "\n")
	text.WriteString("To: " + toValue + "\n")
	text.WriteString("Subject: " + subjectValue + "\n")
	text.WriteString("\n")
	text.WriteString("--" + boundary + "\n")
	text.WriteString(partHeaders)
	text.WriteString("\n")
	text.WriteString(payload)
	text.WriteString("\n--" + boundary + "--\n")

	// smtplib._fix_eols: every remaining LF becomes CRLF. (No lone CR can be
	// left: header values refuse them and the payload is LF- or base64-only.)
	return []byte(strings.ReplaceAll(text.String(), "\n", "\r\n")), nil
}

// base64Lines is email.base64mime.body_encode with its defaults: the input
// in 57-byte groups, each encoded to a 76-character line terminated by LF.
func base64Lines(data []byte) string {
	const groupSize = 57
	var out strings.Builder
	for start := 0; start < len(data); start += groupSize {
		end := start + groupSize
		if end > len(data) {
			end = len(data)
		}
		out.WriteString(base64.StdEncoding.EncodeToString(data[start:end]))
		out.WriteByte('\n')
	}
	return out.String()
}

// envelopeAddress is smtplib.quoteaddr's argument to MAIL FROM / RCPT TO
// without the angle brackets net/smtp adds itself: the address part of a
// "Display Name <addr>" string; a string parseaddr cannot parse is used as it
// is (a "<...>"-wrapped one loses its brackets, so it is not doubled).
func envelopeAddress(value string) string {
	if parsed, err := mail.ParseAddress(value); err == nil {
		return parsed.Address
	}
	trimmed := strings.TrimSpace(value)
	if strings.HasPrefix(trimmed, "<") && strings.HasSuffix(trimmed, ">") {
		return trimmed[1 : len(trimmed)-1]
	}
	return value
}
