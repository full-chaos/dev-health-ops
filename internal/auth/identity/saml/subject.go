package saml

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

// NameID formats with a special meaning here.
const (
	NameIDFormatUnspecified = "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified"
	NameIDFormatTransient   = "urn:oasis:names:tc:SAML:2.0:nameid-format:transient"
)

const (
	subjectVersion  = "saml1"
	maxSubjectBytes = 1024
)

// ErrSubject reports a NameID that cannot key an identity.
var ErrSubject = errors.New("saml: NameID cannot key an identity")

// NameID is the part of an assertion's Subject that identifies the person.
type NameID struct {
	Format          string
	SPNameQualifier string
	Value           string
}

// Subject encodes a NameID as the identity subject:
// "saml1|<format>|<sp name qualifier>|<value>", each part percent-escaped so
// that two different NameIDs never encode to the same subject. An absent
// Format is the SAML default, unspecified. A transient NameID changes on every
// sign-in, so it is refused, as is an empty value, a value with a control
// character or invalid UTF-8, and a subject longer than 1024 bytes.
func Subject(n NameID) (string, error) {
	format := n.Format
	if format == "" {
		format = NameIDFormatUnspecified
	}
	if format == NameIDFormatTransient || n.Value == "" {
		return "", ErrSubject
	}
	for _, part := range []string{format, n.SPNameQualifier, n.Value} {
		if !utf8.ValidString(part) {
			return "", ErrSubject
		}
		for _, r := range part {
			if unicode.IsControl(r) {
				return "", ErrSubject
			}
		}
	}
	out := subjectVersion + "|" + escapePart(format) + "|" + escapePart(n.SPNameQualifier) + "|" + escapePart(n.Value)
	if len(out) > maxSubjectBytes {
		return "", ErrSubject
	}
	return out, nil
}

// ParseSubject is the inverse of Subject.
func ParseSubject(s string) (NameID, error) {
	parts := strings.Split(s, "|")
	if len(parts) != 4 || parts[0] != subjectVersion {
		return NameID{}, ErrSubject
	}
	var decoded [3]string
	for i, p := range parts[1:] {
		v, ok := unescapePart(p)
		if !ok {
			return NameID{}, ErrSubject
		}
		decoded[i] = v
	}
	n := NameID{Format: decoded[0], SPNameQualifier: decoded[1], Value: decoded[2]}
	again, err := Subject(n)
	if err != nil || again != s {
		return NameID{}, ErrSubject
	}
	return n, nil
}

const hexDigits = "0123456789ABCDEF"

func escapePart(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '%' || c == '|' || c <= ' ' || c == 0x7f {
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0xf])
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

func unescapePart(s string) (string, bool) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '%' {
			if c == '|' || c <= ' ' || c == 0x7f {
				return "", false
			}
			b.WriteByte(c)
			continue
		}
		if i+2 >= len(s) {
			return "", false
		}
		hi, lo := strings.IndexByte(hexDigits, s[i+1]), strings.IndexByte(hexDigits, s[i+2])
		if hi < 0 || lo < 0 {
			return "", false
		}
		b.WriteByte(byte(hi<<4 | lo))
		i += 2
	}
	return b.String(), true
}
