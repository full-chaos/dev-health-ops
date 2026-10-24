//go:build integration

package authflowvenue_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/smtpcapture"
)

func tokenlessMail(subject string) smtpcapture.Mail { return mailWithBody(subject, "<p>hello</p>") }

func mailWithBody(subject, html string) smtpcapture.Mail {
	data := "From: Dev Health <auth@example.test>\r\nTo: <owner@example.test>\r\nSubject: " + subject + "\r\n" +
		"MIME-Version: 1.0\r\nContent-Type: multipart/alternative; boundary=\"XQZBND7\"\r\n\r\n" +
		"--XQZBND7\r\nContent-Type: text/html; charset=utf-8\r\nContent-Transfer-Encoding: 7bit\r\n\r\n" + html + "\r\n--XQZBND7--\r\n"
	return smtpcapture.Mail{MailFrom: "auth@example.test", RcptTo: []string{"owner@example.test"}, Data: data}
}

func TestDecodeMailAcceptsATokenlessMailOnlyForTheAccountExistsNotice(t *testing.T) {
	notice := decodeMail(t, tokenlessMail(accountExistsSubject))
	if notice.token != "" || !strings.Contains(notice.html, "hello") {
		t.Fatalf("notice decoded as %+v", notice)
	}
	for _, subject := range []string{"Verify your email address", "Reset your password", ""} {
		if tokenOptional(subject) {
			t.Errorf("a tokenless mail with subject %q would be accepted", subject)
		}
	}
}

func TestNoticeLinkOrTokenNamesAVerifyOrResetLinkAndAToken(t *testing.T) {
	for body, want := range map[string]string{
		"<a href=\"https://app/auth/signin\">Sign in</a> <a href=\"https://app/auth/forgot-password\">reset</a>": "",
		"<a href=\"https://app/auth/verify?token=abc\">x</a>":                                                    "token=",
		"<a href=\"https://app/auth/reset-password\">x</a>":                                                      "/auth/reset-password",
	} {
		if got := noticeLinkOrToken(body); got != want {
			t.Errorf("noticeLinkOrToken(%q) = %q, want %q", body, got, want)
		}
	}
}
