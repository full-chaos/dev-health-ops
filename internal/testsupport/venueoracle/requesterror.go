package venueoracle

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
)

var (
	// queryValue is the value of a query parameter inside any text: a link token rides there.
	queryValue = regexp.MustCompile(`([?&][^=&\s"'<>#]+=)[^&\s"'<>#]*`)
	// userinfo is the user and password of a URL.
	userinfo = regexp.MustCompile(`(://)[^/\s@"'<>]+@`)
)

// RedactText is text with the query values and the userinfo of every URL in it removed.
func RedactText(text string) string {
	text = userinfo.ReplaceAllString(text, "${1}<redacted>@")
	return queryValue.ReplaceAllString(text, "${1}<redacted>")
}

// RedactError is err as text for a failure message: the request URL of a transport failure keeps its path and loses its
// query values and userinfo.
func RedactError(err error) string {
	var failure *url.Error
	if errors.As(err, &failure) {
		return RedactText(failure.Op + " " + failure.URL + ": " + failure.Err.Error())
	}
	return RedactText(err.Error())
}

// DescribeDifference is the failure text of one compared answer pair: the request name, then each plane's status, body and
// compared headers, with the query values of any URL in it removed.
func DescribeDifference(name string, python, goResponse Response, compared []string) string {
	return RedactText(fmt.Sprintf("%s:\n python %d %s %v\n go     %d %s %v", name, python.Status, python.Body, pick(python.Headers, compared),
		goResponse.Status, goResponse.Body, pick(goResponse.Headers, compared)))
}
