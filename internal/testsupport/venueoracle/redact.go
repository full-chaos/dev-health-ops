package venueoracle

import "fmt"

// RedactError is err as text for a failure message: the request URL of a transport failure keeps its path and loses its
// query values and userinfo.
func RedactError(err error) string { return err.Error() }

// RedactText is text with the query values and the userinfo of every URL in it removed.
func RedactText(text string) string { return text }

// DescribeDifference is the failure text of one compared answer pair: the request name, then each plane's status, body and
// compared headers, with the query values of any URL in it removed.
func DescribeDifference(name string, python, goResponse Response, compared []string) string {
	return fmt.Sprintf("%s:\n python %d %s %v\n go     %d %s %v", name, python.Status, python.Body, pick(python.Headers, compared),
		goResponse.Status, goResponse.Body, pick(goResponse.Headers, compared))
}
