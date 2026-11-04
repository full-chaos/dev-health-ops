//go:build integration

package admin_test

// The Go pins of orgClaimRuling, by test (testdata/orgclaim).
const (
	governanceOrgClaimPinSHA256        = ""
	llmSettingsStatusOrgClaimPinSHA256 = ""
	pagerDutyBindingsOrgClaimPinSHA256 = ""
)

var (
	llmSettingsOrgClaimPinSHA256        = map[string]string{"keyed": "", "nokey": ""}
	pagerDutyAuthorizeOrgClaimPinSHA256 = map[string]string{"full": "", "noclientid": "", "nokey": ""}
)
