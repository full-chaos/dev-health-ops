//go:build integration

package admin_test

// The Go pins of orgClaimRuling, by test (testdata/orgclaim).
const (
	governanceOrgClaimPinSHA256        = "08e3b2ff30ee2094764b0526843ce1f82729225794c40184faaa41e607a57f45"
	llmSettingsStatusOrgClaimPinSHA256 = "195b0bba23aa3d30069abe859336fa2e9cdb50dee1625a3c7a22cba9573d72dc"
	pagerDutyBindingsOrgClaimPinSHA256 = "9d6319a7dde62fc04332307187128be7a3a622ccce5a6b949c1fc796fc4bd0fd"

	llmSettingsEncryptedOrgClaimPinSHA256   = "23e72a0f1df5844c0860937d86586fdefe731ffef8fe0474d6879a834152cfea"
	llmSettingsUnencryptedOrgClaimPinSHA256 = "23e72a0f1df5844c0860937d86586fdefe731ffef8fe0474d6879a834152cfea"

	pagerDutyAuthorizeFullOrgClaimPinSHA256        = "7196d19a7e4646bcbc7f68981572ea0069879ae2c9541d0606248912a2c9657a"
	pagerDutyAuthorizeNoClientIDOrgClaimPinSHA256  = "f0e4ec5fc33a9a7eb9d2bc80ab94f733b5a135f25ec4b9143bed14a74948ef8a"
	pagerDutyAuthorizeUnencryptedOrgClaimPinSHA256 = "f0e4ec5fc33a9a7eb9d2bc80ab94f733b5a135f25ec4b9143bed14a74948ef8a"
)

// The pins by run mode (the mode names the pin file).
var (
	llmSettingsOrgClaimPinSHA256        = map[string]string{"keyed": llmSettingsEncryptedOrgClaimPinSHA256, "nokey": llmSettingsUnencryptedOrgClaimPinSHA256}
	pagerDutyAuthorizeOrgClaimPinSHA256 = map[string]string{"full": pagerDutyAuthorizeFullOrgClaimPinSHA256,
		"noclientid": pagerDutyAuthorizeNoClientIDOrgClaimPinSHA256, "nokey": pagerDutyAuthorizeUnencryptedOrgClaimPinSHA256}
)
