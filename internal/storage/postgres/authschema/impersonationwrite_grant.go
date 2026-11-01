package authschema

const impersonationTable = "impersonation_sessions"

func ImpersonationEndColumns() []string { return nil }

func ImpersonationStartColumns() []string { return nil }

func ImpersonationLegacyReadColumns() []string { return nil }

func isImpersonationWriteColumn(bool, string, string, string) bool { return false }
