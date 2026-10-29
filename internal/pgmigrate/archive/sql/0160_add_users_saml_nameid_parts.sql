-- Revision 0160 (down_revision 0159), written by hand.
-- A SAML account's identity key is (provider, Subject(NameID)), and Subject needs the NameID Format and the
-- SPNameQualifier as well as the value that users.auth_provider_id holds. go-api now writes both at SAML sign-in.
-- NULL = never captured (the account has not signed in since); an empty string = captured, the assertion carried none.
-- Expand-only: two nullable columns, no default, no rewrite of existing rows, no new index.

ALTER TABLE users ADD COLUMN saml_nameid_format text;
ALTER TABLE users ADD COLUMN saml_sp_name_qualifier text;
