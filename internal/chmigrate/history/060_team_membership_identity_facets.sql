ALTER TABLE team_memberships ADD COLUMN IF NOT EXISTS identity_facets Array(String) DEFAULT [];
