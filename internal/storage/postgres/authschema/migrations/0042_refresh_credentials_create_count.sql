-- 0041 the create guard counts a new refresh credential. ADDITIVE ONLY. See 0001's header.
--
-- A refresh exchange commits its rotation with its auth.session.refreshed
-- audit row through audit.Commit (CHAOS-11400), and the successor credential
-- is that mutation's state change. refresh_credentials is unguarded for insert
-- (0006, 0010), so the insert counted nothing and Commit would refuse every
-- accepted refresh as a no-op. 0013's audit_count_create counts it, as 0014
-- does for sessions: AFTER INSERT ROW fires only for a row actually inserted,
-- and outside an audit.Commit transaction it counts nothing and refuses
-- nothing, so a credential written outside Commit (a build without this
-- change) is unchanged.
CREATE TRIGGER refresh_credentials_create_count
    AFTER INSERT ON refresh_credentials
    FOR EACH ROW EXECUTE FUNCTION audit_count_create();
