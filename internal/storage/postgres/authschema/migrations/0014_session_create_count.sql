-- 0014 the create guard counts a new session. ADDITIVE ONLY. See 0001's header.
--
-- A sign-in commits its session with its audit row through audit.Commit, and
-- the session row is that mutation's state change. 0006 leaves sessions
-- unguarded for insert, so the insert counted nothing and Commit refused the
-- sign-in as a no-op. 0013's audit_count_create counts it: AFTER INSERT ROW
-- fires only for a row actually inserted, and outside an audit.Commit
-- transaction it counts nothing and refuses nothing, so a session written
-- outside Commit is unchanged.
CREATE TRIGGER sessions_create_count
    AFTER INSERT ON sessions
    FOR EACH ROW EXECUTE FUNCTION audit_count_create();
