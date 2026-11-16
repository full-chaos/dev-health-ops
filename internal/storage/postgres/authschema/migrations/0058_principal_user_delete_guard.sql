-- 0058 the delete guard of principals and users (CHAOS-13380). ADDITIVE ONLY. See 0001's header.
--
-- An admin force delete while auth-service owns users erases the user's principal and its auth.users row
-- (identity.AdminForceDeleteUser), the first principal delete in the lineage. 0006 leaves both tables unguarded
-- for their high-rate writes, so a DELETE of either was not refused outside audit.Commit and counted nothing in it.
-- The row triggers below make a delete of either run only inside audit.Commit (42501 elsewhere) and count it as a
-- state change, as a guarded table's write does. INSERT and UPDATE keep the 0006 and 0013 rules.
CREATE TRIGGER principals_delete_guard
    BEFORE DELETE ON principals
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER users_delete_guard
    BEFORE DELETE ON users
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
