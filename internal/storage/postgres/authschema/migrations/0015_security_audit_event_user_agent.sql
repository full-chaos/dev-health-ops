-- 0015 the client's user agent on each audit row. ADDITIVE ONLY. See 0001's
-- header.
--
-- user_agent is the request's User-Agent value as the auth service's request
-- context carries it (the same value a session records), or NULL when the
-- request sent none. It is written with the row like client_ip, for every
-- event including a refused sign-in, and never changed (the append-only
-- guards of 0006 cover the whole row). No length bound: the transport's
-- header size cap is the only one.
ALTER TABLE security_audit_events ADD COLUMN user_agent text;
