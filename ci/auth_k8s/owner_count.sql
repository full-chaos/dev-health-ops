-- The relations of the auth schema, and the schema itself, that the role :role does not own. 0 after an owner move.
SELECT (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'auth' AND c.relowner <> r.oid)
     + (SELECT count(*) FROM pg_namespace WHERE nspname = 'auth' AND nspowner <> r.oid)
FROM pg_roles r WHERE r.rolname = :'role';
