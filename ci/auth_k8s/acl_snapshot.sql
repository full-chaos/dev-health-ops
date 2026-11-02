-- Every ACL entry of the auth and public schemas (relations, columns, schemas, functions, types) with its grantor and
-- grantee, the default ACL used where none is stored, one sorted line each. ci/auth_owner_move_k3s.sh compares two of
-- these snapshots byte for byte.
SELECT kind || '|' || object || '|' || grantor || '>' || grantee || '|' || priv || '|' || grantable FROM (
  SELECT 'relation' AS kind, n.nspname || '.' || c.relname AS object, gr.rolname::text AS grantor,
         coalesce(ge.rolname::text, 'PUBLIC') AS grantee, a.privilege_type AS priv, a.is_grantable AS grantable
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault((CASE c.relkind WHEN 'S' THEN 's' ELSE 'r' END)::"char", c.relowner))) a
  JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee
  WHERE n.nspname IN ('auth','public') AND c.relkind IN ('r','p','v','m','f','S')
  UNION ALL
  SELECT 'column', n.nspname || '.' || c.relname || '.' || att.attname, gr.rolname::text,
         coalesce(ge.rolname::text, 'PUBLIC'), a.privilege_type, a.is_grantable
  FROM pg_attribute att JOIN pg_class c ON c.oid = att.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(att.attacl) a
  JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee
  WHERE n.nspname IN ('auth','public') AND NOT att.attisdropped
  UNION ALL
  SELECT 'schema', n.nspname, gr.rolname::text, coalesce(ge.rolname::text, 'PUBLIC'), a.privilege_type, a.is_grantable
  FROM pg_namespace n CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) a
  JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee
  WHERE n.nspname IN ('auth','public')
  UNION ALL
  SELECT 'function', n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')', gr.rolname::text,
         coalesce(ge.rolname::text, 'PUBLIC'), a.privilege_type, a.is_grantable
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
  CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
  JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee
  WHERE n.nspname = 'auth'
) s ORDER BY 1;
