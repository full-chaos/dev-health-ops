-- Every ACL entry (stored or default) of the database, schemas public and river and their objects, one sorted line
-- each. ci/ops_owner_move_k3s.sh compares two of these snapshots byte for byte.
SELECT kind || '|' || object || '|' || grantor || '>' || grantee || '|' || priv || '|' || grantable FROM (
  SELECT 'database' AS kind, d.datname::text AS object, gr.rolname::text AS grantor, coalesce(ge.rolname::text, 'PUBLIC') AS grantee,
         a.privilege_type AS priv, a.is_grantable AS grantable
  FROM pg_database d CROSS JOIN LATERAL aclexplode(coalesce(d.datacl, acldefault('d', d.datdba))) a
  JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee WHERE d.datname = current_database()
  UNION ALL
  SELECT 'schema', n.nspname::text, gr.rolname::text, coalesce(ge.rolname::text, 'PUBLIC'), a.privilege_type, a.is_grantable
  FROM pg_namespace n CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) a
  JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee WHERE n.nspname IN ('public', 'river')
  UNION ALL
  SELECT 'relation', n.nspname || '.' || c.relname, gr.rolname::text, coalesce(ge.rolname::text, 'PUBLIC'), a.privilege_type, a.is_grantable
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault((CASE c.relkind WHEN 'S' THEN 's' ELSE 'r' END)::"char", c.relowner))) a
  JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee
  WHERE n.nspname IN ('public', 'river') AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
  UNION ALL
  SELECT 'column', n.nspname || '.' || c.relname || '.' || att.attname, gr.rolname::text, coalesce(ge.rolname::text, 'PUBLIC'),
         a.privilege_type, a.is_grantable
  FROM pg_attribute att JOIN pg_class c ON c.oid = att.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(att.attacl) a JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee
  WHERE n.nspname IN ('public', 'river') AND NOT att.attisdropped
  UNION ALL
  SELECT 'function', p.oid::regprocedure::text, gr.rolname::text, coalesce(ge.rolname::text, 'PUBLIC'), a.privilege_type, a.is_grantable
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
  CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
  JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee WHERE n.nspname IN ('public', 'river')
  UNION ALL
  SELECT 'type', n.nspname || '.' || t.typname, gr.rolname::text, coalesce(ge.rolname::text, 'PUBLIC'), a.privilege_type, a.is_grantable
  FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
  CROSS JOIN LATERAL aclexplode(coalesce(t.typacl, acldefault('T', t.typowner))) a
  JOIN pg_roles gr ON gr.oid = a.grantor LEFT JOIN pg_roles ge ON ge.oid = a.grantee
  WHERE n.nspname IN ('public', 'river') AND t.typcategory <> 'A'
) s ORDER BY 1;
