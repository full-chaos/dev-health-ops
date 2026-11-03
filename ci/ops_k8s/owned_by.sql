-- Relations, routines and types of public and river, schema river and the database owned by :'role'.
SELECT (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname IN ('public', 'river') AND c.relowner = r.oid)
     + (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname IN ('public', 'river') AND p.proowner = r.oid)
     + (SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname IN ('public', 'river') AND t.typowner = r.oid)
     + (SELECT count(*) FROM pg_namespace WHERE nspname = 'river' AND nspowner = r.oid)
     + (SELECT count(*) FROM pg_database WHERE datname = current_database() AND datdba = r.oid)
FROM pg_roles r WHERE r.rolname = :'role';
