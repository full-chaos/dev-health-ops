-- Planted test-only downgrade 0164: it refuses while planted_walk_b holds a row.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM planted_walk_b) THEN
        RAISE EXCEPTION 'planted_walk_b holds rows';
    END IF;
END
$$;
DROP TABLE planted_walk_b;
