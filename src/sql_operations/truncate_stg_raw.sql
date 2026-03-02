DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'stg_raw'
    LOOP
        EXECUTE format(
            'TRUNCATE TABLE stg_raw.%I RESTART IDENTITY CASCADE;',
            r.tablename
        );
    END LOOP;
END $$;
