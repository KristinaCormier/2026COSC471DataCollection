DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw_stg'
          AND table_type = 'BASE TABLE'
    LOOP

        EXECUTE format($f$
            CREATE OR REPLACE VIEW stg_raw.valid_%I AS
            SELECT *
            FROM raw_stg.%I
            WHERE date IS NOT NULL
              AND volume IS NOT NULL
              AND volume >= 0;
        $f$, rec.table_name, rec.table_name);

    END LOOP;

    RAISE NOTICE 'All validated views created successfully.';
END $$;
