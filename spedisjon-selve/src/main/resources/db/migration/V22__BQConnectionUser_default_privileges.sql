DO $$ BEGIN
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'bigquery-connection-user')
    THEN
        ALTER DEFAULT PRIVILEGES FOR USER spedisjon2 IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO "bigquery-connection-user";
        ALTER DEFAULT PRIVILEGES FOR USER spedisjon2 IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO "bigquery-connection-user";
    END IF;
END $$;
