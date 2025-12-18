DO $$BEGIN
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'bigquery-connection-user')
    THEN
        GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO "bigquery-connection-user";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO "bigquery-connection-user";
END IF;
END$$;