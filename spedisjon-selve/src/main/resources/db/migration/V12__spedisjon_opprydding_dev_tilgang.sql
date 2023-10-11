DO
    $$BEGIN
        IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'spedisjon-opprydding-dev') THEN
            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "spedisjon-opprydding-dev";
            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "spedisjon-opprydding-dev";
        END IF;
END$$;