ALTER TABLE melding ALTER COLUMN data SET DATA TYPE jsonb USING data::jsonb;
