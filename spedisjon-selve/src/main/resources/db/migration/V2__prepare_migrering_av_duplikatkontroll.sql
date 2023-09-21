ALTER TABLE melding ADD COLUMN tmp_duplikatkontroll CHAR(128) DEFAULT null;
ALTER TABLE melding ADD COLUMN tmp_slett BOOLEAN DEFAULT false;
CREATE INDEX melding_tmp_duplikatkontroll ON melding (tmp_duplikatkontroll);

