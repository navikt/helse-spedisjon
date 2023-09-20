DELETE FROM melding WHERE tmp_slett=true AND type IN('ny_søknad','sendt_søknad_arbeidsgiver','sendt_søknad_nav');
UPDATE melding SET duplikatkontroll=tmp_duplikatkontroll WHERE type IN('ny_søknad','sendt_søknad_arbeidsgiver','sendt_søknad_nav');

DROP INDEX melding_tmp_duplikatkontroll;
ALTER TABLE melding DROP COLUMN tmp_duplikatkontroll;
ALTER TABLE melding DROP COLUMN tmp_slett;