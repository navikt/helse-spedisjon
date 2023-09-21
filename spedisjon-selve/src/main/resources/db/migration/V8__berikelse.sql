CREATE TABLE berikelse
(
    fnr              VARCHAR(32) NOT NULL,
    duplikatkontroll CHAR(128) NOT NULL,
    opprettet        TIMESTAMP NOT NULL,
    behov            VARCHAR(255) NOT NULL,
    løsning          JSONB
);

CREATE UNIQUE INDEX berikelse_duplikatkontroll_idx ON berikelse(duplikatkontroll);
CREATE INDEX berikelse_fnr_idx ON berikelse(fnr);
CREATE INDEX berikelse_opprettet_idx ON berikelse(opprettet);