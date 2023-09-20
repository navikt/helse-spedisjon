CREATE TABLE melding
(
    id               BIGSERIAL PRIMARY KEY,
    type             VARCHAR(32) NOT NULL,
    fnr              VARCHAR(32) NOT NULL,
    opprettet        TIMESTAMP   NOT NULL,
    duplikatkontroll CHAR(128)   NOT NULL,
    data             JSONB       NOT NULL
);

CREATE INDEX melding_opprettet_idx ON melding USING BTREE (opprettet);
CREATE INDEX melding_fnr_idx ON melding USING BTREE (fnr);
CREATE INDEX melding_type_idx ON melding USING BTREE (type);
CREATE UNIQUE INDEX melding_duplikatkontroll_idx ON melding (duplikatkontroll);
