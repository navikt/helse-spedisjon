CREATE TABLE inntektsmelding
(
    fnr              VARCHAR(32) NOT NULL,
    duplikatkontroll CHAR(128) NOT NULL,
    orgnummer        VARCHAR(32) NOT NULL,
    mottatt          TIMESTAMP NOT NULL,
    timeout          TIMESTAMP NOT NULL,
    republisert      TIMESTAMP
);

CREATE INDEX inntektsmelding_fnr_orgnummer_idx ON inntektsmelding(fnr, orgnummer);
CREATE INDEX inntektsmelding_timeout_idx ON inntektsmelding(timeout);
CREATE INDEX inntektsmelding_mottatt_idx ON inntektsmelding(mottatt);
CREATE UNIQUE INDEX inntektsmelding_duplikatkontroll_idx ON inntektsmelding(duplikatkontroll);