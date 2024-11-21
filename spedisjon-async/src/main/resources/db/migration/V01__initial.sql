CREATE TABLE inntektsmelding
(
    id                 bigint generated always as identity primary key,
    intern_dokument_id uuid unique,
    fnr                VARCHAR(32) NOT NULL,
    duplikatkontroll   text,
    orgnummer          VARCHAR(32) NOT NULL,
    arbeidsforhold_id  VARCHAR,
    mottatt            TIMESTAMP   NOT NULL,
    timeout            TIMESTAMP   NOT NULL,
    ekspedert          TIMESTAMP
);

CREATE INDEX inntektsmelding_fnr_orgnummer_idx ON inntektsmelding (fnr, orgnummer);
CREATE INDEX inntektsmelding_timeout_idx ON inntektsmelding (timeout);
CREATE INDEX inntektsmelding_mottatt_idx ON inntektsmelding (mottatt);
CREATE UNIQUE INDEX inntektsmelding_duplikatkontroll_idx ON inntektsmelding (duplikatkontroll);