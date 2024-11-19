CREATE TABLE arbeidstabell(
    id bigint primary key generated always as identity,
    fnr text unique,
    arbeid_startet timestamptz,
    arbeid_ferdig timestamptz
);

create index if not exists uferdig_arbeid_idx on arbeidstabell(arbeid_startet) where arbeid_startet is null;

create table umigrert(
    fnr        text   not null,
    melding_id bigint not null -- melding.id i spedisjon-databasen
);
create index if not exists umigrert_fnr_idx on umigrert(fnr);
