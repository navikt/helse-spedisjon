create table melding_alias(
    id bigint generated always as identity,
    melding_id bigint references melding(id) on delete cascade on update cascade,
    intern_dokument_id uuid unique
);

create index idx_melding_alias_melding_id_fk ON melding_alias(melding_id);