CREATE TABLE ekspedering
(
    id                 bigint generated always as identity primary key,
    intern_dokument_id uuid unique,
    ekspedert          TIMESTAMPTZ
);