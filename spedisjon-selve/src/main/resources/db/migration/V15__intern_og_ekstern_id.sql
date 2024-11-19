alter table melding add column ekstern_dokument_id uuid default null;
alter table melding add column intern_dokument_id uuid unique default gen_random_uuid();