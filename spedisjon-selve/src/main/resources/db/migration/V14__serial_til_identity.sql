-- drop the default
alter table melding alter column id drop default;

-- drop the old sequence
drop sequence public.melding_id_seq cascade;

alter table melding
    alter column id
        add generated always as identity;

-- adjust the newly created sequence
select setval(pg_get_serial_sequence('melding', 'id'), max(id))
from melding;