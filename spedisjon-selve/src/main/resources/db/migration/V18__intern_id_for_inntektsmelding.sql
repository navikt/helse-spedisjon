-- 1. legge til ny kolonne som med null som default
alter table inntektsmelding add column intern_dokument_id uuid unique;

-- 2. migrere inn intern_dokument_id i alle rader
update inntektsmelding i
    set intern_dokument_id = m.intern_dokument_id
from melding m
where m.duplikatkontroll = i.duplikatkontroll;

-- 3. fjerne nullability
alter table inntektsmelding alter column intern_dokument_id set not null;