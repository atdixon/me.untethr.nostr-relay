-- Careful with edits here. Our poor-man's parser expects each ddl statement
-- to be separated by one or more full comment lines.
--
pragma encoding = 'UTF-8';
--
pragma journal_mode = WAL;
--
pragma main.synchronous = NORMAL;
--
pragma foreign_keys = OFF;
--
create table if not exists n_events
(
    id         varchar(64) not null unique,
    pubkey     varchar(64) not null,
    created_at integer     not null,
    kind       integer     not null,
    raw_event  text        not null,
    deleted_   integer     not null default 0,
    sys_ts     timestamp            default current_timestamp
);
--
-- sqlite does not support adding columns if they don't already exist,
-- so, in our code, we will explicitly forgive when we execute alter table
-- ... add column schema statements:
alter table n_events
    add column channel_id varchar(36);
--
create index if not exists idx_event_id on n_events (id);
--
create index if not exists idx_pubkey on n_events (pubkey);
--
create index if not exists idx_created_at on n_events (created_at);
--
create index if not exists idx_kind on n_events (kind);
--
create index if not exists idx_kind_pubkey on n_events (kind, pubkey);
--
create table if not exists e_tags
(
    source_event_id varchar(64) not null,
    tagged_event_id varchar(64) not null,
    unique (source_event_id, tagged_event_id)
);
--
create index if not exists idx_tagged_event_id on e_tags (tagged_event_id);
-- clean up non-latest kind 0 and kind 3 events on insert; this trigger
-- is dispensable if ultimately costly.
create trigger if not exists insert_singleton_kind
    after insert
    on n_events
    when NEW.kind in (0, 3)
begin
    update n_events
    set deleted_ =
            (case
                 when created_at
                     <> (select max(created_at)
                        from n_events
                        where kind = NEW.kind
                          and pubkey = NEW.pubkey)
                     then 1
                 else 0 end)
    where kind = NEW.kind
      and pubkey = NEW.pubkey;
end;
--
create table if not exists p_tags
(
    source_event_id varchar(64) not null,
    tagged_pubkey   varchar(64) not null,
    unique (source_event_id, tagged_pubkey)
);
--
create index if not exists idx_tagged_pubkey on p_tags (tagged_pubkey);
--
create table if not exists x_tags
(
    source_event_id varchar(64)   not null,
    generic_tag     varchar(1)    not null,
    tagged_value    varchar(2056) not null,
    unique (source_event_id, generic_tag, tagged_value)
);
--
create index if not exists idx_tagged_value on x_tags (generic_tag, tagged_value);
--
create table if not exists channels
(
    channel_id varchar(36) not null,
    ip_addr    varchar(45) not null,
    sys_ts     timestamp default current_timestamp,
    unique (channel_id)
);
