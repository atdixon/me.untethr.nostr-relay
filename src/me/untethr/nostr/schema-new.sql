-- Careful with edits here. Our poor-man's parser expects each ddl statement
-- to be separated by one or more full comment lines.
--
pragma encoding = 'UTF-8';
--
pragma journal_mode = WAL;
--
pragma journal_size_limit = 16777216;
--
-- pragma cache_size = -2000;
-- pragma page_size = 4096;
--
-- auto_vacuum = incremental only valid for newly created tables:
--   https://www.sqlite.org/pragma.html#pragma_auto_vacuum
--   note: not sure if auto_vacuum applied in jan deployment due to pragma
--          handling at the time
pragma auto_vacuum = INCREMENTAL;
--
-- taking control of autocheckpoint means we'll have to do it ourself:
pragma wal_autocheckpoint = -1;
-- would autocheckpoint work if we never close write conn?
-- pragma wal_autocheckpoint = 1000;
--
-- @see https://stackoverflow.com/questions/1711631/improve-insert-per-second-performance-of-sqlite
pragma synchronous = OFF;
-- pragma synchronous = NORMAL;
--
pragma foreign_keys = OFF;
--
--
-- Tables.
--
-- core tables will get an alias for rowid, id, which we'll use as part of
--  pagination, in combination with created_at, leveraging an index that
--  includes it.
--
create table if not exists n_events
(
    id         integer primary key,
    event_id   varchar(64) not null unique,
    pubkey     varchar(64) not null,
    kind       integer     not null,
    created_at integer     not null,
    deleted_   integer     not null default 0,
    sys_ts     timestamp            default current_timestamp,
    channel_id varchar(36)
);
--
create table if not exists e_tags
(
    id                      integer primary key,
    source_event_id         varchar(64)        not null,
    tagged_event_id         varchar(64)        not null,
    -- denormalized columns to avoid joins in common queries
    source_event_kind       integer            not null,
    source_event_created_at created_at integer not null,
    source_event_deleted_   integer            not null default 0,
    unique (source_event_id, tagged_event_id)
);
--
create table if not exists p_tags
(
    id                      integer primary key,
    source_event_id         varchar(64)        not null,
    tagged_pubkey           varchar(64)        not null,
    -- denormalized columns to avoid joins in common queries
    source_event_pubkey     varchar(64)        not null,
    source_event_kind       integer            not null,
    source_event_created_at created_at integer not null,
    source_event_deleted_   integer            not null default 0,
    unique (source_event_id, tagged_pubkey)
);
--
create table if not exists x_tags
(
    id                      integer primary key,
    source_event_id         varchar(64)        not null,
    generic_tag             varchar(1)         not null,
    tagged_value            varchar(2056)      not null,
    -- denormalized columns to avoid joins in common queries
    source_event_created_at created_at integer not null,
    source_event_deleted_   integer            not null default 0,
    unique (source_event_id, generic_tag, tagged_value)
);
--
create table if not exists channels
(
    channel_id varchar(36) not null,
    ip_addr    varchar(45) not null,
    sys_ts     timestamp default current_timestamp,
    unique (channel_id)
);
--
-- Indices. Note most here assume deleted_ is part of query and won't be
--  effective without such.
--
--
-- Support id and pubkey prefix queries:
--  Note that there just isn't a way to get a pagination / sort with limit on created_at
--  here:
drop index if exists idx_event_id_created_at;
create index if not exists idx_event_id_ on n_events (deleted_, event_id collate nocase);
drop index if exists idx_event_pubkey_created_at;
create index if not exists idx_event_pubkey_ on n_events (deleted_, pubkey collate nocase);
-- Support kind in (?,...) ... order by created_at ... limit ...:
create index if not exists idx_event_kind_created_at on n_events (kind, deleted_, created_at);
-- Support pubkey in (?,...) ... order by created_at ... limit ...:
create index if not exists idx_event_pubkey_created_at on n_events (pubkey, deleted_, created_at);
-- Support pubkey in (?,...) and kind in (?...) ... order by created_at ... limit ...:
create index if not exists idx_event_pubkey_kind_created_at on n_events (pubkey, kind, deleted_, created_at);
-- Support trigger deletion of kind 0, 3 and kind [10000, 19999] events.
create index if not exists idx_kind_pubkey on n_events (kind, pubkey, deleted_);
-- Support global timeline queries, say. (Also support getting max created_at in db, etc...)
create index if not exists idx_event_created_at on n_events (deleted_, created_at);
--
-- Support #e in (?,...) ... order by created_at ... limit ...:
-- We also like that we get a single index here on tagged_event_id to support our trigger
--   that updates source_event_deleted_ to match the actual tagged event's deleted_.
create index if not exists idx_e_tag_created_at on e_tags (tagged_event_id, source_event_deleted_, source_event_created_at);
-- Support #e in (?,...) and kind in (?...) ... order by create_at ... limit ...:
create index if not exists idx_e_tag_kind_created_at on e_tags (source_event_kind, tagged_event_id, source_event_deleted_, source_event_created_at);
--
-- Support #p in (?,...) ... order by created_at ... limit ...:
create index if not exists idx_p_tag_created_at on p_tags (tagged_pubkey, source_event_deleted_, source_event_created_at);
-- Support #p in (?,...) and kind in (?...) ... order by create_at ... limit ...:
create index if not exists idx_p_tag_kind_created_at_fixed on p_tags (source_event_kind, tagged_pubkey, source_event_deleted_, source_event_created_at);
drop index if exists idx_p_tag_kind_created_at;
-- dropped: create index if not exists idx_p_tag_kind_created_at on p_tags (source_event_kind, source_event_id, source_event_deleted_, source_event_created_at);
-- Support trigger setting source_event_delete_ = 1 for non-latest kind 3 p_tag table rows. (Note: we are not
-- going to support doing this for x_tags on kind 3 events b/c nip-02 does not indicate generic tags for kind 3s.
-- Specifically, we will allow kind 3 events with generic tags and write them to raw_event but we will not
-- index non-#p tags and support non-#p tag queries for kind 3s.)
create index if not exists idx_p_tag_pubkey_kind on p_tags (source_event_pubkey, source_event_kind, source_event_created_at);
--
-- Support #t in (?,...) ... order by created_at ... limit ...:
create index if not exists idx_x_tag_created_at on x_tags (generic_tag, tagged_value, source_event_deleted_, source_event_created_at);
--
--
-- Triggers.
--
--
-- Deletes event kinds that are singletons for kind + pubkey. The n_events
-- table update should in almost all cases update at most one row, using our
-- kind,pubkey,deleted_ index).
-- Note that we might insert a replaceable kind with a created_at that precedes
-- a newer one, so the trigger here must ensure that we don't delete the latest
-- if that occurs.
create trigger if not exists insert_replaceable_kind
    after insert
    on n_events
    when NEW.kind in (0, 3) or NEW.kind between 10000 and 19999
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
-- Whenever we insert a new replaceable n_events it may nor may not insert
-- as deleted_ depending on the prior trigger (b/c it may not be the latest
-- event of the replaceable kind for the pubkey). So we need insert triggers
-- for each denormalized tag table so their source_event_deleted_ inserts
-- with the same deleted_ status as the source event.
--
-- Note ALSO that we insert tags via "continuations" - in the case that we
-- have a new replaceable event arrive but we are processing the tags for the
-- *replaced* event via continuations, we want those subsequent continuation
-- tag inserts to obtain the proper source_event_deleted_ status. Win/win here.
--
create trigger if not exists insert_replaceable_kind_e_tags
    after insert
    on e_tags
begin
    update e_tags
    set source_event_deleted_ =
            (select v.deleted_
             from n_events v
             where v.event_id = NEW.source_event_id)
    where source_event_id = NEW.source_event_id;
end;
--
create trigger if not exists insert_replaceable_kind_p_tags
    after insert
    on p_tags
begin
    update p_tags
    set source_event_deleted_ =
            (select v.deleted_
             from n_events v
             where v.event_id = NEW.source_event_id)
    where source_event_id = NEW.source_event_id;
end;
--
create trigger if not exists insert_replaceable_kind_x_tags
    after insert
    on x_tags
begin
    update x_tags
    set source_event_deleted_ =
            (select v.deleted_
             from n_events v
             where v.event_id = NEW.source_event_id)
    where source_event_id = NEW.source_event_id;
end;
--
-- Any deleted_ update, whether manual or by prior trigger, needs to ensure
-- that the associated denormalized tag tables' source_event_deleted_ are updated,
-- as well.
--
create trigger if not exists update_deleted_columns
    after update
        of deleted_
    on n_events
    for each row
begin
    update e_tags
    set source_event_deleted_ = NEW.deleted_
    where source_event_id = OLD.event_id;
    update p_tags
    set source_event_deleted_ = NEW.deleted_
    where source_event_id = OLD.event_id;
    update x_tags
    set source_event_deleted_ = NEW.deleted_
    where source_event_id = OLD.event_id;
end;
--
create trigger if not exists update_deleted_rows
    after delete
    on n_events
    for each row
begin
    delete from e_tags
    where source_event_id = OLD.event_id;
    delete from p_tags
    where source_event_id = OLD.event_id;
    delete from x_tags
    where source_event_id = OLD.event_id;
end;
--
