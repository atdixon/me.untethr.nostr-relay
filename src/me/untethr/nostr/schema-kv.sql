-- Careful with edits here. Our poor-man's parser expects each ddl statement
-- to be separated by one or more full comment lines.
--
pragma encoding = 'UTF-8';
--
pragma journal_mode = WAL;
--
pragma journal_size_limit = 16777216;
--
-- todo see "cache=" DB.java sqlite sharing cache
-- https://www.sqlite.org/pragma.html#pragma_cache_size
-- we'll set it to eight times the default for now, ultimately may want to make
-- this app config but first need to benchmark analysis
pragma cache_size = -160000;
--
-- https://www.sqlite.org/pragma.html#pragma_page_size
-- https://www2.sqlite.org/matrix/intern-v-extern-blob.html
--   (value is bytes and must be power of 2)
pragma page_size = 16384;
-- auto_vacuum = incremental only valid for newly created tables:
--   https://www.sqlite.org/pragma.html#pragma_auto_vacuum
pragma auto_vacuum = incremental;
--
-- taking control of autocheckpoint means we'll have to do it ourself:
pragma wal_autocheckpoint = -1;
-- would autocheckpoint work if we never close write conn?
-- pragma wal_autocheckpoint = 1000;
--
pragma synchronous = NORMAL;
--
pragma foreign_keys = OFF;
--
--
-- Tables.
--
create table if not exists n_kv_events
(
    event_id   varchar(64) not null primary key,
    raw_event  text        not null
);
--
