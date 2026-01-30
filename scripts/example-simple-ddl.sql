-- Minimal schema for a single-table MV example.
-- Ensure the directory `test1` exists in your database before running.

CREATE TABLE `test1/main_table` (
    id Int64 NOT NULL,
    c1 Text,
    PRIMARY KEY (id)
);

CREATE TABLE `test1/mv_simple` (
    id Int64 NOT NULL,
    c1 Text,
    PRIMARY KEY (id)
);

ALTER TABLE `test1/main_table` ADD CHANGEFEED `cf1` WITH (
    FORMAT = 'JSON',
    MODE = 'NEW_AND_OLD_IMAGES'
);
-- Minimal schema for a single-table MV example.
-- Ensure the directory `test1` exists in your database before running.

CREATE TABLE `test1/main_table` (
    id Int64 NOT NULL,
    c1 Text,
    PRIMARY KEY (id)
);

CREATE TABLE `test1/mv_simple` (
    id Int64 NOT NULL,
    c1 Text,
    PRIMARY KEY (id)
);

ALTER TABLE `test1/main_table` ADD CHANGEFEED `cf1` WITH (
    FORMAT = 'JSON',
    MODE = 'NEW_AND_OLD_IMAGES'
);
